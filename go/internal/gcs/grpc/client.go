// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package gcsgrpc implements a pure-Go GCS client used by components that
// avoid the cgo GCS client (and therefore the go_runtime.so plugin): the log
// monitor (internal KV reads and log batch publishing) and the dashboard agent
// (KV writes, node info, job listing, job-id allocation, driver node lookup,
// and node-resource-usage / error publishing).
package gcsgrpc

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
)

// Compile-time interface assertions.
var (
	_ gcs.Client              = (*Client)(nil)
	_ gcs.RuntimeEnvURIPinner = (*Client)(nil)
)

// Client is a pure-Go GCS gRPC client. It satisfies gcs.Client,
// gcs.RuntimeEnvURIPinner, gcs.LogBatchPublisher, gcs.ErrorInfoPublisher,
// gcs.NodeResourceUsagePublisher, and the plugin.GCSHandshakeClient subset.
type Client struct {
	conn       *grpc.ClientConn
	clusterID  string
	rpcTimeout time.Duration
	kv         proto.InternalKVGcsServiceClient
	pubsub     proto.ObservabilityPubSubServiceClient
	runtimeEnv proto.RuntimeEnvGcsServiceClient
	autoscaler proto.AutoscalerStateServiceClient
	nodeInfo   proto.NodeInfoGcsServiceClient
	closed     atomic.Bool
}

// NewClient dials GCS and initializes the KV and pubsub service clients.
// TLS and bearer-token authentication are resolved from the Ray environment
// (RAY_USE_TLS/RAY_TLS_*, RAY_AUTH_MODE/RAY_AUTH_TOKEN*) via
// gcs.DialOptionsFromEnv, mirroring the C++ BuildChannel and Python
// init_grpc_channel. opts.TimeoutMs, when positive, is applied as the gRPC
// ConnectTimeoutMs so the dial waits for the channel to become Ready within
// that bound.
func NewClient(ctx context.Context, opts gcs.ClientOptions) (*Client, error) {
	var dialOpts gcs.DialOptions
	if opts.DialOptions != nil {
		// Caller resolved the options once (e.g. outside a retry loop) so the
		// token/TLS files are not re-read on every attempt.
		dialOpts = *opts.DialOptions
	} else {
		var err error
		dialOpts, err = gcs.DialOptionsFromEnv()
		if err != nil {
			return nil, fmt.Errorf("resolve GCS dial options: %w", err)
		}
	}
	dialOpts.ConnectTimeoutMs = opts.TimeoutMs
	// Default to the plain-TCP dialer the head also uses: grpc-go's default
	// dialer can stall the HTTP/2 cleartext handshake against the Ray GCS
	// server (see gcs.PlainTCPDialer), and the log monitor that consumes this
	// client dials the same server on the same node. A caller that set its own
	// dialer is left untouched; a nil Dialer — unset or deliberately empty —
	// falls back to the plain-TCP dialer.
	if dialOpts.Dialer == nil {
		dialOpts.Dialer = gcs.PlainTCPDialer
	}
	conn, err := gcs.DialGCS(ctx, opts.Address, dialOpts)
	if err != nil {
		return nil, fmt.Errorf("dial GCS %s: %w", opts.Address, err)
	}
	// A nil ClusterID must be sent as no metadata at all: AppendClusterID omits
	// the header only for "", whereas NilClusterID().Hex() is 56 'f' chars and
	// would be rejected by GCS cluster-ID auth (Unauthenticated/WrongClusterID).
	clusterID := ""
	if !opts.ClusterID.IsNil() {
		clusterID = opts.ClusterID.Hex()
	}
	var rpcTimeout time.Duration
	if opts.TimeoutMs > 0 {
		rpcTimeout = time.Duration(opts.TimeoutMs) * time.Millisecond
	}
	return &Client{
		conn:       conn,
		clusterID:  clusterID,
		rpcTimeout: rpcTimeout,
		kv:         proto.NewInternalKVGcsServiceClient(conn),
		// The GCS server registers ObservabilityPubSubService
		// (src/ray/gcs/grpc_services.cc), the same service the Python
		// subscriber uses. InternalPubSubGcsService exists only in the proto
		// and is never registered by the Ray GCS server, so publishing
		// through it fails with Unimplemented.
		pubsub:     proto.NewObservabilityPubSubServiceClient(conn),
		runtimeEnv: proto.NewRuntimeEnvGcsServiceClient(conn),
		autoscaler: proto.NewAutoscalerStateServiceClient(conn),
		nodeInfo:   proto.NewNodeInfoGcsServiceClient(conn),
	}, nil
}

func (c *Client) withClusterID(ctx context.Context) context.Context {
	return gcs.AppendClusterID(ctx, c.clusterID)
}

func (c *Client) withCallContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx = c.withClusterID(ctx)
	if _, ok := ctx.Deadline(); !ok && c.rpcTimeout > 0 {
		return context.WithTimeout(ctx, c.rpcTimeout)
	}
	return ctx, func() {}
}

// Get reads a value from the GCS internal KV store, mapping a missing key to
// gcs.ErrKeyNotFound. The GCS server normally reports it as ray::StatusCode
// NotFound encoded in reply.Status.Code with an OK transport; a gRPC NOT_FOUND
// status is mapped the same way for servers that use the transport instead.
func (c *Client) Get(ctx context.Context, ns, key string) ([]byte, error) {
	callCtx, cancel := c.withCallContext(ctx)
	defer cancel()
	reply, err := c.kv.InternalKVGet(callCtx, &proto.InternalKVGetRequest{
		Namespace: []byte(ns),
		Key:       []byte(key),
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, gcs.ErrKeyNotFound
		}
		return nil, fmt.Errorf("internal kv get %s/%s: %w", ns, key, err)
	}
	if reply.Status != nil && reply.Status.Code != 0 {
		if reply.Status.Code == gcs.StatusNotFoundCode {
			return nil, gcs.ErrKeyNotFound
		}
		return nil, fmt.Errorf("internal kv get %s/%s: status code %d: %s",
			ns, key, reply.Status.Code, reply.Status.Message)
	}
	return reply.Value, nil
}

// PublishLogBatch publishes one log batch on the RAY_LOG_CHANNEL through the
// ObservabilityPubSubService, matching the wire format of the C++
// PublisherAccessor::PublishLogs (src/ray/gcs_rpc_client/accessor.cc): channel,
// key_id and log_batch_message.
func (c *Client) PublishLogBatch(ctx context.Context, payload gcs.LogBatchPayload) error {
	req := &proto.GcsPublishRequest{
		PubMessages: []*proto.PubMessage{{
			ChannelType: proto.ChannelType_RAY_LOG_CHANNEL,
			KeyId:       []byte(payload.JobID),
			InnerMessage: &proto.PubMessage_LogBatchMessage{
				LogBatchMessage: &proto.LogBatch{
					Ip:        payload.IP,
					Pid:       payload.PID,
					JobId:     payload.JobID,
					IsError:   payload.IsError,
					Lines:     payload.Lines,
					ActorName: payload.ActorName,
					TaskName:  payload.TaskName,
				},
			},
		}},
	}
	_, err := c.pubsub.GcsPublish(c.withClusterID(ctx), req)
	if err != nil {
		return fmt.Errorf("publish log batch: %w", err)
	}
	return nil
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	c.closed.Store(true)
	return c.conn.Close()
}

// IsClosed reports whether Close has been called.
func (c *Client) IsClosed() bool { return c.closed.Load() }

// MultiGet reads multiple keys from the GCS internal KV store.
func (c *Client) MultiGet(ctx context.Context, ns string, keys []string) (map[string][]byte, error) {
	rawKeys := make([][]byte, len(keys))
	for i, k := range keys {
		rawKeys[i] = []byte(k)
	}
	callCtx, cancel := c.withCallContext(ctx)
	defer cancel()
	reply, err := c.kv.InternalKVMultiGet(callCtx, &proto.InternalKVMultiGetRequest{
		Namespace: []byte(ns),
		Keys:      rawKeys,
	})
	if err != nil {
		return nil, fmt.Errorf("internal kv multi_get %s: %w", ns, err)
	}
	if err := kvStatusErr(fmt.Sprintf("internal kv multi_get %s", ns), reply.Status); err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(reply.Results))
	for _, entry := range reply.Results {
		out[string(entry.Key)] = entry.Value
	}
	return out, nil
}

// Exists checks whether key exists in the GCS internal KV store.
func (c *Client) Exists(ctx context.Context, ns, key string) (bool, error) {
	callCtx, cancel := c.withCallContext(ctx)
	defer cancel()
	reply, err := c.kv.InternalKVExists(callCtx, &proto.InternalKVExistsRequest{
		Namespace: []byte(ns),
		Key:       []byte(key),
	})
	if err != nil {
		return false, fmt.Errorf("internal kv exists %s/%s: %w", ns, key, err)
	}
	if err := kvStatusErr(fmt.Sprintf("internal kv exists %s/%s", ns, key), reply.Status); err != nil {
		return false, err
	}
	return reply.Exists, nil
}

// GetAutoscalerStatus fetches the cluster status via AutoscalerStateService.GetClusterStatus.
func (c *Client) GetAutoscalerStatus(ctx context.Context) (*proto.GetClusterStatusReply, error) {
	ac := c.autoscaler
	if ac == nil {
		ac = proto.NewAutoscalerStateServiceClient(c.conn)
	}
	callCtx, cancel := c.withCallContext(ctx)
	defer cancel()
	reply, err := ac.GetClusterStatus(callCtx, &proto.GetClusterStatusRequest{})
	if err != nil {
		return nil, fmt.Errorf("get cluster status: %w", err)
	}
	return reply, nil
}

// ReportAutoscalingState unmarshals protobuf-serialized AutoscalingState bytes
// and reports them to GCS via AutoscalerStateService.ReportAutoscalingState.
func (c *Client) ReportAutoscalingState(autoscalingState string) error {
	if len(autoscalingState) == 0 {
		return fmt.Errorf("autoscaling state must not be empty")
	}
	var state proto.AutoscalingState
	if err := gproto.Unmarshal([]byte(autoscalingState), &state); err != nil {
		return fmt.Errorf("unmarshal autoscaling state: %w", err)
	}
	ac := c.autoscaler
	if ac == nil {
		ac = proto.NewAutoscalerStateServiceClient(c.conn)
	}
	timeout := c.rpcTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if _, err := ac.ReportAutoscalingState(c.withClusterID(ctx), &proto.ReportAutoscalingStateRequest{
		AutoscalingState: &state,
	}); err != nil {
		return fmt.Errorf("report autoscaling state: %w", err)
	}
	return nil
}

// DrainNode requests GCS AutoscalerStateService to drain a single node.
func (c *Client) DrainNode(
	ctx context.Context,
	nodeID ids.NodeID,
	reason proto.DrainNodeReason,
	reasonMessage string,
	deadlineTimestampMs int64,
) (bool, string, error) {
	ac := c.autoscaler
	if ac == nil {
		ac = proto.NewAutoscalerStateServiceClient(c.conn)
	}
	callCtx, cancel := c.withCallContext(ctx)
	defer cancel()
	reply, err := ac.DrainNode(callCtx, &proto.DrainNodeRequest{
		NodeId:              nodeID.Binary(),
		Reason:              reason,
		ReasonMessage:       reasonMessage,
		DeadlineTimestampMs: deadlineTimestampMs,
	})
	if err != nil {
		return false, "", fmt.Errorf("drain node %s: %w", nodeID.Hex(), err)
	}
	return reply.GetIsAccepted(), reply.GetRejectionReasonMessage(), nil
}

// DrainNodes requests GCS NodeInfoGcsService to drain a batch of nodes.
func (c *Client) DrainNodes(ctx context.Context, nodeIDs []ids.NodeID) ([]ids.NodeID, error) {
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	nc := c.nodeInfo
	if nc == nil {
		nc = proto.NewNodeInfoGcsServiceClient(c.conn)
	}
	data := make([]*proto.DrainNodeData, len(nodeIDs))
	for i, id := range nodeIDs {
		data[i] = &proto.DrainNodeData{NodeId: id.Binary()}
	}
	callCtx, cancel := c.withCallContext(ctx)
	defer cancel()
	reply, err := nc.DrainNode(callCtx, &proto.GcsDrainNodeRequest{
		DrainNodeData: data,
	})
	if err != nil {
		return nil, fmt.Errorf("drain nodes: %w", err)
	}
	if err := nodeInfoStatusErr("drain nodes", reply.GetStatus()); err != nil {
		return nil, err
	}
	drained := make([]ids.NodeID, 0, len(reply.GetDrainNodeStatus()))
	for _, st := range reply.GetDrainNodeStatus() {
		id, err := ids.NodeIDFromBinary(st.GetNodeId())
		if err != nil {
			continue
		}
		drained = append(drained, id)
	}
	return drained, nil
}

// ListJobs lists all jobs from GCS via ListJobsWithFilter.
func (c *Client) ListJobs(ctx context.Context) ([]*proto.JobTableData, error) {
	return c.ListJobsWithFilter(ctx, "", false, false)
}

// Unused gcs.Client interface methods return gcs.ErrNotImplemented.
func (c *Client) GetAvailableResources(context.Context, ids.NodeID) (*proto.AvailableResources, error) {
	return nil, gcs.ErrNotImplemented
}
func (c *Client) GetTotalResources(context.Context, ids.NodeID) (*proto.TotalResources, error) {
	return nil, gcs.ErrNotImplemented
}
func (c *Client) GetActorInfo(context.Context, ids.ActorID) (*proto.ActorTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (c *Client) ListActors(context.Context, *ids.JobID) ([]*proto.ActorTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (c *Client) ListActorsByFilter(context.Context, *ids.JobID, *gcs.ActorStateName) ([]*proto.ActorTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (c *Client) GetJobInfo(context.Context, ids.JobID) (*proto.JobTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (c *Client) GetWorkerInfo(context.Context, ids.WorkerID) (*proto.WorkerTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (c *Client) ListWorkers(context.Context) ([]*proto.WorkerTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (c *Client) GetPlacementGroup(context.Context, ids.PlacementGroupID) (*proto.PlacementGroupTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (c *Client) GetPlacementGroupByName(context.Context, string, string) (*proto.PlacementGroupTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (c *Client) ListPlacementGroups(context.Context) ([]*proto.PlacementGroupTableData, error) {
	return nil, gcs.ErrNotImplemented
}

// PinRuntimeEnvURI pins a runtime environment package URI in GCS for
// expirationS seconds via RuntimeEnvGcsService.PinRuntimeEnvURI.
func (c *Client) PinRuntimeEnvURI(ctx context.Context, uri string, expirationS int) error {
	if c.runtimeEnv == nil {
		return fmt.Errorf("runtime env gcs service client is not initialized")
	}
	if expirationS < 0 || expirationS > math.MaxInt32 {
		return fmt.Errorf("pin runtime env uri %q: invalid expiration %d", uri, expirationS)
	}
	callCtx, cancel := c.withCallContext(ctx)
	defer cancel()
	reply, err := c.runtimeEnv.PinRuntimeEnvURI(callCtx, &proto.PinRuntimeEnvURIRequest{
		Uri:         uri,
		ExpirationS: int32(expirationS),
	})
	if err != nil {
		return fmt.Errorf("pin runtime env uri %q: %w", uri, err)
	}
	if reply.GetStatus() != nil && reply.GetStatus().GetCode() != 0 {
		return fmt.Errorf("pin runtime env uri %q: status code %d: %s",
			uri, reply.GetStatus().GetCode(), reply.GetStatus().GetMessage())
	}
	return nil
}

// The methods below extend the pure-Go GCS client with the method set the
// dashboard agent needs beyond the log monitor's KV-read + log-publish subset.
// The log-monitor path (NewClient/Get/PublishLogBatch/Close) is untouched.

// Address returns the GCS server address (host:port) this client dialed.
func (c *Client) Address() string {
	return c.conn.Target()
}

// ClusterID returns the cluster id this client is bound to.
func (c *Client) ClusterID() ids.ClusterID {
	id, err := ids.ClusterIDFromHex(c.clusterID)
	if err != nil {
		return ids.NilClusterID()
	}
	return id
}

// kvStatusErr converts a non-OK GCS reply status into an error, mirroring the
// NotFound handling in Get. The GCS server encodes ray::StatusCode into
// reply.Status.Code while keeping the gRPC transport OK.
func kvStatusErr(op string, replyStatus *proto.GcsStatus) error {
	if replyStatus == nil || replyStatus.Code == 0 {
		return nil
	}
	if replyStatus.Code == gcs.StatusNotFoundCode {
		return gcs.ErrKeyNotFound
	}
	return fmt.Errorf("internal kv %s: status code %d: %s", op, replyStatus.Code, replyStatus.Message)
}

// Put writes a value into the GCS internal KV store, reporting whether a new
// entry was added (false when overwrite is false and the key already exists).
func (c *Client) Put(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
	reply, err := c.kv.InternalKVPut(c.withClusterID(ctx), &proto.InternalKVPutRequest{
		Namespace: []byte(ns),
		Key:       []byte(key),
		Value:     value,
		Overwrite: overwrite,
	})
	if err != nil {
		return false, fmt.Errorf("internal kv put %s/%s: %w", ns, key, err)
	}
	if err := kvStatusErr("put", reply.Status); err != nil {
		return false, err
	}
	return reply.Added, nil
}

// Del deletes a key (or keys by prefix) from the GCS internal KV store,
// returning the number of entries deleted.
func (c *Client) Del(ctx context.Context, ns, key string, delByPrefix bool) (int, error) {
	reply, err := c.kv.InternalKVDel(c.withClusterID(ctx), &proto.InternalKVDelRequest{
		Namespace:   []byte(ns),
		Key:         []byte(key),
		DelByPrefix: delByPrefix,
	})
	if err != nil {
		return 0, fmt.Errorf("internal kv del %s/%s: %w", ns, key, err)
	}
	if err := kvStatusErr("del", reply.Status); err != nil {
		return 0, err
	}
	return int(reply.DeletedNum), nil
}

// Keys lists the keys in the GCS internal KV store under a namespace and
// prefix.
func (c *Client) Keys(ctx context.Context, ns, prefix string) ([]string, error) {
	reply, err := c.kv.InternalKVKeys(c.withClusterID(ctx), &proto.InternalKVKeysRequest{
		Namespace: []byte(ns),
		Prefix:    []byte(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("internal kv keys %s/%s: %w", ns, prefix, err)
	}
	if err := kvStatusErr("keys", reply.Status); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(reply.Results))
	for _, k := range reply.Results {
		keys = append(keys, string(k))
	}
	return keys, nil
}

// nodeInfoStatusErr converts a non-OK node-info / job-info reply status into
// an error.
func nodeInfoStatusErr(op string, replyStatus *proto.GcsStatus) error {
	if replyStatus == nil || replyStatus.Code == 0 {
		return nil
	}
	return fmt.Errorf("%s: status code %d: %s", op, replyStatus.Code, replyStatus.Message)
}

// GetAll fetches node info. When nodeIDs is non-empty it is filtered to those
// nodes, otherwise all nodes are returned, mirroring the native GetAll
// (nodes_cgo.go) which fetches the full node table and filters in Go.
func (c *Client) GetAll(ctx context.Context, nodeIDs []ids.NodeID) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	reply, err := proto.NewNodeInfoGcsServiceClient(c.conn).GetAllNodeInfo(
		c.withClusterID(ctx), &proto.GetAllNodeInfoRequest{})
	if err != nil {
		return nil, fmt.Errorf("get all node info: %w", err)
	}
	if err := nodeInfoStatusErr("get all node info", reply.Status); err != nil {
		return nil, err
	}

	// Filter to the requested node ids when provided. The raw proto bytes are
	// the map key, so no hex round-trip is needed.
	selected := reply.NodeInfoList
	if len(nodeIDs) > 0 {
		want := make(map[string]struct{}, len(nodeIDs))
		for _, id := range nodeIDs {
			want[string(id.Binary())] = struct{}{}
		}
		selected = nil
		for _, ni := range reply.NodeInfoList {
			if _, ok := want[string(ni.NodeId)]; ok {
				selected = append(selected, ni)
			}
		}
	}

	result := make(map[ids.NodeID]*proto.GcsNodeInfo, len(selected))
	for _, ni := range selected {
		nodeID, err := ids.NodeIDFromBinary(ni.NodeId)
		if err != nil {
			return nil, fmt.Errorf("invalid node ID: %w", err)
		}
		result[nodeID] = ni
	}
	return result, nil
}

// CheckAlive reports which of the given nodes are alive, mirroring the native
// CheckAlive (nodes_cgo.go) and the Python async_check_alive.
func (c *Client) CheckAlive(ctx context.Context, nodeIDs []ids.NodeID) ([]bool, error) {
	if len(nodeIDs) == 0 {
		return []bool{}, nil
	}
	reqIDs := make([][]byte, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		reqIDs = append(reqIDs, id.Binary())
	}
	reply, err := proto.NewNodeInfoGcsServiceClient(c.conn).CheckAlive(
		c.withClusterID(ctx), &proto.CheckAliveRequest{NodeIds: reqIDs})
	if err != nil {
		return nil, fmt.Errorf("check alive: %w", err)
	}
	if err := nodeInfoStatusErr("check alive", reply.Status); err != nil {
		return nil, err
	}
	alive := make([]bool, len(nodeIDs))
	for i := range nodeIDs {
		alive[i] = i < len(reply.RayletAlive) && reply.RayletAlive[i]
	}
	return alive, nil
}

// ListJobsWithFilter lists jobs optionally filtered by job id / submission id
// and with the heavy JobSubmissionInfo / IsRunningTasks fields skipped,
// mirroring the GetAllJobInfo GCS RPC the Python dashboard agent drives via
// async_get_all_job_info. An empty jobOrSubmissionID returns all jobs.
func (c *Client) ListJobsWithFilter(ctx context.Context, jobOrSubmissionID string, skipSubmissionJobInfoField, skipIsRunningTasksField bool) ([]*proto.JobTableData, error) {
	req := &proto.GetAllJobInfoRequest{
		SkipSubmissionJobInfoField: &skipSubmissionJobInfoField,
		SkipIsRunningTasksField:    &skipIsRunningTasksField,
	}
	// Only set the filter when non-empty: the server skips filtering on an
	// ABSENT field but a present-but-empty value matches nothing
	// (gcs_job_manager.cc:279-298), and the C++/Python callers this mirrors omit
	// the field for an empty filter too.
	if jobOrSubmissionID != "" {
		req.JobOrSubmissionId = &jobOrSubmissionID
	}
	reply, err := proto.NewJobInfoGcsServiceClient(c.conn).GetAllJobInfo(c.withClusterID(ctx), req)
	if err != nil {
		return nil, fmt.Errorf("list jobs with filter: %w", err)
	}
	if err := nodeInfoStatusErr("list jobs with filter", reply.Status); err != nil {
		return nil, err
	}
	return reply.JobInfoList, nil
}

// PublishNodeResourceUsage publishes the node resource usage of a node to GCS
// on the RAY_NODE_RESOURCE_USAGE_CHANNEL, matching the wire format of the C++
// PublisherAccessor::AsyncPublishNodeResourceUsage (accessor.cc): channel,
// key_id and node_resource_usage_message.json.
func (c *Client) PublishNodeResourceUsage(ctx context.Context, keyID, jsonPayload string) error {
	req := &proto.GcsPublishRequest{
		PubMessages: []*proto.PubMessage{{
			ChannelType: proto.ChannelType_RAY_NODE_RESOURCE_USAGE_CHANNEL,
			KeyId:       []byte(keyID),
			InnerMessage: &proto.PubMessage_NodeResourceUsageMessage{
				NodeResourceUsageMessage: &proto.NodeResourceUsage{Json: jsonPayload},
			},
		}},
	}
	if _, err := c.pubsub.GcsPublish(c.withClusterID(ctx), req); err != nil {
		return fmt.Errorf("publish node resource usage: %w", err)
	}
	return nil
}

// PublishErrorInfo publishes one error to GCS on the RAY_ERROR_INFO_CHANNEL,
// matching the wire format of the cgo ErrorPublisher.PublishErrorInfo
// (error_publisher_cgo.go) and the C++ PublisherAccessor::PublishError
// (accessor.cc): channel, key_id and the ErrorTableData fields. The timestamp
// is stamped here, the same way the cgo wrapper stamps it, because
// gcs.ErrorInfoPayload carries none. Unlike the cgo boundary there is no NUL
// scrubbing: a protobuf string carries NUL bytes verbatim, so the raw raylet
// tail reaches drivers untruncated. The context deadline is not required —
// gRPC observes ctx cancellation natively, so a deadline-less context is safe
// here (the cgo variant rejects one because its synchronous call cannot).
func (c *Client) PublishErrorInfo(ctx context.Context, keyID string, payload gcs.ErrorInfoPayload) error {
	req := &proto.GcsPublishRequest{
		PubMessages: []*proto.PubMessage{{
			ChannelType: proto.ChannelType_RAY_ERROR_INFO_CHANNEL,
			KeyId:       []byte(keyID),
			InnerMessage: &proto.PubMessage_ErrorInfoMessage{
				ErrorInfoMessage: &proto.ErrorTableData{
					JobId:        payload.JobID,
					Type:         payload.Type,
					ErrorMessage: payload.Message,
					Timestamp:    float64(time.Now().UnixNano()) / 1e9,
				},
			},
		}},
	}
	if _, err := c.pubsub.GcsPublish(c.withClusterID(ctx), req); err != nil {
		return fmt.Errorf("publish error info: %w", err)
	}
	return nil
}

// getAliveNodeWithSelector fetches the first ALIVE node matching a
// node_ip_address selector, mirroring the C++ NodeInfoAccessor::GetAllNoCache
// with state_filter=ALIVE (accessor.cc:359). An empty result is not an error.
func (c *Client) getAliveNodeWithSelector(ctx context.Context, nodeIP string) ([]*proto.GcsNodeInfo, error) {
	selector := &proto.GetAllNodeInfoRequest_NodeSelector{
		NodeSelector: &proto.GetAllNodeInfoRequest_NodeSelector_NodeIpAddress{NodeIpAddress: nodeIP},
	}
	alive := proto.GcsNodeInfo_ALIVE
	reply, err := proto.NewNodeInfoGcsServiceClient(c.conn).GetAllNodeInfo(
		c.withClusterID(ctx), &proto.GetAllNodeInfoRequest{
			StateFilter:   &alive,
			NodeSelectors: []*proto.GetAllNodeInfoRequest_NodeSelector{selector},
		})
	if err != nil {
		return nil, fmt.Errorf("get alive node with selector %s: %w", nodeIP, err)
	}
	if err := nodeInfoStatusErr("get alive node with selector", reply.Status); err != nil {
		return nil, err
	}
	return reply.NodeInfoList, nil
}

// GetNodeToConnect returns the node a driver should connect to for the given
// node IP, mirroring C++ GlobalStateAccessor::GetNodeToConnectForDriver
// (global_state_accessor.cc:471-530). It filters for ALIVE nodes only and tries
// the node IP, then the GCS address, then the localhost IP (the last only when
// the requested IP equals the GCS address), looping until raylet_start_wait_time_s
// (default 30 s) elapses.
func (c *Client) GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error) {
	wait := rayletStartWaitTime()
	end := time.Now().Add(wait)

	for {
		// Each lookup is bounded by the remaining wait budget so a stalled GCS
		// RPC cannot outlive the window; the C++ GetAllNoCache bounds every call
		// the same way (global_state_accessor.cc:466-500). The caller's context
		// usually carries no deadline (the job manager passes its process
		// context), so without this a blackholed connection would block the
		// submission handler far past raylet_start_wait_time_s.
		remaining := time.Until(end)
		if remaining <= 0 {
			return nil, fmt.Errorf("this node has an IP address of %s, and Ray expects this IP address to be either the GCS address or one of the Raylet addresses; connected to GCS at %s and found no ALIVE Raylet with this IP address", nodeIpAddress, c.gcsHost())
		}
		qctx, cancel := context.WithTimeout(ctx, remaining)
		nodes, err := c.getAliveNodeWithSelector(qctx, nodeIpAddress)
		cancel()
		if err != nil {
			return nil, err
		}
		if len(nodes) > 0 {
			return nodes[0], nil
		}

		gcsIP := c.gcsHost()
		qctx, cancel = context.WithTimeout(ctx, time.Until(end))
		nodes, err = c.getAliveNodeWithSelector(qctx, gcsIP)
		cancel()
		if err != nil {
			return nil, err
		}
		if len(nodes) == 0 && nodeIpAddress == gcsIP {
			qctx, cancel = context.WithTimeout(ctx, time.Until(end))
			nodes, err = c.getAliveNodeWithSelector(qctx, common.GetLocalhostIP())
			cancel()
			if err != nil {
				return nil, err
			}
		}
		if len(nodes) > 0 {
			return nodes[0], nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// gcsHost returns the host (IP) portion of the GCS server address this client
// dialed, for use as a node_ip_address selector.
func (c *Client) gcsHost() string {
	target := c.Address()
	if host, _, err := net.SplitHostPort(target); err == nil && host != "" {
		return host
	}
	return target
}

// NextJobID allocates a fresh JobID from GCS, mirroring the C++
// GlobalStateAccessor::GetNextJobID (the reply carries an int32 that becomes
// the low bytes of the JobID, matching JobID::FromInt).
func (c *Client) NextJobID(ctx context.Context) (ids.JobID, error) {
	reply, err := proto.NewJobInfoGcsServiceClient(c.conn).GetNextJobID(
		c.withClusterID(ctx), &proto.GetNextJobIDRequest{})
	if err != nil {
		return ids.NilJobID(), fmt.Errorf("get next job id: %w", err)
	}
	if err := nodeInfoStatusErr("get next job id", reply.Status); err != nil {
		return ids.NilJobID(), err
	}
	return ids.JobIDFromInt(uint32(reply.JobId)), nil
}

// AddEvents forwards aggregated Ray events and task metadata to GCS via
// RayEventExportGcsService.AddEvents.
func (c *Client) AddEvents(ctx context.Context, req *proto.AddEventsRequest) (*proto.AddEventsReply, error) {
	reply, err := proto.NewRayEventExportGcsServiceClient(c.conn).AddEvents(
		c.withClusterID(ctx), req)
	if err != nil {
		return nil, fmt.Errorf("add task events to gcs: %w", err)
	}
	return reply, nil
}

// rayletStartWaitTime returns how long GetNodeToConnect waits for a raylet to
// appear, honoring the RAY_raylet_start_wait_time_s env override (seconds) and
// defaulting to 30 s, matching the C++ RayConfig default
// (src/ray/common/ray_config_def.h:815-821).
func rayletStartWaitTime() time.Duration {
	if v := os.Getenv("RAY_raylet_start_wait_time_s"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			return time.Duration(f * float64(time.Second))
		}
	}
	return 30 * time.Second
}
