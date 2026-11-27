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

package gcsgrpc

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
)

type fakeGCS struct {
	proto.UnimplementedInternalKVGcsServiceServer
	proto.UnimplementedObservabilityPubSubServiceServer
	proto.UnimplementedNodeInfoGcsServiceServer
	proto.UnimplementedJobInfoGcsServiceServer
	proto.UnimplementedRayEventExportGcsServiceServer
	proto.UnimplementedRuntimeEnvGcsServiceServer
	proto.UnimplementedAutoscalerStateServiceServer
	gotAddEvents *proto.AddEventsRequest
	addEventsErr error
	kvValue      []byte
	kvNotFound   bool
	kvErr        error
	gotPub       *proto.GcsPublishRequest
	gotMD        metadata.MD

	// Scriptable replies for the KV write/delete/list family. A nil kvStatus
	// means the OK status.
	kvStatus     *proto.GcsStatus
	kvPutAdded   bool
	kvDeletedNum int32
	kvKeys       [][]byte

	// Scriptable replies for the node-info / job-info family. A nil status
	// field means the OK status.
	nodeInfoStatus   *proto.GcsStatus
	nodeInfoList     []*proto.GcsNodeInfo
	rayletAlive      []bool
	jobInfoStatus    *proto.GcsStatus
	jobInfoList      []*proto.JobTableData
	nextJobID        int32

	kvExists            bool
	pinnedURI           string
	pinnedExp           int32
	reportedState       *proto.AutoscalingState
	clusterStatusReply  *proto.GetClusterStatusReply
	drainAccepted       bool
	drainRejection      string
	drainedNodeStatuses []*proto.DrainNodeStatus
	gotGetAllNode    *proto.GetAllNodeInfoRequest
	gotCheckAlive    *proto.CheckAliveRequest
	gotGetAllJobInfo *proto.GetAllJobInfoRequest
}

func (f *fakeGCS) InternalKVGet(_ context.Context, req *proto.InternalKVGetRequest) (*proto.InternalKVGetReply, error) {
	if f.kvErr != nil {
		return nil, f.kvErr
	}
	if f.kvNotFound {
		// Match the real GCS server wire behavior (GCS_RPC_SEND_REPLY in
		// src/ray/gcs/grpc_service_interfaces.h): transport OK with the error
		// encoded in reply.Status.Code (ray::StatusCode::NotFound == 17).
		return &proto.InternalKVGetReply{
			Status: &proto.GcsStatus{Code: 17, Message: "Failed to find the key"},
		}, nil
	}
	return &proto.InternalKVGetReply{Status: &proto.GcsStatus{Code: 0}, Value: f.kvValue}, nil
}

func (f *fakeGCS) InternalKVPut(_ context.Context, req *proto.InternalKVPutRequest) (*proto.InternalKVPutReply, error) {
	st := f.kvStatus
	if st == nil {
		st = &proto.GcsStatus{Code: 0}
	}
	return &proto.InternalKVPutReply{Status: st, Added: f.kvPutAdded}, nil
}

func (f *fakeGCS) InternalKVDel(_ context.Context, req *proto.InternalKVDelRequest) (*proto.InternalKVDelReply, error) {
	st := f.kvStatus
	if st == nil {
		st = &proto.GcsStatus{Code: 0}
	}
	return &proto.InternalKVDelReply{Status: st, DeletedNum: f.kvDeletedNum}, nil
}

func (f *fakeGCS) InternalKVKeys(_ context.Context, req *proto.InternalKVKeysRequest) (*proto.InternalKVKeysReply, error) {
	st := f.kvStatus
	if st == nil {
		st = &proto.GcsStatus{Code: 0}
	}
	return &proto.InternalKVKeysReply{Status: st, Results: f.kvKeys}, nil
}

func (f *fakeGCS) GetAllNodeInfo(_ context.Context, req *proto.GetAllNodeInfoRequest) (*proto.GetAllNodeInfoReply, error) {
	f.gotGetAllNode = req
	st := f.nodeInfoStatus
	if st == nil {
		st = &proto.GcsStatus{Code: 0}
	}
	return &proto.GetAllNodeInfoReply{Status: st, NodeInfoList: f.nodeInfoList}, nil
}

func (f *fakeGCS) CheckAlive(_ context.Context, req *proto.CheckAliveRequest) (*proto.CheckAliveReply, error) {
	f.gotCheckAlive = req
	st := f.nodeInfoStatus
	if st == nil {
		st = &proto.GcsStatus{Code: 0}
	}
	return &proto.CheckAliveReply{Status: st, RayletAlive: f.rayletAlive}, nil
}

func (f *fakeGCS) GetAllJobInfo(_ context.Context, req *proto.GetAllJobInfoRequest) (*proto.GetAllJobInfoReply, error) {
	f.gotGetAllJobInfo = req
	st := f.jobInfoStatus
	if st == nil {
		st = &proto.GcsStatus{Code: 0}
	}
	return &proto.GetAllJobInfoReply{Status: st, JobInfoList: f.jobInfoList}, nil
}

func (f *fakeGCS) GetNextJobID(_ context.Context, req *proto.GetNextJobIDRequest) (*proto.GetNextJobIDReply, error) {
	st := f.jobInfoStatus
	if st == nil {
		st = &proto.GcsStatus{Code: 0}
	}
	return &proto.GetNextJobIDReply{Status: st, JobId: f.nextJobID}, nil
}

func (f *fakeGCS) GcsPublish(ctx context.Context, req *proto.GcsPublishRequest) (*proto.GcsPublishReply, error) {
	f.gotPub = req
	f.gotMD, _ = metadata.FromIncomingContext(ctx)
	return &proto.GcsPublishReply{Status: &proto.GcsStatus{Code: 0}}, nil
}

func (f *fakeGCS) InternalKVExists(ctx context.Context, req *proto.InternalKVExistsRequest) (*proto.InternalKVExistsReply, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		f.gotMD = md
	}
	return &proto.InternalKVExistsReply{Status: f.kvStatus, Exists: f.kvExists}, nil
}

func (f *fakeGCS) InternalKVMultiGet(ctx context.Context, req *proto.InternalKVMultiGetRequest) (*proto.InternalKVMultiGetReply, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		f.gotMD = md
	}
	var results []*proto.MapFieldEntry
	for _, k := range req.Keys {
		results = append(results, &proto.MapFieldEntry{Key: k, Value: f.kvValue})
	}
	return &proto.InternalKVMultiGetReply{Status: f.kvStatus, Results: results}, nil
}

func (f *fakeGCS) PinRuntimeEnvURI(ctx context.Context, req *proto.PinRuntimeEnvURIRequest) (*proto.PinRuntimeEnvURIReply, error) {
	f.pinnedURI = req.Uri
	f.pinnedExp = req.ExpirationS
	return &proto.PinRuntimeEnvURIReply{Status: f.kvStatus}, nil
}

func (f *fakeGCS) GetClusterStatus(ctx context.Context, _ *proto.GetClusterStatusRequest) (*proto.GetClusterStatusReply, error) {
	if f.clusterStatusReply != nil {
		return f.clusterStatusReply, nil
	}
	return &proto.GetClusterStatusReply{}, nil
}

func (f *fakeGCS) ReportAutoscalingState(ctx context.Context, req *proto.ReportAutoscalingStateRequest) (*proto.ReportAutoscalingStateReply, error) {
	f.reportedState = req.GetAutoscalingState()
	return &proto.ReportAutoscalingStateReply{}, nil
}

func (f *fakeGCS) DrainNode(ctx context.Context, req *proto.DrainNodeRequest) (*proto.DrainNodeReply, error) {
	return &proto.DrainNodeReply{
		IsAccepted:             f.drainAccepted,
		RejectionReasonMessage: f.drainRejection,
	}, nil
}

func (f *fakeGCS) startDrainBatch(ctx context.Context, req *proto.GcsDrainNodeRequest) (*proto.GcsDrainNodeReply, error) {
	return &proto.GcsDrainNodeReply{
		Status:          f.nodeInfoStatus,
		DrainNodeStatus: f.drainedNodeStatuses,
	}, nil
}

type fakeNodeInfoDrain struct {
	*fakeGCS
}

func (f *fakeNodeInfoDrain) DrainNode(ctx context.Context, req *proto.GcsDrainNodeRequest) (*proto.GcsDrainNodeReply, error) {
	return f.startDrainBatch(ctx, req)
}

func startMockGCS(t *testing.T, f *fakeGCS) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	proto.RegisterInternalKVGcsServiceServer(server, f)
	proto.RegisterObservabilityPubSubServiceServer(server, f)
	proto.RegisterNodeInfoGcsServiceServer(server, &fakeNodeInfoDrain{f})
	proto.RegisterJobInfoGcsServiceServer(server, f)
	proto.RegisterRayEventExportGcsServiceServer(server, f)
	proto.RegisterRuntimeEnvGcsServiceServer(server, f)
	proto.RegisterAutoscalerStateServiceServer(server, f)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	return lis.Addr().String()
}

func newTestClient(t *testing.T, f *fakeGCS) *Client {
	t.Helper()
	addr := startMockGCS(t, f)
	cc, err := grpc.DialContext(context.Background(), addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return &Client{
		conn:       cc,
		kv:         proto.NewInternalKVGcsServiceClient(cc),
		pubsub:     proto.NewObservabilityPubSubServiceClient(cc),
		runtimeEnv: proto.NewRuntimeEnvGcsServiceClient(cc),
		autoscaler: proto.NewAutoscalerStateServiceClient(cc),
		nodeInfo:   proto.NewNodeInfoGcsServiceClient(cc),
	}
}

func TestGetNotFound(t *testing.T) {
	f := &fakeGCS{kvNotFound: true}
	c := newTestClient(t, f)
	_, err := c.Get(context.Background(), "__autoscaler", "k")
	if !errors.Is(err, gcs.ErrKeyNotFound) {
		t.Fatalf("Get() error = %v, want gcs.ErrKeyNotFound", err)
	}
}

func TestGetNotFoundTransportStatus(t *testing.T) {
	f := &fakeGCS{kvErr: status.Error(codes.NotFound, "no such key")}
	c := newTestClient(t, f)
	_, err := c.Get(context.Background(), "__autoscaler", "k")
	if !errors.Is(err, gcs.ErrKeyNotFound) {
		t.Fatalf("Get() error = %v, want gcs.ErrKeyNotFound", err)
	}
}

func TestGetValue(t *testing.T) {
	f := &fakeGCS{kvValue: []byte("1")}
	c := newTestClient(t, f)
	got, err := c.Get(context.Background(), "__autoscaler", "k")
	if err != nil || string(got) != "1" {
		t.Fatalf("Get() = %q, %v; want %q, nil", got, err, "1")
	}
}

func TestNewClientUsesPreResolvedDialOptions(t *testing.T) {
	f := &fakeGCS{kvValue: []byte("1")}
	addr := startMockGCS(t, f)
	pre := gcs.DialOptions{ConnectTimeoutMs: 5000}
	// RAY_USE_TLS without its certificate variables makes DialOptionsFromEnv
	// fail, and the absurd auth mode is never a valid setting: either being
	// consulted would surface, so reaching the server proves the pre-resolved
	// options were used verbatim.
	t.Setenv("RAY_USE_TLS", "1")
	t.Setenv("RAY_AUTH_MODE", "definitely-not-a-real-auth-mode")
	c, err := NewClient(context.Background(), gcs.ClientOptions{
		Address:     addr,
		TimeoutMs:   5000,
		DialOptions: &pre,
	})
	if err != nil {
		t.Fatalf("NewClient() with pre-resolved DialOptions error = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	got, err := c.Get(context.Background(), "__autoscaler", "k")
	if err != nil || string(got) != "1" {
		t.Fatalf("Get() = %q, %v; want %q, nil", got, err, "1")
	}
}

func TestPublishLogBatchWireFormat(t *testing.T) {
	f := &fakeGCS{}
	c := newTestClient(t, f)
	payload := gcs.LogBatchPayload{
		IP: "1.2.3.4", PID: "123", JobID: "01000000",
		IsError: true, Lines: []string{"l1"}, ActorName: "a", TaskName: "t",
	}
	if err := c.PublishLogBatch(context.Background(), payload); err != nil {
		t.Fatalf("PublishLogBatch() error = %v", err)
	}
	if len(f.gotPub.PubMessages) != 1 {
		t.Fatalf("PubMessages = %d, want 1 message", len(f.gotPub.PubMessages))
	}
	msg := f.gotPub.PubMessages[0]
	if msg.ChannelType != proto.ChannelType_RAY_LOG_CHANNEL {
		t.Fatalf("channel = %v, want RAY_LOG_CHANNEL", msg.ChannelType)
	}
	if string(msg.KeyId) != "01000000" {
		t.Fatalf("key_id = %q, want 01000000", msg.KeyId)
	}
	lb := msg.GetLogBatchMessage()
	if lb.Ip != "1.2.3.4" || lb.Pid != "123" || lb.JobId != "01000000" || !lb.IsError ||
		len(lb.Lines) != 1 || lb.Lines[0] != "l1" || lb.ActorName != "a" || lb.TaskName != "t" {
		t.Fatalf("LogBatch = %#v, want all payload fields", lb)
	}
}

func TestPublishLogBatchSendsClusterIDMetadata(t *testing.T) {
	f := &fakeGCS{}
	addr := startMockGCS(t, f)
	cc, err := grpc.DialContext(context.Background(), addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	c := &Client{
		conn:      cc,
		clusterID: "abc123",
		kv:        proto.NewInternalKVGcsServiceClient(cc),
		pubsub:    proto.NewObservabilityPubSubServiceClient(cc),
	}

	if err := c.PublishLogBatch(context.Background(), gcs.LogBatchPayload{JobID: "01000000", Lines: []string{"l1"}}); err != nil {
		t.Fatalf("PublishLogBatch() error = %v", err)
	}
	if got := f.gotMD.Get("ray_cluster_id"); len(got) != 1 || got[0] != "abc123" {
		t.Fatalf("ray_cluster_id metadata = %v, want [abc123]", got)
	}
}

func TestPutDelKeysWireFormat(t *testing.T) {
	f := &fakeGCS{kvPutAdded: true, kvDeletedNum: 2, kvKeys: [][]byte{[]byte("a"), []byte("b")}}
	c := newTestClient(t, f)

	added, err := c.Put(context.Background(), "ns", "k", []byte("v"), true)
	if err != nil || !added {
		t.Fatalf("Put() = %v, %v; want true, nil", added, err)
	}
	n, err := c.Del(context.Background(), "ns", "k", true)
	if err != nil || n != 2 {
		t.Fatalf("Del() = %d, %v; want 2, nil", n, err)
	}
	keys, err := c.Keys(context.Background(), "ns", "p")
	if err != nil || len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("Keys() = %v, %v; want [a b], nil", keys, err)
	}

	// A non-OK status (transport OK) must surface as an error; a NotFound
	// status maps to gcs.ErrKeyNotFound like Get does.
	f.kvStatus = &proto.GcsStatus{Code: 5, Message: "boom"}
	if _, err := c.Put(context.Background(), "ns", "k", nil, true); err == nil || !strings.Contains(err.Error(), "status code 5") {
		t.Fatalf("Put() error = %v, want a status code 5 failure", err)
	}
	f.kvStatus = &proto.GcsStatus{Code: gcs.StatusNotFoundCode, Message: "missing"}
	if _, err := c.Del(context.Background(), "ns", "k", false); !errors.Is(err, gcs.ErrKeyNotFound) {
		t.Fatalf("Del() error = %v, want gcs.ErrKeyNotFound", err)
	}
}

func TestExistsMultiGetPinAndAutoscalerRPCs(t *testing.T) {
	nodeID, err := ids.NodeIDFromHex(strings.Repeat("11", 28))
	if err != nil {
		t.Fatalf("NodeIDFromHex: %v", err)
	}
	f := &fakeGCS{
		kvExists:           true,
		kvValue:            []byte("val"),
		clusterStatusReply: &proto.GetClusterStatusReply{AutoscalingState: &proto.AutoscalingState{AutoscalerStateVersion: 7}},
		drainAccepted:      true,
		drainedNodeStatuses: []*proto.DrainNodeStatus{
			{NodeId: nodeID.Binary()},
		},
	}
	c := newTestClient(t, f)
	ctx := context.Background()

	if ok, err := c.Exists(ctx, "ns", "k"); err != nil || !ok {
		t.Fatalf("Exists() = (%v, %v), want (true, nil)", ok, err)
	}
	m, err := c.MultiGet(ctx, "ns", []string{"k1", "k2"})
	if err != nil || string(m["k1"]) != "val" || string(m["k2"]) != "val" {
		t.Fatalf("MultiGet() = (%v, %v)", m, err)
	}
	if err := c.PinRuntimeEnvURI(ctx, "gcs://pkg.zip", 120); err != nil {
		t.Fatalf("PinRuntimeEnvURI: %v", err)
	}
	if f.pinnedURI != "gcs://pkg.zip" || f.pinnedExp != 120 {
		t.Fatalf("pinned = (%q, %d), want (gcs://pkg.zip, 120)", f.pinnedURI, f.pinnedExp)
	}
	if err := c.PinRuntimeEnvURI(ctx, "gcs://pkg.zip", -1); err == nil {
		t.Fatal("PinRuntimeEnvURI(-1) = nil, want range error")
	}

	st, err := c.GetAutoscalerStatus(ctx)
	if err != nil || st.GetAutoscalingState().GetAutoscalerStateVersion() != 7 {
		t.Fatalf("GetAutoscalerStatus = (%+v, %v)", st, err)
	}
	rawState, err := gproto.Marshal(&proto.AutoscalingState{AutoscalerStateVersion: 9})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := c.ReportAutoscalingState(string(rawState)); err != nil {
		t.Fatalf("ReportAutoscalingState: %v", err)
	}
	if f.reportedState.GetAutoscalerStateVersion() != 9 {
		t.Fatalf("reported version = %d, want 9", f.reportedState.GetAutoscalerStateVersion())
	}
	if err := c.ReportAutoscalingState(""); err == nil {
		t.Fatal("ReportAutoscalingState(empty) = nil, want error")
	}

	accepted, reject, err := c.DrainNode(ctx, nodeID, proto.DrainNodeReason_DRAIN_NODE_REASON_IDLE_TERMINATION, "idle", 0)
	if err != nil || !accepted || reject != "" {
		t.Fatalf("DrainNode = (%v, %q, %v)", accepted, reject, err)
	}
	drained, err := c.DrainNodes(ctx, []ids.NodeID{nodeID})
	if err != nil || len(drained) != 1 || drained[0] != nodeID {
		t.Fatalf("DrainNodes = (%v, %v)", drained, err)
	}
}

func TestNewClientNilClusterIDOmitsMetadata(t *testing.T) {
	f := &fakeGCS{kvExists: true}
	addr := startMockGCS(t, f)

	c, err := NewClient(context.Background(), gcs.ClientOptions{
		Address:   addr,
		ClusterID: ids.NilClusterID(),
		TimeoutMs: 5000,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	if _, err := c.Exists(context.Background(), "ns", "k"); err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if got := f.gotMD.Get("ray_cluster_id"); len(got) != 0 {
		t.Fatalf("ray_cluster_id metadata = %v, want empty for NilClusterID", got)
	}
	if c.rpcTimeout != 5*time.Second {
		t.Fatalf("rpcTimeout = %v, want 5s", c.rpcTimeout)
	}
}

func TestGetAllFiltersNodeIDs(t *testing.T) {
	idA, err := ids.NodeIDFromHex(strings.Repeat("aa", 28))
	if err != nil {
		t.Fatalf("NodeIDFromHex: %v", err)
	}
	idB, err := ids.NodeIDFromHex(strings.Repeat("bb", 28))
	if err != nil {
		t.Fatalf("NodeIDFromHex: %v", err)
	}
	f := &fakeGCS{nodeInfoList: []*proto.GcsNodeInfo{
		{NodeId: idA.Binary(), NodeManagerPort: 1},
		{NodeId: idB.Binary(), NodeManagerPort: 2},
	}}
	c := newTestClient(t, f)

	all, err := c.GetAll(context.Background(), nil)
	if err != nil || len(all) != 2 {
		t.Fatalf("GetAll(nil) = %d nodes, %v; want 2, nil", len(all), err)
	}
	got, err := c.GetAll(context.Background(), []ids.NodeID{idB})
	if err != nil || len(got) != 1 {
		t.Fatalf("GetAll([idB]) = %d nodes, %v; want 1, nil", len(got), err)
	}
	if _, ok := got[idB]; !ok {
		t.Fatalf("GetAll([idB]) result lacks idB: %v", got)
	}

	// A non-OK reply status must surface as an error.
	f.nodeInfoStatus = &proto.GcsStatus{Code: 5, Message: "boom"}
	if _, err := c.GetAll(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "status code 5") {
		t.Fatalf("GetAll() error = %v, want a status code 5 failure", err)
	}
}

func TestCheckAliveMapsReply(t *testing.T) {
	idA, err := ids.NodeIDFromHex(strings.Repeat("aa", 28))
	if err != nil {
		t.Fatalf("NodeIDFromHex: %v", err)
	}
	idB, err := ids.NodeIDFromHex(strings.Repeat("bb", 28))
	if err != nil {
		t.Fatalf("NodeIDFromHex: %v", err)
	}
	f := &fakeGCS{rayletAlive: []bool{true, false}}
	c := newTestClient(t, f)

	alive, err := c.CheckAlive(context.Background(), []ids.NodeID{idA, idB})
	if err != nil {
		t.Fatalf("CheckAlive() error = %v", err)
	}
	if len(alive) != 2 || !alive[0] || alive[1] {
		t.Fatalf("CheckAlive() = %v, want [true false]", alive)
	}
	if len(f.gotCheckAlive.NodeIds) != 2 || string(f.gotCheckAlive.NodeIds[0]) != string(idA.Binary()) {
		t.Fatalf("CheckAlive request node_ids = %v, want the binary ids", f.gotCheckAlive.NodeIds)
	}

	empty, err := c.CheckAlive(context.Background(), nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("CheckAlive(nil) = %v, %v; want [], nil", empty, err)
	}
}

func TestListJobsWithFilterSendsSkipFlags(t *testing.T) {
	f := &fakeGCS{jobInfoList: []*proto.JobTableData{{JobId: []byte("01000000")}}}
	c := newTestClient(t, f)

	jobs, err := c.ListJobsWithFilter(context.Background(), "01000000", true, true)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("ListJobsWithFilter() = %d jobs, %v; want 1, nil", len(jobs), err)
	}
	req := f.gotGetAllJobInfo
	if req.JobOrSubmissionId == nil || *req.JobOrSubmissionId != "01000000" {
		t.Fatalf("job_or_submission_id = %v, want 01000000", req.JobOrSubmissionId)
	}
	if req.SkipSubmissionJobInfoField == nil || !*req.SkipSubmissionJobInfoField ||
		req.SkipIsRunningTasksField == nil || !*req.SkipIsRunningTasksField {
		t.Fatalf("skip flags = %v/%v, want both true", req.SkipSubmissionJobInfoField, req.SkipIsRunningTasksField)
	}

	// An empty filter must OMIT the field entirely: a present-but-empty value
	// matches nothing server-side while an absent field returns all jobs.
	if _, err := c.ListJobsWithFilter(context.Background(), "", true, true); err != nil {
		t.Fatalf("ListJobsWithFilter(\"\") error = %v", err)
	}
	if f.gotGetAllJobInfo.JobOrSubmissionId != nil {
		t.Fatalf("job_or_submission_id = %v, want the field omitted for an empty filter", f.gotGetAllJobInfo.JobOrSubmissionId)
	}

	// A non-OK reply status must surface as an error.
	f.jobInfoStatus = &proto.GcsStatus{Code: 5, Message: "boom"}
	if _, err := c.ListJobsWithFilter(context.Background(), "", true, true); err == nil || !strings.Contains(err.Error(), "status code 5") {
		t.Fatalf("ListJobsWithFilter() error = %v, want a status code 5 failure", err)
	}
}

func TestPublishNodeResourceUsageWireFormat(t *testing.T) {
	f := &fakeGCS{}
	c := newTestClient(t, f)

	if err := c.PublishNodeResourceUsage(context.Background(), "node-key", `{"usage":1}`); err != nil {
		t.Fatalf("PublishNodeResourceUsage() error = %v", err)
	}
	if len(f.gotPub.PubMessages) != 1 {
		t.Fatalf("PubMessages = %d, want 1 message", len(f.gotPub.PubMessages))
	}
	msg := f.gotPub.PubMessages[0]
	if msg.ChannelType != proto.ChannelType_RAY_NODE_RESOURCE_USAGE_CHANNEL {
		t.Fatalf("channel = %v, want RAY_NODE_RESOURCE_USAGE_CHANNEL", msg.ChannelType)
	}
	if string(msg.KeyId) != "node-key" {
		t.Fatalf("key_id = %q, want node-key", msg.KeyId)
	}
	if got := msg.GetNodeResourceUsageMessage().GetJson(); got != `{"usage":1}` {
		t.Fatalf("json payload = %q, want the input verbatim", got)
	}
}

func TestPublishErrorInfoWireFormat(t *testing.T) {
	f := &fakeGCS{}
	c := newTestClient(t, f)

	payload := gcs.ErrorInfoPayload{
		JobID:   ids.NilJobID().Binary(),
		Type:    "worker_died",
		Message: "raylet.out tail",
	}
	before := time.Now()
	if err := c.PublishErrorInfo(context.Background(), "ffffffff", payload); err != nil {
		t.Fatalf("PublishErrorInfo() error = %v", err)
	}
	if len(f.gotPub.PubMessages) != 1 {
		t.Fatalf("PubMessages = %d, want 1 message", len(f.gotPub.PubMessages))
	}
	msg := f.gotPub.PubMessages[0]
	if msg.ChannelType != proto.ChannelType_RAY_ERROR_INFO_CHANNEL {
		t.Fatalf("channel = %v, want RAY_ERROR_INFO_CHANNEL", msg.ChannelType)
	}
	if string(msg.KeyId) != "ffffffff" {
		t.Fatalf("key_id = %q, want ffffffff", msg.KeyId)
	}
	et := msg.GetErrorInfoMessage()
	if et == nil {
		t.Fatal("inner message is nil, want ErrorTableData")
	}
	if string(et.JobId) != string(ids.NilJobID().Binary()) || et.Type != "worker_died" || et.ErrorMessage != "raylet.out tail" {
		t.Fatalf("ErrorTableData = %#v, want the payload fields verbatim", et)
	}
	// The publisher stamps the timestamp (the payload carries none), as float
	// seconds like the cgo wrapper and the Python agent's time.time().
	if et.Timestamp < float64(before.UnixNano())/1e9 || et.Timestamp > float64(time.Now().UnixNano())/1e9 {
		t.Fatalf("timestamp = %v, want a stamp between %v and now", et.Timestamp, before)
	}
}

func TestNextJobID(t *testing.T) {
	f := &fakeGCS{nextJobID: 7}
	c := newTestClient(t, f)

	id, err := c.NextJobID(context.Background())
	if err != nil {
		t.Fatalf("NextJobID() error = %v", err)
	}
	if !id.Equal(ids.JobIDFromInt(7)) {
		t.Fatalf("NextJobID() = %s, want JobIDFromInt(7)", id.Hex())
	}

	f.jobInfoStatus = &proto.GcsStatus{Code: 5, Message: "boom"}
	if _, err := c.NextJobID(context.Background()); err == nil || !strings.Contains(err.Error(), "status code 5") {
		t.Fatalf("NextJobID() error = %v, want a status code 5 failure", err)
	}
}

func TestGetNodeToConnectSelectorAndHit(t *testing.T) {
	idA, err := ids.NodeIDFromHex(strings.Repeat("aa", 28))
	if err != nil {
		t.Fatalf("NodeIDFromHex: %v", err)
	}
	f := &fakeGCS{nodeInfoList: []*proto.GcsNodeInfo{{NodeId: idA.Binary(), NodeManagerPort: 12345}}}
	c := newTestClient(t, f)

	node, err := c.GetNodeToConnect(context.Background(), "10.0.0.5")
	if err != nil {
		t.Fatalf("GetNodeToConnect() error = %v", err)
	}
	if string(node.NodeId) != string(idA.Binary()) {
		t.Fatalf("GetNodeToConnect() = %v, want node A", node)
	}
	req := f.gotGetAllNode
	if len(req.NodeSelectors) != 1 || req.NodeSelectors[0].GetNodeIpAddress() != "10.0.0.5" {
		t.Fatalf("node selectors = %v, want one node_ip_address=10.0.0.5", req.NodeSelectors)
	}
	if req.StateFilter == nil || *req.StateFilter != proto.GcsNodeInfo_ALIVE {
		t.Fatalf("state_filter = %v, want ALIVE", req.StateFilter)
	}
}

func TestGetNodeToConnectTimesOut(t *testing.T) {
	f := &fakeGCS{}
	c := newTestClient(t, f)

	// Shrink the wait so the miss path returns promptly; the loop polls every
	// 100 ms until the raylet_start_wait_time_s window elapses.
	t.Setenv("RAY_raylet_start_wait_time_s", "0.05")
	_, err := c.GetNodeToConnect(context.Background(), "10.0.0.5")
	if err == nil || !strings.Contains(err.Error(), "found no ALIVE Raylet") {
		t.Fatalf("GetNodeToConnect() error = %v, want the no-alive-raylet failure", err)
	}
}

func TestClusterIDRoundTrip(t *testing.T) {
	f := &fakeGCS{}
	c := newTestClient(t, f)
	c.clusterID = strings.Repeat("ab", 28)
	if got := c.ClusterID(); got.Hex() != strings.Repeat("ab", 28) {
		t.Fatalf("ClusterID() = %s, want the hex round trip", got.Hex())
	}
	c.clusterID = "not-hex"
	if got := c.ClusterID(); !got.IsNil() {
		t.Fatalf("ClusterID() with unparsable hex = %v, want NilClusterID", got)
	}
	if c.Address() == "" {
		t.Fatal("Address() is empty, want the dialed target")
	}
}

func (f *fakeGCS) AddEvents(_ context.Context, req *proto.AddEventsRequest) (*proto.AddEventsReply, error) {
	f.gotAddEvents = req
	if f.addEventsErr != nil {
		return nil, f.addEventsErr
	}
	return &proto.AddEventsReply{Status: &proto.AddEventsStatus{Code: 0}}, nil
}

func TestAddEvents(t *testing.T) {
	f := &fakeGCS{}
	c := newTestClient(t, f)
	c.clusterID = strings.Repeat("11", 28)

	req := &proto.AddEventsRequest{
		EventsData: &proto.RayEventsData{
			Events: []*proto.RayEvent{
				{EventId: []byte("evt-1"), EventType: proto.RayEvent_TASK_DEFINITION_EVENT},
			},
		},
	}
	reply, err := c.AddEvents(context.Background(), req)
	if err != nil {
		t.Fatalf("AddEvents() error = %v", err)
	}
	if reply.GetStatus().GetCode() != 0 {
		t.Fatalf("AddEvents() status = %v, want 0", reply.GetStatus().GetCode())
	}
	if f.gotAddEvents == nil || len(f.gotAddEvents.GetEventsData().GetEvents()) != 1 {
		t.Fatalf("fakeGCS gotAddEvents = %v, want 1 event", f.gotAddEvents)
	}
}
