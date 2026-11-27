// Copyright 2026 The Ray Authors.
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

package cloud_providers

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

const (
	k8sSATokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	k8sSACAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	k8sSANamespace = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

	kubeRayLabelCluster  = "ray.io/cluster"
	kubeRayLabelNodeKind = "ray.io/node-type"
	kubeRayLabelGroup    = "ray.io/group"
	kubeRayHeadGroup     = "headgroup"
)

type kubeRayCluster struct {
	Spec struct {
		HeadGroupSpec struct {
			RayStartParams map[string]string `json:"rayStartParams"`
		} `json:"headGroupSpec"`
		WorkerGroupSpecs []kubeRayWorkerGroupSpec `json:"workerGroupSpecs"`
	} `json:"spec"`
}

type kubeRayWorkerGroupSpec struct {
	GroupName     string `json:"groupName"`
	Replicas      *int   `json:"replicas"`
	MinReplicas   *int   `json:"minReplicas"`
	MaxReplicas   *int   `json:"maxReplicas"`
	NumOfHosts    *int   `json:"numOfHosts"`
	ScaleStrategy *struct {
		WorkersToDelete []string `json:"workersToDelete,omitempty"`
	} `json:"scaleStrategy,omitempty"`
}

type kubeRayPod struct {
	Metadata struct {
		Name              string            `json:"name"`
		Namespace         string            `json:"namespace"`
		Labels            map[string]string `json:"labels"`
		DeletionTimestamp string            `json:"deletionTimestamp"`
	} `json:"metadata"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

type kubeRayPodList struct {
	Items []kubeRayPod `json:"items"`
}

type jsonPatchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

// KubeRayProvider implements autoscaler.ICloudInstanceProvider by reading
// RayCluster/Pod objects and patching workerGroupSpecs on the Kubernetes API.
type KubeRayProvider struct {
	clusterName string
	namespace   string
	baseURL     string
	tokenPath   string
	httpClient  *http.Client

	mu                sync.Mutex
	processedRequests map[string]struct{}
	errorsQueue       []error
}

// NewKubeRayProvider creates a KubeRayProvider using in-cluster Kubernetes
// credentials (service account CA & per-request rotated token).
func NewKubeRayProvider(clusterName string, providerConfig map[string]any) (*KubeRayProvider, error) {
	ns, _ := providerConfig["namespace"].(string)
	if strings.TrimSpace(ns) == "" {
		ns = os.Getenv("RAY_CLUSTER_NAMESPACE")
	}
	if strings.TrimSpace(ns) == "" {
		if b, err := os.ReadFile(k8sSANamespace); err == nil {
			ns = strings.TrimSpace(string(b))
		}
	}
	if strings.TrimSpace(ns) == "" {
		ns = "default"
	}
	if strings.TrimSpace(clusterName) == "" {
		clusterName = os.Getenv("RAY_CLUSTER_NAME")
	}
	if strings.TrimSpace(clusterName) == "" {
		return nil, fmt.Errorf("kuberay provider requires cluster_name")
	}

	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if port == "" {
		port = "443"
	}
	if host == "" {
		return nil, fmt.Errorf("KUBERNETES_SERVICE_HOST is not set")
	}

	caBytes, err := os.ReadFile(k8sSACAPath)
	if err != nil {
		return nil, fmt.Errorf("read k8s CA certificate: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("failed to parse k8s CA certificate")
	}

	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		},
	}

	return &KubeRayProvider{
		clusterName:       clusterName,
		namespace:         ns,
		baseURL:           "https://" + net.JoinHostPort(host, port),
		tokenPath:         k8sSATokenPath,
		httpClient:        client,
		processedRequests: make(map[string]struct{}),
	}, nil
}

// newKubeRayProviderWithClient constructs a KubeRayProvider with an explicit
// baseURL and http.Client for unit tests.
func newKubeRayProviderWithClient(clusterName, namespace, baseURL string, client *http.Client) *KubeRayProvider {
	return &KubeRayProvider{
		clusterName:       clusterName,
		namespace:         namespace,
		baseURL:           strings.TrimRight(baseURL, "/"),
		httpClient:        client,
		processedRequests: make(map[string]struct{}),
	}
}

func (p *KubeRayProvider) doJSON(ctx context.Context, method, path, contentType string, body any, dst any) error {
	var bodyReader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.baseURL+path, bodyReader)
	if err != nil {
		return err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if p.tokenPath != "" {
		b, err := os.ReadFile(p.tokenPath)
		if err != nil {
			return fmt.Errorf("read service account token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(b)))
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("k8s %s %s status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(respBytes)))
	}
	if dst != nil {
		if err := json.Unmarshal(respBytes, dst); err != nil {
			return fmt.Errorf("decode k8s response %s: %w", path, err)
		}
	}
	return nil
}

func (p *KubeRayProvider) rayClusterPath() string {
	return fmt.Sprintf("/apis/ray.io/v1/namespaces/%s/rayclusters/%s",
		url.PathEscape(p.namespace), url.PathEscape(p.clusterName))
}

func (p *KubeRayProvider) getRayCluster(ctx context.Context) (*kubeRayCluster, error) {
	var cr kubeRayCluster
	if err := p.doJSON(ctx, http.MethodGet, p.rayClusterPath(), "", nil, &cr); err != nil {
		return nil, err
	}
	return &cr, nil
}

func (p *KubeRayProvider) getPods(ctx context.Context) ([]kubeRayPod, error) {
	labelSel := url.QueryEscape(fmt.Sprintf("%s=%s", kubeRayLabelCluster, p.clusterName))
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods?labelSelector=%s", url.PathEscape(p.namespace), labelSel)
	var list kubeRayPodList
	if err := p.doJSON(ctx, http.MethodGet, path, "", nil, &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// GetNonTerminated returns all active head and worker pods mapped to CloudInstance.
func (p *KubeRayProvider) GetNonTerminated() (map[string]autoscaler.CloudInstance, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pods, err := p.getPods(ctx)
	if err != nil {
		return nil, fmt.Errorf("list kuberay pods: %w", err)
	}

	out := make(map[string]autoscaler.CloudInstance, len(pods))
	for _, pod := range pods {
		if pod.Status.Phase == "Succeeded" || pod.Status.Phase == "Failed" {
			continue
		}
		if pod.Metadata.DeletionTimestamp != "" {
			continue
		}
		var kind proto.NodeKind
		switch pod.Metadata.Labels[kubeRayLabelNodeKind] {
		case "head":
			kind = proto.NodeKind_HEAD
		case "worker":
			kind = proto.NodeKind_WORKER
		default:
			// Non-Ray pods (e.g. KubeRay's redis-cleanup job) carry the cluster
			// label as well; skip them like the Python provider does.
			continue
		}
		nodeType := pod.Metadata.Labels[kubeRayLabelGroup]
		if nodeType == "" && kind == proto.NodeKind_HEAD {
			nodeType = kubeRayHeadGroup
		}
		out[pod.Metadata.Name] = autoscaler.CloudInstance{
			CloudInstanceId: pod.Metadata.Name,
			NodeKind:        kind,
			NodeType:        nodeType,
			IsRunning:       pod.Status.Phase == "Running",
		}
	}
	return out, nil
}

func (p *KubeRayProvider) recordError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.errorsQueue = append(p.errorsQueue, err)
}

// PollErrors drains asynchronous launch/terminate errors accumulated since the last poll.
func (p *KubeRayProvider) PollErrors() []error {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.errorsQueue
	p.errorsQueue = nil
	return out
}

func (p *KubeRayProvider) isRequestProcessed(requestID string) bool {
	if requestID == "" {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.processedRequests[requestID]
	return ok
}

func (p *KubeRayProvider) markRequestProcessed(requestID string) {
	if requestID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.processedRequests[requestID] = struct{}{}
}

func workerGroupNumHosts(wg kubeRayWorkerGroupSpec) int {
	if wg.NumOfHosts != nil && *wg.NumOfHosts > 0 {
		return *wg.NumOfHosts
	}
	return 1
}

func workerGroupReplicas(wg kubeRayWorkerGroupSpec) int {
	if wg.Replicas != nil {
		return *wg.Replicas
	}
	return 0
}

func pendingDeletesInGroup(wg kubeRayWorkerGroupSpec, livePods map[string]autoscaler.CloudInstance) []string {
	if wg.ScaleStrategy == nil || len(wg.ScaleStrategy.WorkersToDelete) == 0 {
		return nil
	}
	var pending []string
	for _, podName := range wg.ScaleStrategy.WorkersToDelete {
		if _, alive := livePods[podName]; alive {
			pending = append(pending, podName)
		}
	}
	return pending
}

// Launch increases workerGroupSpecs[idx].replicas to satisfy shape.
func (p *KubeRayProvider) Launch(shape map[string]int, requestID string) error {
	if p.isRequestProcessed(requestID) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cr, err := p.getRayCluster(ctx)
	if err != nil {
		for nt, cnt := range shape {
			p.recordError(&autoscaler.LaunchNodeError{
				CloudInstanceProviderError: autoscaler.CloudInstanceProviderError{Msg: err.Error(), TimestampNs: time.Now().UnixNano()},
				NodeType:                   nt, Count: cnt, RequestId: requestID, Cause: err,
			})
		}
		return err
	}
	livePods, err := p.GetNonTerminated()
	if err != nil {
		for nt, cnt := range shape {
			p.recordError(&autoscaler.LaunchNodeError{
				CloudInstanceProviderError: autoscaler.CloudInstanceProviderError{Msg: err.Error(), TimestampNs: time.Now().UnixNano()},
				NodeType:                   nt, Count: cnt, RequestId: requestID, Cause: err,
			})
		}
		return err
	}

	unmatched := make(map[string]int, len(shape))
	for nt, cnt := range shape {
		if cnt > 0 {
			unmatched[nt] = cnt
		}
	}

	var patches []jsonPatchOp
	for idx, wg := range cr.Spec.WorkerGroupSpecs {
		if pending := pendingDeletesInGroup(wg, livePods); len(pending) > 0 {
			err := fmt.Errorf("worker group %s has pending deletes %v; waiting before scaling up", wg.GroupName, pending)
			for nt, cnt := range shape {
				p.recordError(&autoscaler.LaunchNodeError{
					CloudInstanceProviderError: autoscaler.CloudInstanceProviderError{Msg: err.Error(), TimestampNs: time.Now().UnixNano()},
					NodeType:                   nt, Count: cnt, RequestId: requestID, Cause: err,
				})
			}
			return err
		}
		if wg.ScaleStrategy != nil && len(wg.ScaleStrategy.WorkersToDelete) > 0 {
			patches = append(patches, jsonPatchOp{
				Op:    "add",
				Path:  fmt.Sprintf("/spec/workerGroupSpecs/%d/scaleStrategy/workersToDelete", idx),
				Value: []string{},
			})
		}

		addCount := shape[wg.GroupName]
		delete(unmatched, wg.GroupName)
		if addCount <= 0 {
			continue
		}
		numHosts := workerGroupNumHosts(wg)
		curReplicas := workerGroupReplicas(wg)
		desired := curReplicas + (addCount+numHosts-1)/numHosts
		if wg.MaxReplicas != nil && desired > *wg.MaxReplicas {
			rejected := (desired - *wg.MaxReplicas) * numHosts
			desired = *wg.MaxReplicas
			if rejected > 0 {
				p.recordError(&autoscaler.LaunchNodeError{
					CloudInstanceProviderError: autoscaler.CloudInstanceProviderError{
						Msg:         "Launch capped by maxReplicas limit.",
						TimestampNs: time.Now().UnixNano(),
					},
					NodeType:  wg.GroupName,
					Count:     rejected,
					RequestId: requestID,
					Cause:     fmt.Errorf("maxReplicas=%d reached", *wg.MaxReplicas),
				})
			}
		}
		if desired != curReplicas {
			patches = append(patches, jsonPatchOp{
				Op:    "add",
				Path:  fmt.Sprintf("/spec/workerGroupSpecs/%d/replicas", idx),
				Value: desired,
			})
		}
	}

	for nt, cnt := range unmatched {
		p.recordError(&autoscaler.LaunchNodeError{
			CloudInstanceProviderError: autoscaler.CloudInstanceProviderError{
				Msg:         fmt.Sprintf("unknown worker group %q in RayCluster", nt),
				TimestampNs: time.Now().UnixNano(),
			},
			NodeType:  nt,
			Count:     cnt,
			RequestId: requestID,
			Cause:     fmt.Errorf("unknown worker group %q", nt),
		})
	}

	if len(patches) > 0 {
		if err := p.doJSON(ctx, http.MethodPatch, p.rayClusterPath(), "application/json-patch+json", patches, nil); err != nil {
			for nt, cnt := range shape {
				p.recordError(&autoscaler.LaunchNodeError{
					CloudInstanceProviderError: autoscaler.CloudInstanceProviderError{Msg: err.Error(), TimestampNs: time.Now().UnixNano()},
					NodeType:                   nt, Count: cnt, RequestId: requestID, Cause: err,
				})
			}
			return err
		}
	}
	p.markRequestProcessed(requestID)
	return nil
}

// Terminate decrements workerGroupSpecs[idx].replicas and records workersToDelete.
func (p *KubeRayProvider) Terminate(ids []string, requestID string) error {
	if len(ids) == 0 || p.isRequestProcessed(requestID) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cr, err := p.getRayCluster(ctx)
	if err != nil {
		for _, id := range ids {
			p.recordError(&autoscaler.TerminateNodeError{
				CloudInstanceProviderError: autoscaler.CloudInstanceProviderError{Msg: err.Error(), TimestampNs: time.Now().UnixNano()},
				CloudInstanceId:            id, RequestId: requestID, Cause: err,
			})
		}
		return err
	}
	livePods, err := p.GetNonTerminated()
	if err != nil {
		for _, id := range ids {
			p.recordError(&autoscaler.TerminateNodeError{
				CloudInstanceProviderError: autoscaler.CloudInstanceProviderError{Msg: err.Error(), TimestampNs: time.Now().UnixNano()},
				CloudInstanceId:            id, RequestId: requestID, Cause: err,
			})
		}
		return err
	}

	byGroup := make(map[string][]string)
	var missingGroupErr error
	for _, id := range ids {
		inst, ok := livePods[id]
		if !ok {
			log.Log.V(1).Info("kuberay terminate: pod not in live set", "pod", id)
			continue
		}
		if inst.NodeType == "" {
			err := fmt.Errorf("pod %s missing ray.io/group label", id)
			p.recordError(&autoscaler.TerminateNodeError{
				CloudInstanceProviderError: autoscaler.CloudInstanceProviderError{
					Msg:         err.Error(),
					TimestampNs: time.Now().UnixNano(),
				},
				CloudInstanceId: id,
				RequestId:       requestID,
				Cause:           err,
			})
			if missingGroupErr == nil {
				missingGroupErr = err
			}
			continue
		}
		byGroup[inst.NodeType] = append(byGroup[inst.NodeType], id)
	}
	if missingGroupErr != nil {
		return missingGroupErr
	}

	var patches []jsonPatchOp
	for idx, wg := range cr.Spec.WorkerGroupSpecs {
		if pending := pendingDeletesInGroup(wg, livePods); len(pending) > 0 {
			err := fmt.Errorf("worker group %s has pending deletes %v; waiting before terminating", wg.GroupName, pending)
			for _, id := range ids {
				p.recordError(&autoscaler.TerminateNodeError{
					CloudInstanceProviderError: autoscaler.CloudInstanceProviderError{Msg: err.Error(), TimestampNs: time.Now().UnixNano()},
					CloudInstanceId:            id, RequestId: requestID, Cause: err,
				})
			}
			return err
		}
		toDel := byGroup[wg.GroupName]
		if len(toDel) == 0 {
			continue
		}
		numHosts := workerGroupNumHosts(wg)
		curReplicas := workerGroupReplicas(wg)
		minRep := 0
		if wg.MinReplicas != nil {
			minRep = *wg.MinReplicas
		}
		maxDeletable := (curReplicas - minRep) * numHosts
		if maxDeletable < 0 {
			maxDeletable = 0
		}
		if len(toDel) > maxDeletable {
			for _, id := range toDel[maxDeletable:] {
				p.recordError(&autoscaler.TerminateNodeError{
					CloudInstanceProviderError: autoscaler.CloudInstanceProviderError{
						Msg:         "Terminate capped by minReplicas limit.",
						TimestampNs: time.Now().UnixNano(),
					},
					CloudInstanceId: id,
					RequestId:       requestID,
					Cause:           fmt.Errorf("minReplicas=%d reached", minRep),
				})
			}
			toDel = toDel[:maxDeletable]
		}
		if len(toDel) == 0 {
			continue
		}
		desired := curReplicas - (len(toDel)+numHosts-1)/numHosts
		if desired < minRep {
			desired = minRep
		}
		patches = append(patches, jsonPatchOp{
			Op:    "add",
			Path:  fmt.Sprintf("/spec/workerGroupSpecs/%d/replicas", idx),
			Value: desired,
		})
		if wg.ScaleStrategy == nil {
			patches = append(patches, jsonPatchOp{
				Op:    "add",
				Path:  fmt.Sprintf("/spec/workerGroupSpecs/%d/scaleStrategy", idx),
				Value: map[string]any{"workersToDelete": toDel},
			})
		} else {
			patches = append(patches, jsonPatchOp{
				Op:    "add",
				Path:  fmt.Sprintf("/spec/workerGroupSpecs/%d/scaleStrategy/workersToDelete", idx),
				Value: toDel,
			})
		}
	}

	if len(patches) > 0 {
		if err := p.doJSON(ctx, http.MethodPatch, p.rayClusterPath(), "application/json-patch+json", patches, nil); err != nil {
			for _, id := range ids {
				p.recordError(&autoscaler.TerminateNodeError{
					CloudInstanceProviderError: autoscaler.CloudInstanceProviderError{Msg: err.Error(), TimestampNs: time.Now().UnixNano()},
					CloudInstanceId:            id, RequestId: requestID, Cause: err,
				})
			}
			return err
		}
	}
	p.markRequestProcessed(requestID)
	return nil
}
