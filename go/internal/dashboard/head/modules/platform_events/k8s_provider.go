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

package platform_events

import (
	"bufio"
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

	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	k8sServiceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	k8sServiceAccountCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	k8sClusterLabelKey         = "ray.io/cluster"
)

type k8sWatchLine struct {
	Type   string          `json:"type"`
	Object json.RawMessage `json:"object"`
}

type k8sObjectMeta struct {
	UID             string `json:"uid"`
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	ResourceVersion string `json:"resourceVersion"`
}

type k8sPod struct {
	Metadata k8sObjectMeta `json:"metadata"`
}

type k8sEvent struct {
	Metadata       k8sObjectMeta `json:"metadata"`
	Type           string        `json:"type"`
	Reason         string        `json:"reason"`
	Message        string        `json:"message"`
	EventTime      string        `json:"eventTime"`
	LastTimestamp  string        `json:"lastTimestamp"`
	FirstTimestamp string        `json:"firstTimestamp"`
	Series         struct {
		LastObservedTime string `json:"lastObservedTime"`
		Count            int32  `json:"count"`
	} `json:"series"`
	InvolvedObject struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	} `json:"involvedObject"`
	Source struct {
		Component string `json:"component"`
	} `json:"source"`
	ReportingComponent string `json:"reportingComponent"`
}

type k8sPodList struct {
	Items []k8sPod `json:"items"`
}

type k8sEventList struct {
	Metadata struct {
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
}

// KubernetesEventProvider watches KubeRay cluster pods and Kubernetes Events
// over the Kubernetes REST watch API using pure-Go net/http.
type KubernetesEventProvider struct {
	baseURL     string
	tokenPath   string
	token       string
	httpClient  *http.Client
	namespace   string
	clusterName string
	callback    func(*proto.RayEvent)

	mu          sync.RWMutex
	clusterPods map[string]struct{}
}

// NewInClusterKubernetesEventProvider creates a provider configured from the
// pod's service account credentials and RAY_CLUSTER_NAME / RAY_CLUSTER_NAMESPACE.
func NewInClusterKubernetesEventProvider(callback func(*proto.RayEvent)) (*KubernetesEventProvider, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if port == "" {
		port = "443"
	}
	clusterName := os.Getenv("RAY_CLUSTER_NAME")
	namespace := os.Getenv("RAY_CLUSTER_NAMESPACE")
	if host == "" || clusterName == "" || namespace == "" {
		return nil, fmt.Errorf("missing required K8s/RayCluster environment variables")
	}

	tokenBytes, err := os.ReadFile(k8sServiceAccountTokenPath)
	if err != nil {
		return nil, fmt.Errorf("read k8s service account token: %w", err)
	}
	caBytes, err := os.ReadFile(k8sServiceAccountCAPath)
	if err != nil {
		return nil, fmt.Errorf("read k8s service account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("failed to parse k8s CA certificate")
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		},
	}

	return &KubernetesEventProvider{
		baseURL:     "https://" + net.JoinHostPort(host, port),
		tokenPath:   k8sServiceAccountTokenPath,
		token:       strings.TrimSpace(string(tokenBytes)),
		httpClient:  client,
		namespace:   namespace,
		clusterName: clusterName,
		callback:    callback,
		clusterPods: make(map[string]struct{}),
	}, nil
}

// Run starts watching cluster pods and namespace events until ctx is cancelled.
func (p *KubernetesEventProvider) Run(ctx context.Context) {
	// Seed pod membership before consuming namespace events so initial events
	// for existing pods are not dropped by isClusterObject.
	if err := p.listClusterPods(ctx); err != nil && ctx.Err() == nil {
		log.Log.Error(err, "k8s initial pod list failed; pod events may be dropped until watch syncs")
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		p.watchLoop(ctx, p.watchPods)
	}()
	go func() {
		defer wg.Done()
		p.watchLoop(ctx, p.watchEvents)
	}()
	wg.Wait()
}

func (p *KubernetesEventProvider) watchLoop(ctx context.Context, fn func(context.Context) error) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		err := fn(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Log.Error(err, "k8s watch stream ended; retrying")
		} else {
			backoff = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if err != nil && backoff < 15*time.Second {
			backoff *= 2
		}
	}
}

func (p *KubernetesEventProvider) currentToken() string {
	if p.tokenPath != "" {
		if b, err := os.ReadFile(p.tokenPath); err == nil {
			return strings.TrimSpace(string(b))
		}
	}
	return p.token
}

func (p *KubernetesEventProvider) doRequest(ctx context.Context, path string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	if tok := p.currentToken(); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("k8s api %s status %d", path, resp.StatusCode)
	}
	return resp.Body, nil
}

func (p *KubernetesEventProvider) listClusterPods(ctx context.Context) error {
	labelSel := url.QueryEscape(fmt.Sprintf("%s=%s", k8sClusterLabelKey, p.clusterName))
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods?labelSelector=%s&resourceVersion=0", url.PathEscape(p.namespace), labelSel)
	body, err := p.doRequest(ctx, path)
	if err != nil {
		return err
	}
	defer body.Close()

	var listResp k8sPodList
	if err := json.NewDecoder(body).Decode(&listResp); err != nil {
		return err
	}
	nextPods := make(map[string]struct{}, len(listResp.Items))
	for _, pod := range listResp.Items {
		if pod.Metadata.Name != "" {
			nextPods[pod.Metadata.Name] = struct{}{}
		}
	}
	p.mu.Lock()
	p.clusterPods = nextPods
	p.mu.Unlock()
	return nil
}

func (p *KubernetesEventProvider) latestEventsResourceVersion(ctx context.Context) (string, error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/events?limit=1&resourceVersion=0", url.PathEscape(p.namespace))
	body, err := p.doRequest(ctx, path)
	if err != nil {
		return "", err
	}
	defer body.Close()

	var listResp k8sEventList
	if err := json.NewDecoder(body).Decode(&listResp); err != nil {
		return "", err
	}
	return listResp.Metadata.ResourceVersion, nil
}

func (p *KubernetesEventProvider) watchPods(ctx context.Context) error {
	labelSel := url.QueryEscape(fmt.Sprintf("%s=%s", k8sClusterLabelKey, p.clusterName))
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods?watch=true&labelSelector=%s", url.PathEscape(p.namespace), labelSel)
	body, err := p.doRequest(ctx, path)
	if err != nil {
		return err
	}
	defer body.Close()

	// Fresh watch without resourceVersion replays a full snapshot; reset map
	// so pods deleted during a disconnect gap are pruned.
	p.mu.Lock()
	p.clusterPods = make(map[string]struct{})
	p.mu.Unlock()

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var line k8sWatchLine
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			continue
		}
		var pod k8sPod
		if err := json.Unmarshal(line.Object, &pod); err != nil || pod.Metadata.Name == "" {
			continue
		}
		p.mu.Lock()
		if strings.EqualFold(line.Type, "DELETED") {
			delete(p.clusterPods, pod.Metadata.Name)
		} else {
			p.clusterPods[pod.Metadata.Name] = struct{}{}
		}
		p.mu.Unlock()
	}
	return scanner.Err()
}

func (p *KubernetesEventProvider) watchEvents(ctx context.Context) error {
	rv, err := p.latestEventsResourceVersion(ctx)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/api/v1/namespaces/%s/events?watch=true", url.PathEscape(p.namespace))
	if rv != "" {
		path += "&resourceVersion=" + url.QueryEscape(rv)
	}
	body, err := p.doRequest(ctx, path)
	if err != nil {
		return err
	}
	defer body.Close()

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var line k8sWatchLine
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			continue
		}
		if strings.EqualFold(line.Type, "ERROR") {
			return fmt.Errorf("k8s events watch returned ERROR; relisting")
		}
		var ev k8sEvent
		if err := json.Unmarshal(line.Object, &ev); err != nil {
			continue
		}
		if rayEv := p.convertK8sEvent(&ev); rayEv != nil {
			p.callback(rayEv)
		}
	}
	return scanner.Err()
}

func (p *KubernetesEventProvider) isClusterObject(kind, name string) bool {
	if kind == "Pod" {
		p.mu.RLock()
		_, ok := p.clusterPods[name]
		p.mu.RUnlock()
		return ok
	}
	return kind == "RayCluster" && name == p.clusterName
}

func (p *KubernetesEventProvider) convertK8sEvent(ev *k8sEvent) *proto.RayEvent {
	if ev.Metadata.UID == "" {
		return nil
	}
	if !p.isClusterObject(ev.InvolvedObject.Kind, ev.InvolvedObject.Name) {
		return nil
	}

	severity := proto.RayEvent_INFO
	if strings.EqualFold(ev.Type, "Warning") {
		severity = proto.RayEvent_WARNING
	}

	// Prefer most recent occurrence, aligned with Python _extract_k8s_event_timestamp.
	ts := time.Now().UTC()
	for _, rawTS := range []string{ev.Series.LastObservedTime, ev.LastTimestamp, ev.EventTime, ev.FirstTimestamp} {
		if rawTS != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, rawTS); err == nil {
				ts = parsed
				break
			}
		}
	}

	component := ev.Source.Component
	if component == "" {
		component = ev.ReportingComponent
	}

	ns := ev.Metadata.Namespace
	if ns == "" {
		ns = p.namespace
	}

	return &proto.RayEvent{
		EventId:    []byte(ev.Metadata.UID),
		SourceType: proto.RayEvent_CLUSTER_LIFECYCLE,
		EventType:  proto.RayEvent_PLATFORM_EVENT,
		Timestamp:  timestamppb.New(ts),
		Severity:   severity,
		Message:    ev.Message,
		PlatformEvent: &proto.PlatformEvent{
			Source: &proto.Source{
				Platform:  proto.Source_KUBERNETES,
				Component: component,
				Metadata: map[string]string{
					"namespace":        ns,
					"ray_cluster_name": p.clusterName,
				},
			},
			ObjectKind: ev.InvolvedObject.Kind,
			ObjectName: ev.InvolvedObject.Name,
			Message:    ev.Message,
			Reason:     ev.Reason,
		},
	}
}
