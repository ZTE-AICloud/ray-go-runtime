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

package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
	gproto "google.golang.org/protobuf/proto"
)

type fakeAddEventsGCS struct {
	kvAddr string
	kvErr  error
	gotReq *proto.AddEventsRequest
	reply  *proto.AddEventsReply
	rpcErr error
	kvGets atomic.Int32
}

func (f *fakeAddEventsGCS) Get(_ context.Context, namespace, key string) ([]byte, error) {
	f.kvGets.Add(1)
	if f.kvErr != nil {
		return nil, f.kvErr
	}
	if namespace == dashboardKVNamespace && key == common.DASHBOARD_ADDRESS {
		return []byte(f.kvAddr), nil
	}
	return nil, errors.New("not found")
}
func (f *fakeAddEventsGCS) Put(context.Context, string, string, []byte, bool) (bool, error) {
	return false, nil
}
func (f *fakeAddEventsGCS) Del(context.Context, string, string, bool) (int, error) {
	return 0, nil
}
func (f *fakeAddEventsGCS) Keys(context.Context, string, string) ([]string, error) {
	return nil, nil
}
func (f *fakeAddEventsGCS) GetAll(context.Context, []ids.NodeID) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	return nil, nil
}
func (f *fakeAddEventsGCS) CheckAlive(context.Context, []ids.NodeID) ([]bool, error) {
	return nil, nil
}
func (f *fakeAddEventsGCS) GetNodeToConnect(context.Context, string) (*proto.GcsNodeInfo, error) {
	return nil, nil
}
func (f *fakeAddEventsGCS) ListJobsWithFilter(context.Context, string, bool, bool) ([]*proto.JobTableData, error) {
	return nil, nil
}
func (f *fakeAddEventsGCS) NextJobID(context.Context) (ids.JobID, error) { return ids.NilJobID(), nil }
func (f *fakeAddEventsGCS) PublishNodeResourceUsage(context.Context, string, string) error {
	return nil
}
func (f *fakeAddEventsGCS) PublishErrorInfo(context.Context, string, gcs.ErrorInfoPayload) error {
	return nil
}
func (f *fakeAddEventsGCS) Address() string { return "" }
func (f *fakeAddEventsGCS) Close() error    { return nil }
func (f *fakeAddEventsGCS) AddEvents(_ context.Context, req *proto.AddEventsRequest) (*proto.AddEventsReply, error) {
	f.gotReq = req
	if f.rpcErr != nil {
		return nil, f.rpcErr
	}
	if f.reply != nil {
		return f.reply, nil
	}
	return &proto.AddEventsReply{Status: &proto.AddEventsStatus{Code: 0}}, nil
}

func TestHTTPPublisherClientFilterMarshalAndRedact(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	filter := NewEventFilterFromCSV("TASK_DEFINITION_EVENT")
	client := NewHTTPPublisherClient(srv.URL, srv.Client(), filter, true, true)

	evTask := &proto.RayEvent{
		EventId:   []byte("t1"),
		EventType: proto.RayEvent_TASK_DEFINITION_EVENT,
		TaskDefinitionEvent: &proto.TaskDefinitionEvent{
			SerializedRuntimeEnv: `{"env_vars":{"SECRET":"topsecret","TOKEN":"abc"}}`,
		},
	}
	evProfile := &proto.RayEvent{
		EventId:   []byte("p1"),
		EventType: proto.RayEvent_TASK_PROFILE_EVENT,
	}

	stats, err := client.Publish(context.Background(), PublishBatch{
		Events: []*proto.RayEvent{evTask, evProfile},
	})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if !stats.Success || stats.NumPublished != 1 || stats.NumFilteredOut != 1 {
		t.Fatalf("stats = %+v, want Success=true NumPublished=1 NumFilteredOut=1", stats)
	}

	var decoded []map[string]any
	if err := json.Unmarshal(gotBody, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("decoded len = %d, want 1", len(decoded))
	}
	taskDef := decoded[0]["task_definition_event"].(map[string]any)
	ser := taskDef["serialized_runtime_env"].(string)
	var envObj map[string]map[string]string
	if err := json.Unmarshal([]byte(ser), &envObj); err != nil {
		t.Fatalf("unmarshal serialized_runtime_env: %v", err)
	}
	if envObj["env_vars"]["SECRET"] != redactedPlaceholder || envObj["env_vars"]["TOKEN"] != redactedPlaceholder {
		t.Fatalf("env_vars not redacted: %+v", envObj["env_vars"])
	}
}

func TestGCSTaskEventsPublisherClient(t *testing.T) {
	gcsFake := &fakeAddEventsGCS{}
	client := NewGCSTaskEventsPublisherClient(gcsFake)

	stats, err := client.Publish(context.Background(), PublishBatch{
		Events: []*proto.RayEvent{
			{EventId: []byte("t1"), EventType: proto.RayEvent_TASK_DEFINITION_EVENT},
			{EventId: []byte("n1"), EventType: proto.RayEvent_NODE_DEFINITION_EVENT},
		},
		TaskEventsMetadata: &proto.TaskEventsMetadata{
			DroppedTaskAttempts: []*proto.TaskAttempt{{TaskId: []byte("drop-1"), AttemptNumber: 1}},
		},
	})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if !stats.Success || stats.NumPublished != 1 || stats.NumFilteredOut != 1 {
		t.Fatalf("stats = %+v, want Success=true NumPublished=1 NumFilteredOut=1", stats)
	}
	if len(gcsFake.gotReq.GetEventsData().GetEvents()) != 1 ||
		len(gcsFake.gotReq.GetEventsData().GetTaskEventsMetadata().GetDroppedTaskAttempts()) != 1 {
		t.Fatalf("unexpected AddEventsRequest: %+v", gcsFake.gotReq)
	}
}

func TestDashboardHeadPublisherClient(t *testing.T) {
	var gotPath string
	var gotContentType string
	var gotReq proto.AddEventsRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotContentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		if err := gproto.Unmarshal(body, &gotReq); err != nil {
			t.Fatalf("unmarshal protobuf request: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	gcsFake := &fakeAddEventsGCS{kvAddr: srv.URL}
	client := NewDashboardHeadPublisherClient(gcsFake, srv.Client())

	stats, err := client.Publish(context.Background(), PublishBatch{
		Events: []*proto.RayEvent{
			{EventId: []byte("t1"), EventType: proto.RayEvent_TASK_LIFECYCLE_EVENT},
			{EventId: []byte("a1"), EventType: proto.RayEvent_ACTOR_DEFINITION_EVENT},
		},
	})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if !stats.Success || stats.NumPublished != 1 || stats.NumFilteredOut != 1 {
		t.Fatalf("stats = %+v, want 1 published, 1 filtered", stats)
	}
	if gotPath != dashboardTaskEventsEP || gotContentType != "application/x-protobuf" {
		t.Fatalf("got path=%q content-type=%q", gotPath, gotContentType)
	}
	if len(gotReq.GetEventsData().GetEvents()) != 1 {
		t.Fatalf("got %d events, want 1", len(gotReq.GetEventsData().GetEvents()))
	}
}

// TestDashboardHeadPublisherClient_KeepsEndpointOnHTTPError pins the cache
// policy: a reachable head answering non-2xx must not invalidate the endpoint
// resolved from GCS KV, matching the Python async publisher which re-resolves
// only on connection errors and timeouts.
func TestDashboardHeadPublisherClient_KeepsEndpointOnHTTPError(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	gcsFake := &fakeAddEventsGCS{kvAddr: srv.URL}
	client := NewDashboardHeadPublisherClient(gcsFake, srv.Client())
	batch := PublishBatch{
		Events: []*proto.RayEvent{{EventId: []byte("t1"), EventType: proto.RayEvent_TASK_LIFECYCLE_EVENT}},
	}

	if _, err := client.Publish(context.Background(), batch); err == nil {
		t.Fatalf("first Publish should fail on the injected 500")
	}
	if _, err := client.Publish(context.Background(), batch); err != nil {
		t.Fatalf("second Publish: %v", err)
	}
	if got := gcsFake.kvGets.Load(); got != 1 {
		t.Fatalf("KV lookups = %d, want 1: a non-2xx response must not clear the cached endpoint", got)
	}
}

type flakyPublisherClient struct {
	attempts atomic.Int32
	failUpTo int32
}

func (c *flakyPublisherClient) Publish(_ context.Context, batch PublishBatch) (PublishStats, error) {
	n := c.attempts.Add(1)
	if n <= c.failUpTo {
		return PublishStats{}, errors.New("transient error")
	}
	return PublishStats{Success: true, NumPublished: len(batch.Events)}, nil
}

func TestPublisherRetriesAndSucceeds(t *testing.T) {
	buf := NewMultiConsumerEventBuffer(10, 10)
	_ = buf.RegisterConsumer("flaky")
	flaky := &flakyPublisherClient{failUpTo: 2}
	pub := NewPublisher("flaky", flaky, buf, nil, PublisherConfig{
		Timeout:               time.Second,
		MaxRetries:            3,
		InitialBackoff:        time.Millisecond,
		MaxBackoff:            5 * time.Millisecond,
		JitterRatio:           0.1,
		MaxBufferSendInterval: 5 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pub.Run(ctx)
	}()

	buf.AddEvent(makeTestEvent("e1"))
	deadline := time.Now().Add(time.Second)
	for flaky.attempts.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	if got := flaky.attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
}
