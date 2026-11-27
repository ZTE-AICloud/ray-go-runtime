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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	gproto "google.golang.org/protobuf/proto"
)

func TestAggregatorAgentEndToEndAllPublishers(t *testing.T) {
	var mu sync.Mutex
	var gotHTTP [][]map[string]any
	var gotDashHead []*proto.AddEventsRequest

	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var decoded []map[string]any
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Errorf("unmarshal http payload: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		gotHTTP = append(gotHTTP, decoded)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer httpSrv.Close()

	dashSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req proto.AddEventsRequest
		if err := gproto.Unmarshal(body, &req); err != nil {
			t.Errorf("unmarshal dash head payload: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		gotDashHead = append(gotDashHead, &req)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer dashSrv.Close()

	t.Setenv(envPublishToExternalHTTP, "true")
	t.Setenv(envPublishToGCS, "true")
	t.Setenv(envPublishToDashHead, "true")
	t.Setenv("RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER_MAX_BUFFER_SEND_INTERVAL_SECONDS", "0.01")

	gcsFake := &fakeAddEventsGCS{kvAddr: dashSrv.URL}
	mod, err := New(agent.Config{
		GCS:              gcsFake,
		EventsExportAddr: httpSrv.URL,
		HTTPClient:       httpSrv.Client(),
	})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	if err := mod.RegisterGRPC(grpcSrv); err != nil {
		t.Fatalf("RegisterGRPC: %v", err)
	}
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cc, err := grpc.DialContext(ctx, lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cc.Close()

	client := proto.NewEventAggregatorServiceClient(cc)
	reply, err := client.AddEvents(ctx, &proto.AddEventsRequest{
		EventsData: &proto.RayEventsData{
			Events: []*proto.RayEvent{
				{EventId: []byte("task-1"), EventType: proto.RayEvent_TASK_DEFINITION_EVENT},
				{EventId: []byte("node-1"), EventType: proto.RayEvent_NODE_DEFINITION_EVENT},
			},
			TaskEventsMetadata: &proto.TaskEventsMetadata{
				DroppedTaskAttempts: []*proto.TaskAttempt{{TaskId: []byte("drop-1"), AttemptNumber: 2}},
			},
		},
	})
	if err != nil {
		t.Fatalf("AddEvents RPC: %v", err)
	}
	if reply.GetStatus().GetCode() != 0 {
		t.Fatalf("AddEvents code = %d, want 0", reply.GetStatus().GetCode())
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		httpCount := len(gotHTTP)
		dashCount := len(gotDashHead)
		mu.Unlock()
		if httpCount > 0 && dashCount > 0 && gcsFake.gotReq != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	mod.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(gotHTTP) == 0 || len(gotHTTP[0]) != 2 {
		t.Fatalf("external HTTP got %v, want 2 events", gotHTTP)
	}
	if len(gotDashHead) == 0 || len(gotDashHead[0].GetEventsData().GetEvents()) != 1 {
		t.Fatalf("dashboard head got %v, want 1 task event", gotDashHead)
	}
	if gcsFake.gotReq == nil || len(gcsFake.gotReq.GetEventsData().GetEvents()) != 1 {
		t.Fatalf("GCS got %v, want 1 task event", gcsFake.gotReq)
	}
}
