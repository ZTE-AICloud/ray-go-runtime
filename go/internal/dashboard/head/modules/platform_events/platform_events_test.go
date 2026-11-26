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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPlatformEventsHeadCacheAndRoute(t *testing.T) {
	mod := New(&head.HeadConfig{}, nil)
	mux := http.NewServeMux()
	if err := mod.RegisterHTTP(mux); err != nil {
		t.Fatalf("RegisterHTTP: %v", err)
	}

	t1 := time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)
	t2 := time.Date(2026, 1, 1, 0, 0, 2, 0, time.UTC)
	mod.RecordEvent(&proto.RayEvent{
		EventId:   []byte("uid-1"),
		EventType: proto.RayEvent_PLATFORM_EVENT,
		Timestamp: timestamppb.New(t1),
		Message:   "older",
	})
	mod.RecordEvent(&proto.RayEvent{
		EventId:   []byte("uid-2"),
		EventType: proto.RayEvent_PLATFORM_EVENT,
		Timestamp: timestamppb.New(t2),
		Message:   "newer",
	})

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v0/platform_events", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /api/v0/platform_events = %d, want 200", rr.Code)
	}

	var resp struct {
		Result bool `json:"result"`
		Data   struct {
			Events []map[string]any `json:"events"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !resp.Result || len(resp.Data.Events) != 2 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Data.Events[0]["message"] != "newer" || resp.Data.Events[1]["message"] != "older" {
		t.Fatalf("expected descending timestamp order, got %+v", resp.Data.Events)
	}
}

func TestKubernetesEventProviderWatchPodsAndEvents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/namespaces/test-ns/pods":
			_, _ = w.Write([]byte(`{"type":"ADDED","object":{"metadata":{"name":"ray-head-1","uid":"p1"}}}` + "\n"))
		case "/api/v1/namespaces/test-ns/events":
			if r.URL.Query().Get("watch") == "" {
				_, _ = w.Write([]byte(`{"metadata":{"resourceVersion":"10"}}`))
				return
			}
			_, _ = w.Write([]byte(`{"type":"ADDED","object":{"metadata":{"uid":"ev-1","name":"ev-1"},"type":"Warning","reason":"BackOff","message":"restarting","involvedObject":{"kind":"Pod","name":"ray-head-1"}}}` + "\n"))
			_, _ = w.Write([]byte(`{"type":"ADDED","object":{"metadata":{"uid":"ev-other","name":"ev-other"},"type":"Normal","reason":"Pulled","message":"ignored","involvedObject":{"kind":"Pod","name":"unrelated-pod"}}}` + "\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	var got []*proto.RayEvent
	p := &KubernetesEventProvider{
		baseURL:     srv.URL,
		httpClient:  srv.Client(),
		namespace:   "test-ns",
		clusterName: "ray-cluster",
		clusterPods: make(map[string]struct{}),
		callback: func(ev *proto.RayEvent) {
			got = append(got, ev)
		},
	}

	if err := p.watchPods(context.Background()); err != nil {
		t.Fatalf("watchPods: %v", err)
	}
	if err := p.watchEvents(context.Background()); err != nil {
		t.Fatalf("watchEvents: %v", err)
	}
	if len(got) != 1 || string(got[0].EventId) != "ev-1" || got[0].Severity != proto.RayEvent_WARNING {
		t.Fatalf("got events = %+v, want 1 Warning event ev-1", got)
	}
}
