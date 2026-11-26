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

// Package task_events implements the TaskEventsHead dashboard module, which
// receives Ray task events pushed by node-level aggregators on POST /api/task_events
// and answers State API queries on POST /api/task_events/query.
package task_events

import (
	"context"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
	gproto "google.golang.org/protobuf/proto"
)

// TaskEventsHead implements head.HeadModule for in-head task event storage and querying.
type TaskEventsHead struct {
	cfg    *head.HeadConfig
	client *head.GCSClient
	store  *TaskEventStore
}

// New creates a TaskEventsHead module.
func New(cfg *head.HeadConfig, client *head.GCSClient) *TaskEventsHead {
	maxItems := defaultMaxNumTaskEvents
	if v := strings.TrimSpace(os.Getenv("RAY_task_events_max_num_task_in_gcs")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxItems = n
		}
	}
	return &TaskEventsHead{
		cfg:    cfg,
		client: client,
		store:  NewTaskEventStore(maxItems),
	}
}

// IsEnabled reports whether task-events-to-dashboard-head mode is enabled.
func IsEnabled() bool {
	v := strings.TrimSpace(os.Getenv("RAY_enable_task_events_to_dashboard_head"))
	return strings.EqualFold(v, "true") || v == "1"
}

// Name returns the module name matching Python's TaskEventsHead.
func (h *TaskEventsHead) Name() string {
	return "TaskEventsHead"
}

// RegisterHTTP mounts /api/task_events and /api/task_events/query on mux.
func (h *TaskEventsHead) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("POST /api/task_events", h.handleAddTaskEvents)
	mux.HandleFunc("POST /api/task_events/query", h.handleQueryTaskEvents)
	return nil
}

// Start runs the module lifecycle until ctx is cancelled.
func (h *TaskEventsHead) Start(ctx context.Context) error {
	go func() {
		<-ctx.Done()
	}()
	return nil
}

// Healthy reports whether the module is healthy.
func (h *TaskEventsHead) Healthy() bool {
	return true
}

// Query answers a GetTaskEventsRequest directly from the in-memory store.
func (h *TaskEventsHead) Query(_ context.Context, req *proto.GetTaskEventsRequest) (*proto.GetTaskEventsReply, error) {
	return h.store.Query(req), nil
}

// NumStored returns the number of task attempts currently stored.
func (h *TaskEventsHead) NumStored() int {
	return h.store.Len()
}

func (h *TaskEventsHead) handleAddTaskEvents(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	var req proto.AddEventsRequest
	if err := gproto.Unmarshal(body, &req); err != nil {
		log.Log.Error(err, "failed to deserialize task events request")
		head.RESTResponse(w, http.StatusBadRequest, "Failed to deserialize task events request: "+err.Error(), nil)
		return
	}
	taskEvents, dropped, err := convertToTaskEvents(&req)
	if err != nil {
		head.RESTResponse(w, http.StatusBadRequest, err.Error(), nil)
		return
	}
	h.store.RecordDroppedAttempts(dropped)
	for _, te := range taskEvents {
		h.store.AddOrReplace(te)
	}
	head.RESTResponse(w, http.StatusOK, "", nil)
}

func (h *TaskEventsHead) handleQueryTaskEvents(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read query body", http.StatusBadRequest)
		return
	}
	var req proto.GetTaskEventsRequest
	if err := gproto.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reply := h.store.Query(&req)
	raw, err := gproto.Marshal(reply)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}
