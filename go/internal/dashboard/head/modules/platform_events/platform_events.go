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

// Package platform_events implements the PlatformEventsHead dashboard module,
// caching cluster infrastructure events (such as Kubernetes events) and serving
// them on GET /api/v0/platform_events.
package platform_events

import (
	"container/list"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

const maxEventsToCache = 1000

type cachedPlatformEvent struct {
	uid   string
	event *proto.RayEvent
	elem  *list.Element
}

// PlatformEventsHead implements head.HeadModule for platform infrastructure events.
type PlatformEventsHead struct {
	cfg    *head.HeadConfig
	client *head.GCSClient

	mu     sync.RWMutex
	events map[string]*cachedPlatformEvent
	order  *list.List
}

// New creates a PlatformEventsHead module.
func New(cfg *head.HeadConfig, client *head.GCSClient) *PlatformEventsHead {
	return &PlatformEventsHead{
		cfg:    cfg,
		client: client,
		events: make(map[string]*cachedPlatformEvent),
		order:  list.New(),
	}
}

// IsEnabled reports whether RAY_DASHBOARD_INGEST_PLATFORM_EVENTS is enabled.
func IsEnabled() bool {
	v := strings.TrimSpace(os.Getenv("RAY_DASHBOARD_INGEST_PLATFORM_EVENTS"))
	return strings.EqualFold(v, "true") || v == "1"
}

// Name returns the module name matching Python's PlatformEventsHead.
func (h *PlatformEventsHead) Name() string {
	return "PlatformEventsHead"
}

// Healthy reports whether the module is healthy.
func (h *PlatformEventsHead) Healthy() bool {
	return true
}

// RegisterHTTP mounts GET /api/v0/platform_events on mux.
func (h *PlatformEventsHead) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("GET /api/v0/platform_events", h.handleGetPlatformEvents)
	return nil
}

// Start runs the Kubernetes event watcher when running in-cluster.
func (h *PlatformEventsHead) Start(ctx context.Context) error {
	if os.Getenv("KUBERNETES_SERVICE_HOST") == "" {
		return nil
	}
	provider, err := NewInClusterKubernetesEventProvider(h.RecordEvent)
	if err != nil {
		log.Log.Info("platform_events k8s provider not started", "reason", err.Error())
		return nil
	}
	go provider.Run(ctx)
	return nil
}

// RecordEvent inserts or updates a RayEvent in the bounded cache.
func (h *PlatformEventsHead) RecordEvent(ev *proto.RayEvent) {
	if ev == nil {
		return
	}
	uid := string(ev.GetEventId())
	if uid == "" {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if existing, ok := h.events[uid]; ok {
		existing.event = ev
		h.order.MoveToBack(existing.elem)
		return
	}
	if len(h.events) >= maxEventsToCache {
		if front := h.order.Front(); front != nil {
			oldest := front.Value.(*cachedPlatformEvent)
			h.order.Remove(front)
			delete(h.events, oldest.uid)
		}
	}
	entry := &cachedPlatformEvent{uid: uid, event: ev}
	entry.elem = h.order.PushBack(entry)
	h.events[uid] = entry
}

func (h *PlatformEventsHead) handleGetPlatformEvents(w http.ResponseWriter, _ *http.Request) {
	h.mu.RLock()
	sorted := make([]*proto.RayEvent, 0, len(h.events))
	for _, entry := range h.events {
		sorted = append(sorted, entry.event)
	}
	h.mu.RUnlock()

	sort.Slice(sorted, func(i, j int) bool {
		ti := sorted[i].GetTimestamp()
		tj := sorted[j].GetTimestamp()
		if ti.GetSeconds() != tj.GetSeconds() {
			return ti.GetSeconds() > tj.GetSeconds()
		}
		return ti.GetNanos() > tj.GetNanos()
	})

	marshalOpts := protojson.MarshalOptions{EmitUnpopulated: true}
	encoded := make([]json.RawMessage, 0, len(sorted))
	for _, ev := range sorted {
		raw, err := marshalOpts.Marshal(ev)
		if err != nil {
			head.RESTResponse(w, http.StatusInternalServerError, err.Error(), nil)
			return
		}
		encoded = append(encoded, raw)
	}

	head.RESTResponse(w, http.StatusOK, "Retrieved platform events", map[string]any{
		"events": encoded,
	})
}
