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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/protobuf/encoding/protojson"
	gproto "google.golang.org/protobuf/proto"
)

const (
	defaultHTTPExposableEventTypes = "TASK_DEFINITION_EVENT,TASK_LIFECYCLE_EVENT,ACTOR_TASK_DEFINITION_EVENT," +
		"DRIVER_JOB_DEFINITION_EVENT,DRIVER_JOB_LIFECYCLE_EVENT," +
		"ACTOR_DEFINITION_EVENT,ACTOR_LIFECYCLE_EVENT," +
		"NODE_DEFINITION_EVENT,NODE_LIFECYCLE_EVENT," +
		"WORKER_DEFINITION_EVENT,WORKER_LIFECYCLE_EVENT," +
		"PLATFORM_EVENT"

	dashboardKVNamespace  = "dashboard"
	dashboardTaskEventsEP = "/api/task_events"
	redactedPlaceholder   = "<redacted>"
)

var defaultTaskEventTypes = map[proto.RayEvent_EventType]struct{}{
	proto.RayEvent_TASK_DEFINITION_EVENT:       {},
	proto.RayEvent_TASK_LIFECYCLE_EVENT:        {},
	proto.RayEvent_TASK_PROFILE_EVENT:          {},
	proto.RayEvent_ACTOR_TASK_DEFINITION_EVENT: {},
}

var eventTypesWithoutRuntimeEnv = map[proto.RayEvent_EventType]struct{}{
	proto.RayEvent_TASK_LIFECYCLE_EVENT:             {},
	proto.RayEvent_TASK_PROFILE_EVENT:               {},
	proto.RayEvent_DRIVER_JOB_LIFECYCLE_EVENT:       {},
	proto.RayEvent_NODE_DEFINITION_EVENT:            {},
	proto.RayEvent_NODE_LIFECYCLE_EVENT:             {},
	proto.RayEvent_ACTOR_LIFECYCLE_EVENT:            {},
	proto.RayEvent_PLACEMENT_GROUP_DEFINITION_EVENT: {},
	proto.RayEvent_PLACEMENT_GROUP_LIFECYCLE_EVENT:  {},
	proto.RayEvent_SUBMISSION_JOB_LIFECYCLE_EVENT:   {},
	proto.RayEvent_WORKER_DEFINITION_EVENT:          {},
	proto.RayEvent_WORKER_LIFECYCLE_EVENT:           {},
}

// AddEventsClient is the narrow GCS interface needed by GCSTaskEventsPublisherClient.
type AddEventsClient interface {
	AddEvents(ctx context.Context, req *proto.AddEventsRequest) (*proto.AddEventsReply, error)
}

// PublishBatch holds a batch of events and optional task metadata to publish.
type PublishBatch struct {
	Events             []*proto.RayEvent
	TaskEventsMetadata *proto.TaskEventsMetadata
}

// PublishStats summarizes a single publish attempt.
type PublishStats struct {
	Success          bool
	NumPublished     int
	NumFilteredOut   int
}

// PublisherClient delivers a PublishBatch to a destination.
type PublisherClient interface {
	Publish(ctx context.Context, batch PublishBatch) (PublishStats, error)
}

// PublisherConfig configures retry/backoff and batching for a Publisher.
type PublisherConfig struct {
	Timeout               time.Duration
	MaxRetries            int // < 0 retries indefinitely
	InitialBackoff        time.Duration
	MaxBackoff            time.Duration
	JitterRatio           float64
	MaxBufferSendInterval time.Duration
}

// PublisherConfigFromEnv loads publisher settings from Ray environment variables.
func PublisherConfigFromEnv() PublisherConfig {
	const prefix = "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER"
	return PublisherConfig{
		Timeout:               envDurationSeconds(prefix+"_TIMEOUT_SECONDS", 3*time.Second),
		MaxRetries:            envInt(prefix+"_MAX_RETRIES", -1),
		InitialBackoff:        envDurationSeconds(prefix+"_INITIAL_BACKOFF_SECONDS", 10*time.Millisecond),
		MaxBackoff:            envDurationSeconds(prefix+"_MAX_BACKOFF_SECONDS", 5*time.Second),
		JitterRatio:           envFloat(prefix+"_JITTER_RATIO", 0.1),
		MaxBufferSendInterval: envDurationSeconds(prefix+"_MAX_BUFFER_SEND_INTERVAL_SECONDS", 100*time.Millisecond),
	}
}

// EventFilter decides whether a RayEvent is exposable by a publisher.
type EventFilter struct {
	allowAll bool
	allowed  map[proto.RayEvent_EventType]struct{}
}

// NewEventFilterFromCSV parses a comma-separated EventType list or "ALL".
func NewEventFilterFromCSV(spec string) EventFilter {
	trimmed := strings.TrimSpace(spec)
	if strings.EqualFold(trimmed, "ALL") {
		return EventFilter{allowAll: true}
	}
	allowed := make(map[proto.RayEvent_EventType]struct{})
	for _, part := range strings.Split(trimmed, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		if v, ok := proto.RayEvent_EventType_value[name]; ok {
			allowed[proto.RayEvent_EventType(v)] = struct{}{}
		}
	}
	return EventFilter{allowed: allowed}
}

// NewTaskEventFilter returns the fixed filter for task-event destinations.
func NewTaskEventFilter() EventFilter {
	allowed := make(map[proto.RayEvent_EventType]struct{}, len(defaultTaskEventTypes))
	for k := range defaultTaskEventTypes {
		allowed[k] = struct{}{}
	}
	return EventFilter{allowed: allowed}
}

// Allows reports whether ev passes the filter.
func (f EventFilter) Allows(ev *proto.RayEvent) bool {
	if ev == nil {
		return false
	}
	if f.allowAll {
		return true
	}
	_, ok := f.allowed[ev.EventType]
	return ok
}

func (f EventFilter) filterEvents(events []*proto.RayEvent) (filtered []*proto.RayEvent, filteredOut int) {
	if len(events) == 0 {
		return nil, 0
	}
	filtered = make([]*proto.RayEvent, 0, len(events))
	for _, ev := range events {
		if f.Allows(ev) {
			filtered = append(filtered, ev)
		} else {
			filteredOut++
		}
	}
	return filtered, filteredOut
}

// Publisher consumes batches from MultiConsumerEventBuffer and publishes them
// with exponential backoff + jitter.
type Publisher struct {
	name    string
	client  PublisherClient
	buf     *MultiConsumerEventBuffer
	metaBuf *TaskEventsMetadataBuffer
	cfg     PublisherConfig
}

// NewPublisher creates a Publisher bound to consumer name in buf.
func NewPublisher(
	name string,
	client PublisherClient,
	buf *MultiConsumerEventBuffer,
	metaBuf *TaskEventsMetadataBuffer,
	cfg PublisherConfig,
) *Publisher {
	if cfg.MaxBufferSendInterval <= 0 {
		cfg.MaxBufferSendInterval = 100 * time.Millisecond
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 10 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 5 * time.Second
	}
	return &Publisher{
		name:    name,
		client:  client,
		buf:     buf,
		metaBuf: metaBuf,
		cfg:     cfg,
	}
}

// Run loops reading batches and publishing them until ctx is cancelled.
func (p *Publisher) Run(ctx context.Context) {
	for {
		events, err := p.buf.WaitForBatch(ctx, p.name, p.cfg.MaxBufferSendInterval)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Log.Error(err, "aggregator publisher failed waiting for batch", "publisher", p.name)
			return
		}
		var meta *proto.TaskEventsMetadata
		if p.metaBuf != nil {
			meta = p.metaBuf.Pop()
		}
		p.publishWithRetry(ctx, PublishBatch{
			Events:             events,
			TaskEventsMetadata: meta,
		})
	}
}

func (p *Publisher) publishWithRetry(ctx context.Context, batch PublishBatch) {
	backoff := p.cfg.InitialBackoff
	attempt := 0
	for {
		attempt++
		pubCtx := ctx
		var cancel context.CancelFunc
		if p.cfg.Timeout > 0 {
			pubCtx, cancel = context.WithTimeout(ctx, p.cfg.Timeout)
		}
		stats, err := p.client.Publish(pubCtx, batch)
		if cancel != nil {
			cancel()
		}
		if err == nil && stats.Success {
			return
		}
		if ctx.Err() != nil {
			return
		}
		if p.cfg.MaxRetries >= 0 && attempt > p.cfg.MaxRetries {
			log.Log.Error(err, "aggregator publisher exhausted retries; dropping batch",
				"publisher", p.name, "attempts", attempt, "events", len(batch.Events))
			return
		}

		delay := applyJitter(backoff, p.cfg.JitterRatio)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff *= 2
		if backoff > p.cfg.MaxBackoff {
			backoff = p.cfg.MaxBackoff
		}
	}
}

func applyJitter(d time.Duration, ratio float64) time.Duration {
	if ratio <= 0 {
		return d
	}
	// Uniformly scale within [1-ratio, 1+ratio].
	factor := (1 - ratio) + rand.Float64()*(2*ratio)
	out := time.Duration(float64(d) * factor)
	if out < 0 {
		return 0
	}
	return out
}

// HTTPPublisherClient publishes filtered RayEvents as a JSON array to an external HTTP endpoint.
type HTTPPublisherClient struct {
	endpoint         string
	httpClient       *http.Client
	filter           EventFilter
	marshalOpts      protojson.MarshalOptions
	redactRuntimeEnv bool
}

// NewHTTPPublisherClient creates an HTTPPublisherClient.
func NewHTTPPublisherClient(
	endpoint string,
	httpClient *http.Client,
	filter EventFilter,
	preserveProtoFieldName bool,
	redactRuntimeEnv bool,
) *HTTPPublisherClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &HTTPPublisherClient{
		endpoint:   endpoint,
		httpClient: httpClient,
		filter:     filter,
		marshalOpts: protojson.MarshalOptions{
			EmitUnpopulated: true,
			UseProtoNames:   preserveProtoFieldName,
		},
		redactRuntimeEnv: redactRuntimeEnv,
	}
}

func (c *HTTPPublisherClient) Publish(ctx context.Context, batch PublishBatch) (PublishStats, error) {
	if len(batch.Events) == 0 {
		return PublishStats{Success: true}, nil
	}
	filtered, filteredOut := c.filter.filterEvents(batch.Events)
	if len(filtered) == 0 {
		return PublishStats{Success: true, NumFilteredOut: filteredOut}, nil
	}

	encoded := make([]json.RawMessage, 0, len(filtered))
	for _, ev := range filtered {
		raw, err := c.eventToJSON(ev)
		if err != nil {
			return PublishStats{}, fmt.Errorf("marshal ray event: %w", err)
		}
		encoded = append(encoded, raw)
	}
	payload, err := json.Marshal(encoded)
	if err != nil {
		return PublishStats{}, fmt.Errorf("marshal events array: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return PublishStats{}, fmt.Errorf("build http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return PublishStats{}, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return PublishStats{}, fmt.Errorf("http publisher status %d", resp.StatusCode)
	}

	return PublishStats{
		Success:        true,
		NumPublished:   len(filtered),
		NumFilteredOut: filteredOut,
	}, nil
}

func (c *HTTPPublisherClient) eventToJSON(ev *proto.RayEvent) (json.RawMessage, error) {
	raw, err := c.marshalOpts.Marshal(ev)
	if err != nil {
		return nil, err
	}
	if !c.redactRuntimeEnv {
		return raw, nil
	}
	if _, skip := eventTypesWithoutRuntimeEnv[ev.EventType]; skip {
		return raw, nil
	}
	var obj any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	redactRuntimeEnvDeep(obj)
	return json.Marshal(obj)
}

// redactRuntimeEnvDeep masks env_vars values inside nested runtime_env maps and
// serialized_runtime_env / serializedRuntimeEnv JSON strings.
func redactRuntimeEnvDeep(v any) {
	switch val := v.(type) {
	case map[string]any:
		for k, child := range val {
			switch k {
			case "runtime_env", "runtimeEnv", "worker_runtime_env", "workerRuntimeEnv":
				if m, ok := child.(map[string]any); ok {
					redactEnvVarsMap(m)
				}
			case "serialized_runtime_env", "serializedRuntimeEnv":
				if s, ok := child.(string); ok && s != "" {
					val[k] = redactSerializedRuntimeEnvJSON(s)
				}
			default:
				redactRuntimeEnvDeep(child)
			}
		}
	case []any:
		for _, elem := range val {
			redactRuntimeEnvDeep(elem)
		}
	}
}

func redactEnvVarsMap(runtimeEnv map[string]any) {
	for _, key := range []string{"env_vars", "envVars"} {
		if rawVars, ok := runtimeEnv[key]; ok {
			if vars, ok := rawVars.(map[string]any); ok {
				for k := range vars {
					vars[k] = redactedPlaceholder
				}
			}
		}
	}
}

func redactSerializedRuntimeEnvJSON(raw string) string {
	var parsed map[string]any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return raw
	}
	redactEnvVarsMap(parsed)
	out, err := json.Marshal(parsed)
	if err != nil {
		return raw
	}
	return string(out)
}

// GCSTaskEventsPublisherClient sends task events and metadata to GCS TaskInfoGcsService.AddEvents.
type GCSTaskEventsPublisherClient struct {
	gcs    AddEventsClient
	filter EventFilter
}

// NewGCSTaskEventsPublisherClient creates a GCSTaskEventsPublisherClient.
func NewGCSTaskEventsPublisherClient(gcsClient AddEventsClient) *GCSTaskEventsPublisherClient {
	return &GCSTaskEventsPublisherClient{
		gcs:    gcsClient,
		filter: NewTaskEventFilter(),
	}
}

func (c *GCSTaskEventsPublisherClient) Publish(ctx context.Context, batch PublishBatch) (PublishStats, error) {
	hasDropped := len(batch.TaskEventsMetadata.GetDroppedTaskAttempts()) > 0
	if len(batch.Events) == 0 && !hasDropped {
		return PublishStats{Success: true}, nil
	}
	filtered, filteredOut := c.filter.filterEvents(batch.Events)
	if len(filtered) == 0 && !hasDropped {
		return PublishStats{Success: true, NumFilteredOut: filteredOut}, nil
	}

	req := &proto.AddEventsRequest{
		EventsData: &proto.RayEventsData{
			Events:             filtered,
			TaskEventsMetadata: batch.TaskEventsMetadata,
		},
	}
	reply, err := c.gcs.AddEvents(ctx, req)
	if err != nil {
		return PublishStats{}, err
	}
	if code := reply.GetStatus().GetCode(); code != 0 {
		return PublishStats{}, fmt.Errorf("gcs AddEvents failed: code=%d msg=%s", code, reply.GetStatus().GetMessage())
	}
	return PublishStats{
		Success:        true,
		NumPublished:   len(filtered),
		NumFilteredOut: filteredOut,
	}, nil
}

// DashboardHeadPublisherClient sends task events as protobuf AddEventsRequest to
// the dashboard head's /api/task_events endpoint, discovering the address from GCS KV.
type DashboardHeadPublisherClient struct {
	gcs        agent.GCSClient
	httpClient *http.Client
	filter     EventFilter

	mu       sync.Mutex
	cachedEP string
}

// NewDashboardHeadPublisherClient creates a DashboardHeadPublisherClient.
func NewDashboardHeadPublisherClient(gcsClient agent.GCSClient, httpClient *http.Client) *DashboardHeadPublisherClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &DashboardHeadPublisherClient{
		gcs:        gcsClient,
		httpClient: httpClient,
		filter:     NewTaskEventFilter(),
	}
}

func (c *DashboardHeadPublisherClient) resolveEndpoint(ctx context.Context) (string, error) {
	c.mu.Lock()
	ep := c.cachedEP
	c.mu.Unlock()
	if ep != "" {
		return ep, nil
	}

	rawAddr, err := c.gcs.Get(ctx, dashboardKVNamespace, common.DASHBOARD_ADDRESS)
	if err != nil {
		return "", fmt.Errorf("lookup dashboard address from gcs kv: %w", err)
	}
	addr := strings.TrimSpace(string(rawAddr))
	if addr == "" {
		return "", errors.New("empty dashboard address in gcs kv")
	}
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	addr = strings.TrimRight(addr, "/") + dashboardTaskEventsEP

	c.mu.Lock()
	c.cachedEP = addr
	c.mu.Unlock()
	return addr, nil
}

func (c *DashboardHeadPublisherClient) invalidateEndpoint() {
	c.mu.Lock()
	c.cachedEP = ""
	c.mu.Unlock()
}

func (c *DashboardHeadPublisherClient) Publish(ctx context.Context, batch PublishBatch) (PublishStats, error) {
	hasDropped := len(batch.TaskEventsMetadata.GetDroppedTaskAttempts()) > 0
	if len(batch.Events) == 0 && !hasDropped {
		return PublishStats{Success: true}, nil
	}
	filtered, filteredOut := c.filter.filterEvents(batch.Events)
	if len(filtered) == 0 && !hasDropped {
		return PublishStats{Success: true, NumFilteredOut: filteredOut}, nil
	}

	endpoint, err := c.resolveEndpoint(ctx)
	if err != nil {
		return PublishStats{}, err
	}

	reqMsg := &proto.AddEventsRequest{
		EventsData: &proto.RayEventsData{
			Events:             filtered,
			TaskEventsMetadata: batch.TaskEventsMetadata,
		},
	}
	body, err := gproto.Marshal(reqMsg)
	if err != nil {
		return PublishStats{}, fmt.Errorf("marshal AddEventsRequest: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return PublishStats{}, fmt.Errorf("build dashboard head request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.invalidateEndpoint()
		return PublishStats{}, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		c.invalidateEndpoint()
		return PublishStats{}, fmt.Errorf("dashboard head status %d", resp.StatusCode)
	}

	return PublishStats{
		Success:        true,
		NumPublished:   len(filtered),
		NumFilteredOut: filteredOut,
	}, nil
}

func envInt(name string, def int) int {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return def
}

func envFloat(name string, def float64) float64 {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			return parsed
		}
	}
	return def
}

func envDurationSeconds(name string, def time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if parsed, err := strconv.ParseFloat(v, 64); err == nil && parsed >= 0 {
			return time.Duration(parsed * float64(time.Second))
		}
	}
	return def
}

func envBool(name string, def bool) bool {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		switch strings.ToLower(v) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return def
}
