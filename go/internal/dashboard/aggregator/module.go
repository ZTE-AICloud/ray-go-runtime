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

// Package aggregator implements the dashboard agent's event aggregator module,
// receiving RayEventsData via gRPC (EventAggregatorService.AddEvents) and
// forwarding events/task metadata to configured external HTTP, GCS, and
// dashboard-head publishers.
package aggregator

import (
	"context"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
)

const (
	defaultMaxEventBufferSize    = 100000
	defaultMaxEventSendBatchSize = 1000

	envMaxEventBufferSize    = "RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_EVENT_BUFFER_SIZE"
	envMaxEventSendBatchSize = "RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_EVENT_SEND_BATCH_SIZE"
	envEventsExportAddr      = "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR"
	envPublishToExternalHTTP = "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SERVICE"
	envPublishToGCS          = "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_GCS"
	envPublishToDashHead     = "RAY_enable_task_events_to_dashboard_head"
	envPublishToDashHeadAlt  = "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_TASK_EVENTS_TO_DASHBOARD_HEAD"
	envExposableEventTypes   = "RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES"
	envPreserveProtoField    = "RAY_DASHBOARD_AGGREGATOR_AGENT_PRESERVE_PROTO_FIELD_NAME"
	envRedactRuntimeEnv      = "RAY_DASHBOARD_REDACT_RUNTIME_ENV"
)

// AggregatorAgent implements agent.Module and EventAggregatorServiceServer.
type AggregatorAgent struct {
	proto.UnimplementedEventAggregatorServiceServer

	eventBuf   *MultiConsumerEventBuffer
	publishers []*Publisher
	metaBufs   []*TaskEventsMetadataBuffer
	wg         sync.WaitGroup
}

// New constructs an AggregatorAgent configured from cfg and Ray environment variables.
func New(cfg agent.Config) (*AggregatorAgent, error) {
	maxBufSize := envInt(envMaxEventBufferSize, defaultMaxEventBufferSize)
	maxBatchSize := envInt(envMaxEventSendBatchSize, defaultMaxEventSendBatchSize)
	eventBuf := NewMultiConsumerEventBuffer(maxBufSize, maxBatchSize)
	pubCfg := PublisherConfigFromEnv()

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	m := &AggregatorAgent{
		eventBuf: eventBuf,
	}

	// 1. External HTTP service publisher.
	exportAddr := strings.TrimSpace(cfg.EventsExportAddr)
	if exportAddr == "" {
		exportAddr = strings.TrimSpace(os.Getenv(envEventsExportAddr))
	}
	if envBool(envPublishToExternalHTTP, true) && exportAddr != "" {
		exposableSpec := os.Getenv(envExposableEventTypes)
		if strings.TrimSpace(exposableSpec) == "" {
			exposableSpec = defaultHTTPExposableEventTypes
		}
		client := NewHTTPPublisherClient(
			exportAddr,
			httpClient,
			NewEventFilterFromCSV(exposableSpec),
			envBool(envPreserveProtoField, false),
			envBool(envRedactRuntimeEnv, false),
		)
		if err := m.addPublisher("external_http", client, nil, pubCfg); err != nil {
			return nil, err
		}
	}

	// 2. GCS TaskEvents publisher.
	if envBool(envPublishToGCS, false) && cfg.GCS != nil {
		if addEventsGCS, ok := cfg.GCS.(AddEventsClient); ok {
			client := NewGCSTaskEventsPublisherClient(addEventsGCS)
			metaBuf := NewTaskEventsMetadataBuffer(1000, 100)
			if err := m.addPublisher("gcs_task_events", client, metaBuf, pubCfg); err != nil {
				return nil, err
			}
		} else {
			log.Log.Info("aggregator GCS task-events publisher enabled but GCS client does not implement AddEvents")
		}
	}

	// 3. Dashboard Head TaskEvents publisher.
	publishDashHead := envBool(envPublishToDashHead, false) || envBool(envPublishToDashHeadAlt, false)
	if publishDashHead && cfg.GCS != nil {
		client := NewDashboardHeadPublisherClient(cfg.GCS, httpClient)
		metaBuf := NewTaskEventsMetadataBuffer(1000, 100)
		if err := m.addPublisher("dashboard_head", client, metaBuf, pubCfg); err != nil {
			return nil, err
		}
	}

	return m, nil
}

func (m *AggregatorAgent) addPublisher(
	name string,
	client PublisherClient,
	metaBuf *TaskEventsMetadataBuffer,
	cfg PublisherConfig,
) error {
	if err := m.eventBuf.RegisterConsumer(name); err != nil {
		return err
	}
	if metaBuf != nil {
		m.metaBufs = append(m.metaBufs, metaBuf)
	}
	m.publishers = append(m.publishers, NewPublisher(name, client, m.eventBuf, metaBuf, cfg))
	return nil
}

// Start launches background publisher loops and stops them when ctx is cancelled.
func (m *AggregatorAgent) Start(ctx context.Context) error {
	for _, pub := range m.publishers {
		p := pub
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			p.Run(ctx)
		}()
	}
	return nil
}

// Wait blocks until all publisher goroutines have exited.
func (m *AggregatorAgent) Wait() {
	m.wg.Wait()
}

// RegisterHTTP is a no-op for the aggregator module.
func (m *AggregatorAgent) RegisterHTTP(_ *http.ServeMux) error {
	return nil
}

// RegisterGRPC registers EventAggregatorServiceServer on s.
func (m *AggregatorAgent) RegisterGRPC(s *grpc.Server) error {
	proto.RegisterEventAggregatorServiceServer(s, m)
	return nil
}

// AddEvents receives RayEventsData from workers/raylet and fans it out to
// registered publisher buffers.
func (m *AggregatorAgent) AddEvents(_ context.Context, req *proto.AddEventsRequest) (*proto.AddEventsReply, error) {
	if len(m.publishers) == 0 {
		return &proto.AddEventsReply{Status: &proto.AddEventsStatus{Code: 0}}, nil
	}
	data := req.GetEventsData()
	if data == nil {
		return &proto.AddEventsReply{Status: &proto.AddEventsStatus{Code: 0}}, nil
	}
	if meta := data.GetTaskEventsMetadata(); meta != nil {
		for _, mb := range m.metaBufs {
			mb.Merge(meta)
		}
	}
	for _, ev := range data.GetEvents() {
		m.eventBuf.AddEvent(ev)
	}
	return &proto.AddEventsReply{Status: &proto.AddEventsStatus{Code: 0}}, nil
}
