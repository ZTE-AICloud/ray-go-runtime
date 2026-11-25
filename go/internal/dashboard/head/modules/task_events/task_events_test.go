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

package task_events

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/proto"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTaskEventsHeadIngestMergeAndQuery(t *testing.T) {
	mod := New(&head.HeadConfig{}, nil)
	mux := http.NewServeMux()
	if err := mod.RegisterHTTP(mux); err != nil {
		t.Fatalf("RegisterHTTP: %v", err)
	}

	taskID := []byte("0123456789abcdef01234567")
	jobID := []byte("0100")

	// 1. Ingest task definition + lifecycle events via POST /api/task_events.
	addReq := &proto.AddEventsRequest{
		EventsData: &proto.RayEventsData{
			Events: []*proto.RayEvent{
				{
					EventId:   []byte("e1"),
					EventType: proto.RayEvent_TASK_DEFINITION_EVENT,
					TaskDefinitionEvent: &proto.TaskDefinitionEvent{
						TaskId:      taskID,
						TaskAttempt: 0,
						JobId:       jobID,
						TaskType:    proto.TaskType_NORMAL_TASK,
						TaskName:    "my_task",
						Language:    proto.Language_PYTHON,
						TaskFunc: &proto.FunctionDescriptor{
							FunctionDescriptor: &proto.FunctionDescriptor_PythonFunctionDescriptor{
								PythonFunctionDescriptor: &proto.PythonFunctionDescriptor{
									ModuleName:   "mod",
									FunctionName: "mod.my_task",
								},
							},
						},
					},
				},
				{
					EventId:   []byte("e2"),
					EventType: proto.RayEvent_TASK_LIFECYCLE_EVENT,
					TaskLifecycleEvent: &proto.TaskLifecycleEvent{
						TaskId:      taskID,
						TaskAttempt: 0,
						JobId:       jobID,
						StateTransitions: []*proto.TaskLifecycleEvent_StateTransition{
							{
								State:     proto.TaskStatus_FINISHED,
								Timestamp: timestamppb.Now(),
							},
						},
					},
				},
			},
			TaskEventsMetadata: &proto.TaskEventsMetadata{
				DroppedTaskAttempts: []*proto.TaskAttempt{{TaskId: []byte("drop"), AttemptNumber: 1}},
			},
		},
	}
	rawAdd, err := gproto.Marshal(addReq)
	if err != nil {
		t.Fatalf("marshal AddEventsRequest: %v", err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/task_events", bytes.NewReader(rawAdd)))
	if rr.Code != http.StatusOK {
		t.Fatalf("POST /api/task_events = %d (%s), want 200", rr.Code, rr.Body.String())
	}
	if mod.NumStored() != 1 {
		t.Fatalf("NumStored() = %d, want 1", mod.NumStored())
	}

	// 2. Query via POST /api/task_events/query with state filter.
	queryReq := &proto.GetTaskEventsRequest{
		Filters: &proto.GetTaskEventsRequest_Filters{
			StateFilters: []*proto.GetTaskEventsRequest_Filters_StateFilter{
				{Predicate: proto.FilterPredicate_EQUAL, State: "FINISHED"},
			},
		},
	}
	rawQuery, err := gproto.Marshal(queryReq)
	if err != nil {
		t.Fatalf("marshal GetTaskEventsRequest: %v", err)
	}
	qrr := httptest.NewRecorder()
	mux.ServeHTTP(qrr, httptest.NewRequest(http.MethodPost, "/api/task_events/query", bytes.NewReader(rawQuery)))
	if qrr.Code != http.StatusOK {
		t.Fatalf("POST /api/task_events/query = %d, want 200", qrr.Code)
	}
	var reply proto.GetTaskEventsReply
	if err := gproto.Unmarshal(qrr.Body.Bytes(), &reply); err != nil {
		t.Fatalf("unmarshal GetTaskEventsReply: %v", err)
	}
	if len(reply.EventsByTask) != 1 || reply.EventsByTask[0].GetTaskInfo().GetName() != "my_task" {
		t.Fatalf("unexpected reply: %+v", reply.EventsByTask)
	}
	if reply.GetNumStatusTaskEventsDropped() != 1 {
		t.Fatalf("NumStatusTaskEventsDropped = %d, want 1", reply.GetNumStatusTaskEventsDropped())
	}

	// 3. Query method directly (used by StateAPIManager).
	directReply, err := mod.Query(context.Background(), &proto.GetTaskEventsRequest{})
	if err != nil || len(directReply.EventsByTask) != 1 {
		t.Fatalf("Query() = (%d, %v), want (1, nil)", len(directReply.EventsByTask), err)
	}
}
