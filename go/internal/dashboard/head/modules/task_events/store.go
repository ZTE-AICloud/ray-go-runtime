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
	"container/list"
	"strings"
	"sync"

	"github.com/ray-project/ray/go/proto"
	gproto "google.golang.org/protobuf/proto"
)

const defaultMaxNumTaskEvents = 100000

type taskAttemptKey struct {
	taskID  string // raw binary string
	attempt int32
}

type storedTaskEvent struct {
	key   taskAttemptKey
	jobID string
	event *proto.TaskEvents
	elem  *list.Element
}

// TaskEventStore is a bounded, thread-safe in-memory store for TaskEvents,
// keyed by (task_id, attempt_number) with secondary indexes by task_id and job_id.
type TaskEventStore struct {
	mu       sync.RWMutex
	maxItems int

	byAttempt map[taskAttemptKey]*storedTaskEvent
	byTask    map[string]map[int32]*storedTaskEvent
	byJob     map[string]map[taskAttemptKey]*storedTaskEvent
	lru       *list.List // oldest at front, newest at back

	droppedStatusCount int64
}

// NewTaskEventStore creates a TaskEventStore with the given capacity.
func NewTaskEventStore(maxItems int) *TaskEventStore {
	if maxItems <= 0 {
		maxItems = defaultMaxNumTaskEvents
	}
	return &TaskEventStore{
		maxItems:  maxItems,
		byAttempt: make(map[taskAttemptKey]*storedTaskEvent),
		byTask:    make(map[string]map[int32]*storedTaskEvent),
		byJob:     make(map[string]map[taskAttemptKey]*storedTaskEvent),
		lru:       list.New(),
	}
}

// Len returns the number of task attempts currently stored.
func (s *TaskEventStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byAttempt)
}

// RecordDroppedAttempts records worker-side dropped task attempts.
func (s *TaskEventStore) RecordDroppedAttempts(attempts []*proto.TaskAttempt) {
	if len(attempts) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.droppedStatusCount += int64(len(attempts))
}

// AddOrReplace merges incoming TaskEvents into the store, evicting the oldest
// entries when capacity is exceeded.
func (s *TaskEventStore) AddOrReplace(ev *proto.TaskEvents) {
	if ev == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	key := taskAttemptKey{taskID: string(ev.GetTaskId()), attempt: ev.GetAttemptNumber()}
	if existing, ok := s.byAttempt[key]; ok {
		mergeTaskEvents(existing.event, ev)
		s.lru.MoveToBack(existing.elem)
		return
	}

	for len(s.byAttempt) >= s.maxItems {
		s.evictOldestLocked()
	}

	cloned := gproto.Clone(ev).(*proto.TaskEvents)
	rec := &storedTaskEvent{
		key:   key,
		jobID: string(ev.GetJobId()),
		event: cloned,
	}
	rec.elem = s.lru.PushBack(rec)
	s.byAttempt[key] = rec

	if s.byTask[key.taskID] == nil {
		s.byTask[key.taskID] = make(map[int32]*storedTaskEvent)
	}
	s.byTask[key.taskID][key.attempt] = rec

	if rec.jobID != "" {
		if s.byJob[rec.jobID] == nil {
			s.byJob[rec.jobID] = make(map[taskAttemptKey]*storedTaskEvent)
		}
		s.byJob[rec.jobID][key] = rec
	}
}

func (s *TaskEventStore) evictOldestLocked() {
	front := s.lru.Front()
	if front == nil {
		return
	}
	rec := front.Value.(*storedTaskEvent)
	s.lru.Remove(front)
	delete(s.byAttempt, rec.key)
	if m := s.byTask[rec.key.taskID]; m != nil {
		delete(m, rec.key.attempt)
		if len(m) == 0 {
			delete(s.byTask, rec.key.taskID)
		}
	}
	if rec.jobID != "" {
		if m := s.byJob[rec.jobID]; m != nil {
			delete(m, rec.key)
			if len(m) == 0 {
				delete(s.byJob, rec.jobID)
			}
		}
	}
	s.droppedStatusCount++
}

func mergeTaskEvents(dst, src *proto.TaskEvents) {
	if len(dst.JobId) == 0 && len(src.JobId) > 0 {
		dst.JobId = append([]byte(nil), src.JobId...)
	}
	if src.TaskInfo != nil {
		if dst.TaskInfo == nil {
			dst.TaskInfo = gproto.Clone(src.TaskInfo).(*proto.TaskInfoEntry)
		} else {
			gproto.Merge(dst.TaskInfo, src.TaskInfo)
		}
	}
	if src.StateUpdates != nil {
		if dst.StateUpdates == nil {
			dst.StateUpdates = gproto.Clone(src.StateUpdates).(*proto.TaskStateUpdate)
		} else {
			mergeStateUpdates(dst.StateUpdates, src.StateUpdates)
		}
	}
	if src.ProfileEvents != nil {
		if dst.ProfileEvents == nil {
			dst.ProfileEvents = gproto.Clone(src.ProfileEvents).(*proto.ProfileEvents)
		} else {
			dst.ProfileEvents.Events = append(dst.ProfileEvents.Events, src.ProfileEvents.Events...)
		}
	}
}

func mergeStateUpdates(dst, src *proto.TaskStateUpdate) {
	if len(src.NodeId) > 0 {
		dst.NodeId = append([]byte(nil), src.NodeId...)
	}
	if len(src.WorkerId) > 0 {
		dst.WorkerId = append([]byte(nil), src.WorkerId...)
	}
	if src.WorkerPid != nil {
		v := *src.WorkerPid
		dst.WorkerPid = &v
	}
	if src.ErrorInfo != nil {
		dst.ErrorInfo = gproto.Clone(src.ErrorInfo).(*proto.RayErrorInfo)
	}
	if src.IsDebuggerPaused != nil {
		v := *src.IsDebuggerPaused
		dst.IsDebuggerPaused = &v
	}
	if src.ActorReprName != nil {
		v := *src.ActorReprName
		dst.ActorReprName = &v
	}
	if src.TaskLogInfo != nil {
		if dst.TaskLogInfo == nil {
			dst.TaskLogInfo = gproto.Clone(src.TaskLogInfo).(*proto.TaskLogInfo)
		} else {
			gproto.Merge(dst.TaskLogInfo, src.TaskLogInfo)
		}
	}
	if len(src.StateTsNs) > 0 {
		if dst.StateTsNs == nil {
			dst.StateTsNs = make(map[int32]int64, len(src.StateTsNs))
		}
		for k, v := range src.StateTsNs {
			dst.StateTsNs[k] = v
		}
	}
}

// Query answers a GetTaskEventsRequest against the in-memory store.
func (s *TaskEventStore) Query(req *proto.GetTaskEventsRequest) *proto.GetTaskEventsReply {
	s.mu.RLock()
	defer s.mu.RUnlock()

	candidates := s.selectCandidatesLocked(req.GetFilters())
	limit := int(req.GetLimit())
	matched := make([]*proto.TaskEvents, 0, len(candidates))
	var truncated int64

	for _, rec := range candidates {
		if !passesFilters(rec.event, req.GetFilters()) {
			continue
		}
		if limit > 0 && len(matched) >= limit {
			truncated++
			continue
		}
		matched = append(matched, gproto.Clone(rec.event).(*proto.TaskEvents))
	}

	return &proto.GetTaskEventsReply{
		Status:                     &proto.GcsStatus{Code: 0},
		EventsByTask:               matched,
		NumProfileTaskEventsDropped: 0,
		NumStatusTaskEventsDropped:  int32(s.droppedStatusCount),
		NumTotalStored:             int64(len(s.byAttempt)),
		NumTruncated:               truncated,
	}
}

func (s *TaskEventStore) selectCandidatesLocked(filters *proto.GetTaskEventsRequest_Filters) []*storedTaskEvent {
	if filters != nil {
		// Index lookup by task_id if there is a single EQUAL task filter.
		var eqTaskIDs []string
		for _, f := range filters.GetTaskFilters() {
			if f.GetPredicate() == proto.FilterPredicate_EQUAL {
				eqTaskIDs = append(eqTaskIDs, string(f.GetTaskId()))
			}
		}
		if len(eqTaskIDs) == 1 {
			m := s.byTask[eqTaskIDs[0]]
			out := make([]*storedTaskEvent, 0, len(m))
			for _, rec := range m {
				out = append(out, rec)
			}
			return out
		}

		// Index lookup by job_id if there is a single EQUAL job filter.
		var eqJobIDs []string
		for _, f := range filters.GetJobFilters() {
			if f.GetPredicate() == proto.FilterPredicate_EQUAL {
				eqJobIDs = append(eqJobIDs, string(f.GetJobId()))
			}
		}
		if len(eqJobIDs) == 1 {
			m := s.byJob[eqJobIDs[0]]
			out := make([]*storedTaskEvent, 0, len(m))
			for _, rec := range m {
				out = append(out, rec)
			}
			return out
		}
	}

	out := make([]*storedTaskEvent, 0, len(s.byAttempt))
	for _, rec := range s.byAttempt {
		out = append(out, rec)
	}
	return out
}

func applyPredicate[T comparable](pred proto.FilterPredicate, actual, expected T) bool {
	switch pred {
	case proto.FilterPredicate_EQUAL:
		return actual == expected
	case proto.FilterPredicate_NOT_EQUAL:
		return actual != expected
	default:
		return false
	}
}

func latestStateName(ev *proto.TaskEvents) string {
	latest := proto.TaskStatus_NIL
	for statusInt := range ev.GetStateUpdates().GetStateTsNs() {
		st := proto.TaskStatus(statusInt)
		if st > latest {
			latest = st
		}
	}
	return latest.String()
}

func passesFilters(ev *proto.TaskEvents, filters *proto.GetTaskEventsRequest_Filters) bool {
	if filters == nil {
		return ev.TaskInfo != nil
	}
	ti := ev.GetTaskInfo()
	if ti == nil {
		return false
	}
	if filters.GetExcludeDriver() && ti.GetType() == proto.TaskType_DRIVER_TASK {
		return false
	}
	for _, f := range filters.GetTaskFilters() {
		if !applyPredicate(f.GetPredicate(), string(ev.GetTaskId()), string(f.GetTaskId())) {
			return false
		}
	}
	for _, f := range filters.GetJobFilters() {
		if !applyPredicate(f.GetPredicate(), string(ti.GetJobId()), string(f.GetJobId())) {
			return false
		}
	}
	for _, f := range filters.GetActorFilters() {
		if !applyPredicate(f.GetPredicate(), string(ti.GetActorId()), string(f.GetActorId())) {
			return false
		}
	}
	for _, f := range filters.GetTaskNameFilters() {
		if !applyPredicate(f.GetPredicate(), strings.ToLower(ti.GetName()), strings.ToLower(f.GetTaskName())) {
			return false
		}
	}
	if len(filters.GetStateFilters()) > 0 {
		stateName := strings.ToLower(latestStateName(ev))
		for _, f := range filters.GetStateFilters() {
			if !applyPredicate(f.GetPredicate(), stateName, strings.ToLower(f.GetState())) {
				return false
			}
		}
	}
	return true
}
