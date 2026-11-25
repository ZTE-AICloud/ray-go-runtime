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
	"fmt"
	"strings"

	"github.com/ray-project/ray/go/proto"
	gproto "google.golang.org/protobuf/proto"
)

func shortName(name string) string {
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		return name[idx+1:]
	}
	return name
}

func callString(fd *proto.FunctionDescriptor) string {
	if fd == nil {
		return ""
	}
	switch d := fd.FunctionDescriptor.(type) {
	case *proto.FunctionDescriptor_PythonFunctionDescriptor:
		p := d.PythonFunctionDescriptor
		if p.GetClassName() == "" {
			return shortName(p.GetFunctionName())
		}
		return shortName(p.GetClassName()) + "." + shortName(p.GetFunctionName())
	case *proto.FunctionDescriptor_JavaFunctionDescriptor:
		j := d.JavaFunctionDescriptor
		if j.GetClassName() == "" {
			return j.GetFunctionName()
		}
		return j.GetClassName() + "." + j.GetFunctionName()
	case *proto.FunctionDescriptor_CppFunctionDescriptor:
		return d.CppFunctionDescriptor.GetFunctionName()
	default:
		return ""
	}
}

func populateTaskRuntimeAndFunctionInfo(
	taskInfo *proto.TaskInfoEntry,
	serializedRuntimeEnv string,
	fd *proto.FunctionDescriptor,
	requiredResources map[string]doubleVal,
	language proto.Language,
) {
	taskInfo.Language = language
	taskInfo.RuntimeEnvInfo = &proto.RuntimeEnvInfo{
		SerializedRuntimeEnv: serializedRuntimeEnv,
	}
	taskInfo.FuncOrClassName = callString(fd)
	if len(requiredResources) > 0 {
		taskInfo.RequiredResources = make(map[string]float64, len(requiredResources))
		for k, v := range requiredResources {
			taskInfo.RequiredResources[k] = float64(v)
		}
	}
}

type doubleVal = float64

func convertTaskDefinitionEvent(ev *proto.TaskDefinitionEvent) *proto.TaskEvents {
	te := &proto.TaskEvents{
		TaskId:        ev.GetTaskId(),
		AttemptNumber: ev.GetTaskAttempt(),
		JobId:         ev.GetJobId(),
		TaskInfo: &proto.TaskInfoEntry{
			Type:             ev.GetTaskType(),
			Name:             ev.GetTaskName(),
			TaskId:           ev.GetTaskId(),
			JobId:            ev.GetJobId(),
			ParentTaskId:     ev.GetParentTaskId(),
			PlacementGroupId: ev.GetPlacementGroupId(),
			IsDetachedActor:  ev.GetTaskType() == proto.TaskType_ACTOR_CREATION_TASK && ev.GetIsDetachedActor(),
		},
	}
	// TaskID is 24 bytes: first 8 bytes (or last 16 bytes? ActorID is bytes[len-16:] = 16 bytes).
	// In Ray TaskID(24 bytes), ActorID is the last 16 bytes when TaskType == ACTOR_CREATION_TASK.
	if ev.GetTaskType() == proto.TaskType_ACTOR_CREATION_TASK && len(ev.GetTaskId()) >= 16 {
		tid := ev.GetTaskId()
		te.TaskInfo.ActorId = append([]byte(nil), tid[len(tid)-16:]...)
	}
	if ev.CallSite != nil {
		cs := ev.GetCallSite()
		te.TaskInfo.CallSite = &cs
	}
	if len(ev.GetLabelSelector()) > 0 {
		te.TaskInfo.LabelSelector = make(map[string]string, len(ev.GetLabelSelector()))
		for k, v := range ev.GetLabelSelector() {
			te.TaskInfo.LabelSelector[k] = v
		}
	}
	if ev.FallbackStrategy != nil {
		te.TaskInfo.FallbackStrategy = gproto.Clone(ev.FallbackStrategy).(*proto.FallbackStrategy)
	}
	populateTaskRuntimeAndFunctionInfo(
		te.TaskInfo,
		ev.GetSerializedRuntimeEnv(),
		ev.GetTaskFunc(),
		ev.GetRequiredResources(),
		ev.GetLanguage(),
	)
	return te
}

func convertTaskLifecycleEvent(ev *proto.TaskLifecycleEvent) *proto.TaskEvents {
	su := &proto.TaskStateUpdate{
		NodeId:    ev.GetNodeId(),
		WorkerId:  ev.GetWorkerId(),
		StateTsNs: make(map[int32]int64, len(ev.GetStateTransitions())),
	}
	if ev.GetWorkerPid() != 0 {
		pid := ev.GetWorkerPid()
		su.WorkerPid = &pid
	}
	if ev.RayErrorInfo != nil {
		su.ErrorInfo = gproto.Clone(ev.RayErrorInfo).(*proto.RayErrorInfo)
	}
	if ev.IsDebuggerPaused != nil {
		v := ev.GetIsDebuggerPaused()
		su.IsDebuggerPaused = &v
	}
	if ev.ActorReprName != nil {
		v := ev.GetActorReprName()
		su.ActorReprName = &v
	}
	if ev.TaskLogInfo != nil {
		su.TaskLogInfo = gproto.Clone(ev.TaskLogInfo).(*proto.TaskLogInfo)
	}
	for _, tr := range ev.GetStateTransitions() {
		if tr.GetTimestamp() != nil {
			su.StateTsNs[int32(tr.GetState())] = tr.GetTimestamp().GetSeconds()*1e9 + int64(tr.GetTimestamp().GetNanos())
		} else {
			su.StateTsNs[int32(tr.GetState())] = 0
		}
	}
	return &proto.TaskEvents{
		TaskId:        ev.GetTaskId(),
		AttemptNumber: ev.GetTaskAttempt(),
		JobId:         ev.GetJobId(),
		StateUpdates:  su,
	}
}

func convertActorTaskDefinitionEvent(ev *proto.ActorTaskDefinitionEvent) *proto.TaskEvents {
	te := &proto.TaskEvents{
		TaskId:        ev.GetTaskId(),
		AttemptNumber: ev.GetTaskAttempt(),
		JobId:         ev.GetJobId(),
		TaskInfo: &proto.TaskInfoEntry{
			Type:             proto.TaskType_ACTOR_TASK,
			Name:             ev.GetActorTaskName(),
			TaskId:           ev.GetTaskId(),
			JobId:            ev.GetJobId(),
			ParentTaskId:     ev.GetParentTaskId(),
			PlacementGroupId: ev.GetPlacementGroupId(),
			ActorId:          ev.GetActorId(),
			IsDetachedActor:  ev.GetIsDetachedActor(),
		},
	}
	if ev.CallSite != nil {
		cs := ev.GetCallSite()
		te.TaskInfo.CallSite = &cs
	}
	if len(ev.GetLabelSelector()) > 0 {
		te.TaskInfo.LabelSelector = make(map[string]string, len(ev.GetLabelSelector()))
		for k, v := range ev.GetLabelSelector() {
			te.TaskInfo.LabelSelector[k] = v
		}
	}
	if ev.FallbackStrategy != nil {
		te.TaskInfo.FallbackStrategy = gproto.Clone(ev.FallbackStrategy).(*proto.FallbackStrategy)
	}
	populateTaskRuntimeAndFunctionInfo(
		te.TaskInfo,
		ev.GetSerializedRuntimeEnv(),
		ev.GetActorFunc(),
		ev.GetRequiredResources(),
		ev.GetLanguage(),
	)
	return te
}

func convertTaskProfileEvents(ev *proto.TaskProfileEvents) *proto.TaskEvents {
	te := &proto.TaskEvents{
		TaskId:        ev.GetTaskId(),
		AttemptNumber: ev.GetAttemptNumber(),
		JobId:         ev.GetJobId(),
	}
	if ev.ProfileEvents != nil {
		te.ProfileEvents = gproto.Clone(ev.ProfileEvents).(*proto.ProfileEvents)
	}
	return te
}

func convertToTaskEvents(req *proto.AddEventsRequest) ([]*proto.TaskEvents, []*proto.TaskAttempt, error) {
	data := req.GetEventsData()
	if data == nil {
		return nil, nil, nil
	}
	out := make([]*proto.TaskEvents, 0, len(data.GetEvents()))
	for _, ev := range data.GetEvents() {
		switch ev.GetEventType() {
		case proto.RayEvent_TASK_DEFINITION_EVENT:
			out = append(out, convertTaskDefinitionEvent(ev.GetTaskDefinitionEvent()))
		case proto.RayEvent_TASK_LIFECYCLE_EVENT:
			out = append(out, convertTaskLifecycleEvent(ev.GetTaskLifecycleEvent()))
		case proto.RayEvent_TASK_PROFILE_EVENT:
			out = append(out, convertTaskProfileEvents(ev.GetTaskProfileEvents()))
		case proto.RayEvent_ACTOR_TASK_DEFINITION_EVENT:
			out = append(out, convertActorTaskDefinitionEvent(ev.GetActorTaskDefinitionEvent()))
		default:
			return nil, nil, fmt.Errorf("unsupported event type for task events: %s", ev.GetEventType().String())
		}
	}
	return out, data.GetTaskEventsMetadata().GetDroppedTaskAttempts(), nil
}
