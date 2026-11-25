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
	"testing"
	"time"

	"github.com/ray-project/ray/go/proto"
)

func makeTestEvent(id string) *proto.RayEvent {
	return &proto.RayEvent{
		EventId:   []byte(id),
		EventType: proto.RayEvent_TASK_DEFINITION_EVENT,
	}
}

func TestMultiConsumerEventBufferIndependentCursorsAndEviction(t *testing.T) {
	buf := NewMultiConsumerEventBuffer(10, 2)
	if err := buf.RegisterConsumer("c1"); err != nil {
		t.Fatalf("RegisterConsumer(c1): %v", err)
	}
	if err := buf.RegisterConsumer("c2"); err != nil {
		t.Fatalf("RegisterConsumer(c2): %v", err)
	}
	if err := buf.RegisterConsumer("c1"); err == nil {
		t.Fatal("duplicate RegisterConsumer(c1) = nil, want error")
	}

	buf.AddEvent(makeTestEvent("e1"))
	buf.AddEvent(makeTestEvent("e2"))
	buf.AddEvent(makeTestEvent("e3"))

	ctx := context.Background()
	b1, err := buf.WaitForBatch(ctx, "c1", 20*time.Millisecond)
	if err != nil || len(b1) != 2 {
		t.Fatalf("c1 WaitForBatch = (%d, %v), want (2, nil)", len(b1), err)
	}
	if string(b1[0].EventId) != "e1" || string(b1[1].EventId) != "e2" {
		t.Fatalf("c1 batch = %v, want [e1, e2]", b1)
	}
	// c2 has not consumed anything yet, so all 3 events remain retained.
	if got := buf.Size(); got != 3 {
		t.Fatalf("Size() = %d, want 3 while c2 lags", got)
	}

	b2, err := buf.WaitForBatch(ctx, "c2", 20*time.Millisecond)
	if err != nil || len(b2) != 2 {
		t.Fatalf("c2 WaitForBatch = (%d, %v), want (2, nil)", len(b2), err)
	}
	// Now both c1 and c2 have consumed e1 and e2 -> prefix of 2 evicted.
	if got := buf.Size(); got != 1 {
		t.Fatalf("Size() = %d, want 1 after both consumers advance", got)
	}
}

func TestMultiConsumerEventBufferOverflowTracksEvicted(t *testing.T) {
	buf := NewMultiConsumerEventBuffer(2, 10)
	if err := buf.RegisterConsumer("slow"); err != nil {
		t.Fatalf("RegisterConsumer: %v", err)
	}
	buf.AddEvent(makeTestEvent("e1"))
	buf.AddEvent(makeTestEvent("e2"))
	buf.AddEvent(makeTestEvent("e3")) // evicts e1 before slow reads it

	if got := buf.EvictedCount("slow"); got != 1 {
		t.Fatalf("EvictedCount(slow) = %d, want 1", got)
	}
	batch, err := buf.WaitForBatch(context.Background(), "slow", 10*time.Millisecond)
	if err != nil || len(batch) != 2 {
		t.Fatalf("WaitForBatch = (%d, %v), want (2, nil)", len(batch), err)
	}
	if string(batch[0].EventId) != "e2" || string(batch[1].EventId) != "e3" {
		t.Fatalf("got [%s, %s], want [e2, e3]", batch[0].EventId, batch[1].EventId)
	}
}

func TestMultiConsumerEventBufferWaitCancelled(t *testing.T) {
	buf := NewMultiConsumerEventBuffer(4, 2)
	_ = buf.RegisterConsumer("c1")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if _, err := buf.WaitForBatch(ctx, "c1", time.Second); err == nil {
		t.Fatal("WaitForBatch on empty buffer with cancelled ctx = nil, want error")
	}
}

func TestTaskEventsMetadataBufferBatchAndOverflow(t *testing.T) {
	// maxBufferSize=3 => queue maxLen=2 (+ 1 active currentBatch), maxDroppedAttempts=2.
	mb := NewTaskEventsMetadataBuffer(3, 2)
	mb.Merge(&proto.TaskEventsMetadata{
		DroppedTaskAttempts: []*proto.TaskAttempt{
			{TaskId: []byte("t1"), AttemptNumber: 1},
			{TaskId: []byte("t2"), AttemptNumber: 2},
			{TaskId: []byte("t3"), AttemptNumber: 3},
			{TaskId: []byte("t4"), AttemptNumber: 4},
			{TaskId: []byte("t5"), AttemptNumber: 5},
			{TaskId: []byte("t6"), AttemptNumber: 6},
			{TaskId: []byte("t7"), AttemptNumber: 7},
		},
	})
	// 2 attempts per batch -> [t1,t2] evicted (droppedCount=2), queue holds [t3,t4], [t5,t6], currentBatch holds [t7].
	if got := mb.DroppedCount(); got != 2 {
		t.Fatalf("DroppedCount() = %d, want 2", got)
	}
	p1 := mb.Pop()
	p2 := mb.Pop()
	p3 := mb.Pop()
	p4 := mb.Pop()
	if len(p1.DroppedTaskAttempts) != 2 || string(p1.DroppedTaskAttempts[0].TaskId) != "t3" {
		t.Fatalf("p1 = %v, want [t3, t4]", p1.DroppedTaskAttempts)
	}
	if len(p2.DroppedTaskAttempts) != 2 || string(p2.DroppedTaskAttempts[0].TaskId) != "t5" {
		t.Fatalf("p2 = %v, want [t5, t6]", p2.DroppedTaskAttempts)
	}
	if len(p3.DroppedTaskAttempts) != 1 || string(p3.DroppedTaskAttempts[0].TaskId) != "t7" {
		t.Fatalf("p3 = %v, want [t7]", p3.DroppedTaskAttempts)
	}
	if len(p4.DroppedTaskAttempts) != 0 {
		t.Fatalf("p4 = %v, want empty", p4.DroppedTaskAttempts)
	}
}
