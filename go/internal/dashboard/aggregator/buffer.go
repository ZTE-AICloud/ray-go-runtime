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
	"fmt"
	"sync"
	"time"

	"github.com/ray-project/ray/go/proto"
	gproto "google.golang.org/protobuf/proto"
)

type consumerState struct {
	cursorIndex  int
	evictedCount int64
}

// MultiConsumerEventBuffer is a bounded FIFO event buffer supporting multiple
// independent consumers, each tracking its own read cursor. Prefix events are
// evicted once every registered consumer has advanced past them.
type MultiConsumerEventBuffer struct {
	mu           sync.Mutex
	notifyCh     chan struct{}
	buffer       []*proto.RayEvent
	maxSize      int
	maxBatchSize int
	consumers    map[string]*consumerState
}

// NewMultiConsumerEventBuffer creates a MultiConsumerEventBuffer with the given
// maximum capacity and batch size.
func NewMultiConsumerEventBuffer(maxSize, maxBatchSize int) *MultiConsumerEventBuffer {
	if maxSize <= 0 {
		maxSize = 1
	}
	if maxBatchSize <= 0 {
		maxBatchSize = 1
	}
	return &MultiConsumerEventBuffer{
		notifyCh:     make(chan struct{}),
		maxSize:      maxSize,
		maxBatchSize: maxBatchSize,
		consumers:    make(map[string]*consumerState),
	}
}

func (b *MultiConsumerEventBuffer) signalLocked() {
	close(b.notifyCh)
	b.notifyCh = make(chan struct{})
}

// RegisterConsumer registers a named consumer starting at the current head of
// the buffer.
func (b *MultiConsumerEventBuffer) RegisterConsumer(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.consumers[name]; exists {
		return fmt.Errorf("consumer %q already registered", name)
	}
	b.consumers[name] = &consumerState{cursorIndex: 0}
	return nil
}

// AddEvent appends an event to the buffer. When the buffer is at maxSize, the
// oldest event is dropped and consumer cursors are adjusted accordingly.
func (b *MultiConsumerEventBuffer) AddEvent(event *proto.RayEvent) {
	if event == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	dropped := false
	if len(b.buffer) >= b.maxSize {
		b.buffer = b.buffer[1:]
		dropped = true
	}
	b.buffer = append(b.buffer, event)

	if dropped {
		for _, cs := range b.consumers {
			if cs.cursorIndex == 0 {
				cs.evictedCount++
			} else {
				cs.cursorIndex--
			}
		}
	}

	b.signalLocked()
}

// evictOldEventsLocked drops prefix events already consumed by every registered consumer.
func (b *MultiConsumerEventBuffer) evictOldEventsLocked() {
	if len(b.consumers) == 0 {
		return
	}
	minCursor := len(b.buffer)
	for _, cs := range b.consumers {
		if cs.cursorIndex < minCursor {
			minCursor = cs.cursorIndex
		}
	}
	if minCursor <= 0 {
		return
	}
	// Release references so GC can reclaim dropped proto pointers.
	for i := 0; i < minCursor; i++ {
		b.buffer[i] = nil
	}
	b.buffer = b.buffer[minCursor:]
	for _, cs := range b.consumers {
		cs.cursorIndex -= minCursor
	}
}

// WaitForBatch blocks until at least one event is available for consumerName
// (or ctx is cancelled), then collects up to maxBatchSize events within timeout.
func (b *MultiConsumerEventBuffer) WaitForBatch(ctx context.Context, consumerName string, timeout time.Duration) ([]*proto.RayEvent, error) {
	b.mu.Lock()
	cs, ok := b.consumers[consumerName]
	if !ok {
		b.mu.Unlock()
		return nil, fmt.Errorf("unknown consumer %q", consumerName)
	}

	// Phase 1: wait indefinitely for the first event.
	for cs.cursorIndex >= len(b.buffer) {
		ch := b.notifyCh
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
		b.mu.Lock()
		cs = b.consumers[consumerName]
		if cs == nil {
			b.mu.Unlock()
			return nil, fmt.Errorf("unknown consumer %q", consumerName)
		}
	}

	batch := make([]*proto.RayEvent, 0, min(b.maxBatchSize, len(b.buffer)-cs.cursorIndex))
	batch = append(batch, b.buffer[cs.cursorIndex])
	cs.cursorIndex++

	// Phase 2: drain additional events up to maxBatchSize or timeout.
	if len(batch) < b.maxBatchSize && timeout > 0 {
		deadline := time.Now().Add(timeout)
		for len(batch) < b.maxBatchSize {
			for len(batch) < b.maxBatchSize && cs.cursorIndex < len(b.buffer) {
				batch = append(batch, b.buffer[cs.cursorIndex])
				cs.cursorIndex++
			}
			if len(batch) >= b.maxBatchSize {
				break
			}
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			ch := b.notifyCh
			b.mu.Unlock()

			timer := time.NewTimer(remaining)
			select {
			case <-ctx.Done():
				timer.Stop()
				b.mu.Lock()
				b.evictOldEventsLocked()
				b.mu.Unlock()
				return batch, nil
			case <-ch:
				timer.Stop()
			case <-timer.C:
				b.mu.Lock()
				b.evictOldEventsLocked()
				b.mu.Unlock()
				return batch, nil
			}
			b.mu.Lock()
		}
	}

	b.evictOldEventsLocked()
	b.mu.Unlock()
	return batch, nil
}

// Size returns the current number of events retained in the buffer.
func (b *MultiConsumerEventBuffer) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.buffer)
}

// EvictedCount returns the total number of unconsumed events dropped for consumerName.
func (b *MultiConsumerEventBuffer) EvictedCount(consumerName string) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cs, ok := b.consumers[consumerName]; ok {
		return cs.evictedCount
	}
	return 0
}

// TaskEventsMetadataBuffer accumulates TaskEventsMetadata (specifically
// dropped_task_attempts) into bounded batches for a single publisher.
type TaskEventsMetadataBuffer struct {
	mu                 sync.Mutex
	bufferMaxLen       int
	maxDroppedAttempts int
	queue              []*proto.TaskEventsMetadata
	currentBatch       *proto.TaskEventsMetadata
	droppedCount       int64
}

// NewTaskEventsMetadataBuffer creates a bounded TaskEventsMetadataBuffer.
func NewTaskEventsMetadataBuffer(maxBufferSize, maxDroppedAttemptsPerEntry int) *TaskEventsMetadataBuffer {
	if maxBufferSize <= 0 {
		maxBufferSize = 1000
	}
	if maxDroppedAttemptsPerEntry <= 0 {
		maxDroppedAttemptsPerEntry = 100
	}
	maxLen := maxBufferSize - 1
	if maxLen < 1 {
		maxLen = 1
	}
	return &TaskEventsMetadataBuffer{
		bufferMaxLen:       maxLen,
		maxDroppedAttempts: maxDroppedAttemptsPerEntry,
		currentBatch:       &proto.TaskEventsMetadata{},
	}
}

// Merge appends dropped task attempts from newMeta into the buffer, rolling
// into queued batches when maxDroppedAttempts is reached and dropping the oldest
// batch if the queue is full.
func (b *TaskEventsMetadataBuffer) Merge(newMeta *proto.TaskEventsMetadata) {
	if newMeta == nil || len(newMeta.DroppedTaskAttempts) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, attempt := range newMeta.DroppedTaskAttempts {
		if len(b.currentBatch.DroppedTaskAttempts) >= b.maxDroppedAttempts {
			if len(b.queue) >= b.bufferMaxLen {
				oldest := b.queue[0]
				b.queue = b.queue[1:]
				b.droppedCount += int64(len(oldest.DroppedTaskAttempts))
			}
			b.queue = append(b.queue, gproto.Clone(b.currentBatch).(*proto.TaskEventsMetadata))
			b.currentBatch.Reset()
		}
		b.currentBatch.DroppedTaskAttempts = append(
			b.currentBatch.DroppedTaskAttempts,
			gproto.Clone(attempt).(*proto.TaskAttempt),
		)
	}
}

// Pop returns the oldest queued TaskEventsMetadata batch, or snapshots and resets
// the current in-progress batch if the queue is empty.
func (b *TaskEventsMetadataBuffer) Pop() *proto.TaskEventsMetadata {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.queue) == 0 {
		snap := gproto.Clone(b.currentBatch).(*proto.TaskEventsMetadata)
		b.currentBatch.Reset()
		return snap
	}
	head := b.queue[0]
	b.queue = b.queue[1:]
	return head
}

// DroppedCount returns the total number of dropped_task_attempts evicted due to queue overflow.
func (b *TaskEventsMetadataBuffer) DroppedCount() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.droppedCount
}
