// Copyright 2026 The Ray Authors.
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

package job

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRayException implements object.RayException so the supervisor error
// classification can be exercised without a real core worker.
type fakeRayException struct {
	msg  string
	code int
}

func (f *fakeRayException) Error() string   { return f.msg }
func (f *fakeRayException) ToBytes() []byte { return nil }
func (f *fakeRayException) ErrorCode() int  { return f.code }

func TestClassifySupervisorError(t *testing.T) {
	// RPC exception codes
	msg, et := classifySupervisorError(&fakeRayException{msg: "x", code: rpcErrorTypeRuntimeEnvSetupFailed})
	assert.Equal(t, JobErrorTypeRuntimeEnvSetupFailure, et)
	assert.Contains(t, *msg, "runtime_env setup failed")

	msg, et = classifySupervisorError(&fakeRayException{msg: "x", code: rpcErrorTypeActorUnschedulable})
	assert.Equal(t, JobErrorTypeJobSupervisorActorUnschedulable, et)

	msg, et = classifySupervisorError(&fakeRayException{msg: "x", code: object.RpcErrorTypeActorDied})
	assert.Equal(t, JobErrorTypeJobSupervisorActorDied, et)

	msg, et = classifySupervisorError(&fakeRayException{msg: "x", code: object.ErrorCodeActorDied})
	assert.Equal(t, JobErrorTypeJobSupervisorActorDied, et)

	// typed errors
	msg, et = classifySupervisorError(newActorError("runtime_env_setup", errors.New("boom")))
	assert.Equal(t, JobErrorTypeRuntimeEnvSetupFailure, et)

	msg, et = classifySupervisorError(newActorError("unschedulable", errors.New("boom")))
	assert.Equal(t, JobErrorTypeJobSupervisorActorUnschedulable, et)

	msg, et = classifySupervisorError(newActorError("died", errors.New("boom")))
	assert.Equal(t, JobErrorTypeJobSupervisorActorDied, et)

	// string matching fallbacks
	msg, et = classifySupervisorError(errors.New("ActorUnschedulableError occurred"))
	assert.Equal(t, JobErrorTypeJobSupervisorActorUnschedulable, et)
	assert.Contains(t, *msg, "could not be scheduled")

	msg, et = classifySupervisorError(errors.New("RuntimeEnvSetupError"))
	assert.Equal(t, JobErrorTypeRuntimeEnvSetupFailure, et)

	msg, et = classifySupervisorError(errors.New("actor died"))
	assert.Equal(t, JobErrorTypeJobSupervisorActorDied, et)

	// unknown -> ActorUnknownFailure
	msg, et = classifySupervisorError(errors.New("something else"))
	assert.Equal(t, JobErrorTypeJobSupervisorActorUnknownFailure, et)
	assert.Contains(t, *msg, "Unexpected error occurred")
}

func TestContainsAny(t *testing.T) {
	assert.True(t, containsAny("hello world", "world"))
	assert.True(t, containsAny("abc", "a", "z"))
	assert.False(t, containsAny("abc", "x", "y"))
	assert.False(t, containsAny("", "a"))
}

func TestIsActorNotFoundError(t *testing.T) {
	assert.True(t, isActorNotFoundError(errors.New("actor not found")))
	assert.False(t, isActorNotFoundError(errors.New("other error")))
	assert.False(t, isActorNotFoundError(nil))
}

func TestSupervisorMetadata(t *testing.T) {
	m := map[string]string{"k": "v"}
	assert.Equal(t, m, supervisorMetadata(m))
	assert.Equal(t, map[string]string{}, supervisorMetadata(nil))
}

func TestJoinLines(t *testing.T) {
	assert.Equal(t, "ab\ncd\n", joinLines([]string{"ab\n", "cd\n"}))
	assert.Equal(t, "", joinLines(nil))
}

func TestJobStartTimeoutSeconds(t *testing.T) {
	t.Setenv(rayJobStartTimeoutSecondsEnvVar, "")
	assert.Equal(t, float64(defaultJobStartTimeoutSeconds), jobStartTimeoutSeconds())

	t.Setenv(rayJobStartTimeoutSecondsEnvVar, "30")
	assert.Equal(t, 30.0, jobStartTimeoutSeconds())

	t.Setenv(rayJobStartTimeoutSecondsEnvVar, "not-a-number")
	assert.Equal(t, float64(defaultJobStartTimeoutSeconds), jobStartTimeoutSeconds())
}

func TestSupervisorCodeSearchPath(t *testing.T) {
	paths := supervisorCodeSearchPath()
	require.NotEmpty(t, paths)
	assert.NotEmpty(t, paths[0])
}

func TestApplySchedulingStrategyNoHeadNode(t *testing.T) {
	// With a fake client that has no head node id, and resources specified,
	// the strategy must be nil (default scheduling).
	jm := &JobManager{jobInfoClient: NewJobInfoStorageClient(newFakeGCSClient()), ctx: t.Context()}
	opts := jm.applySchedulingStrategy("sub1", true)
	assert.Nil(t, opts)
}

func TestApplySchedulingStrategyPinsToHeadNode(t *testing.T) {
	ctx := t.Context()
	client := newFakeGCSClient()
	_, err := client.Put(ctx, kvNamespaceJob, kvHeadNodeIDKey, []byte("head-node-id"), false)
	require.NoError(t, err)
	jm := &JobManager{jobInfoClient: NewJobInfoStorageClient(client), ctx: ctx}

	opts := jm.applySchedulingStrategy("sub1", false)
	require.NotNil(t, opts)
	assert.Equal(t, "head-node-id", opts.NodeID)
	assert.False(t, opts.Soft, "head-node pinning must be strict")
}

func TestApplySchedulingStrategyAllowDriverOnWorkers(t *testing.T) {
	t.Setenv(rayJobAllowDriverOnWorkerNodesEnvVar, "1")
	ctx := t.Context()
	client := newFakeGCSClient()
	_, err := client.Put(ctx, kvNamespaceJob, kvHeadNodeIDKey, []byte("head-node-id"), false)
	require.NoError(t, err)
	jm := &JobManager{jobInfoClient: NewJobInfoStorageClient(client), ctx: ctx}

	assert.Nil(t, jm.applySchedulingStrategy("sub1", false))
}

func TestAppendDriverLog(t *testing.T) {
	jm := &JobManager{ctx: t.Context(), logClient: NewJobLogStorageClient(t.TempDir())}
	jm.appendDriverLog("raysubmit_1", "Runtime env is setting up.")
	jm.appendDriverLog("raysubmit_1", "Job failed.")
	assert.Equal(t, "Runtime env is setting up.\nJob failed.\n", jm.getJobLogs("raysubmit_1"))
}

// TestPutStatusAfterTerminal proves a terminal status is not overwritten.
func TestPutStatusAfterTerminal(t *testing.T) {
	ctx := t.Context()
	jm := &JobManager{jobInfoClient: NewJobInfoStorageClient(newFakeGCSClient()), ctx: ctx}
	_, err := jm.jobInfoClient.PutInfo(ctx, "job1", &JobInfo{Entrypoint: "python -c pass", Status: JobStatusPending}, false)
	require.NoError(t, err)

	require.NoError(t, jm.putStatus(ctx, "job1", JobStatusSucceeded, nil, nil, nil))
	err = jm.putStatus(ctx, "job1", JobStatusFailed, nil, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "terminal")

	status, err := jm.getJobStatus(ctx, "job1")
	require.NoError(t, err)
	require.NotNil(t, status)
	assert.Equal(t, JobStatusSucceeded, *status)
}

// overlapTrackingGCSClient wraps the in-memory fake and detects overlapping
// internal-KV reads, which is what a non-serialized read-modify-write looks
// like from the storage client's point of view.
type overlapTrackingGCSClient struct {
	*fakeGCSClient

	mu       sync.Mutex
	inFlight int
	overlaps int
	delay    time.Duration
}

func (c *overlapTrackingGCSClient) Get(ctx context.Context, ns, key string) ([]byte, error) {
	c.mu.Lock()
	c.inFlight++
	if c.inFlight > 1 {
		c.overlaps++
	}
	c.mu.Unlock()
	time.Sleep(c.delay)
	defer func() {
		c.mu.Lock()
		c.inFlight--
		c.mu.Unlock()
	}()
	return c.fakeGCSClient.Get(ctx, ns, key)
}

func (c *overlapTrackingGCSClient) overlapCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.overlaps
}

// TestPutStatusConcurrentTransitionsDoNotResurrect pins the JobManager
// serialization of status writes: two concurrent transitions must not both
// read the pending status and then write, which would let a FAILED write
// overwrite a terminal SUCCEEDED status (the storage client has no CAS).
func TestPutStatusConcurrentTransitionsDoNotResurrect(t *testing.T) {
	ctx := t.Context()
	client := &overlapTrackingGCSClient{fakeGCSClient: newFakeGCSClient(), delay: 25 * time.Millisecond}
	jm := &JobManager{jobInfoClient: NewJobInfoStorageClient(client), ctx: ctx}
	_, err := jm.jobInfoClient.PutInfo(ctx, "job1", &JobInfo{Entrypoint: "python -c pass", Status: JobStatusPending}, false)
	require.NoError(t, err)

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		errs <- jm.putStatus(ctx, "job1", JobStatusSucceeded, nil, nil, nil)
	}()
	go func() {
		defer wg.Done()
		<-start
		errs <- jm.putStatus(ctx, "job1", JobStatusFailed, nil, nil, nil)
	}()
	close(start)
	wg.Wait()
	close(errs)

	accepted := 0
	for err := range errs {
		if err == nil {
			accepted++
		}
	}
	assert.Equal(t, 1, accepted, "exactly one of the racing transitions must win")
	assert.Zero(t, client.overlapCount(), "status read-modify-write must not overlap")

	status, err := jm.getJobStatus(ctx, "job1")
	require.NoError(t, err)
	require.NotNil(t, status)
	assert.True(t, status.IsTerminal())
}
