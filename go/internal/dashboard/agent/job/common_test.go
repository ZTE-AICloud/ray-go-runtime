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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobActorName(t *testing.T) {
	assert.Equal(t, "_ray_internal_job_actor_abc123", jobActorName("abc123"))
}

func TestJobLogsPath(t *testing.T) {
	assert.Equal(t, "job-driver-sub1.log", jobLogsPath("sub1"))
}

func TestJobDataKeyFor(t *testing.T) {
	assert.Equal(t, "_ray_internal_job_info_sub1", jobDataKeyFor("sub1"))
}

func TestJobStatusIsTerminal(t *testing.T) {
	cases := map[JobStatus]bool{
		JobStatusPending:   false,
		JobStatusRunning:   false,
		JobStatusStopped:   true,
		JobStatusSucceeded: true,
		JobStatusFailed:    true,
	}
	for status, want := range cases {
		assert.Equal(t, want, status.IsTerminal(), "status=%s", status)
	}
}

func TestDefaultMessage(t *testing.T) {
	base := &JobInfo{}
	assert.Equal(t, "Job has not started yet.", *defaultMessage(base, JobStatusPending))
	assert.Equal(t, "Job is currently running.", *defaultMessage(&JobInfo{}, JobStatusRunning))
	assert.Equal(t, "Job was intentionally stopped.", *defaultMessage(&JobInfo{}, JobStatusStopped))
	assert.Equal(t, "Job finished successfully.", *defaultMessage(&JobInfo{}, JobStatusSucceeded))
	assert.Equal(t, "Job failed.", *defaultMessage(&JobInfo{}, JobStatusFailed))
	assert.Nil(t, defaultMessage(&JobInfo{}, "UNKNOWN"))
}

func TestDefaultMessagePendingResourceHints(t *testing.T) {
	cpus := 1.0
	info := &JobInfo{EntrypointNumCPUs: &cpus}
	msg := *defaultMessage(info, JobStatusPending)
	assert.Contains(t, msg, "may be waiting for resources")

	re := "{}"
	info2 := &JobInfo{RuntimeEnvJSON: &re}
	msg2 := *defaultMessage(info2, JobStatusPending)
	assert.NotContains(t, msg2, "runtime environment")

	re3 := `{"pip":["x"]}`
	info3 := &JobInfo{RuntimeEnvJSON: &re3}
	msg3 := *defaultMessage(info3, JobStatusPending)
	assert.Contains(t, msg3, "runtime environment")
}

func TestJobInfoStorageClientRoundTrip(t *testing.T) {
	ctx := context.Background()
	c := NewJobInfoStorageClient(newFakeGCSClient())

	info := &JobInfo{Status: JobStatusPending, Entrypoint: "echo hi"}
	ok, err := c.PutInfo(ctx, "sub1", info, false)
	require.NoError(t, err)
	assert.True(t, ok)

	// overwrite=false on an existing key reports false and keeps the old value.
	ok, err = c.PutInfo(ctx, "sub1", info, false)
	require.NoError(t, err)
	assert.False(t, ok)

	got, err := c.GetInfo(ctx, "sub1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, JobStatusPending, got.Status)
	assert.Equal(t, "echo hi", got.Entrypoint)

	// A missing key yields (nil, nil).
	missing, err := c.GetInfo(ctx, "nope")
	require.NoError(t, err)
	assert.Nil(t, missing)

	require.NoError(t, c.DeleteInfo(ctx, "sub1"))
	got, err = c.GetInfo(ctx, "sub1")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestJobInfoStorageClientPutStatusTerminalLock(t *testing.T) {
	ctx := context.Background()
	c := NewJobInfoStorageClient(newFakeGCSClient())

	msg := "done"
	require.NoError(t, c.PutStatus(ctx, "sub1", JobStatusSucceeded, &msg, nil, nil))

	// A terminal status must not transition back to a non-terminal one.
	require.Error(t, c.PutStatus(ctx, "sub1", JobStatusRunning, &msg, nil, nil))

	st, err := c.GetStatus(ctx, "sub1")
	require.NoError(t, err)
	require.NotNil(t, st)
	assert.Equal(t, JobStatusSucceeded, *st)
}

func TestJobInfoStorageClientPutStatusNewInfo(t *testing.T) {
	ctx := context.Background()
	c := NewJobInfoStorageClient(newFakeGCSClient())

	require.NoError(t, c.PutStatus(ctx, "newjob", JobStatusFailed, nil, nil, nil))
	info, err := c.GetInfo(ctx, "newjob")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "Entrypoint not found.", info.Entrypoint)
	assert.Equal(t, JobStatusFailed, info.Status)
	require.NotNil(t, info.Message)
	assert.Equal(t, "Job failed.", *info.Message)
	require.NotNil(t, info.EndTime)
}

func TestJobInfoStorageClientPutStatusErrorType(t *testing.T) {
	ctx := context.Background()
	c := NewJobInfoStorageClient(newFakeGCSClient())

	et := JobErrorTypeJobSupervisorActorDied
	require.NoError(t, c.PutStatus(ctx, "subA", JobStatusFailed, nil, nil, &et))
	info, err := c.GetInfo(ctx, "subA")
	require.NoError(t, err)
	require.NotNil(t, info)
	require.NotNil(t, info.ErrorType)
	assert.Equal(t, "JOB_SUPERVISOR_ACTOR_DIED", *info.ErrorType)

	// A nil errorType clears the field on a non-terminal update.
	require.NoError(t, c.PutStatus(ctx, "subB", JobStatusPending, nil, nil, &et))
	require.NoError(t, c.PutStatus(ctx, "subB", JobStatusPending, nil, nil, nil))
	info, err = c.GetInfo(ctx, "subB")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Nil(t, info.ErrorType)
}

func TestJobInfoStorageClientGetAllJobs(t *testing.T) {
	ctx := context.Background()
	c := NewJobInfoStorageClient(newFakeGCSClient())

	require.NoError(t, c.PutStatus(ctx, "jobA", JobStatusRunning, nil, nil, nil))
	require.NoError(t, c.PutStatus(ctx, "jobB", JobStatusSucceeded, nil, nil, nil))
	// A key outside the job info prefix is ignored by GetAllJobs.
	_, err := c.client.Put(ctx, kvNamespaceJob, "other_key", []byte("x"), true)
	require.NoError(t, err)

	all, err := c.GetAllJobs(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 2)
	assert.Equal(t, JobStatusRunning, all["jobA"].Status)
	assert.Equal(t, JobStatusSucceeded, all["jobB"].Status)
}

func TestJobInfoStorageClientGetHeadNodeID(t *testing.T) {
	ctx := context.Background()
	c := NewJobInfoStorageClient(newFakeGCSClient())

	// A missing key returns an empty id and no error.
	id, err := c.GetHeadNodeID(ctx)
	require.NoError(t, err)
	assert.Equal(t, "", id)

	_, err = c.client.Put(ctx, kvNamespaceJob, kvHeadNodeIDKey, []byte("node-1"), true)
	require.NoError(t, err)
	id, err = c.GetHeadNodeID(ctx)
	require.NoError(t, err)
	assert.Equal(t, "node-1", id)
}

func TestJobInfoStorageClientGetInfoUnmarshalError(t *testing.T) {
	ctx := context.Background()
	c := NewJobInfoStorageClient(newFakeGCSClient())
	_, err := c.client.Put(ctx, kvNamespaceJob, jobDataKeyFor("bad"), []byte("{not-json"), true)
	require.NoError(t, err)
	_, err = c.GetInfo(ctx, "bad")
	require.Error(t, err)
}
