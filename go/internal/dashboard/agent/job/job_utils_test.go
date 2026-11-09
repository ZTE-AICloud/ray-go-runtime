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
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ray-project/ray/go/internal/dashboard/jobutil"
	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateJobID(t *testing.T) {
	id := generateJobID()
	assert.True(t, strings.HasPrefix(id, "raysubmit_"))
	assert.Len(t, id, len("raysubmit_")+16)

	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		seen[generateJobID()] = true
	}
	assert.Len(t, seen, 100)
}

func TestStripKeysWithValueNone(t *testing.T) {
	in := map[string]interface{}{
		"a": "x",
		"b": nil,
		"c": 1,
	}
	out := stripKeysWithValueNone(in)
	assert.Equal(t, map[string]interface{}{"a": "x", "c": 1}, out)
}

func TestRedactURLPassword(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"https://user:secret@example.com/x", "https://user:<redacted>@example.com/x"},
		{"http://a:b@host:123", "http://a:<redacted>@host:123"},
		{"https://example.com/noauth", "https://example.com/noauth"},
		{"", ""},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, redactURLPassword(c.in), "input=%q", c.in)
	}
}

func TestFastTailLastNLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.log")
	content := "line1\nline2\nline3\nline4\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	assert.Equal(t, "", fastTailLastNLines(path, -1, 100))
	assert.Equal(t, "", fastTailLastNLines(path, 0, 100))
	assert.Equal(t, "", fastTailLastNLines(path, 3, 0))
	assert.Equal(t, "", fastTailLastNLines(filepath.Join(dir, "missing"), 3, 100))

	// numLines >= the number of lines returns the whole content (subject to
	// maxChars).
	assert.Equal(t, content, fastTailLastNLines(path, 5, 10000))
	// The last 2 lines.
	assert.Equal(t, "line3\nline4\n", fastTailLastNLines(path, 2, 10000))
	// maxChars truncates to the tail.
	assert.Equal(t, "4\n", fastTailLastNLines(path, 2, 2))
}

func TestFastTailLastNLinesEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.log")
	require.NoError(t, os.WriteFile(path, []byte{}, 0o644))
	assert.Equal(t, "", fastTailLastNLines(path, 5, 10000))
}

func TestValidateJobSubmitRequest(t *testing.T) {
	assert.NoError(t, validateJobSubmitRequest(map[string]interface{}{}))
	assert.NoError(t, validateJobSubmitRequest(map[string]interface{}{
		"entrypoint":           "echo",
		"submission_id":        "s1",
		"runtime_env":          map[string]interface{}{"x": 1},
		"metadata":             map[string]interface{}{"k": "v"},
		"entrypoint_num_cpus":  2.0,
		"entrypoint_num_gpus":  1.0,
		"entrypoint_memory":    128.0,
		"entrypoint_resources": map[string]interface{}{"r": 1.0},
	}))

	// Invalid entrypoint type.
	err := validateJobSubmitRequest(map[string]interface{}{"entrypoint": 1})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "entrypoint must be a string")

	// Invalid submission_id type.
	err = validateJobSubmitRequest(map[string]interface{}{"submission_id": 1})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "submission_id must be a string")

	// Invalid runtime_env.
	err = validateJobSubmitRequest(map[string]interface{}{"runtime_env": "nope"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "runtime_env must be a dict")

	// Invalid metadata.
	err = validateJobSubmitRequest(map[string]interface{}{"metadata": map[string]interface{}{"k": 1}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metadata values must be strings")

	// Invalid entrypoint_num_cpus.
	err = validateJobSubmitRequest(map[string]interface{}{"entrypoint_num_cpus": "many"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "entrypoint_num_cpus must be a number")

	// Invalid entrypoint_resources.
	err = validateJobSubmitRequest(map[string]interface{}{"entrypoint_resources": map[string]interface{}{"r": "x"}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "entrypoint_resources values must be numbers")
}

func TestJSONTypeName(t *testing.T) {
	assert.Equal(t, "str", jsonTypeName("x"))
	assert.Equal(t, "float", jsonTypeName(1.5))
	assert.Equal(t, "bool", jsonTypeName(true))
	assert.Equal(t, "dict", jsonTypeName(map[string]interface{}{}))
	assert.Equal(t, "list", jsonTypeName([]interface{}{}))
	assert.Equal(t, "NoneType", jsonTypeName(nil))
}

func TestParseAndValidateRequest(t *testing.T) {
	body := `{"entrypoint":"echo hi","metadata":{"k":"v"},"entrypoint_num_cpus":2}`
	r, err := http.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	require.NoError(t, err)
	req, err := parseAndValidateRequest(r)
	require.NoError(t, err)
	require.NotNil(t, req)
	assert.Equal(t, "echo hi", req.Entrypoint)
	require.NotNil(t, req.EntrypointNumCPUs)
	assert.Equal(t, 2.0, *req.EntrypointNumCPUs)

	// An empty entrypoint is rejected.
	r2, _ := http.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	_, err = parseAndValidateRequest(r2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "entrypoint must be a non-empty string")

	// A malformed body is rejected.
	r3, _ := http.NewRequest(http.MethodPost, "/", strings.NewReader(`not-json`))
	_, err = parseAndValidateRequest(r3)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse request body")
}

func TestGetDriverJobs(t *testing.T) {
	ctx := context.Background()
	client := newFakeGCSClient()

	jobs, submissionDrivers, err := getDriverJobs(ctx, client, "")
	require.NoError(t, err)
	assert.Empty(t, jobs)
	assert.Empty(t, submissionDrivers)
}

func TestSubmissionJobDetails(t *testing.T) {
	info := &JobInfo{Status: JobStatusRunning, Entrypoint: "echo"}
	driver := &DriverInfo{ID: "abc", NodeIPAddress: "1.2.3.4", PID: "42"}
	details := submissionJobDetails("sub1", info, driver)
	require.NotNil(t, details)
	assert.Equal(t, JobTypeSubmission, details.Type)
	require.NotNil(t, details.SubmissionID)
	assert.Equal(t, "sub1", *details.SubmissionID)
	require.NotNil(t, details.JobID)
	assert.Equal(t, "abc", *details.JobID)
	assert.Equal(t, "echo", details.Entrypoint)

	detailsNoDriver := submissionJobDetails("sub2", info, nil)
	require.NotNil(t, detailsNoDriver)
	assert.Nil(t, detailsNoDriver.JobID)
	assert.Nil(t, detailsNoDriver.DriverInfo)
}

// TestJobutilConstantsDriftGuard checks jobutil's exported constants from the
// consuming side: this package composes its KV keys from
// jobutil.RayInternalNamespacePrefix and reads submission ids through
// jobutil.JobIDMetadataKey. If either literal drifts from the Python
// definitions (job/common.py RAY_INTERNAL_NAMESPACE_PREFIX and
// JOB_ID_METADATA_KEY), the cross-language key layout silently breaks, so the
// values are pinned both here and in jobutil_test.go.
func TestJobutilConstantsDriftGuard(t *testing.T) {
	assert.Equal(t, "_ray_internal_", jobutil.RayInternalNamespacePrefix)
	assert.Equal(t, "job_submission_id", jobutil.JobIDMetadataKey)

	// The composed keys must match the Python side byte for byte.
	assert.Equal(t, "_ray_internal_job_info_", jobDataKeyPrefix)
	assert.Equal(t, "_ray_internal_job_info_sub1", jobDataKeyFor("sub1"))

	_ = &proto.JobTableData{}
}
