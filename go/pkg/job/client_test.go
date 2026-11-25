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

package job

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestJobClientLifecycle(t *testing.T) {
	var polls atomic.Int32
	var gotSubmit SubmitRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/jobs/":
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &gotSubmit)
			_ = json.NewEncoder(w).Encode(SubmitResponse{JobID: "sub_1", SubmissionID: "sub_1"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/jobs/sub_1":
			n := polls.Add(1)
			if n == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			st := "RUNNING"
			if n >= 3 {
				st = "SUCCEEDED"
			}
			_ = json.NewEncoder(w).Encode(JobDetails{Status: st, Entrypoint: gotSubmit.Entrypoint})
		case r.Method == http.MethodGet && r.URL.Path == "/api/jobs/sub_1/logs":
			_ = json.NewEncoder(w).Encode(map[string]string{"logs": "hello ray"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/jobs/":
			_ = json.NewEncoder(w).Encode([]JobDetails{{Status: "SUCCEEDED", Entrypoint: "python train.py"}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/jobs/sub_1/stop":
			_ = json.NewEncoder(w).Encode(map[string]bool{"stopped": true})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/jobs/sub_1":
			_ = json.NewEncoder(w).Encode(map[string]bool{"deleted": true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := NewClient(srv.URL, srv.Client())
	ctx := context.Background()

	sub, err := c.SubmitJob(ctx, &SubmitRequest{
		Entrypoint:              "python train.py",
		EntrypointLabelSelector: map[string]string{"pool": "gpu"},
	})
	if err != nil || sub.SubmissionID != "sub_1" {
		t.Fatalf("SubmitJob = (%+v, %v)", sub, err)
	}
	if gotSubmit.EntrypointLabelSelector["pool"] != "gpu" {
		t.Fatalf("EntrypointLabelSelector = %+v", gotSubmit.EntrypointLabelSelector)
	}

	details, err := c.WaitForCompletion(ctx, "sub_1", 5*time.Millisecond)
	if err != nil || details.Status != "SUCCEEDED" {
		t.Fatalf("WaitForCompletion = (%+v, %v)", details, err)
	}

	logs, err := c.GetJobLogs(ctx, "sub_1")
	if err != nil || logs != "hello ray" {
		t.Fatalf("GetJobLogs = (%q, %v)", logs, err)
	}

	jobs, err := c.ListJobs(ctx)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("ListJobs = (%+v, %v)", jobs, err)
	}

	if stopped, err := c.StopJob(ctx, "sub_1"); err != nil || !stopped {
		t.Fatalf("StopJob = (%v, %v)", stopped, err)
	}
	if deleted, err := c.DeleteJob(ctx, "sub_1"); err != nil || !deleted {
		t.Fatalf("DeleteJob = (%v, %v)", deleted, err)
	}
}
