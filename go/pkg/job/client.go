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

// Package job provides a Go client SDK for the Ray Jobs Submission REST API
// (/api/jobs/*), enabling Go drivers to submit and monitor Python Ray Data,
// Train, Tune, or Serve jobs.
package job

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const defaultDashboardAddress = "http://127.0.0.1:8265"

// SubmitRequest mirrors Ray's JobSubmitRequest payload for POST /api/jobs/.
type SubmitRequest struct {
	Entrypoint              string             `json:"entrypoint"`
	SubmissionID            string             `json:"submission_id,omitempty"`
	RuntimeEnv              map[string]any     `json:"runtime_env,omitempty"`
	Metadata                map[string]string  `json:"metadata,omitempty"`
	EntrypointNumCPUs       *float64           `json:"entrypoint_num_cpus,omitempty"`
	EntrypointNumGPUs       *float64           `json:"entrypoint_num_gpus,omitempty"`
	EntrypointMemory        *uint64            `json:"entrypoint_memory,omitempty"`
	EntrypointResources     map[string]float64 `json:"entrypoint_resources,omitempty"`
	EntrypointLabelSelector map[string]string  `json:"entrypoint_label_selector,omitempty"`
}

// SubmitResponse mirrors POST /api/jobs/ response.
type SubmitResponse struct {
	JobID        string `json:"job_id"`
	SubmissionID string `json:"submission_id"`
}

// JobDetails mirrors GET /api/jobs/{id} response.
type JobDetails struct {
	Type                   string         `json:"type"`
	JobID                  *string        `json:"job_id"`
	SubmissionID           *string        `json:"submission_id"`
	Status                 string         `json:"status"`
	Entrypoint             string         `json:"entrypoint"`
	Message                *string        `json:"message"`
	ErrorType              *string        `json:"error_type"`
	StartTime              *int64         `json:"start_time"`
	EndTime                *int64         `json:"end_time"`
	Metadata               map[string]any `json:"metadata"`
	RuntimeEnv             map[string]any `json:"runtime_env"`
	DriverAgentHTTPAddress *string        `json:"driver_agent_http_address"`
	DriverNodeID           *string        `json:"driver_node_id"`
	DriverExitCode         *int32         `json:"driver_exit_code"`
}

// IsTerminal reports whether the job status is terminal.
func (d *JobDetails) IsTerminal() bool {
	switch d.Status {
	case "SUCCEEDED", "STOPPED", "FAILED":
		return true
	default:
		return false
	}
}

// Client interacts with the Ray Dashboard Job Submission API.
type Client struct {
	dashboardURL string
	httpClient   *http.Client
}

// NewClient creates a Job Submission client. If dashboardURL is empty, it falls
// back to RAY_DASHBOARD_ADDRESS or http://127.0.0.1:8265.
func NewClient(dashboardURL string, httpClient *http.Client) *Client {
	if strings.TrimSpace(dashboardURL) == "" {
		dashboardURL = strings.TrimSpace(os.Getenv("RAY_DASHBOARD_ADDRESS"))
	}
	if dashboardURL == "" {
		dashboardURL = defaultDashboardAddress
	}
	if !strings.HasPrefix(dashboardURL, "http://") && !strings.HasPrefix(dashboardURL, "https://") {
		dashboardURL = "http://" + dashboardURL
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{
		dashboardURL: strings.TrimRight(dashboardURL, "/"),
		httpClient:   httpClient,
	}
}

// SubmitJob submits a new job via POST /api/jobs/.
func (c *Client) SubmitJob(ctx context.Context, reqBody *SubmitRequest) (*SubmitResponse, error) {
	raw, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.dashboardURL+"/api/jobs/", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	var out SubmitResponse
	if err := c.doJSON(req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetJobInfo fetches details for a job or submission ID via GET /api/jobs/{id}.
func (c *Client) GetJobInfo(ctx context.Context, id string) (*JobDetails, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.dashboardURL+"/api/jobs/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, err
	}
	var out JobDetails
	if err := c.doJSON(req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListJobs lists all jobs via GET /api/jobs/.
func (c *Client) ListJobs(ctx context.Context) ([]JobDetails, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.dashboardURL+"/api/jobs/", nil)
	if err != nil {
		return nil, err
	}
	var out []JobDetails
	if err := c.doJSON(req, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetJobLogs retrieves the job driver logs via GET /api/jobs/{id}/logs.
func (c *Client) GetJobLogs(ctx context.Context, id string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.dashboardURL+"/api/jobs/"+url.PathEscape(id)+"/logs", nil)
	if err != nil {
		return "", err
	}
	var resp struct {
		Logs string `json:"logs"`
	}
	if err := c.doJSON(req, &resp); err != nil {
		return "", err
	}
	return resp.Logs, nil
}

// StopJob requests job termination via POST /api/jobs/{id}/stop.
func (c *Client) StopJob(ctx context.Context, id string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.dashboardURL+"/api/jobs/"+url.PathEscape(id)+"/stop", nil)
	if err != nil {
		return false, err
	}
	var resp struct {
		Stopped bool `json:"stopped"`
	}
	if err := c.doJSON(req, &resp); err != nil {
		return false, err
	}
	return resp.Stopped, nil
}

// DeleteJob deletes a terminal job record via DELETE /api/jobs/{id}.
func (c *Client) DeleteJob(ctx context.Context, id string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.dashboardURL+"/api/jobs/"+url.PathEscape(id), nil)
	if err != nil {
		return false, err
	}
	var resp struct {
		Deleted bool `json:"deleted"`
	}
	if err := c.doJSON(req, &resp); err != nil {
		return false, err
	}
	return resp.Deleted, nil
}

// WaitForCompletion polls GetJobInfo until the job reaches a terminal status or ctx is cancelled.
// Transient GetJobInfo errors are tolerated until ctx is done.
func (c *Client) WaitForCompletion(ctx context.Context, id string, pollInterval time.Duration) (*JobDetails, error) {
	if pollInterval <= 0 {
		pollInterval = time.Second
	}
	var lastErr error
	for {
		info, err := c.GetJobInfo(ctx, id)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = err
		} else if info.IsTerminal() {
			return info, nil
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			if lastErr != nil {
				return nil, fmt.Errorf("%w (last poll error: %v)", ctx.Err(), lastErr)
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Client) doJSON(req *http.Request, dst any) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("job api %s %s returned %d: %s", req.Method, req.URL.Path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return fmt.Errorf("decode job response: %w", err)
	}
	return nil
}
