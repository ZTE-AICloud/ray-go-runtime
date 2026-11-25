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

// Package serve provides a Go client SDK for Ray Serve's Dashboard control-plane
// REST API (/api/serve/applications/, /api/v1/applications/.../scale) and
// HTTP inference proxy.
package serve

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

const (
	defaultDashboardAddress = "http://127.0.0.1:8265"
	defaultProxyAddress     = "http://127.0.0.1:8000"
)

// Client interacts with Ray Serve's control plane (Dashboard Head) and HTTP proxy.
type Client struct {
	dashboardURL string
	proxyURL     string
	httpClient   *http.Client
}

// NewClient creates a Ray Serve client. If dashboardURL is empty, it falls back
// to RAY_DASHBOARD_ADDRESS or http://127.0.0.1:8265.
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
		proxyURL:     defaultProxyAddress,
		httpClient:   httpClient,
	}
}

// WithProxyURL sets the Serve HTTP inference proxy base URL (default http://127.0.0.1:8000).
func (c *Client) WithProxyURL(proxyURL string) *Client {
	if strings.TrimSpace(proxyURL) != "" {
		if !strings.HasPrefix(proxyURL, "http://") && !strings.HasPrefix(proxyURL, "https://") {
			proxyURL = "http://" + proxyURL
		}
		c.proxyURL = strings.TrimRight(proxyURL, "/")
	}
	return c
}

// VersionResponse mirrors GET /api/ray/version.
type VersionResponse struct {
	Version     string `json:"version"`
	RayVersion  string `json:"ray_version"`
	RayCommit   string `json:"ray_commit"`
	SessionName string `json:"session_name"`
}

// Deploy deploys or updates Serve applications via PUT /api/serve/applications/.
// config may be a ServeDeploySchema-compatible struct or map[string]any.
func (c *Client) Deploy(ctx context.Context, config any) error {
	raw, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal serve deploy config: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.dashboardURL+"/api/serve/applications/", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.doEmpty(req)
}

// Status fetches the current Serve instance details via GET /api/serve/applications/.
func (c *Client) Status(ctx context.Context) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.dashboardURL+"/api/serve/applications/", nil)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := c.doJSON(req, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Delete deletes all Serve applications via DELETE /api/serve/applications/.
func (c *Client) Delete(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.dashboardURL+"/api/serve/applications/", nil)
	if err != nil {
		return err
	}
	return c.doEmpty(req)
}

// ScaleDeployment updates target_num_replicas for a specific deployment via
// POST /api/v1/applications/{app}/deployments/{dep}/scale.
func (c *Client) ScaleDeployment(ctx context.Context, appName, deploymentName string, targetNumReplicas int) error {
	payload, err := json.Marshal(map[string]int{"target_num_replicas": targetNumReplicas})
	if err != nil {
		return err
	}
	ep := fmt.Sprintf("%s/api/v1/applications/%s/deployments/%s/scale",
		c.dashboardURL, url.PathEscape(appName), url.PathEscape(deploymentName))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.doEmpty(req)
}

// Version returns the Dashboard / Serve API version metadata.
func (c *Client) Version(ctx context.Context) (*VersionResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.dashboardURL+"/api/ray/version", nil)
	if err != nil {
		return nil, err
	}
	var out VersionResponse
	if err := c.doJSON(req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Call sends an HTTP request to the Serve HTTP proxy at the given route path.
func (c *Client) Call(ctx context.Context, method, path string, body []byte, headers map[string]string) (int, []byte, error) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.proxyURL+path, reader)
	if err != nil {
		return 0, nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, respBody, nil
}

func (c *Client) doEmpty(req *http.Request) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("serve api %s %s returned %d: %s", req.Method, req.URL.Path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
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
		return fmt.Errorf("serve api %s %s returned %d: %s", req.Method, req.URL.Path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return fmt.Errorf("decode serve response: %w", err)
	}
	return nil
}
