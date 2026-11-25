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

package serve

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServeClientControlAndInference(t *testing.T) {
	var gotMethod, gotPath string
	var gotScaleBody map[string]int

	dashSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/serve/applications/":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/api/serve/applications/":
			_ = json.NewEncoder(w).Encode(map[string]any{"applications": map[string]any{"app1": map[string]any{"status": "RUNNING"}}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/applications/app1/deployments/dep1/scale":
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &gotScaleBody)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete && r.URL.Path == "/api/serve/applications/":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/api/ray/version":
			_ = json.NewEncoder(w).Encode(VersionResponse{Version: "2", RayVersion: "3.0.0.dev0"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer dashSrv.Close()

	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/predict" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer proxySrv.Close()

	c := NewClient(dashSrv.URL, dashSrv.Client()).WithProxyURL(proxySrv.URL)
	ctx := context.Background()

	if err := c.Deploy(ctx, map[string]any{"applications": []any{}}); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if gotMethod != http.MethodPut || gotPath != "/api/serve/applications/" {
		t.Fatalf("Deploy hit %s %s", gotMethod, gotPath)
	}

	st, err := c.Status(ctx)
	if err != nil || st["applications"] == nil {
		t.Fatalf("Status = (%v, %v)", st, err)
	}

	if err := c.ScaleDeployment(ctx, "app1", "dep1", 3); err != nil {
		t.Fatalf("ScaleDeployment: %v", err)
	}
	if gotScaleBody["target_num_replicas"] != 3 {
		t.Fatalf("ScaleDeployment body = %v, want target_num_replicas=3", gotScaleBody)
	}

	ver, err := c.Version(ctx)
	if err != nil || ver.RayVersion != "3.0.0.dev0" {
		t.Fatalf("Version = (%+v, %v)", ver, err)
	}

	code, body, err := c.Call(ctx, http.MethodPost, "/predict", []byte(`{"x":1}`), map[string]string{"Content-Type": "application/json"})
	if err != nil || code != http.StatusOK || string(body) != `{"ok":true}` {
		t.Fatalf("Call = (%d, %q, %v)", code, body, err)
	}

	if err := c.Delete(ctx); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}
