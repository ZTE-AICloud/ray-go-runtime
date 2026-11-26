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

package cloud_providers

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKubeRayProviderGetLaunchAndTerminate(t *testing.T) {
	var gotPatches [][]jsonPatchOp
	replicas := 1
	maxReplicas := 3

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/zenap/pods":
			_, _ = w.Write([]byte(`{"items":[
				{"metadata":{"name":"ray-head-0","labels":{"ray.io/cluster":"rc1","ray.io/node-type":"head","ray.io/group":"headgroup"}},"status":{"phase":"Running"}},
				{"metadata":{"name":"ray-wg1-0","labels":{"ray.io/cluster":"rc1","ray.io/node-type":"worker","ray.io/group":"wg1"}},"status":{"phase":"Running"}},
				{"metadata":{"name":"ray-wg1-dead","labels":{"ray.io/cluster":"rc1","ray.io/node-type":"worker","ray.io/group":"wg1"}},"status":{"phase":"Succeeded"}}
			]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/apis/ray.io/v1/namespaces/zenap/rayclusters/rc1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"spec": map[string]any{
					"workerGroupSpecs": []map[string]any{
						{
							"groupName":   "wg1",
							"replicas":    replicas,
							"minReplicas": 0,
							"maxReplicas": maxReplicas,
						},
					},
				},
			})
		case r.Method == http.MethodPatch && r.URL.Path == "/apis/ray.io/v1/namespaces/zenap/rayclusters/rc1":
			body, _ := io.ReadAll(r.Body)
			var ops []jsonPatchOp
			require.NoError(t, json.Unmarshal(body, &ops))
			gotPatches = append(gotPatches, ops)
			for _, op := range ops {
				if op.Path == "/spec/workerGroupSpecs/0/replicas" {
					if v, ok := op.Value.(float64); ok {
						replicas = int(v)
					}
				}
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	p := newKubeRayProviderWithClient("rc1", "zenap", srv.URL, srv.Client())

	instances, err := p.GetNonTerminated()
	require.NoError(t, err)
	assert.Len(t, instances, 2)
	assert.Equal(t, proto.NodeKind_HEAD, instances["ray-head-0"].NodeKind)
	assert.Equal(t, proto.NodeKind_WORKER, instances["ray-wg1-0"].NodeKind)
	assert.Equal(t, "wg1", instances["ray-wg1-0"].NodeType)

	// Launch 2 more wg1 workers (1 -> 3).
	require.NoError(t, p.Launch(map[string]int{"wg1": 2}, "req-launch-1"))
	assert.Equal(t, 3, replicas)
	// Idempotent replay does not patch again.
	require.NoError(t, p.Launch(map[string]int{"wg1": 2}, "req-launch-1"))
	assert.Len(t, gotPatches, 1)

	// Terminate ray-wg1-0 (3 -> 2, sets scaleStrategy.workersToDelete).
	require.NoError(t, p.Terminate([]string{"ray-wg1-0"}, "req-term-1"))
	assert.Equal(t, 2, replicas)
	require.Len(t, gotPatches, 2)
	assert.Empty(t, p.PollErrors())
}

func TestKubeRayProviderLaunchCappedByMaxReplicas(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/namespaces/zenap/pods":
			_, _ = w.Write([]byte(`{"items":[]}`))
		case "/apis/ray.io/v1/namespaces/zenap/rayclusters/rc1":
			if r.Method == http.MethodGet {
				_, _ = w.Write([]byte(`{"spec":{"workerGroupSpecs":[{"groupName":"wg1","replicas":1,"maxReplicas":2}]}}`))
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	p := newKubeRayProviderWithClient("rc1", "zenap", srv.URL, srv.Client())
	require.NoError(t, p.Launch(map[string]int{"wg1": 3}, "req-cap"))
	errs := p.PollErrors()
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "maxReplicas")
}

func TestKubeRayProviderTerminateMissingGroupLabel(t *testing.T) {
	patched := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/namespaces/test-ns/pods":
			_, _ = w.Write([]byte(`{"items":[{"metadata":{"name":"ray-worker-nogroup","uid":"uid-1","labels":{"ray.io/cluster":"rc1","ray.io/node-type":"worker"}},"status":{"phase":"Running"}}]}`))
		case "/apis/ray.io/v1/namespaces/test-ns/rayclusters/rc1":
			if r.Method == http.MethodGet {
				_, _ = w.Write([]byte(`{"spec":{"workerGroupSpecs":[{"groupName":"wg1","replicas":1}]}}`))
				return
			}
			patched = true
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	p := newKubeRayProviderWithClient("rc1", "test-ns", srv.URL, srv.Client())
	err := p.Terminate([]string{"ray-worker-nogroup"}, "req-missing-group")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing ray.io/group label")
	assert.False(t, patched)

	errs := p.PollErrors()
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "missing ray.io/group label")

	// Should not be marked processed, so retrying still returns error.
	require.Error(t, p.Terminate([]string{"ray-worker-nogroup"}, "req-missing-group"))
}
