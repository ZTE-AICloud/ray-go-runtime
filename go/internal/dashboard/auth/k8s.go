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

package auth

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// K8s authentication constants aligned with
// src/ray/rpc/authentication/k8s_constants.h.
const (
	k8sServiceHostEnvVar      = "KUBERNETES_SERVICE_HOST"
	k8sServicePortEnvVar      = "KUBERNETES_SERVICE_PORT"
	k8sCaCertPath             = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	k8sSaTokenPath            = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	rayClusterNameEnvVar      = "RAY_CLUSTER_NAME"
	rayClusterNamespaceEnvVar = "RAY_CLUSTER_NAMESPACE"

	k8sTokenReviewPath   = "/apis/authentication.k8s.io/v1/tokenreviews"
	k8sSubjectAccessPath = "/apis/authorization.k8s.io/v1/subjectaccessreviews"
	k8sTokenReviewKind   = "TokenReview"
	k8sSubjectAccessKind = "SubjectAccessReview"
	k8sAuthAPIVersion    = "authentication.k8s.io/v1"
	k8sAuthzAPIVersion   = "authorization.k8s.io/v1"
	k8sResourceGroup     = "ray.io"
	k8sClusterResource   = "rayclusters"
	k8sRayUserVerb       = "ray-user"
)

// k8sTokenCacheTTL is how long a validated k8s token stays cached, aligned
// with the C++ AuthenticationTokenValidator kCacheTTL(5).
const k8sTokenCacheTTL = 5 * time.Minute

// k8sTokenValidatorImpl validates bearer tokens against the Kubernetes API
// using only the standard library (net/http + crypto/tls), avoiding a k8s.io
// dependency. Validated tokens are cached for k8sTokenCacheTTL, aligned with
// the C++ AuthenticationTokenValidator.
type k8sTokenValidatorImpl struct {
	mu     sync.Mutex
	cache  map[string]k8sCacheEntry
	client *http.Client
	sa     string
}

type k8sCacheEntry struct {
	allowed   bool
	expiresAt time.Time
}

// k8sTokenValidator is the shared k8s token validator.
var k8sTokenValidator = &k8sTokenValidatorImpl{cache: map[string]k8sCacheEntry{}}

// validate returns whether the token is authenticated and authorized. It
// returns false when the k8s client could not be initialized (missing env or
// CA), mirroring the C++ k8s_client_initialized check.
func (v *k8sTokenValidatorImpl) validate(token string) bool {
	if !v.init() {
		return false
	}
	v.mu.Lock()
	if entry, ok := v.cache[token]; ok && time.Now().Before(entry.expiresAt) {
		allowed := entry.allowed
		v.mu.Unlock()
		return allowed
	}
	v.mu.Unlock()

	allowed := v.tokenReviewAndAuthorize(token)
	if allowed {
		v.mu.Lock()
		v.cache[token] = k8sCacheEntry{allowed: true, expiresAt: time.Now().Add(k8sTokenCacheTTL)}
		v.mu.Unlock()
	}
	return allowed
}

// init builds the TLS client from the service account env and CA, or returns
// false when any required input is missing (aligned with InitK8sClientConfig).
func (v *k8sTokenValidatorImpl) init() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.client != nil {
		return true
	}
	host := os.Getenv(k8sServiceHostEnvVar)
	port := os.Getenv(k8sServicePortEnvVar)
	if host == "" || port == "" {
		return false
	}
	sa, err := os.ReadFile(k8sSaTokenPath)
	if err != nil || len(sa) == 0 {
		return false
	}
	caData, err := os.ReadFile(k8sCaCertPath)
	if err != nil {
		return false
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caData) {
		return false
	}
	v.sa = strings.TrimSpace(string(sa))
	v.client = &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    pool,
				ServerName: host,
				MinVersion: tls.VersionTLS12,
			},
		},
	}
	return true
}

// tokenReviewAndAuthorize runs the TokenReview then the SubjectAccessReview
// against the Kubernetes API, mirroring k8s_util.cc ValidateToken.
func (v *k8sTokenValidatorImpl) tokenReviewAndAuthorize(token string) bool {
	v.mu.Lock()
	client := v.client
	sa := v.sa
	host := os.Getenv(k8sServiceHostEnvVar)
	port := os.Getenv(k8sServicePortEnvVar)
	v.mu.Unlock()
	if client == nil {
		return false
	}

	userInfo, ok := v.tokenReview(client, host, port, sa, token)
	if !ok {
		return false
	}
	return v.subjectAccessReview(client, host, port, sa, userInfo)
}

// tokenReview posts the TokenReview and returns the status.user object when
// authenticated. It mirrors k8s_util.cc ValidateToken's first half.
func (v *k8sTokenValidatorImpl) tokenReview(client *http.Client, host, port, sa, token string) (map[string]interface{}, bool) {
	body, _ := json.Marshal(map[string]interface{}{
		"apiVersion": k8sAuthAPIVersion,
		"kind":       k8sTokenReviewKind,
		"spec":       map[string]interface{}{"token": token},
	})
	url := fmt.Sprintf("https://%s:%s%s", host, port, k8sTokenReviewPath)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+sa)
	resp, err := client.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, false
	}
	var review struct {
		Status struct {
			Authenticated bool                   `json:"authenticated"`
			Error         string                 `json:"error"`
			User          map[string]interface{} `json:"user"`
		} `json:"status"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&review); err != nil {
		return nil, false
	}
	if !review.Status.Authenticated {
		return nil, false
	}
	return review.Status.User, true
}

// subjectAccessReview posts the SubjectAccessReview with the user/groups/extra
// from the TokenReview and returns whether access is allowed. It mirrors
// k8s_util.cc ValidateToken's second half.
func (v *k8sTokenValidatorImpl) subjectAccessReview(client *http.Client, host, port, sa string, user map[string]interface{}) bool {
	clusterName := os.Getenv(rayClusterNameEnvVar)
	namespace := os.Getenv(rayClusterNamespaceEnvVar)
	if clusterName == "" || namespace == "" {
		return false
	}
	spec := map[string]interface{}{
		"resourceAttributes": map[string]interface{}{
			"group":     k8sResourceGroup,
			"resource":  k8sClusterResource,
			"name":      clusterName,
			"verb":      k8sRayUserVerb,
			"namespace": namespace,
		},
	}
	if username, ok := user["username"].(string); ok && username != "" {
		spec["user"] = username
	}
	if groups, ok := user["groups"].([]interface{}); ok {
		spec["groups"] = groups
	}
	if extra, ok := user["extra"]; ok {
		spec["extra"] = extra
	}
	body, _ := json.Marshal(map[string]interface{}{
		"apiVersion": k8sAuthzAPIVersion,
		"kind":       k8sSubjectAccessKind,
		"spec":       spec,
	})
	url := fmt.Sprintf("https://%s:%s%s", host, port, k8sSubjectAccessPath)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+sa)
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return false
	}
	var review struct {
		Status struct {
			Allowed bool `json:"allowed"`
		} `json:"status"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&review); err != nil {
		return false
	}
	return review.Status.Allowed
}
