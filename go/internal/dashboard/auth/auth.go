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

// Package auth holds the authentication and browser-guard middleware shared by
// the dashboard head and the dashboard agent. It mirrors the Python
// http_token_authentication and dashboard_optional_utils behavior.
package auth

import (
	"crypto/subtle"
	"net/http"
	"os"
	"sync"

	"github.com/ray-project/ray/go/pkg/gcs"
)

// Header/cookie names aligned with python/ray/_private/authentication/authentication_constants.py.
const (
	AuthorizationHeaderName   = "authorization"
	RayAuthorizationHeader    = "x-ray-authorization"
	AuthenticationTokenCookie = "ray-authentication-token"
	BearerPrefix              = "Bearer "
)

// TokenAuthEnabled reports whether token authentication is enabled via the
// RAY_AUTH_MODE environment variable, aligned with the C++ get_authentication_mode.
func TokenAuthEnabled() bool {
	mode := os.Getenv("RAY_AUTH_MODE")
	return mode == "token" || mode == "k8s"
}

// AuthenticationModeName returns the mode name string for the
// /api/authentication_mode endpoint, aligned with
// authentication_utils.get_authentication_mode_name.
func AuthenticationModeName() string {
	if !TokenAuthEnabled() {
		return "disabled"
	}
	return os.Getenv("RAY_AUTH_MODE")
}

// ValidateRequestToken validates an Authorization header containing a Bearer
// token. In token mode it compares against the expected token (RAY_AUTH_TOKEN
// or RAY_AUTH_TOKEN_PATH), constant-time to avoid leaking the expected token
// length. In k8s mode it asks the Kubernetes API for a TokenReview +
// SubjectAccessReview.
func ValidateRequestToken(header string) bool {
	if len(header) <= len(BearerPrefix) || header[:len(BearerPrefix)] != BearerPrefix {
		return false
	}
	token := header[len(BearerPrefix):]
	if token == "" {
		return false
	}
	if os.Getenv("RAY_AUTH_MODE") == "k8s" {
		return k8sTokenValidator.validate(token)
	}
	expected := tokenLoader.expectedToken()
	if expected == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
}

// authTokenLoader caches the expected token in token mode so the file at
// RAY_AUTH_TOKEN_PATH is only re-read when it changes. It mirrors the C++
// AuthenticationTokenLoader precedence: RAY_AUTH_TOKEN env first, then the
// RAY_AUTH_TOKEN_PATH file.
type authTokenLoader struct {
	mu        sync.Mutex
	cached    string
	tokenPath string
}

var tokenLoader = &authTokenLoader{}

// expectedToken returns the expected bearer token for token mode. Precedence
// 1 is the RAY_AUTH_TOKEN env var; precedence 2 is the RAY_AUTH_TOKEN_PATH
// file (first non-empty line, trimmed). The file is re-read on every call so a
// rotated token takes effect without a restart.
func (l *authTokenLoader) expectedToken() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if env := os.Getenv("RAY_AUTH_TOKEN"); env != "" {
		return env
	}
	path := os.Getenv("RAY_AUTH_TOKEN_PATH")
	if path == "" {
		return ""
	}
	if path != l.tokenPath {
		l.cached = readTokenFileOrEmpty(path)
		l.tokenPath = path
		return l.cached
	}
	// Re-read the same path so a rotated token file is picked up.
	l.cached = readTokenFileOrEmpty(path)
	return l.cached
}

// readTokenFileOrEmpty returns the first NON-empty trimmed line of path, or ""
// when the file cannot be read or holds no token. Note: gcs.ReadTokenFile
// skips leading blank lines, unlike the reference implementation's strict
// first-line read, so a leading blank line still yields the token here
// (deliberate preservation of the OSS behavior).
func readTokenFileOrEmpty(path string) string {
	token, err := gcs.ReadTokenFile(path)
	if err != nil {
		return ""
	}
	return token
}

// WithTokenAuth is the token authentication middleware. It passes every
// request through unchanged when token auth is not enabled. When enabled, it
// skips the whitelisted public paths/prefixes and requires a valid bearer
// token for everything else, aligned with
// http_token_authentication.get_token_auth_middleware. The token may come
// from the standard Authorization header, the X-Ray-Authorization header, or
// the authentication cookie.
func WithTokenAuth(publicExact, publicPrefixes []string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !TokenAuthEnabled() {
			next.ServeHTTP(w, r)
			return
		}
		path := r.URL.Path
		for _, p := range publicExact {
			if path == p {
				next.ServeHTTP(w, r)
				return
			}
		}
		for _, p := range publicPrefixes {
			if len(path) >= len(p) && path[:len(p)] == p {
				next.ServeHTTP(w, r)
				return
			}
		}
		authHeader := r.Header.Get(AuthorizationHeaderName)
		if authHeader == "" {
			authHeader = r.Header.Get(RayAuthorizationHeader)
		}
		if authHeader == "" {
			if cookie, err := r.Cookie(AuthenticationTokenCookie); err == nil {
				authHeader = BearerPrefix + cookie.Value
			}
		}
		if authHeader == "" {
			writePlainText(w, http.StatusUnauthorized, "Unauthorized: Missing authentication token")
			return
		}
		if !ValidateRequestToken(authHeader) {
			writePlainText(w, http.StatusForbidden, "Forbidden: Invalid authentication token")
			return
		}
		next.ServeHTTP(w, r)
	})
}
