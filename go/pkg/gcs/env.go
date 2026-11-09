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

package gcs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Environment variable names for GCS TLS and token authentication, aligned
// with the C++ RayConfig (src/ray/common/ray_config_def.h) and the Python
// channel setup (python/ray/_private/grpc_utils.py, tls_utils.py).
const (
	envUseTLS        = "RAY_USE_TLS"
	envTLSCertFile   = "RAY_TLS_SERVER_CERT"
	envTLSKeyFile    = "RAY_TLS_SERVER_KEY"
	envTLSCAFile     = "RAY_TLS_CA_CERT"
	envAuthMode      = "RAY_AUTH_MODE"
	envAuthToken     = "RAY_AUTH_TOKEN"
	envAuthTokenPath = "RAY_AUTH_TOKEN_PATH"
	// envEnableK8sTokenAuth gates reading the Ray service-account token,
	// aligned with the ENABLE_K8S_TOKEN_AUTH RayConfig flag
	// (src/ray/common/ray_config_def.h), read as RAY_ENABLE_K8S_TOKEN_AUTH.
	envEnableK8sTokenAuth = "RAY_ENABLE_K8S_TOKEN_AUTH"
)

// RayServiceAccountTokenPath is the in-pod Ray service-account token file,
// aligned with kRaySaTokenPath in
// src/ray/rpc/authentication/k8s_constants.h.
const RayServiceAccountTokenPath = "/var/run/secrets/ray.io/serviceaccount/token"

// DefaultAuthTokenPath is the user-level token file, resolved against $HOME.
const DefaultAuthTokenPath = ".ray/auth_token"

// rayServiceAccountTokenPath is the path resolveAuthToken reads. It is a
// variable so tests can substitute a fixture; production code leaves it at
// RayServiceAccountTokenPath.
var rayServiceAccountTokenPath = RayServiceAccountTokenPath

// DialOptionsFromEnv builds DialOptions from the Ray environment, mirroring
// the C++ BuildChannel (src/ray/rpc/grpc_client.cc) and the Python
// init_grpc_channel (python/ray/_private/grpc_utils.py):
//
//   - RAY_USE_TLS=1|true enables mutual TLS using RAY_TLS_SERVER_CERT,
//     RAY_TLS_SERVER_KEY and RAY_TLS_CA_CERT. Matching the Python
//     load_certs_from_env, all three must be set when TLS is enabled.
//   - RAY_AUTH_MODE=token (case-insensitive) enables bearer token
//     authentication. Token authentication has exactly two states, on and
//     off: any other value, "k8s" included, attaches no token. The token is
//     resolved with the C++ AuthenticationTokenLoader precedence:
//     RAY_AUTH_TOKEN, then RAY_AUTH_TOKEN_PATH, then the Ray service-account
//     token when RAY_ENABLE_K8S_TOKEN_AUTH is on, then ~/.ray/auth_token.
//
// A missing token is not an error: the client dials without one and degrades
// on GCS rejections instead of failing to start. The only failure is a TLS
// misconfiguration.
func DialOptionsFromEnv() (DialOptions, error) {
	var opts DialOptions

	if isEnvTrue(envUseTLS) {
		certFile := os.Getenv(envTLSCertFile)
		keyFile := os.Getenv(envTLSKeyFile)
		caFile := os.Getenv(envTLSCAFile)
		if certFile == "" || keyFile == "" || caFile == "" {
			return DialOptions{}, fmt.Errorf(
				"%s is set but %s, %s and %s must also be set",
				envUseTLS, envTLSCertFile, envTLSKeyFile, envTLSCAFile)
		}
		opts.CAFile = caFile
		opts.CertFile = certFile
		opts.KeyFile = keyFile
	}

	token, err := resolveAuthToken(os.Getenv(envAuthMode))
	if err != nil {
		return DialOptions{}, err
	}
	opts.Token = token

	return opts, nil
}

// isEnvTrue reports whether the environment variable value is truthy,
// matching the C++ ConvertValue<bool> and the Python RAY_USE_TLS check
// (values "1" and "true", case-insensitive).
func isEnvTrue(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true":
		return true
	default:
		return false
	}
}

// resolveAuthToken loads the bearer token following the C++
// AuthenticationTokenLoader::TryLoadTokenFromSources precedence:
//  1. RAY_AUTH_TOKEN environment variable.
//  2. RAY_AUTH_TOKEN_PATH file (first non-empty line, whitespace-trimmed).
//  3. The Ray service-account token when RAY_ENABLE_K8S_TOKEN_AUTH is on.
//  4. The default ~/.ray/auth_token file (first non-empty line, trimmed).
//
// Any authMode other than "token" (case-insensitive) resolves nothing. Every
// source is best-effort: an unreadable or empty file falls through to the next
// one, and an exhausted list returns ("", nil) so callers degrade on GCS
// rejections instead of refusing to start. No source fails resolution today,
// but the error result lets one report a hard misconfiguration without
// changing the callers.
func resolveAuthToken(authMode string) (string, error) {
	if !strings.EqualFold(strings.TrimSpace(authMode), "token") {
		return "", nil
	}

	if v := strings.TrimSpace(os.Getenv(envAuthToken)); v != "" {
		return v, nil
	}

	if p := os.Getenv(envAuthTokenPath); p != "" {
		if token, err := ReadTokenFile(p); err == nil {
			if token != "" {
				return token, nil
			}
		}
	}

	if isEnvTrue(envEnableK8sTokenAuth) {
		if token, err := ReadTokenFile(rayServiceAccountTokenPath); err == nil {
			if token != "" {
				return token, nil
			}
		}
	}

	if home, err := os.UserHomeDir(); err == nil {
		if token, err := ReadTokenFile(filepath.Join(home, DefaultAuthTokenPath)); err == nil {
			if token != "" {
				return token, nil
			}
		}
	}

	return "", nil
}

// ReadTokenFile returns the first non-empty line of path, whitespace-trimmed,
// or "" when the file holds no token. Skipping blank lines matches the
// dashboard head's readFirstLine (go/internal/dashboard/head/auth.go), so a
// token file whose first line is blank still yields the token. A read failure
// returns the underlying error.
func ReadTokenFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			return trimmed, nil
		}
	}
	return "", nil
}
