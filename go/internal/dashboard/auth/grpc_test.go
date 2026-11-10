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

package auth

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// incomingContext builds a server-side context carrying the given
// authorization metadata value, or no metadata at all when withMetadata is
// false.
func incomingContext(withMetadata bool, authorization string) context.Context {
	if !withMetadata {
		return context.Background()
	}
	md := metadata.MD{}
	if authorization != "" {
		md.Set(AuthorizationHeaderName, authorization)
	}
	return metadata.NewIncomingContext(context.Background(), md)
}

// stubStream implements the parts of grpc.ServerStream the stream interceptor
// touches: only Context() is read before the handler runs.
type stubStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *stubStream) Context() context.Context { return s.ctx }

func TestGRPCUnaryInterceptor(t *testing.T) {
	tests := []struct {
		name       string
		authMode   string
		token      string
		withMeta   bool
		header     string
		wantCalled bool
		wantCode   codes.Code
	}{
		{name: "auth disabled allows everything", authMode: "", wantCalled: true},
		{name: "valid token passes", authMode: "token", token: "secret", withMeta: true, header: "Bearer secret", wantCalled: true},
		{name: "missing metadata rejected", authMode: "token", token: "secret", wantCalled: false, wantCode: codes.Unauthenticated},
		{name: "missing header rejected", authMode: "token", token: "secret", withMeta: true, wantCalled: false, wantCode: codes.Unauthenticated},
		{name: "wrong token rejected", authMode: "token", token: "secret", withMeta: true, header: "Bearer nope", wantCalled: false, wantCode: codes.Unauthenticated},
		{name: "non-bearer header rejected", authMode: "token", token: "secret", withMeta: true, header: "secret", wantCalled: false, wantCode: codes.Unauthenticated},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RAY_AUTH_MODE", tc.authMode)
			t.Setenv("RAY_AUTH_TOKEN", tc.token)

			called := false
			handler := func(context.Context, interface{}) (interface{}, error) {
				called = true
				return "ok", nil
			}
			resp, err := GRPCUnaryServerInterceptor(incomingContext(tc.withMeta, tc.header), nil, &grpc.UnaryServerInfo{}, handler)

			if called != tc.wantCalled {
				t.Fatalf("handler called = %v, want %v", called, tc.wantCalled)
			}
			if tc.wantCalled {
				if err != nil || resp != "ok" {
					t.Fatalf("resp, err = %v, %v; want \"ok\", nil", resp, err)
				}
				return
			}
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("code = %v, want %v", got, tc.wantCode)
			}
			if msg := status.Convert(err).Message(); msg != "Invalid or missing authentication token" {
				t.Fatalf("message = %q, want the Python message verbatim", msg)
			}
		})
	}
}

func TestGRPCStreamInterceptor(t *testing.T) {
	called := false
	handler := func(interface{}, grpc.ServerStream) error {
		called = true
		return nil
	}

	// With authentication disabled the stream is let through untouched.
	t.Setenv("RAY_AUTH_MODE", "")
	t.Setenv("RAY_AUTH_TOKEN", "secret")
	if err := GRPCStreamServerInterceptor(nil, &stubStream{ctx: incomingContext(true, "")}, &grpc.StreamServerInfo{}, handler); err != nil {
		t.Fatalf("auth disabled: %v", err)
	}
	if !called {
		t.Fatal("handler did not run with authentication disabled")
	}

	t.Setenv("RAY_AUTH_MODE", "token")
	called = false

	// A stream without the token must be rejected before the handler runs.
	err := GRPCStreamServerInterceptor(nil, &stubStream{ctx: incomingContext(true, "")}, &grpc.StreamServerInfo{}, handler)
	if called {
		t.Fatal("handler ran for an unauthenticated stream")
	}
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("code = %v, want Unauthenticated", got)
	}

	// The same handler runs once the token is present.
	called = false
	err = GRPCStreamServerInterceptor(nil, &stubStream{ctx: incomingContext(true, "Bearer secret")}, &grpc.StreamServerInfo{}, handler)
	if err != nil {
		t.Fatalf("authenticated stream: %v", err)
	}
	if !called {
		t.Fatal("handler did not run for an authenticated stream")
	}
}

func TestGRPCInterceptorRoutesK8sModeToTheValidator(t *testing.T) {
	// In k8s mode ValidateRequestToken delegates to k8sTokenValidator, which
	// cannot initialize without the Kubernetes service environment. Presenting
	// the same header that token mode would accept therefore proves the
	// interceptor routed to the k8s validator rather than to the constant-time
	// static comparison.
	t.Setenv("RAY_AUTH_MODE", "k8s")
	t.Setenv("RAY_AUTH_TOKEN", "secret")
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	called := false
	handler := func(context.Context, interface{}) (interface{}, error) {
		called = true
		return "ok", nil
	}

	_, err := GRPCUnaryServerInterceptor(incomingContext(true, "Bearer secret"), nil, &grpc.UnaryServerInfo{}, handler)
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("code = %v, want Unauthenticated", got)
	}
	if called {
		t.Fatal("handler ran; the interceptor used the static token instead of the k8s validator")
	}
}

func TestTokenCredentialAttachesBearerToken(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "secret")

	md, err := TokenCredential{}.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	if got := md[AuthorizationHeaderName]; got != "Bearer secret" {
		t.Fatalf("%s = %q, want %q", AuthorizationHeaderName, got, "Bearer secret")
	}
	if (TokenCredential{}).RequireTransportSecurity() {
		t.Fatal("RequireTransportSecurity must be false: agents and heads talk over plaintext in-cluster")
	}
}

func TestTokenCredentialIsEmptyWhenAuthDisabled(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "")
	t.Setenv("RAY_AUTH_TOKEN", "secret")

	md, err := TokenCredential{}.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	if len(md) != 0 {
		t.Fatalf("metadata = %v, want none", md)
	}
}

func TestTokenCredentialRereadsRotatedTokenFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "")
	t.Setenv("RAY_AUTH_TOKEN_PATH", path)

	cred := TokenCredential{}
	first, err := cred.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if got := first[AuthorizationHeaderName]; got != "Bearer first" {
		t.Fatalf("first token = %q, want %q", got, "Bearer first")
	}

	if err := os.WriteFile(path, []byte("second\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	second, err := cred.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if got := second[AuthorizationHeaderName]; got != "Bearer second" {
		t.Fatalf("rotated token = %q, want %q (a cached credential authenticates with a stale value)", got, "Bearer second")
	}
}
