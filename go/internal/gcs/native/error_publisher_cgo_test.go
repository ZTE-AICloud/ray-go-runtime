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

//go:build cgo

package native

import (
	"context"
	"strings"
	"testing"

	"github.com/ray-project/ray/go/pkg/gcs"
)

func TestCgoClient_PublishErrorInfo_NilReceiver(t *testing.T) {
	var pub *cgoClient
	err := pub.PublishErrorInfo(context.Background(), "key", gcs.ErrorInfoPayload{Type: "raylet_died"})
	if err == nil {
		t.Fatal("expected error for nil receiver, got nil")
	}
	if !strings.Contains(err.Error(), "not initialized") {
		t.Errorf("expected 'not initialized' in error, got %q", err)
	}
}

func TestCgoClient_PublishErrorInfo_NullBackingClient(t *testing.T) {
	// A *cgoClient with a nil backing C client reaches the C++ boundary,
	// which rejects the null client and reports an error. This exercises the
	// fail path of publish across the cgo bridge without a live GCS cluster.
	pub := &cgoClient{}
	err := pub.PublishErrorInfo(context.Background(), "00000000000000000000000000000000",
		gcs.ErrorInfoPayload{JobID: make([]byte, 16), Type: "raylet_died", Message: "message"})
	if err == nil {
		t.Fatal("expected error from null backing client, got nil")
	}
	t.Logf("publish against null backing client failed as expected: %v", err)
}
