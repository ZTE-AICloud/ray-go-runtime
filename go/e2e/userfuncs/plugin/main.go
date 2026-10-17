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

// Package plugin is the entry point for the userfuncs plugin.
// It imports userfuncs and calls RegisterFunctions() during initialization.
package main

import (
	"github.com/ray-project/ray/go/e2e/userfuncs"
)

// init is called when the plugin is loaded.
// It registers all user-defined functions with the runtime.
func init() {
	_ = userfuncs.RegisterFunctions()
}

func main() {
	// This is a plugin, not an executable.
	// The plugin runtime calls init() when the plugin is loaded.
}
