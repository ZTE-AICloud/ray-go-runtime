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

package runtime_env

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/log"
)

var pluginLogger = log.WithName("runtime-env-plugin")

// RuntimeEnvPlugin is the abstract interface for a runtime environment plugin.
//
// Every plugin must implement this interface.
type RuntimeEnvPlugin interface {
	// Name returns the plugin name.
	Name() string

	// Priority returns the plugin priority.
	Priority() int

	// Validate validates the user-provided runtime environment configuration.
	// It is called when the runtime environment is being set up and should
	// return an error if validation fails.
	Validate(runtimeEnv *RuntimeEnv) error

	// GetURIs returns the list of URIs associated with the plugin.
	GetURIs(runtimeEnv *RuntimeEnv) []string

	// Create creates and installs the runtime environment.
	// It is called by the runtime environment agent during setup. The uri
	// argument can be used as a cache mechanism. It returns the disk space (in
	// bytes) occupied by the plugin installation.
	Create(ctx context.Context, uri string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) (int64, error)

	// ModifyContext modifies the context to change the worker startup behavior.
	// For example, it can prepend a "cd <dir>" command to the worker startup
	// command or add new environment variables.
	ModifyContext(uris []string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) error

	// DeleteURI deletes the runtime environment for the given URI and returns
	// the freed space in bytes.
	DeleteURI(uri string) int64
}

// PluginSetupContext holds the plugin's name, instance, priority and URI cache.
type PluginSetupContext struct {
	Name          string
	ClassInstance RuntimeEnvPlugin
	Priority      int
	URICache      *URICache
}

// RuntimeEnvPluginManager loads and manages plugins in the runtime environment
// agent.
type RuntimeEnvPluginManager struct {
	plugins      map[string]*PluginSetupContext
	pluginsMutex sync.RWMutex
}

// PluginConfig mirrors an entry in the RAY_RUNTIME_ENV_PLUGINS JSON array:
// {"class": "my_pkg.MyPlugin", "priority": 20}.
type PluginConfig struct {
	Class    string `json:"class"`
	Priority *int   `json:"priority,omitempty"`
}

var (
	customPluginFactoriesMu sync.RWMutex
	customPluginFactories   = make(map[string]func() RuntimeEnvPlugin)
)

// RegisterCustomPluginFactory registers a Go-native RuntimeEnvPlugin factory
// keyed by its class path so RAY_RUNTIME_ENV_PLUGINS can instantiate it without
// spawning Python.
func RegisterCustomPluginFactory(classPath string, factory func() RuntimeEnvPlugin) {
	customPluginFactoriesMu.Lock()
	defer customPluginFactoriesMu.Unlock()
	if factory == nil {
		delete(customPluginFactories, classPath)
		return
	}
	customPluginFactories[classPath] = factory
}

func lookupCustomPluginFactory(classPath string) (func() RuntimeEnvPlugin, bool) {
	customPluginFactoriesMu.RLock()
	defer customPluginFactoriesMu.RUnlock()
	f, ok := customPluginFactories[classPath]
	return f, ok
}

// NewRuntimeEnvPluginManager creates a new plugin manager and loads any custom
// plugins configured in RAY_RUNTIME_ENV_PLUGINS.
func NewRuntimeEnvPluginManager() *RuntimeEnvPluginManager {
	manager := &RuntimeEnvPluginManager{
		plugins: make(map[string]*PluginSetupContext),
	}

	// Load the plugin configuration from the environment variable.
	if pluginConfigStr := strings.TrimSpace(os.Getenv(RayRuntimeEnvPluginsEnvVar)); pluginConfigStr != "" {
		if err := manager.LoadPluginsFromJSON(pluginConfigStr); err != nil {
			log.Log.Error(err, "Failed to load runtime env plugins from environment variable",
				"env", RayRuntimeEnvPluginsEnvVar)
		}
	}

	return manager
}

// LoadPluginsFromJSON parses a RAY_RUNTIME_ENV_PLUGINS JSON array and registers
// all configured plugins.
func (m *RuntimeEnvPluginManager) LoadPluginsFromJSON(raw string) error {
	var configs []PluginConfig
	if err := json.Unmarshal([]byte(raw), &configs); err != nil {
		return fmt.Errorf("invalid %s JSON: %w", RayRuntimeEnvPluginsEnvVar, err)
	}
	return m.LoadPlugins(configs)
}

// LoadPlugins instantiates, validates, and registers each configured plugin.
func (m *RuntimeEnvPluginManager) LoadPlugins(configs []PluginConfig) error {
	for _, cfg := range configs {
		classPath := strings.TrimSpace(cfg.Class)
		if classPath == "" {
			return fmt.Errorf("invalid runtime env plugin config: missing %q field", "class")
		}
		var plugin RuntimeEnvPlugin
		if factory, ok := lookupCustomPluginFactory(classPath); ok {
			plugin = factory()
		} else {
			p, err := newPythonBridgePlugin(classPath)
			if err != nil {
				return fmt.Errorf("load runtime env plugin %q: %w", classPath, err)
			}
			plugin = p
		}
		if err := m.ValidatePluginClass(plugin); err != nil {
			return err
		}
		priority := plugin.Priority()
		if cfg.Priority != nil {
			priority = *cfg.Priority
		}
		if err := m.ValidatePriority(priority); err != nil {
			return err
		}
		uriCache := m.CreateURICacheForPlugin(plugin)
		m.pluginsMutex.Lock()
		m.plugins[plugin.Name()] = &PluginSetupContext{
			Name:          plugin.Name(),
			ClassInstance: plugin,
			Priority:      priority,
			URICache:      uriCache,
		}
		m.pluginsMutex.Unlock()
	}
	return nil
}

// pythonBridgePlugin bridges a Python RuntimeEnvPlugin subclass into Go by
// invoking Python over JSON stdin/stdout for each lifecycle method.
type pythonBridgePlugin struct {
	classPath string
	name      string
	priority  int
	pythonBin string
}

var resolvePluginPython = func() string {
	if p := strings.TrimSpace(os.Getenv("RAY_PYTHON_EXECUTABLE")); p != "" {
		return p
	}
	if p, err := exec.LookPath("python3"); err == nil {
		return p
	}
	return "python"
}

const (
	pythonPluginOpTimeout   = 30 * time.Second
	pythonJSONSentinelBegin = "__RAY_GO_JSON_BEGIN__"
	pythonJSONSentinelEnd   = "__RAY_GO_JSON_END__"
)

func extractSentinelJSON(raw []byte) []byte {
	begin := []byte(pythonJSONSentinelBegin)
	end := []byte(pythonJSONSentinelEnd)
	if start := bytes.LastIndex(raw, begin); start >= 0 {
		rest := raw[start+len(begin):]
		if stop := bytes.Index(rest, end); stop >= 0 {
			return bytes.TrimSpace(rest[:stop])
		}
	}
	return bytes.TrimSpace(raw)
}

const pythonPluginBridgeScript = `
import asyncio, inspect, json, logging, sys
from ray._common.utils import import_attr
from ray._private.runtime_env.context import RuntimeEnvContext

def emit(obj):
    sys.stdout.write("__RAY_GO_JSON_BEGIN__" + json.dumps(obj) + "__RAY_GO_JSON_END__\n")
    sys.stdout.flush()

req = json.loads(sys.stdin.read())
cls = import_attr(req["class"])
op = req["op"]
if op == "meta":
    emit({"name": getattr(cls, "name", "") or "", "priority": int(getattr(cls, "priority", 10))})
    sys.exit(0)
inst = cls()
logger = logging.getLogger("runtime_env.plugin")
if op == "validate":
    inst.validate(req.get("runtime_env") or {})
    emit({"ok": True})
elif op == "get_uris":
    uris = inst.get_uris(req.get("runtime_env") or {}) or []
    emit({"uris": list(uris)})
elif op == "create":
    ctx_in = req.get("context") or {}
    ctx = RuntimeEnvContext(
        command_prefix=ctx_in.get("command_prefix") or [],
        env_vars=ctx_in.get("env_vars") or {},
        py_executable=ctx_in.get("py_executable") or None,
        override_worker_entrypoint=ctx_in.get("override_worker_entrypoint") or None,
        java_jars=ctx_in.get("java_jars") or [],
    )
    res = inst.create(req.get("uri") or "", req.get("runtime_env") or {}, ctx, logger)
    if inspect.isawaitable(res):
        res = asyncio.run(res)
    emit({
        "size_bytes": int(res or 0),
        "command_prefix": ctx.command_prefix,
        "env_vars": ctx.env_vars,
        "py_executable": ctx.py_executable or "",
        "override_worker_entrypoint": ctx.override_worker_entrypoint or "",
        "java_jars": ctx.java_jars or [],
    })
elif op == "modify_context":
    ctx_in = req.get("context") or {}
    ctx = RuntimeEnvContext(
        command_prefix=ctx_in.get("command_prefix") or [],
        env_vars=ctx_in.get("env_vars") or {},
        py_executable=ctx_in.get("py_executable") or None,
        override_worker_entrypoint=ctx_in.get("override_worker_entrypoint") or None,
        java_jars=ctx_in.get("java_jars") or [],
    )
    inst.modify_context(req.get("uris") or [], req.get("runtime_env") or {}, ctx, logger)
    emit({
        "command_prefix": ctx.command_prefix,
        "env_vars": ctx.env_vars,
        "py_executable": ctx.py_executable or "",
        "override_worker_entrypoint": ctx.override_worker_entrypoint or "",
        "java_jars": ctx.java_jars or [],
    })
elif op == "delete_uri":
    freed = inst.delete_uri(req.get("uri") or "", logger)
    emit({"freed_bytes": int(freed or 0)})
`

func newPythonBridgePlugin(classPath string) (*pythonBridgePlugin, error) {
	p := &pythonBridgePlugin{
		classPath: classPath,
		pythonBin: resolvePluginPython(),
	}
	var resp struct {
		Name     string `json:"name"`
		Priority int    `json:"priority"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), pythonPluginOpTimeout)
	defer cancel()
	if err := p.call(ctx, map[string]any{"op": "meta", "class": classPath}, &resp); err != nil {
		return nil, err
	}
	p.name = resp.Name
	p.priority = resp.Priority
	return p, nil
}

func (p *pythonBridgePlugin) call(ctx context.Context, payload map[string]any, out any) error {
	rawIn, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, p.pythonBin, "-c", pythonPluginBridgeScript)
	cmd.Stdin = bytes.NewReader(rawIn)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("python plugin %s op=%v failed: %w: %s", p.classPath, payload["op"], err, strings.TrimSpace(stderr.String()))
	}
	if out != nil {
		if err := json.Unmarshal(extractSentinelJSON(stdout.Bytes()), out); err != nil {
			return fmt.Errorf("decode python plugin %s response: %w", p.classPath, err)
		}
	}
	return nil
}

func (p *pythonBridgePlugin) Name() string  { return p.name }
func (p *pythonBridgePlugin) Priority() int { return p.priority }

func (p *pythonBridgePlugin) Validate(runtimeEnv *RuntimeEnv) error {
	ctx, cancel := context.WithTimeout(context.Background(), pythonPluginOpTimeout)
	defer cancel()
	return p.call(ctx, map[string]any{
		"op":          "validate",
		"class":       p.classPath,
		"runtime_env": runtimeEnv,
	}, nil)
}

func (p *pythonBridgePlugin) GetURIs(runtimeEnv *RuntimeEnv) []string {
	var resp struct {
		URIs []string `json:"uris"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), pythonPluginOpTimeout)
	defer cancel()
	if err := p.call(ctx, map[string]any{
		"op":          "get_uris",
		"class":       p.classPath,
		"runtime_env": runtimeEnv,
	}, &resp); err != nil {
		log.Log.Error(err, "python plugin GetURIs failed", "class", p.classPath)
		return nil
	}
	return resp.URIs
}

func serializeContextForPython(envCtx *RuntimeEnvContext) map[string]any {
	ctxMap := map[string]any{}
	if envCtx != nil {
		ctxMap["command_prefix"] = envCtx.CommandPrefix
		ctxMap["env_vars"] = envCtx.EnvVars
		ctxMap["py_executable"] = envCtx.PyExecutable
		ctxMap["override_worker_entrypoint"] = envCtx.OverrideWorkerEntrypoint
		ctxMap["java_jars"] = envCtx.JavaJars
	}
	return ctxMap
}

func (p *pythonBridgePlugin) Create(ctx context.Context, uri string, runtimeEnv *RuntimeEnv, envCtx *RuntimeEnvContext) (int64, error) {
	var resp struct {
		SizeBytes                int64             `json:"size_bytes"`
		CommandPrefix            []string          `json:"command_prefix"`
		EnvVars                  map[string]string `json:"env_vars"`
		PyExecutable             string            `json:"py_executable"`
		OverrideWorkerEntrypoint string            `json:"override_worker_entrypoint"`
		JavaJars                 []string          `json:"java_jars"`
	}
	if err := p.call(ctx, map[string]any{
		"op":          "create",
		"class":       p.classPath,
		"uri":         uri,
		"runtime_env": runtimeEnv,
		"context":     serializeContextForPython(envCtx),
	}, &resp); err != nil {
		return 0, err
	}
	if envCtx != nil {
		envCtx.CommandPrefix = resp.CommandPrefix
		envCtx.EnvVars = resp.EnvVars
		envCtx.PyExecutable = resp.PyExecutable
		envCtx.OverrideWorkerEntrypoint = resp.OverrideWorkerEntrypoint
		envCtx.JavaJars = resp.JavaJars
	}
	return resp.SizeBytes, nil
}

func (p *pythonBridgePlugin) ModifyContext(uris []string, runtimeEnv *RuntimeEnv, envCtx *RuntimeEnvContext) error {
	var resp struct {
		CommandPrefix            []string          `json:"command_prefix"`
		EnvVars                  map[string]string `json:"env_vars"`
		PyExecutable             string            `json:"py_executable"`
		OverrideWorkerEntrypoint string            `json:"override_worker_entrypoint"`
		JavaJars                 []string          `json:"java_jars"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), pythonPluginOpTimeout)
	defer cancel()
	if err := p.call(ctx, map[string]any{
		"op":          "modify_context",
		"class":       p.classPath,
		"uris":        uris,
		"runtime_env": runtimeEnv,
		"context":     serializeContextForPython(envCtx),
	}, &resp); err != nil {
		return err
	}
	if envCtx != nil {
		envCtx.CommandPrefix = resp.CommandPrefix
		envCtx.EnvVars = resp.EnvVars
		envCtx.PyExecutable = resp.PyExecutable
		envCtx.OverrideWorkerEntrypoint = resp.OverrideWorkerEntrypoint
		envCtx.JavaJars = resp.JavaJars
	}
	return nil
}

func (p *pythonBridgePlugin) DeleteURI(uri string) int64 {
	var resp struct {
		FreedBytes int64 `json:"freed_bytes"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), pythonPluginOpTimeout)
	defer cancel()
	if err := p.call(ctx, map[string]any{
		"op":    "delete_uri",
		"class": p.classPath,
		"uri":   uri,
	}, &resp); err != nil {
		log.Log.Error(err, "python plugin DeleteURI failed", "class", p.classPath)
		return 0
	}
	return resp.FreedBytes
}

// ValidatePluginClass verifies that a plugin class is valid.
// The type check is automatic: if the argument type does not implement all the
// methods of the RuntimeEnvPlugin interface, the code will not compile.
//
// Therefore, this validation only checks conditions that are determined at
// runtime:
// 1. The plugin instance must not be nil.
// 2. The plugin name must not be empty.
// 3. The plugin name must not conflict with an already registered plugin.
func (m *RuntimeEnvPluginManager) ValidatePluginClass(plugin RuntimeEnvPlugin) error {
	if plugin == nil {
		return fmt.Errorf("invalid runtime env plugin: nil")
	}

	name := plugin.Name()
	if name == "" {
		return fmt.Errorf("no valid name in runtime env plugin: %T", plugin)
	}

	m.pluginsMutex.RLock()
	defer m.pluginsMutex.RUnlock()
	if _, exists := m.plugins[name]; exists {
		return fmt.Errorf("the name of runtime env plugin %T conflicts with existing plugin %s",
			plugin, m.plugins[name].Name)
	}

	return nil
}

// ValidatePriority verifies that a priority is valid.
// The priority should be an integer between 0 and 100.
func (m *RuntimeEnvPluginManager) ValidatePriority(priority int) error {
	if priority < RayRuntimeEnvPluginMinPriority || priority > RayRuntimeEnvPluginMaxPriority {
		return fmt.Errorf("invalid runtime env priority %d, it should be an integer between %d and %d",
			priority, RayRuntimeEnvPluginMinPriority, RayRuntimeEnvPluginMaxPriority)
	}
	return nil
}

// AddPlugin adds a single plugin to the manager and creates its URI cache.
func (m *RuntimeEnvPluginManager) AddPlugin(plugin RuntimeEnvPlugin) error {
	if err := m.ValidatePluginClass(plugin); err != nil {
		return err
	}

	priority := plugin.Priority()
	if err := m.ValidatePriority(priority); err != nil {
		return err
	}

	uriCache := m.CreateURICacheForPlugin(plugin)

	m.pluginsMutex.Lock()
	defer m.pluginsMutex.Unlock()
	m.plugins[plugin.Name()] = &PluginSetupContext{
		Name:          plugin.Name(),
		ClassInstance: plugin,
		Priority:      priority,
		URICache:      uriCache,
	}

	return nil
}

// CreateURICacheForPlugin creates a URI cache for a plugin.
func (m *RuntimeEnvPluginManager) CreateURICacheForPlugin(plugin RuntimeEnvPlugin) *URICache {
	// The default cache size is 10 GB.
	cacheSizeEnvVar := fmt.Sprintf("RAY_RUNTIME_ENV_%s_CACHE_SIZE_GB", plugin.Name())
	size := common.EnvFloat(cacheSizeEnvVar, float64(DefaultMaxURICacheSizeBytes)/(1024*1024*1024))
	cacheSizeBytes := int64((1024 * 1024 * 1024) * size)

	return NewURICache(
		WithDeleteFn(plugin.DeleteURI),
		WithMaxTotalSizeBytes(cacheSizeBytes),
	)
}

// SortedPluginSetupContexts returns the plugin setup contexts sorted by
// priority in ascending order.
func (m *RuntimeEnvPluginManager) SortedPluginSetupContexts() []*PluginSetupContext {
	m.pluginsMutex.RLock()
	defer m.pluginsMutex.RUnlock()
	contexts := slices.SortedFunc(maps.Values(m.plugins), func(a, b *PluginSetupContext) int {
		return a.Priority - b.Priority
	})
	return contexts
}

// GetPlugin returns the plugin with the given name.
func (m *RuntimeEnvPluginManager) GetPlugin(name string) (*PluginSetupContext, bool) {
	m.pluginsMutex.RLock()
	defer m.pluginsMutex.RUnlock()
	plugin, exists := m.plugins[name]
	return plugin, exists
}

// GetPlugins returns all plugins.
func (m *RuntimeEnvPluginManager) GetPlugins() map[string]*PluginSetupContext {
	m.pluginsMutex.RLock()
	defer m.pluginsMutex.RUnlock()
	pluginsCopy := make(map[string]*PluginSetupContext, len(m.plugins))
	for k, v := range m.plugins {
		pluginsCopy[k] = v
	}
	return pluginsCopy
}

// PluginCreateContext wraps the arguments of CreateForPluginIfNeeded to avoid
// argument sprawl.
type PluginCreateContext struct {
	Ctx        context.Context
	RuntimeEnv *RuntimeEnv
	Plugin     RuntimeEnvPlugin
	URICache   *URICache
	Context    *RuntimeEnvContext
}

// CreateForPluginIfNeeded sets up the plugin environment if needed (if it has
// not been set up and is not cached).
func CreateForPluginIfNeeded(createCtx *PluginCreateContext) error {
	ctx := createCtx.Ctx
	runtimeEnv := createCtx.RuntimeEnv
	plugin := createCtx.Plugin
	uriCache := createCtx.URICache
	context := createCtx.Context

	// Check whether the plugin is present in the runtimeEnv.
	name := plugin.Name()

	if runtimeEnv == nil || runtimeEnv.Get(name, nil) == nil {
		return nil
	}

	// Validate the plugin configuration.
	if err := plugin.Validate(runtimeEnv); err != nil {
		return fmt.Errorf("plugin %s validation failed: %w", name, err)
	}

	// Get the URIs.
	uris := plugin.GetURIs(runtimeEnv)

	if len(uris) == 0 {
		log.Log.V(1).Info("No URIs for runtime env plugin; create always without checking the cache",
			"plugin", name)
		if _, err := plugin.Create(ctx, "", runtimeEnv, context); err != nil {
			return fmt.Errorf("plugin %s create failed: %w", name, err)
		}
	} else {
		for _, uri := range uris {
			if !uriCache.Contains(uri) {
				log.Log.V(1).Info("Cache miss for URI", "uri", uri)
				sizeBytes, err := plugin.Create(ctx, uri, runtimeEnv, context)
				if err != nil {
					return fmt.Errorf("plugin %s create failed for URI %s: %w", name, uri, err)
				}
				uriCache.Add(uri, sizeBytes)
			} else {
				log.Log.Info("Runtime env is already installed and will be reused",
					"plugin", name, "uri", uri)
				uriCache.MarkUsed(uri)
			}
		}
	}

	// Modify the context.
	if err := plugin.ModifyContext(uris, runtimeEnv, context); err != nil {
		return fmt.Errorf("plugin %s modifyContext failed: %w", name, err)
	}

	return nil
}
