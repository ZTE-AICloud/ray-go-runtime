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

package logmonitor

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/log"
)

// File patterns scanned per directory. These are the log file families the
// Python, C++, Java, and Go core processes write, grouped as the collector treats them.
var (
	gcFilePatterns = []string{
		"job-driver-*.log*",
		"python-core-driver-*.log*",
		"python-core-worker-*.log*",
		"cpp-core-driver-*.log*",
		"cpp-core-worker-*.log*",
		"go-core-driver-*.log*",
		"go-core-worker-*.log*",
		"ray_go_*.log*",
		"runtime_env_setup-*.log*",
		"worker-*.out*",
		"worker-*.err*",
		"java-worker-*.log*",
	}
	serveFilePatterns = []string{
		"controller_*.log*",
		"controller_build_*.log*",
		"replica_*.log*",
		"proxy_*.log*",
		"mm_*.log*",
	}
	eventFilePatterns       = []string{"event_*.log*"}
	exportEventFilePatterns = []string{"event_EXPORT_*.log*"}
)

// defaultMaxValidGCPID is the exclusive upper bound fallback for candidate PIDs
// (Linux 64-bit PID_MAX_LIMIT = 4194304). Real systems can exceed 1,000,000,
// while timestamps (10-digit epoch seconds) remain far above this bound.
const defaultMaxValidGCPID = 4194304 + 1

var (
	detectedMaxPIDOnce sync.Once
	detectedMaxPID     = defaultMaxValidGCPID
)

// maxValidPIDFn returns the exclusive upper bound for valid process PIDs.
// Overridable in tests.
var maxValidPIDFn = func() int {
	detectedMaxPIDOnce.Do(func() {
		raw, err := os.ReadFile("/proc/sys/kernel/pid_max")
		if err != nil {
			return
		}
		v, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err == nil && v > 0 {
			// Accept up to v (inclusive), so exclusive bound is v+1, floored at
			// defaultMaxValidGCPID so cross-host log fixtures with PIDs up to
			// 4194304 are also accepted.
			if v+1 > detectedMaxPID {
				detectedMaxPID = v + 1
			}
		}
	})
	return detectedMaxPID
}

// PID extraction patterns, one per log file family. The capture group index
// is the group holding the PID; it differs per pattern because the families
// place the PID in different positions.
var gcPIDPatterns = []struct {
	re    *regexp.Regexp
	group int
}{
	{regexp.MustCompile(`python-core-driver-[0-9a-f_]+_(\d+)\.log`), 1},
	{regexp.MustCompile(`python-core-worker-[0-9a-f_]+_(\d+)\.log`), 1},
	{regexp.MustCompile(`cpp-core-driver-[0-9a-f_]+_(\d+)\.log`), 1},
	{regexp.MustCompile(`cpp-core-worker-[0-9a-f_]+_(\d+)\.log`), 1},
	{regexp.MustCompile(`go-core-driver-[0-9a-f_]+_(\d+)\.log`), 1},
	{regexp.MustCompile(`go-core-worker-[0-9a-f_]+_(\d+)\.log`), 1},
	{regexp.MustCompile(`ray_go_(\d+)\.log`), 1},
	// The GC worker regex requires the filename to start with "worker"
	// (mirroring C++ ExtractCandidatePid), which is narrower than
	// patterns.go's workerLogPattern (.*worker.*, used for Monitor tracking).
	// The two are intentionally independent: changing the worker filename
	// format requires updating both.
	{regexp.MustCompile(`worker.*-([0-9a-f]+)-(\d+)`), 2},
	{regexp.MustCompile(`java-worker.*-(\d+)`), 1},
	{regexp.MustCompile(`controller_(\d+)\.log`), 1},
	{regexp.MustCompile(`controller_build_.*_(\d+)\.log`), 1},
	{regexp.MustCompile(`event_CORE_WORKER_(\d+)\.log`), 1},
	{regexp.MustCompile(`event_COMMON_(\d+)\.log`), 1},
	{regexp.MustCompile(`event_EXPORT_TASK_(\d+)\.log`), 1},
	{regexp.MustCompile(`replica_.*_(\d+)\.log`), 1},
	{regexp.MustCompile(`proxy_.*_(\d+)\.log`), 1},
	{regexp.MustCompile(`mm_.*_(\d+)\.log`), 1},
}

type gcDeleteResult int

const (
	gcRemoved gcDeleteResult = iota
	gcNotFound
	gcError
)

type gcCandidate struct {
	path  string
	mtime time.Time
	size  int64
	pid   *int // nil when no PID could be extracted
}

// GarbageCollector applies a two-tier log file deletion strategy:
//
//	Tier 1 -- process dead and mtime past retention -> delete
//	Tier 2 -- total size exceeds cap -> evict oldest (skipping live processes)
//
// All methods are intended to be called from the Monitor main loop; they are
// not thread-safe.
type GarbageCollector struct {
	logsDir       string
	isProcAlive   func(int) bool
	onFileDeleted func(string)
	gcInterval    time.Duration
	retention     time.Duration
	maxTotalBytes int64
	enabled       bool
	lastRun       time.Time
}

// NewGarbageCollector reads the GC configuration from the environment,
// mirroring the C++ RayConfig names so the same env vars control both
// implementations.
func NewGarbageCollector(logsDir string, isProcAlive func(int) bool,
	onFileDeleted func(string)) *GarbageCollector {
	intervalSec := common.EnvInteger("RAY_log_gc_interval_seconds", 300)
	retentionSec := common.EnvInteger("RAY_log_gc_retention_seconds", 604800)
	maxTotalBytes := common.EnvInt64("RAY_log_gc_max_total_bytes", 10737418240)

	g := &GarbageCollector{
		logsDir:       logsDir,
		isProcAlive:   isProcAlive,
		onFileDeleted: onFileDeleted,
		gcInterval:    time.Duration(intervalSec) * time.Second,
		retention:     time.Duration(retentionSec) * time.Second,
		maxTotalBytes: maxTotalBytes,
		enabled:       common.EnvBool("RAY_log_gc_enabled", true),
	}
	// Initialize lastRun far enough in the past so the first interval check
	// in MaybeCollect always passes.
	now := time.Now()
	g.lastRun = now.Add(-(g.gcInterval + time.Second))
	return g
}

// MaybeCollect runs a GC cycle only if gcInterval has elapsed since the last
// run; otherwise it is a no-op.
func (g *GarbageCollector) MaybeCollect() {
	if !g.enabled {
		return
	}

	now := time.Now()
	if now.Sub(g.lastRun) < g.gcInterval {
		return
	}

	g.lastRun = now
	g.CollectNow()
}

// CollectNow unconditionally runs a full GC cycle. Exposed for tests.
func (g *GarbageCollector) CollectNow() {
	allCandidates := make([]gcCandidate, 0, 16)
	appendFrom := func(dir string, patterns []string) {
		allCandidates = append(allCandidates, g.scanDirectory(dir, patterns)...)
	}

	appendFrom(g.logsDir, gcFilePatterns)
	appendFrom(filepath.Join(g.logsDir, "old"), gcFilePatterns)
	appendFrom(filepath.Join(g.logsDir, "serve"), serveFilePatterns)
	appendFrom(filepath.Join(g.logsDir, "events"), eventFilePatterns)
	appendFrom(filepath.Join(g.logsDir, "export_events"), exportEventFilePatterns)

	survivors := g.runTier1(allCandidates)
	g.runTier2(survivors)
}

// scanDirectory collects candidates in dirPath whose base name matches any of
// the patterns. Missing directories and stat failures are skipped. Only regular
// files are collected, mirroring the C++ DT_REG glob.
func (g *GarbageCollector) scanDirectory(dirPath string, patterns []string) []gcCandidate {
	var candidates []gcCandidate

	entries, err := readDirEntries(dirPath)
	if err != nil {
		return candidates
	}

	for _, e := range entries {
		if !matchesAnyPattern(e.name, patterns) {
			continue
		}
		// Skip non-regular entries (including symlinks) without Lstat when
		// dirent d_type is known.
		if e.typ != 0 && !e.typ.IsRegular() {
			continue
		}
		// Lstat, not Stat: a symlinked log entry is left alone rather than
		// followed to whatever it points at. Deleting through a link would
		// remove a file this collector never scanned, so the deviation from
		// the reference implementation's stat-based glob is deliberate.
		fileInfo, err := os.Lstat(e.path)
		if err != nil || !fileInfo.Mode().IsRegular() {
			continue
		}
		candidates = append(candidates, gcCandidate{
			path:  e.path,
			mtime: fileInfo.ModTime(),
			size:  fileInfo.Size(),
			pid:   extractCandidatePID(e.name),
		})
	}
	return candidates
}

func matchesAnyPattern(name string, patterns []string) bool {
	for _, pattern := range patterns {
		if matchesPath(pattern, name) {
			return true
		}
	}
	return false
}

// extractCandidatePID returns the PID embedded in a log filename, or nil if
// none matches or the value is out of the valid range (0 < pid < maxValidPIDFn()).
// job-driver-* and runtime_env_setup-* files carry no PID.
func extractCandidatePID(filename string) *int {
	maxPID := maxValidPIDFn()
	for _, pp := range gcPIDPatterns {
		match := pp.re.FindStringSubmatch(filename)
		if match == nil {
			continue
		}
		pid, err := strconv.Atoi(match[pp.group])
		if err != nil {
			return nil
		}
		if pid > 0 && pid < maxPID {
			return &pid
		}
	}
	return nil
}

func (g *GarbageCollector) runTier1(candidates []gcCandidate) []gcCandidate {
	now := time.Now()

	survivors := make([]gcCandidate, 0, len(candidates))
	for _, c := range candidates {
		shouldDelete := false

		if c.pid != nil {
			if !g.isProcAlive(*c.pid) && now.Sub(c.mtime) > g.retention {
				shouldDelete = true
			}
		} else {
			shouldDelete = now.Sub(c.mtime) > g.retention
		}

		if shouldDelete {
			g.deleteLogFile(c.path, "Tier1")
		} else {
			survivors = append(survivors, c)
		}
	}
	return survivors
}

func (g *GarbageCollector) runTier2(survivors []gcCandidate) {
	var totalSize int64
	for _, c := range survivors {
		totalSize += c.size
	}

	if totalSize <= g.maxTotalBytes {
		return
	}

	sort.Slice(survivors, func(i, j int) bool {
		return survivors[i].mtime.Before(survivors[j].mtime)
	})

	for _, c := range survivors {
		if totalSize <= g.maxTotalBytes {
			break
		}

		if c.pid != nil && g.isProcAlive(*c.pid) {
			continue
		}

		result := g.deleteLogFile(c.path, "Tier2")
		if result == gcRemoved || result == gcNotFound {
			totalSize -= c.size
		}
	}
}

func (g *GarbageCollector) deleteLogFile(path, tierTag string) gcDeleteResult {
	err := os.Remove(path)
	if err == nil {
		if g.onFileDeleted != nil {
			g.onFileDeleted(path)
		}
		return gcRemoved
	}
	if os.IsNotExist(err) {
		return gcNotFound
	}
	log.WithName("logmonitor").Error(err, "GC failed to remove log file",
		"tier", tierTag, "path", path)
	return gcError
}
