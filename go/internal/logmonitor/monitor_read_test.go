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
	"strings"
	"testing"
)

func appendLog(t *testing.T, path, contents string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open append file: %v", err)
	}
	defer file.Close()
	if _, err := file.WriteString(contents); err != nil {
		t.Fatalf("append file: %v", err)
	}
}

func TestCheckLogFilesAndPublishUpdatesParity(t *testing.T) {
	logsDir := t.TempDir()
	publisher := &recordingPublisher{}
	logPath := createLogFile(t, logsDir, "worker-6df6d5dd-01000000-47660.out", "First line\n")

	m, err := New("127.0.0.1", logsDir, publisher)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("openClosedFiles() error = %v", err)
	}

	published, err := m.checkLogFilesAndPublishUpdates()
	if err != nil {
		t.Fatalf("checkLogFilesAndPublishUpdates() error = %v", err)
	}
	if !published {
		t.Fatal("expected initial content to be published")
	}
	if got := len(publisher.batches); got != 1 {
		t.Fatalf("len(batches) = %d, want 1", got)
	}
	if publisher.batches[0].Lines[0] != "First line" {
		t.Fatalf("first batch line = %q, want %q", publisher.batches[0].Lines[0], "First line")
	}

	appendLog(t, logPath, logPrefixTaskName+"task\nline\n")
	published, err = m.checkLogFilesAndPublishUpdates()
	if err != nil {
		t.Fatalf("checkLogFilesAndPublishUpdates() second error = %v", err)
	}
	if !published {
		t.Fatal("expected appended content to be published")
	}

	last := publisher.batches[len(publisher.batches)-1]
	if last.TaskName != "task" {
		t.Fatalf("task name = %q, want %q", last.TaskName, "task")
	}
	if len(last.Lines) != 1 || last.Lines[0] != "line" {
		t.Fatalf("lines = %#v, want []string{\"line\"}", last.Lines)
	}
}

func TestMaxLinesPerReadParity(t *testing.T) {
	logsDir := t.TempDir()
	publisher := &recordingPublisher{}
	logPath := createLogFile(t, logsDir, "raylet.err", "")

	m, err := New("127.0.0.1", logsDir, publisher)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	m.maxLinesPerRead = 3

	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("openClosedFiles() error = %v", err)
	}

	appendLog(t, logPath, "1\n2\n3\n4\n5\n")
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("second openClosedFiles() error = %v", err)
	}

	// The first poll must stop at the per-read cap.
	published, err := m.checkLogFilesAndPublishUpdates()
	if err != nil {
		t.Fatalf("checkLogFilesAndPublishUpdates() error = %v", err)
	}
	if !published {
		t.Fatal("expected lines to be published")
	}
	last := publisher.batches[len(publisher.batches)-1]
	if len(last.Lines) != 3 {
		t.Fatalf("len(lines) = %d, want 3", len(last.Lines))
	}
	if last.Lines[0] != "1" || last.Lines[1] != "2" || last.Lines[2] != "3" {
		t.Fatalf("lines = %#v, want [1 2 3]", last.Lines)
	}
	if got := m.openFiles[0].filePosition; got != int64(len("1\n2\n3\n")) {
		t.Fatalf("filePosition = %d, want %d; unconsumed buffered lines must not advance the recorded position", got, len("1\n2\n3\n"))
	}

	// Lines beyond the cap must still arrive on later polls instead of
	// being skipped.
	published, err = m.checkLogFilesAndPublishUpdates()
	if err != nil {
		t.Fatalf("second checkLogFilesAndPublishUpdates() error = %v", err)
	}
	if !published {
		t.Fatal("expected lines left beyond the cap to be published on the next poll")
	}
	last = publisher.batches[len(publisher.batches)-1]
	if len(last.Lines) != 2 || last.Lines[0] != "4" || last.Lines[1] != "5" {
		t.Fatalf("lines = %#v, want [4 5]", last.Lines)
	}

	total := 0
	for _, batch := range publisher.batches {
		total += len(batch.Lines)
	}
	if total != 5 {
		t.Fatalf("total published lines = %d, want 5", total)
	}
}

func TestCheckLogFilesAndPublishUpdatesReusesReader(t *testing.T) {
	logsDir := t.TempDir()
	publisher := &recordingPublisher{}
	createLogFile(t, logsDir, "raylet.err", "line1\nline2\n")

	m, err := New("127.0.0.1", logsDir, publisher)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	m.maxLinesPerRead = 1
	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("openClosedFiles() error = %v", err)
	}

	// Each pass reads at most 1 line; the reader must be reused so no line is
	// lost across iterations (the old per-pass bufio.Reader dropped buffered
	// lines when the OS offset ran ahead of consumed bytes).
	for i := 0; i < 2; i++ {
		if _, err := m.checkLogFilesAndPublishUpdates(); err != nil {
			t.Fatalf("checkLogFilesAndPublishUpdates() pass %d error = %v", i, err)
		}
	}
	if got := len(publisher.batches); got != 2 {
		t.Fatalf("len(batches) = %d, want 2 (line1 then line2)", got)
	}
	if publisher.batches[0].Lines[0] != "line1" || publisher.batches[1].Lines[0] != "line2" {
		t.Fatalf("batches = %#v, want [[line1] [line2]]", publisher.batches)
	}
}

func TestCheckLogFilesAndPublishUpdatesTruncatesLongLine(t *testing.T) {
	logsDir := t.TempDir()
	publisher := &recordingPublisher{}
	logPath := createLogFile(t, logsDir, "worker-6df6d5dd-01000000-47660.out", "")

	m, err := New("127.0.0.1", logsDir, publisher)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	m.maxLineSize = 16
	m.maxLinesPerRead = 10

	appendLog(t, logPath, strings.Repeat("x", 64)+"\nshort\n")
	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("openClosedFiles() error = %v", err)
	}
	if _, err := m.checkLogFilesAndPublishUpdates(); err != nil {
		t.Fatalf("checkLogFilesAndPublishUpdates() error = %v", err)
	}

	if len(publisher.batches) != 1 || len(publisher.batches[0].Lines) != 2 {
		t.Fatalf("batches = %#v, want one batch with two lines", publisher.batches)
	}
	if got := publisher.batches[0].Lines[0]; len(got) != 16 {
		t.Fatalf("long line truncated to %d bytes, want 16", len(got))
	}
	if got := publisher.batches[0].Lines[1]; got != "short" {
		t.Fatalf("second line = %q, want %q", got, "short")
	}
}

func TestCheckLogFilesAndPublishUpdatesFlushesOnByteLimit(t *testing.T) {
	logsDir := t.TempDir()
	publisher := &recordingPublisher{}
	logPath := createLogFile(t, logsDir, "worker-6df6d5dd-01000000-47660.out", "")

	m, err := New("127.0.0.1", logsDir, publisher)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	m.maxLineSize = 1024
	m.maxLinesPerRead = 100
	m.maxBatchBytes = 64

	// Three 40-byte lines: the second line pushes the pending batch over the
	// 64-byte cap and must flush the first two lines early; the third line is
	// then flushed at the end of the pass.
	appendLog(t, logPath,
		strings.Repeat("x", 40)+"\n"+
			strings.Repeat("y", 40)+"\n"+
			strings.Repeat("z", 40)+"\n")
	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("openClosedFiles() error = %v", err)
	}
	if _, err := m.checkLogFilesAndPublishUpdates(); err != nil {
		t.Fatalf("checkLogFilesAndPublishUpdates() error = %v", err)
	}

	if len(publisher.batches) != 2 {
		t.Fatalf("len(batches) = %d, want 2 (flushed on byte cap)", len(publisher.batches))
	}
	if got := publisher.batches[0].Lines; len(got) != 2 ||
		got[0] != strings.Repeat("x", 40) || got[1] != strings.Repeat("y", 40) {
		t.Fatalf("first batch = %#v, want [40 x's, 40 y's]", got)
	}
	if got := publisher.batches[1].Lines; len(got) != 1 || got[0] != strings.Repeat("z", 40) {
		t.Fatalf("second batch = %#v, want [40 z's]", got)
	}
}
