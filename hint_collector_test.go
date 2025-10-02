package nutsdb

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

type mockHintWriter struct {
	entries []*HintEntry
	synced  int
	closed  bool
}

func (m *mockHintWriter) Create(string) error { return nil }

func (m *mockHintWriter) Write(entry *HintEntry) error {
	clone := *entry
	if len(entry.Key) > 0 {
		clone.Key = append([]byte(nil), entry.Key...)
	}
	m.entries = append(m.entries, &clone)
	return nil
}

func (m *mockHintWriter) Sync() error {
	m.synced++
	return nil
}

func (m *mockHintWriter) Close() error {
	m.closed = true
	return nil
}

func TestHintCollectorFlushEvery(t *testing.T) {
	writer := &mockHintWriter{}
	collector := &HintCollector{writer: writer, buf: make([]HintEntry, 0, 2), fileID: 42, flushEvery: 2}

	entry := &HintEntry{Key: []byte("foo")}
	if err := collector.Add(entry); err != nil {
		t.Fatalf("add returned error: %v", err)
	}
	if len(writer.entries) != 0 {
		t.Fatalf("expected no flush before threshold, got %d entries", len(writer.entries))
	}

	if err := collector.Add(&HintEntry{Key: []byte("bar")}); err != nil {
		t.Fatalf("add returned error: %v", err)
	}

	if len(writer.entries) != 2 {
		t.Fatalf("expected 2 flushed entries, got %d", len(writer.entries))
	}

	if !bytes.Equal(writer.entries[0].Key, []byte("foo")) || writer.entries[0].FileID != 42 {
		t.Fatalf("unexpected first entry: %+v", writer.entries[0])
	}
	if !bytes.Equal(writer.entries[1].Key, []byte("bar")) || writer.entries[1].FileID != 42 {
		t.Fatalf("unexpected second entry: %+v", writer.entries[1])
	}
	if writer.synced != 1 {
		t.Fatalf("expected 1 sync, got %d", writer.synced)
	}
}

func TestHintCollectorFlushAndClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.hint")
	writer := &HintFileWriter{}
	if err := writer.Create(path); err != nil {
		t.Fatalf("create writer: %v", err)
	}

	collector := NewHintCollector(7, writer, 2)

	if err := collector.Add(&HintEntry{Key: []byte("a")}); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := collector.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	if err := collector.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read hint file: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("hint file empty")
	}
}
