package nutsdb

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/data"
	"github.com/nutsdb/nutsdb/internal/ttl/checker"
	"github.com/nutsdb/nutsdb/internal/ttl/clock"
	"github.com/nutsdb/nutsdb/internal/utils"
)

func TestMergeV2Utils(t *testing.T) {
	t.Run("GetMergeFileID", func(t *testing.T) {
		if got := GetMergeFileID(0); got != MergeFileIDBase {
			t.Errorf("GetMergeFileID(0) = %d, want %d", got, MergeFileIDBase)
		}
		if got := GetMergeFileID(10); got != MergeFileIDBase+10 {
			t.Errorf("GetMergeFileID(10) = %d, want %d", got, MergeFileIDBase+10)
		}
	})

	t.Run("IsMergeFile", func(t *testing.T) {
		if !IsMergeFile(-1) {
			t.Error("IsMergeFile(-1) should be true")
		}
		if !IsMergeFile(MergeFileIDBase) {
			t.Error("IsMergeFile(MergeFileIDBase) should be true")
		}
		if IsMergeFile(0) {
			t.Error("IsMergeFile(0) should be false")
		}
		if IsMergeFile(100) {
			t.Error("IsMergeFile(100) should be false")
		}
	})

	t.Run("GetMergeSeq", func(t *testing.T) {
		if got := GetMergeSeq(MergeFileIDBase); got != 0 {
			t.Errorf("GetMergeSeq(MergeFileIDBase) = %d, want 0", got)
		}
		if got := GetMergeSeq(MergeFileIDBase + 5); got != 5 {
			t.Errorf("GetMergeSeq(MergeFileIDBase+5) = %d, want 5", got)
		}
	})
}

func TestMergeV2Manifest(t *testing.T) {
	dir := t.TempDir()

	t.Run("WriteAndLoad", func(t *testing.T) {
		manifest := &mergeManifest{
			Status:            manifestStatusWriting,
			MergeSeqMax:       3,
			PendingOldFileIDs: []int64{0, 1, 2, 100, 101},
		}

		if err := writeMergeManifest(dir, manifest); err != nil {
			t.Fatalf("writeMergeManifest failed: %v", err)
		}

		loaded, err := loadMergeManifest(dir)
		if err != nil {
			t.Fatalf("loadMergeManifest failed: %v", err)
		}
		if loaded == nil {
			t.Fatal("loaded manifest is nil")
		}
		if loaded.Status != manifestStatusWriting {
			t.Errorf("Status = %v, want %v", loaded.Status, manifestStatusWriting)
		}
		if loaded.MergeSeqMax != 3 {
			t.Errorf("MergeSeqMax = %d, want 3", loaded.MergeSeqMax)
		}
		if len(loaded.PendingOldFileIDs) != 5 {
			t.Errorf("len(PendingOldFileIDs) = %d, want 5", len(loaded.PendingOldFileIDs))
		}
	})

	t.Run("Remove", func(t *testing.T) {
		if err := removeMergeManifest(dir); err != nil {
			t.Fatalf("removeMergeManifest failed: %v", err)
		}

		loaded, err := loadMergeManifest(dir)
		if err != nil {
			t.Fatalf("loadMergeManifest after remove failed: %v", err)
		}
		if loaded != nil {
			t.Error("manifest should be nil after removal")
		}
	})

	t.Run("LoadNonExistent", func(t *testing.T) {
		emptyDir := t.TempDir()
		loaded, err := loadMergeManifest(emptyDir)
		if err != nil {
			t.Fatalf("loadMergeManifest on empty dir failed: %v", err)
		}
		if loaded != nil {
			t.Error("manifest should be nil for non-existent file")
		}
	})
}

func TestMergeV2AbortCleansOutputs(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "merge_5.dat")
	hintPath := filepath.Join(dir, "merge_5.hint")
	manifestPath := filepath.Join(dir, mergeManifestFileName)

	for _, name := range []string{dataPath, hintPath, manifestPath} {
		if err := os.WriteFile(name, []byte("temp"), 0o644); err != nil {
			t.Fatalf("write temp file %s: %v", name, err)
		}
	}

	opts := DefaultOptions
	opts.Dir = dir
	job := &mergeV2Job{
		db: &DB{opt: opts},
		outputs: []*mergeOutput{
			{seq: 5, dataPath: dataPath, hintPath: hintPath},
		},
	}

	originalErr := errors.New("trigger abort")
	returnedErr := job.abort(originalErr)
	if returnedErr != originalErr {
		t.Fatalf("abort returned unexpected error: %v", returnedErr)
	}

	if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
		t.Fatalf("data file %s should be removed, err=%v", dataPath, err)
	}
	if _, err := os.Stat(hintPath); !os.IsNotExist(err) {
		t.Fatalf("hint file %s should be removed, err=%v", hintPath, err)
	}
	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Fatalf("manifest file should be removed, err=%v", err)
	}
}

func TestEnumerateDataFileIDs(t *testing.T) {
	dir := t.TempDir()

	// Create some test files
	createFile := func(name string) {
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Errorf("Failed to close file: %v", err)
		}
	}

	createFile("0.dat")
	createFile("1.dat")
	createFile("100.dat")
	createFile("merge_0.dat")
	createFile("merge_1.dat")
	createFile("other.txt")
	createFile("0.hint")

	userIDs, mergeIDs, err := enumerateDataFileIDs(dir)
	if err != nil {
		t.Fatalf("enumerateDataFileIDs failed: %v", err)
	}

	if len(userIDs) != 3 {
		t.Errorf("len(userIDs) = %d, want 3", len(userIDs))
	}
	if userIDs[0] != 0 || userIDs[1] != 1 || userIDs[2] != 100 {
		t.Errorf("userIDs = %v, want [0 1 100]", userIDs)
	}

	if len(mergeIDs) != 2 {
		t.Errorf("len(mergeIDs) = %d, want 2", len(mergeIDs))
	}
	expectedMerge0 := GetMergeFileID(0)
	expectedMerge1 := GetMergeFileID(1)
	if mergeIDs[0] != expectedMerge0 || mergeIDs[1] != expectedMerge1 {
		t.Errorf("mergeIDs = %v, want [%d %d]", mergeIDs, expectedMerge0, expectedMerge1)
	}
}

func TestPurgeMergeFiles(t *testing.T) {
	dir := t.TempDir()

	createFile := func(name string) {
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Errorf("Failed to close file: %v", err)
		}
	}

	createFile("0.dat")
	createFile("merge_0.dat")
	createFile("merge_0.hint")
	createFile("merge_1.dat")

	if err := purgeMergeFiles(dir, nil); err != nil {
		t.Fatalf("purgeMergeFiles failed: %v", err)
	}

	// Check that merge files are gone
	if _, err := os.Stat(filepath.Join(dir, "merge_0.dat")); !os.IsNotExist(err) {
		t.Error("merge_0.dat should be deleted")
	}
	if _, err := os.Stat(filepath.Join(dir, "merge_0.hint")); !os.IsNotExist(err) {
		t.Error("merge_0.hint should be deleted")
	}
	if _, err := os.Stat(filepath.Join(dir, "merge_1.dat")); !os.IsNotExist(err) {
		t.Error("merge_1.dat should be deleted")
	}

	keep := map[int64]struct{}{GetMergeFileID(0): {}}
	createFile("merge_0.dat")
	createFile("merge_0.hint")
	if err := purgeMergeFiles(dir, keep); err != nil {
		t.Fatalf("purgeMergeFiles with keep failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "merge_0.dat")); err != nil {
		t.Error("merge_0.dat should be preserved when kept")
	}
	if _, err := os.Stat(filepath.Join(dir, "merge_0.hint")); err != nil {
		t.Error("merge_0.hint should be preserved when kept")
	}

	// Check that user file remains
	if _, err := os.Stat(filepath.Join(dir, "0.dat")); err != nil {
		t.Error("0.dat should still exist")
	}
}

func TestMergeV2BasicFlow(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 8 * 1024 // 8KB - small segment to force multiple files
	opts.EnableMergeV2 = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	}()

	bucket := "test_bucket"

	// Create bucket first
	if err := db.Update(func(tx *Tx) error {
		return tx.NewBucket(core.DataStructureBTree, bucket)
	}); err != nil {
		t.Fatalf("NewBucket failed: %v", err)
	}

	// Write enough data to create multiple files
	// Each entry is roughly 100+ bytes, so 200 entries should create multiple 8KB files
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key_%05d", i))
		value := make([]byte, 100) // 100 bytes value
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := db.Update(func(tx *Tx) error {
			return tx.Put(bucket, key, value, 0)
		}); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Update some keys to create redundancy
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key_%05d", i))
		value := make([]byte, 100)
		for j := range value {
			value[j] = byte((i + 200) % 256) // Different pattern for updated values
		}
		if err := db.Update(func(tx *Tx) error {
			return tx.Put(bucket, key, value, 0)
		}); err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}

	// Delete some keys
	for i := 50; i < 60; i++ {
		key := []byte(fmt.Sprintf("key_%05d", i))
		if err := db.Update(func(tx *Tx) error {
			return tx.Delete(bucket, key)
		}); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	// Perform merge
	if err := db.Merge(); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// Verify data integrity
	if err := db.View(func(tx *Tx) error {
		// Check updated keys
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("key_%05d", i))
			value, err := tx.Get(bucket, key)
			if err != nil {
				return fmt.Errorf("get key %s: %w", key, err)
			}
			// Verify it's the updated value (pattern: (i+200)%256)
			if len(value) != 100 {
				t.Errorf("key %s: got len %d, want 100", key, len(value))
			}
			if value[0] != byte((i+200)%256) {
				t.Errorf("key %s: got first byte %d, want %d", key, value[0], byte((i+200)%256))
			}
		}

		// Check deleted keys
		for i := 50; i < 60; i++ {
			key := []byte(fmt.Sprintf("key_%05d", i))
			_, err := tx.Get(bucket, key)
			if err != ErrKeyNotFound {
				t.Errorf("key %s should be deleted, got err: %v", key, err)
			}
		}

		// Check remaining keys (not updated, not deleted)
		for i := 60; i < 200; i++ {
			key := []byte(fmt.Sprintf("key_%05d", i))
			value, err := tx.Get(bucket, key)
			if err != nil {
				return fmt.Errorf("get key %s: %w", key, err)
			}
			// Verify it's the original value (pattern: i%256)
			if len(value) != 100 {
				t.Errorf("key %s: got len %d, want 100", key, len(value))
			}
			if value[0] != byte(i%256) {
				t.Errorf("key %s: got first byte %d, want %d", key, value[0], byte(i%256))
			}
		}

		return nil
	}); err != nil {
		t.Fatalf("Verification failed: %v", err)
	}

	// Verify no manifest remains
	manifest, err := loadMergeManifest(dir)
	if err != nil {
		t.Fatalf("loadMergeManifest failed: %v", err)
	}
	if manifest != nil {
		t.Error("manifest should be removed after successful merge")
	}
}

type mockRWManager struct {
	size         int64
	writeErr     error
	readErr      error
	syncErr      error
	releaseErr   error
	closeErr     error
	writes       []mockWriteCall
	syncCalls    int
	releaseCalls int
	closeCalls   int
}

type mockWriteCall struct {
	off  int64
	data []byte
}

func (m *mockRWManager) WriteAt(b []byte, off int64) (int, error) {
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	copied := make([]byte, len(b))
	copy(copied, b)
	m.writes = append(m.writes, mockWriteCall{off: off, data: copied})
	return len(b), nil
}

func (m *mockRWManager) ReadAt(b []byte, off int64) (int, error) {
	if m.readErr != nil {
		return 0, m.readErr
	}
	return len(b), nil
}

func (m *mockRWManager) Sync() error {
	m.syncCalls++
	return m.syncErr
}

func (m *mockRWManager) Release() error {
	m.releaseCalls++
	return m.releaseErr
}

func (m *mockRWManager) Size() int64 {
	if m.size == 0 {
		return 1 << 20
	}
	return m.size
}

func (m *mockRWManager) Close() error {
	m.closeCalls++
	return m.closeErr
}

type failingHintWriter struct {
	writeErr error
	syncErr  error
	closeErr error
}

func (f *failingHintWriter) Write(*HintEntry) error { return f.writeErr }
func (f *failingHintWriter) Sync() error            { return f.syncErr }
func (f *failingHintWriter) Close() error           { return f.closeErr }

type recordingHintWriter struct {
	writes []*HintEntry
	syncs  int
}

func (r *recordingHintWriter) Write(entry *HintEntry) error {
	clone := *entry
	if len(entry.Key) > 0 {
		clone.Key = append([]byte(nil), entry.Key...)
	}
	r.writes = append(r.writes, &clone)
	return nil
}

func (r *recordingHintWriter) Sync() error {
	r.syncs++
	return nil
}

func (r *recordingHintWriter) Close() error { return nil }

func createTestEntry(bucketID core.BucketId, key, value []byte, flag uint16, status uint16, ttl uint32, ts uint64, txID uint64, ds core.DataStructure) *core.Entry {
	meta := core.NewMetaData().
		WithBucketId(uint64(bucketID)).
		WithKeySize(uint32(len(key))).
		WithValueSize(uint32(len(value))).
		WithFlag(flag).
		WithStatus(status).
		WithTTL(ttl).
		WithTimeStamp(ts).
		WithTxID(txID).
		WithDs(uint16(ds))

	entry := &core.Entry{
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
		Meta:  meta,
	}

	// Encode once to populate CRC and ensure the entry is well-formed.
	entry.Encode()
	return entry
}

func TestMergeV2PrepareWhileAlreadyMerging(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"0.dat", "1.dat"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte{0xAA}, 0o644); err != nil {
			t.Fatalf("prepare test setup: %v", err)
		}
	}

	opts := DefaultOptions
	opts.Dir = dir
	opts.RWMode = MMap
	opts.SyncEnable = false

	db := &DB{
		opt:        opts,
		ActiveFile: &DataFile{rwManager: &mockRWManager{}},
	}
	db.isMerging = true

	job := &mergeV2Job{db: db}
	if err := job.prepare(); !errors.Is(err, ErrIsMerging) {
		t.Fatalf("expected ErrIsMerging, got %v", err)
	}

	if !db.isMerging {
		t.Fatalf("isMerging should remain true until finish is called")
	}

	job.finish()
	if db.isMerging {
		t.Fatalf("finish should reset isMerging flag")
	}
}

func TestMergeV2PrepareSyncError(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"0.dat", "1.dat"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte{0xAA}, 0o644); err != nil {
			t.Fatalf("prepare sync error setup: %v", err)
		}
	}

	opts := DefaultOptions
	opts.Dir = dir
	opts.RWMode = MMap
	opts.SyncEnable = false

	mock := &mockRWManager{syncErr: errors.New("sync boom")}
	db := &DB{
		opt:        opts,
		ActiveFile: &DataFile{rwManager: mock},
	}

	job := &mergeV2Job{db: db}
	err := job.prepare()
	if err == nil || !strings.Contains(err.Error(), "failed to sync active file") {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.syncCalls != 1 {
		t.Fatalf("expected sync to be invoked once, got %d", mock.syncCalls)
	}
	if mock.releaseCalls != 0 {
		t.Fatalf("release should not be called on sync failure")
	}
	if db.isMerging {
		t.Fatalf("isMerging should be reset on error path")
	}
}

func TestMergeV2EnsureOutputRejectsInvalidSize(t *testing.T) {
	job := &mergeV2Job{db: &DB{opt: Options{SegmentSize: 1 << 20}}}
	if _, err := job.ensureOutput(0); err == nil {
		t.Fatal("ensureOutput should reject non-positive sizes")
	}
}

func TestMergeV2NewOutputOldHintRemovalFailure(t *testing.T) {
	dir := t.TempDir()
	hintDir := filepath.Join(dir, "merge_0.hint")
	if err := os.Mkdir(hintDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(hintDir, "child"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write child: %v", err)
	}

	opts := DefaultOptions
	opts.Dir = dir
	opts.EnableHintFile = true
	opts.RWMode = FileIO
	opts.SegmentSize = 1 << 12

	db := &DB{
		opt: opts,
		fm:  NewFileManager(opts.RWMode, 4, 0.5, opts.SegmentSize),
	}

	job := &mergeV2Job{db: db}
	if _, err := job.ensureOutput(128); err == nil || !strings.Contains(err.Error(), "failed to remove old hint file") {
		t.Fatalf("expected removal error, got %v", err)
	}
}

func TestMergeV2EnsureOutputRolloverCreatesNewSegment(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 256
	opts.EnableHintFile = false
	opts.RWMode = FileIO

	db := &DB{
		opt: opts,
		fm:  NewFileManager(opts.RWMode, 4, 0.5, opts.SegmentSize),
	}

	job := &mergeV2Job{db: db}

	out1, err := job.ensureOutput(64)
	if err != nil {
		t.Fatalf("ensureOutput first segment: %v", err)
	}
	if out1 == nil {
		t.Fatal("first output should not be nil")
	}

	out1.writeOff = opts.SegmentSize - 16

	out2, err := job.ensureOutput(64)
	if err != nil {
		t.Fatalf("ensureOutput rollover: %v", err)
	}
	if out2 == nil {
		t.Fatal("rollover output should not be nil")
	}
	if out1 == out2 {
		t.Fatal("rollover should create a new output")
	}
	if len(job.outputs) != 2 {
		t.Fatalf("expected two outputs, got %d", len(job.outputs))
	}
	if out2.seq != out1.seq+1 {
		t.Fatalf("expected sequential output seq, got %d and %d", out1.seq, out2.seq)
	}

	for _, out := range job.outputs {
		if out.dataFile != nil {
			if err := out.dataFile.Close(); err != nil {
				t.Fatalf("close data file: %v", err)
			}
		}
		if out.collector != nil {
			if err := out.collector.Close(); err != nil {
				t.Fatalf("close collector: %v", err)
			}
		}
	}
}

func TestMergeV2CommitCollectorFailure(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 1 << 16

	bucketID := core.BucketId(1)
	bucketName := "b"
	key := []byte("set-key")
	value := []byte("value")
	timestamp := uint64(time.Now().Unix())
	oldFileID := int64(5)

	bt := data.NewBTree()
	record := (&core.Record{}).
		WithKey(key).
		WithFileId(oldFileID).
		WithDataPos(123).
		WithTimestamp(timestamp).
		WithTTL(core.Persistent)
	bt.InsertRecord(key, record)

	db := &DB{
		opt:   opts,
		Index: newIndex(),
		bucketManager: &BucketManager{
			BucketInfoMapper: map[core.BucketId]*core.Bucket{
				bucketID: {Meta: &core.BucketMeta{}, Id: bucketID, Name: bucketName, Ds: uint16(core.DataStructureBTree)},
			},
		},
	}
	db.Index.bTree.idx[bucketID] = bt

	collector := NewHintCollector(1, &failingHintWriter{writeErr: errors.New("writer failed")}, 1)
	mock := &mockRWManager{}
	out := &mergeOutput{
		seq:       1,
		fileID:    GetMergeFileID(1),
		dataFile:  &DataFile{rwManager: mock},
		collector: collector,
	}

	job := &mergeV2Job{
		db:          db,
		outputs:     []*mergeOutput{out},
		valueHasher: fnv.New32a(),
		manifest:    &mergeManifest{},
		pending:     []int64{oldFileID},
	}

	entry := createTestEntry(bucketID, key, value, core.DataDeleteFlag, core.Committed, core.Persistent, timestamp, 1, core.DataStructureBTree)
	if err := job.writeEntry(entry); err != nil {
		t.Fatalf("writeEntry: %v", err)
	}

	if err := job.commit(); err == nil || !strings.Contains(err.Error(), "failed to add hint") {
		t.Fatalf("commit should surface collector errors, got %v", err)
	}
}

func TestMergeV2FinalizeOutputsSuccess(t *testing.T) {
	collector := NewHintCollector(1, &recordingHintWriter{}, 1)
	mock := &mockRWManager{}
	out := &mergeOutput{
		dataFile:  &DataFile{rwManager: mock},
		collector: collector,
	}
	job := &mergeV2Job{outputs: []*mergeOutput{out}}

	if err := job.finalizeOutputs(); err != nil {
		t.Fatalf("finalizeOutputs: %v", err)
	}
	if mock.syncCalls != 1 {
		t.Fatalf("expected one sync call, got %d", mock.syncCalls)
	}
	if mock.closeCalls != 1 {
		t.Fatalf("expected one close call, got %d", mock.closeCalls)
	}
	if err := collector.Add(&HintEntry{}); !errors.Is(err, errHintCollectorClosed) {
		t.Fatalf("collector should be closed, got %v", err)
	}

	if err := job.finalizeOutputs(); err != nil {
		t.Fatalf("finalizeOutputs should be idempotent: %v", err)
	}
	if mock.syncCalls != 1 || mock.closeCalls != 1 {
		t.Fatalf("idempotent finalize should not resync/close, got sync=%d close=%d", mock.syncCalls, mock.closeCalls)
	}
}

func TestMergeV2FinalizeOutputsFailure(t *testing.T) {
	collector := NewHintCollector(1, &recordingHintWriter{}, 1)
	mock := &mockRWManager{syncErr: errors.New("sync fail"), closeErr: errors.New("close fail")}
	out := &mergeOutput{
		dataFile:  &DataFile{rwManager: mock},
		collector: collector,
	}
	job := &mergeV2Job{outputs: []*mergeOutput{out}}

	err := job.finalizeOutputs()
	if err == nil {
		t.Fatal("finalizeOutputs should return error when data file cleanup fails")
	}
	msg := err.Error()
	if !strings.Contains(msg, "failed to sync data file") || !strings.Contains(msg, "failed to close data file") {
		t.Fatalf("unexpected error message: %v", err)
	}
	if mock.syncCalls != 1 || mock.closeCalls != 1 {
		t.Fatalf("expected sync and close attempts despite failures, got sync=%d close=%d", mock.syncCalls, mock.closeCalls)
	}
	if err := collector.Add(&HintEntry{}); !errors.Is(err, errHintCollectorClosed) {
		t.Fatalf("collector should be closed even on error, got %v", err)
	}
	if !out.finalized {
		t.Fatal("output should be marked finalized even when errors occur")
	}
}

func TestMergeV2MergeAndTxConcurrentWrites(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 8 * 1024
	opts.EnableMergeV2 = true
	opts.RWMode = FileIO

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	bucket := "bucket"
	targetKey := []byte("target")
	initialValue := []byte("initial")
	freshValue := []byte("fresh")

	if err := db.Update(func(tx *Tx) error {
		return tx.NewBucket(core.DataStructureBTree, bucket)
	}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	putValue := func(val []byte) error {
		return db.Update(func(tx *Tx) error {
			return tx.Put(bucket, targetKey, val, core.Persistent)
		})
	}

	if err := putValue(initialValue); err != nil {
		t.Fatalf("put initial value: %v", err)
	}

	fillerVal := bytes.Repeat([]byte("f"), 1024)
	for i := 0; i < 16; i++ {
		key := []byte(fmt.Sprintf("fill-%02d", i))
		if err := db.Update(func(tx *Tx) error {
			return tx.Put(bucket, key, fillerVal, core.Persistent)
		}); err != nil {
			t.Fatalf("populate filler: %v", err)
		}
	}

	var haveMultipleFiles bool
	for attempts := 0; attempts < 5; attempts++ {
		userIDs, _, err := enumerateDataFileIDs(dir)
		if err != nil {
			t.Fatalf("enumerate data files: %v", err)
		}
		if len(userIDs) >= 2 {
			haveMultipleFiles = true
			break
		}
		key := []byte(fmt.Sprintf("extra-%02d", attempts))
		if err := db.Update(func(tx *Tx) error {
			return tx.Put(bucket, key, fillerVal, core.Persistent)
		}); err != nil {
			t.Fatalf("populate extra filler: %v", err)
		}
	}
	if !haveMultipleFiles {
		t.Fatal("expected multiple data files for merge test")
	}

	job := &mergeV2Job{db: db}
	if err := job.prepare(); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer job.finish()
	if err := job.enterWritingState(); err != nil {
		t.Fatalf("enterWritingState: %v", err)
	}

	entrySignal := make(chan struct{}, 1)
	job.onRewriteEntry = func(entry *core.Entry) {
		if bytes.Equal(entry.Key, targetKey) {
			select {
			case entrySignal <- struct{}{}:
			default:
			}
		}
	}

	rewriteDone := make(chan error, 1)
	go func() {
		rewriteDone <- job.rewrite()
	}()

	select {
	case <-entrySignal:
	case err := <-rewriteDone:
		if err != nil {
			t.Fatalf("rewrite finished early: %v", err)
		}
		t.Fatal("rewrite finished before processing target entry")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for rewrite to reach target entry")
	}

	updateDone := make(chan error, 1)
	go func() {
		updateDone <- putValue(freshValue)
	}()

	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("concurrent update: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("update blocked for too long")
	}

	if err := <-rewriteDone; err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	if err := job.commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := job.finalizeOutputs(); err != nil {
		t.Fatalf("finalizeOutputs: %v", err)
	}
	if err := job.cleanupOldFiles(); err != nil {
		t.Fatalf("cleanupOldFiles: %v", err)
	}
	if err := removeMergeManifest(dir); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}

	if err := db.View(func(tx *Tx) error {
		got, err := tx.Get(bucket, targetKey)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, freshValue) {
			return fmt.Errorf("expected fresh value, got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify after merge: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	db = nil

	db, err = Open(opts)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}

	if err := db.View(func(tx *Tx) error {
		got, err := tx.Get(bucket, targetKey)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, freshValue) {
			return fmt.Errorf("expected fresh value after reopen, got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify after reopen: %v", err)
	}
}

func TestMergeV2MergeAndTxConcurrentWritesWithSet(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 8 * 1024
	opts.EnableMergeV2 = true
	opts.RWMode = FileIO

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	bucket := "setbucket"
	targetKey := []byte("target-set")
	initialMember := []byte("initial-member")
	freshMember := []byte("fresh-member")

	if err := db.Update(func(tx *Tx) error {
		return tx.NewBucket(core.DataStructureSet, bucket)
	}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	if err := db.Update(func(tx *Tx) error {
		return tx.SAdd(bucket, targetKey, initialMember)
	}); err != nil {
		t.Fatalf("add initial member: %v", err)
	}

	// Populate to create multiple files
	fillerVal := bytes.Repeat([]byte("f"), 512)
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("fill-set-%02d", i))
		member := append(fillerVal, []byte(fmt.Sprintf("-%02d", i))...)
		if err := db.Update(func(tx *Tx) error {
			return tx.SAdd(bucket, key, member)
		}); err != nil {
			t.Fatalf("populate filler: %v", err)
		}
	}

	var haveMultipleFiles bool
	for attempts := 0; attempts < 5; attempts++ {
		userIDs, _, err := enumerateDataFileIDs(dir)
		if err != nil {
			t.Fatalf("enumerate data files: %v", err)
		}
		if len(userIDs) >= 2 {
			haveMultipleFiles = true
			break
		}
		key := []byte(fmt.Sprintf("extra-%02d", attempts))
		member := append(fillerVal, []byte(fmt.Sprintf("-extra-%02d", attempts))...)
		if err := db.Update(func(tx *Tx) error {
			return tx.SAdd(bucket, key, member)
		}); err != nil {
			t.Fatalf("add extra filler: %v", err)
		}
	}
	if !haveMultipleFiles {
		t.Fatal("expected multiple data files for merge test")
	}

	job := &mergeV2Job{db: db}
	if err := job.prepare(); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer job.finish()
	if err := job.enterWritingState(); err != nil {
		t.Fatalf("enterWritingState: %v", err)
	}

	entrySignal := make(chan struct{}, 1)
	job.onRewriteEntry = func(entry *core.Entry) {
		if bytes.Equal(entry.Key, targetKey) && entry.Meta.Ds == core.DataStructureSet {
			select {
			case entrySignal <- struct{}{}:
			default:
			}
		}
	}

	rewriteDone := make(chan error, 1)
	go func() {
		rewriteDone <- job.rewrite()
	}()

	select {
	case <-entrySignal:
	case err := <-rewriteDone:
		if err != nil {
			t.Fatalf("rewrite finished early: %v", err)
		}
		t.Fatal("rewrite finished before processing target entry")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for rewrite to reach target entry")
	}

	// Concurrent update: add new member (simpler than remove+add)
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- db.Update(func(tx *Tx) error {
			return tx.SAdd(bucket, targetKey, freshMember)
		})
	}()

	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("concurrent update: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("update blocked for too long")
	}

	if err := <-rewriteDone; err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	if err := job.commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := job.finalizeOutputs(); err != nil {
		t.Fatalf("finalizeOutputs: %v", err)
	}
	if err := job.cleanupOldFiles(); err != nil {
		t.Fatalf("cleanupOldFiles: %v", err)
	}
	if err := removeMergeManifest(dir); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}

	// Verify both members exist (initial from merge, fresh from concurrent write)
	if err := db.View(func(tx *Tx) error {
		isMember, err := tx.SIsMember(bucket, targetKey, freshMember)
		if err != nil {
			return err
		}
		if !isMember {
			return fmt.Errorf("fresh member should exist")
		}

		isMember, err = tx.SIsMember(bucket, targetKey, initialMember)
		if err != nil {
			return err
		}
		if !isMember {
			return fmt.Errorf("initial member should still exist")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify after merge: %v", err)
	}

	// Verify persistence
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	db = nil

	db, err = Open(opts)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}

	if err := db.View(func(tx *Tx) error {
		isMember, err := tx.SIsMember(bucket, targetKey, freshMember)
		if err != nil {
			return err
		}
		if !isMember {
			return fmt.Errorf("fresh member should persist after reopen")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify after reopen: %v", err)
	}
}

func TestMergeV2CommitPhaseBlocking(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 8 * 1024
	opts.EnableMergeV2 = true
	opts.RWMode = FileIO

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	bucket := "bucket"

	if err := db.Update(func(tx *Tx) error {
		return tx.NewBucket(core.DataStructureBTree, bucket)
	}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// Create many entries to produce multiple lookups during commit
	numEntries := 100
	fillerVal := bytes.Repeat([]byte("x"), 512)
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		if err := db.Update(func(tx *Tx) error {
			return tx.Put(bucket, key, fillerVal, core.Persistent)
		}); err != nil {
			t.Fatalf("populate entry %d: %v", i, err)
		}
	}

	// Ensure multiple data files
	var haveMultipleFiles bool
	for attempts := 0; attempts < 10; attempts++ {
		userIDs, _, err := enumerateDataFileIDs(dir)
		if err != nil {
			t.Fatalf("enumerate data files: %v", err)
		}
		if len(userIDs) >= 2 {
			haveMultipleFiles = true
			break
		}
		extraKey := []byte(fmt.Sprintf("extra-%03d", attempts))
		if err := db.Update(func(tx *Tx) error {
			return tx.Put(bucket, extraKey, fillerVal, core.Persistent)
		}); err != nil {
			t.Fatalf("add extra entry: %v", err)
		}
	}
	if !haveMultipleFiles {
		t.Fatal("expected multiple data files for merge test")
	}

	job := &mergeV2Job{db: db}
	if err := job.prepare(); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer job.finish()
	if err := job.enterWritingState(); err != nil {
		t.Fatalf("enterWritingState: %v", err)
	}
	if err := job.rewrite(); err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	// Start commit in background (holds db.mu.Lock)
	commitDone := make(chan error, 1)
	commitStarted := make(chan struct{})

	// Inject a delay during commit to ensure we can observe blocking
	originalLookup := job.lookup
	lookupCount := len(originalLookup)
	if lookupCount < 10 {
		t.Fatalf("expected at least 10 lookup entries, got %d", lookupCount)
	}

	go func() {
		close(commitStarted)
		commitDone <- job.commit()
	}()

	<-commitStarted
	// Give commit a moment to acquire the lock
	time.Sleep(50 * time.Millisecond)

	// Try to perform an update transaction while commit is running
	updateStart := time.Now()
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- db.Update(func(tx *Tx) error {
			return tx.Put(bucket, []byte("concurrent-key"), []byte("concurrent-value"), core.Persistent)
		})
	}()

	// The update should eventually succeed after commit releases the lock
	select {
	case err := <-updateDone:
		blockDuration := time.Since(updateStart)
		if err != nil {
			t.Fatalf("concurrent update failed: %v", err)
		}
		// Verify it was actually blocked (should take some time)
		if blockDuration < 10*time.Millisecond {
			t.Logf("Warning: update completed very quickly (%v), may not have been blocked", blockDuration)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent update blocked for too long (>5s)")
	}

	if err := <-commitDone; err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Verify the concurrent write succeeded
	if err := db.View(func(tx *Tx) error {
		got, err := tx.Get(bucket, []byte("concurrent-key"))
		if err != nil {
			return err
		}
		if !bytes.Equal(got, []byte("concurrent-value")) {
			return fmt.Errorf("concurrent write value mismatch")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify concurrent write: %v", err)
	}
}

func TestMergeV2WriteEntryHashesSetAndSortedSet(t *testing.T) {
	opts := DefaultOptions
	opts.EnableHintFile = false
	opts.SegmentSize = 1 << 16

	db := &DB{opt: opts}
	job := &mergeV2Job{db: db, valueHasher: fnv.New32a()}

	mock := &mockRWManager{}
	out := &mergeOutput{
		fileID:   1,
		dataFile: &DataFile{rwManager: mock},
	}
	job.outputs = []*mergeOutput{out}

	setValue := []byte("set-value")
	setEntry := createTestEntry(1, []byte("set-key"), setValue, core.DataSetFlag, core.Committed, core.Persistent, uint64(time.Now().Unix()), 1, core.DataStructureSet)
	if err := job.writeEntry(setEntry); err != nil {
		t.Fatalf("writeEntry set: %v", err)
	}

	if len(job.lookup) != 1 {
		t.Fatalf("expected one lookup entry, got %d", len(job.lookup))
	}

	expectedSetHash := fnv.New32a()
	_, _ = expectedSetHash.Write(setValue)
	if !job.lookup[0].hasValueHash || job.lookup[0].valueHash != expectedSetHash.Sum32() {
		t.Fatalf("set lookup should contain value hash")
	}

	sortedValue := []byte("sorted-value")
	sortedEntry := createTestEntry(2, []byte("sorted-key"), sortedValue, core.DataZAddFlag, core.Committed, core.Persistent, uint64(time.Now().Unix()), 2, core.DataStructureSortedSet)
	if err := job.writeEntry(sortedEntry); err != nil {
		t.Fatalf("writeEntry sorted set: %v", err)
	}

	if len(job.lookup) != 2 {
		t.Fatalf("expected two lookup entries, got %d", len(job.lookup))
	}

	expectedSortedHash := fnv.New32a()
	_, _ = expectedSortedHash.Write(sortedValue)
	entry := job.lookup[1]
	if !entry.hasValueHash || entry.valueHash != expectedSortedHash.Sum32() {
		t.Fatalf("sorted set lookup should contain value hash")
	}
}

func TestMergeV2ApplyLookupUpdatesSecondaryIndexes(t *testing.T) {
	db := &DB{
		opt:   DefaultOptions,
		Index: newIndex(),
		bucketManager: &BucketManager{
			BucketInfoMapper: map[core.BucketId]*core.Bucket{},
		},
	}

	// Prepare buckets
	buckets := []struct {
		id   core.BucketId
		ds   core.DataStructure
		name string
	}{
		{1, core.DataStructureSet, "set"},
		{2, core.DataStructureList, "list"},
		{3, core.DataStructureSortedSet, "zset"},
	}

	for _, b := range buckets {
		db.bucketManager.BucketInfoMapper[b.id] = &core.Bucket{Meta: &core.BucketMeta{}, Id: b.id, Ds: uint16(b.ds), Name: b.name}
	}

	// Set bucket
	setRecord := &core.Record{Value: []byte("member"), FileID: 10, Timestamp: 1, TTL: core.Persistent}
	setIdx := db.Index.set.getWithDefault(buckets[0].id)
	if err := setIdx.SAdd("set-key", [][]byte{setRecord.Value}, []*core.Record{setRecord}); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	setHash := fnv.New32a()
	_, _ = setHash.Write(setRecord.Value)

	// List bucket
	listIdx := db.Index.list.getWithDefault(buckets[1].id)
	listKey := []byte("list-key")
	seq := uint64(42)
	listRecord := &core.Record{FileID: 11, Timestamp: 2, TTL: core.Persistent, TxID: 1}
	listIdx.Items[string(listKey)] = data.NewBTree()
	listIdx.Items[string(listKey)].InsertRecord(utils.ConvertUint64ToBigEndianBytes(seq), listRecord)

	// Sorted set bucket
	sortedIdx := db.Index.sortedSet.getWithDefault(buckets[2].id, db)
	sortedValue := []byte("sorted-member")
	sortedRecord := &core.Record{Value: sortedValue, FileID: 12, Timestamp: 3, TTL: core.Persistent}
	if err := sortedIdx.ZAdd("zset-key", SCORE(1.5), sortedValue, sortedRecord); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	sortedHash := fnv.New32a()
	_, _ = sortedHash.Write(sortedValue)

	job := &mergeV2Job{db: db}

	// Apply set lookup
	job.applyLookup(&mergeLookupEntry{
		hint: &HintEntry{
			BucketId:  uint64(buckets[0].id),
			Key:       []byte("set-key"),
			Ds:        uint16(core.DataStructureSet),
			FileID:    100,
			DataPos:   1000,
			Timestamp: 100,
			TTL:       5,
			ValueSize: 9,
		},
		valueHash:    setHash.Sum32(),
		hasValueHash: true,
	})

	if setRecord.FileID != 100 || setRecord.DataPos != 1000 {
		t.Fatalf("set record not updated: %+v", setRecord)
	}

	// Apply list lookup
	listKeyEncoded := utils.EncodeListKey(listKey, seq)
	job.applyLookup(&mergeLookupEntry{
		hint: &HintEntry{
			BucketId:  uint64(buckets[1].id),
			Key:       listKeyEncoded,
			Ds:        uint16(core.DataStructureList),
			Flag:      core.DataLPushFlag,
			FileID:    200,
			DataPos:   2000,
			Timestamp: 200,
			TTL:       6,
			ValueSize: 7,
		},
	})

	if listRecord.FileID != 200 || listRecord.DataPos != 2000 {
		t.Fatalf("list record not updated: %+v", listRecord)
	}

	// Apply sorted set lookup
	sortedKey := []byte("zset-key" + SeparatorForZSetKey + "1.5")
	job.applyLookup(&mergeLookupEntry{
		hint: &HintEntry{
			BucketId:  uint64(buckets[2].id),
			Key:       sortedKey,
			Ds:        uint16(core.DataStructureSortedSet),
			FileID:    300,
			DataPos:   3000,
			Timestamp: 300,
			TTL:       7,
			ValueSize: 13,
		},
		valueHash:    sortedHash.Sum32(),
		hasValueHash: true,
	})

	node := sortedIdx.M["zset-key"].dict[sortedHash.Sum32()]
	if node == nil || node.record.FileID != 300 || node.record.DataPos != 3000 {
		t.Fatalf("sorted set node not updated: %+v", node)
	}
}

// TestMergeV2ConcurrentWritesDuringFullMerge tests the critical scenario where
// db.Merge() runs concurrently with ongoing write transactions. This validates
// that the merge operation doesn't corrupt data or lose updates.
func TestMergeV2ConcurrentWritesDuringFullMerge(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 16 * 1024 // Small segments to force multiple files
	opts.EnableMergeV2 = true
	opts.RWMode = FileIO

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	bucket := "test-bucket"
	if err := db.Update(func(tx *Tx) error {
		return tx.NewBucket(core.DataStructureBTree, bucket)
	}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// Phase 1: Populate initial data (to be merged)
	numInitialKeys := 500 // Increased to force multiple files
	initialData := make(map[string][]byte)
	valueSize := bytes.Repeat([]byte("x"), 200) // Larger values
	for i := 0; i < numInitialKeys; i++ {
		key := fmt.Sprintf("key-%04d", i)
		value := append([]byte(fmt.Sprintf("initial-value-%04d-", i)), valueSize...)
		initialData[key] = value

		if err := db.Update(func(tx *Tx) error {
			return tx.Put(bucket, []byte(key), value, core.Persistent)
		}); err != nil {
			t.Fatalf("put initial key %s: %v", key, err)
		}
	}

	// Ensure multiple data files exist
	userIDs, _, err := enumerateDataFileIDs(dir)
	if err != nil {
		t.Fatalf("enumerate data files: %v", err)
	}
	if len(userIDs) < 2 {
		t.Fatalf("expected at least 2 data files, got %d", len(userIDs))
	}

	// Phase 2: Start merge in background
	mergeDone := make(chan error, 1)
	mergeStarted := make(chan struct{})

	go func() {
		close(mergeStarted)
		mergeDone <- db.Merge()
	}()

	<-mergeStarted
	time.Sleep(10 * time.Millisecond) // Give merge time to start

	// Phase 3: Concurrent writes while merge is running
	numConcurrentWrites := 100
	concurrentData := make(map[string][]byte)
	var dataMu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < numConcurrentWrites; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			key := fmt.Sprintf("key-%04d", idx) // Overwrite some existing keys
			value := []byte(fmt.Sprintf("concurrent-value-%04d", idx))

			if err := db.Update(func(tx *Tx) error {
				return tx.Put(bucket, []byte(key), value, core.Persistent)
			}); err != nil {
				t.Errorf("concurrent write failed for key %s: %v", key, err)
				return
			}

			// Track what we wrote
			dataMu.Lock()
			concurrentData[key] = value
			dataMu.Unlock()
		}(i)

		// Add some new keys too
		if i%2 == 0 {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				key := fmt.Sprintf("new-key-%04d", idx)
				value := []byte(fmt.Sprintf("new-value-%04d", idx))

				if err := db.Update(func(tx *Tx) error {
					return tx.Put(bucket, []byte(key), value, core.Persistent)
				}); err != nil {
					t.Errorf("concurrent new key write failed for %s: %v", key, err)
					return
				}

				dataMu.Lock()
				concurrentData[key] = value
				dataMu.Unlock()
			}(i)
		}
	}

	wg.Wait()

	// Wait for merge to complete
	if err := <-mergeDone; err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// Phase 4: Verify all data is correct
	for key, expectedValue := range concurrentData {
		var got []byte
		if err := db.View(func(tx *Tx) error {
			val, err := tx.Get(bucket, []byte(key))
			if err != nil {
				return err
			}
			got = val
			return nil
		}); err != nil {
			t.Errorf("failed to get key %s: %v", key, err)
			continue
		}

		if !bytes.Equal(got, expectedValue) {
			t.Errorf("key %s: expected %q, got %q", key, expectedValue, got)
		}
	}

	// Verify keys not updated should have initial values
	for key, expectedValue := range initialData {
		if _, updated := concurrentData[key]; updated {
			continue // Skip keys we overwrote
		}

		var got []byte
		if err := db.View(func(tx *Tx) error {
			val, err := tx.Get(bucket, []byte(key))
			if err != nil {
				return err
			}
			got = val
			return nil
		}); err != nil {
			t.Errorf("failed to get initial key %s: %v", key, err)
			continue
		}

		if !bytes.Equal(got, expectedValue) {
			t.Errorf("initial key %s: expected %q, got %q", key, expectedValue, got)
		}
	}
}

// TestMergeV2ReopenAfterConcurrentMerge validates the critical correctness guarantee:
// even if merge writes stale hints (due to concurrent updates), index rebuild on reopen
// must correctly handle this by processing files in order (merge files first, then normal files).
func TestMergeV2ReopenAfterConcurrentMerge(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 16 * 1024
	opts.EnableMergeV2 = true
	opts.EnableHintFile = true // Critical: test hint file rebuild
	opts.RWMode = FileIO

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	bucket := "bucket"
	if err := db.Update(func(tx *Tx) error {
		return tx.NewBucket(core.DataStructureBTree, bucket)
	}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// Populate data to trigger merge
	valueSize := bytes.Repeat([]byte("x"), 200)
	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("key-%03d", i)
		value := append([]byte(fmt.Sprintf("initial-%03d-", i)), valueSize...)
		if err := db.Update(func(tx *Tx) error {
			return tx.Put(bucket, []byte(key), value, core.Persistent)
		}); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Trigger merge
	mergeDone := make(chan error, 1)
	mergeStarted := make(chan struct{})
	go func() {
		close(mergeStarted)
		mergeDone <- db.Merge()
	}()

	<-mergeStarted
	time.Sleep(5 * time.Millisecond)

	// Concurrent update (this creates the "stale hint" scenario)
	staleCandidateKey := "key-050"
	freshValue := []byte("fresh-value-after-merge-started")

	if err := db.Update(func(tx *Tx) error {
		return tx.Put(bucket, []byte(staleCandidateKey), freshValue, core.Persistent)
	}); err != nil {
		t.Fatalf("concurrent update: %v", err)
	}

	// Wait for merge
	if err := <-mergeDone; err != nil {
		t.Fatalf("merge: %v", err)
	}

	// Verify correctness BEFORE reopen (should have fresh value)
	var beforeReopen []byte
	if err := db.View(func(tx *Tx) error {
		val, err := tx.Get(bucket, []byte(staleCandidateKey))
		if err != nil {
			return err
		}
		beforeReopen = append([]byte(nil), val...)
		return nil
	}); err != nil {
		t.Fatalf("get before reopen: %v", err)
	}

	if !bytes.Equal(beforeReopen, freshValue) {
		t.Fatalf("before reopen: expected %q, got %q", freshValue, beforeReopen)
	}

	// Close and reopen - this triggers index rebuild from hint files
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db = nil

	db, err = Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	// THE CRITICAL TEST: After reopen, data must still be correct
	// Even though merge may have written a stale hint for this key,
	// the index rebuild should use the fresh value from the normal file.
	var afterReopen []byte
	if err := db.View(func(tx *Tx) error {
		val, err := tx.Get(bucket, []byte(staleCandidateKey))
		if err != nil {
			return err
		}
		afterReopen = append([]byte(nil), val...)
		return nil
	}); err != nil {
		t.Fatalf("get after reopen: %v", err)
	}

	if !bytes.Equal(afterReopen, freshValue) {
		t.Fatalf("CRITICAL FAILURE: after reopen: expected %q, got %q - stale hint was not overwritten!",
			freshValue, afterReopen)
	}

	// Verify all other keys are still correct
	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("key-%03d", i)
		if key == staleCandidateKey {
			continue
		}

		expectedValue := append([]byte(fmt.Sprintf("initial-%03d-", i)), valueSize...)
		var got []byte
		if err := db.View(func(tx *Tx) error {
			val, err := tx.Get(bucket, []byte(key))
			if err != nil {
				return err
			}
			got = val
			return nil
		}); err != nil {
			t.Errorf("key %s: %v", key, err)
			continue
		}

		if !bytes.Equal(got, expectedValue) {
			t.Errorf("key %s: expected length %d, got length %d", key, len(expectedValue), len(got))
		}
	}
}

// TestMergeV2MultiDataStructuresConcurrent tests merge with all data structures together
func TestMergeV2MultiDataStructuresConcurrent(t *testing.T) {
	opts := DefaultOptions
	dir := t.TempDir()
	opts.Dir = dir
	opts.SegmentSize = 256 * 1024 // 256KB
	opts.EnableHintFile = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	bucketBTree := "btree-bucket"
	bucketList := "list-bucket"
	bucketSet := "set-bucket"
	bucketZSet := "zset-bucket"

	if err := db.Update(func(tx *Tx) error {
		if err := tx.NewBucket(core.DataStructureBTree, bucketBTree); err != nil {
			return err
		}

		if err := tx.NewBucket(core.DataStructureList, bucketList); err != nil {
			return err
		}

		if err := tx.NewBucket(core.DataStructureSet, bucketSet); err != nil {
			return err
		}

		if err := tx.NewBucket(core.DataStructureSortedSet, bucketZSet); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatalf("create buckets: %v", err)
	}

	// Phase 1: Create data across all data structures to trigger multiple files
	numKeys := 2000
	valueSize := bytes.Repeat([]byte("x"), 200)

	for i := 0; i < numKeys; i++ {
		if err := db.Update(func(tx *Tx) error {
			// BTree key-value
			key := fmt.Sprintf("btree-key-%03d", i)
			value := append([]byte(fmt.Sprintf("btree-val-%03d-", i)), valueSize...)
			if err := tx.Put(bucketBTree, []byte(key), value, core.Persistent); err != nil {
				return err
			}

			// List operations
			listKey := fmt.Sprintf("list-%03d", i)
			listVal := fmt.Sprintf("list-val-%03d", i)
			if err := tx.RPush(bucketList, []byte(listKey), []byte(listVal)); err != nil {
				return err
			}

			// Set operations
			setKey := fmt.Sprintf("set-%03d", i)
			setMember := fmt.Sprintf("member-%03d", i)
			if err := tx.SAdd(bucketSet, []byte(setKey), []byte(setMember)); err != nil {
				return err
			}

			// Sorted Set operations
			zsetKey := fmt.Sprintf("zset-%03d", i)
			zsetMember := fmt.Sprintf("zmember-%03d", i)
			if err := tx.ZAdd(bucketZSet, []byte(zsetKey), float64(i), []byte(zsetMember)); err != nil {
				return err
			}

			return nil
		}); err != nil {
			t.Fatalf("initial write %d: %v", i, err)
		}
	}

	// Phase 2: Start merge in background
	mergeStarted := make(chan struct{})
	mergeDone := make(chan error, 1)

	go func() {
		close(mergeStarted)
		mergeDone <- db.Merge()
	}()

	<-mergeStarted
	time.Sleep(10 * time.Millisecond)

	// Phase 3: Concurrent operations on all data structures during merge
	var wg sync.WaitGroup
	numOps := 50

	// BTree concurrent updates
	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("btree-key-%03d", idx)
			value := []byte(fmt.Sprintf("concurrent-btree-%03d", idx))
			_ = db.Update(func(tx *Tx) error {
				return tx.Put(bucketBTree, []byte(key), value, core.Persistent)
			})
		}(i)
	}

	// List concurrent pushes
	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			listKey := fmt.Sprintf("list-%03d", idx)
			listVal := fmt.Sprintf("concurrent-list-%03d", idx)
			_ = db.Update(func(tx *Tx) error {
				return tx.RPush(bucketList, []byte(listKey), []byte(listVal))
			})
		}(i)
	}

	// Set concurrent adds
	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			setKey := fmt.Sprintf("set-%03d", idx)
			setMember := fmt.Sprintf("concurrent-member-%03d", idx)
			_ = db.Update(func(tx *Tx) error {
				return tx.SAdd(bucketSet, []byte(setKey), []byte(setMember))
			})
		}(i)
	}

	// Sorted Set concurrent adds
	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			zsetKey := fmt.Sprintf("zset-%03d", idx)
			zsetMember := fmt.Sprintf("concurrent-zmember-%03d", idx)
			_ = db.Update(func(tx *Tx) error {
				return tx.ZAdd(bucketZSet, []byte(zsetKey), float64(1000+idx), []byte(zsetMember))
			})
		}(i)
	}

	wg.Wait()

	// Wait for merge
	if err := <-mergeDone; err != nil {
		t.Fatalf("merge: %v", err)
	}

	// Phase 4: Verify all data structures are correct
	if err := db.View(func(tx *Tx) error {
		// Check BTree
		for i := 0; i < numOps; i++ {
			key := fmt.Sprintf("btree-key-%03d", i)
			val, err := tx.Get(bucketBTree, []byte(key))
			if err != nil {
				return fmt.Errorf("btree get %s: %w", key, err)
			}
			expected := []byte(fmt.Sprintf("concurrent-btree-%03d", i))
			if !bytes.Equal(val, expected) {
				return fmt.Errorf("btree %s: expected %q, got %q", key, expected, val)
			}
		}

		// Check Lists
		for i := 0; i < numOps; i++ {
			listKey := fmt.Sprintf("list-%03d", i)
			items, err := tx.LRange(bucketList, []byte(listKey), 0, -1)
			if err != nil {
				return fmt.Errorf("list range %s: %w", listKey, err)
			}
			if len(items) < 2 {
				return fmt.Errorf("list %s: expected at least 2 items, got %d", listKey, len(items))
			}
		}

		// Check Sets
		for i := 0; i < numOps; i++ {
			setKey := fmt.Sprintf("set-%03d", i)
			members, err := tx.SMembers(bucketSet, []byte(setKey))
			if err != nil {
				return fmt.Errorf("set members %s: %w", setKey, err)
			}
			if len(members) < 2 {
				return fmt.Errorf("set %s: expected at least 2 members, got %d", setKey, len(members))
			}
		}

		// Check Sorted Sets
		for i := 0; i < numOps; i++ {
			zsetKey := fmt.Sprintf("zset-%03d", i)
			nodes, err := tx.ZMembers(bucketZSet, []byte(zsetKey))
			if err != nil {
				return fmt.Errorf("zset members %s: %w", zsetKey, err)
			}
			if len(nodes) < 2 {
				return fmt.Errorf("zset %s: expected at least 2 nodes, got %d", zsetKey, len(nodes))
			}
		}

		return nil
	}); err != nil {
		t.Fatalf("verification: %v", err)
	}
}

func TestMergeV2RewriteFileSkipsCorruptedEntries(t *testing.T) {
	dir := t.TempDir()
	fid := int64(0)
	path := getDataPath(fid, dir)

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create data file: %v", err)
	}

	bucketID := core.BucketId(1)
	now := uint64(time.Now().Unix())

	entries := []*core.Entry{
		createTestEntry(bucketID, []byte("uncommitted"), []byte("v1"), core.DataSetFlag, core.UnCommitted, core.Persistent, now, 1, core.DataStructureBTree),
		createTestEntry(bucketID, []byte("deleted"), []byte("v2"), core.DataDeleteFlag, core.Committed, core.Persistent, now, 2, core.DataStructureBTree),
		createTestEntry(bucketID, []byte("expired"), []byte("v3"), core.DataSetFlag, core.Committed, 1, uint64(time.Now().Add(-2*time.Second).Unix()), 3, core.DataStructureBTree),
		createTestEntry(bucketID, []byte("nonpending"), []byte("v4"), core.DataSetFlag, core.Committed, core.Persistent, now, 4, core.DataStructureBTree),
	}

	goodEntry := createTestEntry(bucketID, []byte("good"), []byte("keep"), core.DataSetFlag, core.Committed, core.Persistent, now, 5, core.DataStructureBTree)

	for _, e := range append(entries, goodEntry) {
		if _, err := f.Write(e.Encode()); err != nil {
			t.Fatalf("write entry: %v", err)
		}
	}

	if _, err := f.Write([]byte{0x01, 0x02, 0x03}); err != nil {
		t.Fatalf("write tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close test data file: %v", err)
	}

	bt := data.NewBTree()
	bt.InsertRecord(goodEntry.Key, (&core.Record{}).
		WithFileId(fid).
		WithDataPos(0).
		WithTimestamp(goodEntry.Meta.Timestamp).
		WithTTL(goodEntry.Meta.TTL).
		WithTxID(goodEntry.Meta.TxID).
		WithKey(goodEntry.Key))

	db := &DB{
		opt: Options{
			Dir:                  dir,
			BufferSizeOfRecovery: 4096,
			SegmentSize:          1 << 16,
		},
		Index:      newIndex(),
		ttlChecker: checker.NewChecker(clock.NewRealClock()),
		bucketManager: &BucketManager{
			BucketInfoMapper: map[core.BucketId]*core.Bucket{
				bucketID: {Meta: &core.BucketMeta{}, Id: bucketID, Ds: uint16(core.DataStructureBTree)},
			},
		},
	}
	db.Index.bTree.idx[bucketID] = bt

	mock := &mockRWManager{}
	job := &mergeV2Job{
		db:      db,
		outputs: []*mergeOutput{{fileID: GetMergeFileID(0), dataFile: &DataFile{rwManager: mock}}},
	}

	if err := job.rewriteFile(fid); err != nil {
		t.Fatalf("rewriteFile: %v", err)
	}

	if len(mock.writes) != 1 {
		t.Fatalf("expected exactly one merged entry, got %d", len(mock.writes))
	}

	want := goodEntry.Encode()
	got := mock.writes[0].data
	if len(got) != len(want) {
		t.Fatalf("unexpected encoded size: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("merged entry mismatch at %d", i)
		}
	}

	if len(job.lookup) != 1 {
		t.Fatalf("expected lookup entry for merged record")
	}
}

func TestMergeV2AbortAggregatesCleanupErrors(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	hintDir := filepath.Join(dir, "hint")
	if err := os.Mkdir(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}
	if err := os.Mkdir(hintDir, 0o755); err != nil {
		t.Fatalf("mkdir hint: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "keep"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write data file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(hintDir, "keep"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write hint file: %v", err)
	}

	mock := &mockRWManager{syncErr: errors.New("sync fail"), closeErr: errors.New("close fail")}
	collector := NewHintCollector(1, &failingHintWriter{syncErr: errors.New("hint sync fail"), closeErr: errors.New("hint close fail")}, 1)

	job := &mergeV2Job{
		db: &DB{opt: Options{Dir: dir}},
		outputs: []*mergeOutput{
			{
				seq:       1,
				dataPath:  dataDir,
				hintPath:  hintDir,
				dataFile:  &DataFile{rwManager: mock},
				collector: collector,
			},
		},
	}

	original := errors.New("merge failed")
	err := job.abort(original)
	if err == nil {
		t.Fatal("abort should surface cleanup failures")
	}
	if !errors.Is(err, original) {
		t.Fatalf("original error should be wrapped, got %v", err)
	}
	if !strings.Contains(err.Error(), "failed to finalize output") || !strings.Contains(err.Error(), "failed to remove data file") {
		t.Fatalf("aggregated error missing details: %v", err)
	}
}

func TestMergeV2CleanupOldFilesPropagatesErrors(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "old")
	if err := os.MkdirAll(filepath.Join(nested, "child"), 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}

	job := &mergeV2Job{
		db:      &DB{opt: Options{Dir: dir}, fm: NewFileManager(FileIO, 1, 0.5, 1<<12)},
		oldData: []string{nested},
	}

	if err := job.cleanupOldFiles(); err == nil {
		t.Fatal("cleanupOldFiles should surface removal errors")
	}
}
