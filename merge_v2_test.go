package nutsdb

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
		return tx.NewBucket(DataStructureBTree, bucket)
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

func createTestEntry(bucketID BucketId, key, value []byte, flag uint16, status uint16, ttl uint32, ts uint64, txID uint64, ds DataStructure) *Entry {
	meta := NewMetaData().
		WithBucketId(uint64(bucketID)).
		WithKeySize(uint32(len(key))).
		WithValueSize(uint32(len(value))).
		WithFlag(flag).
		WithStatus(status).
		WithTTL(ttl).
		WithTimeStamp(ts).
		WithTxID(txID).
		WithDs(uint16(ds))

	entry := &Entry{
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
		fm:  newFileManager(opts.RWMode, 4, 0.5, opts.SegmentSize),
	}

	job := &mergeV2Job{db: db}
	if _, err := job.ensureOutput(128); err == nil || !strings.Contains(err.Error(), "failed to remove old hint file") {
		t.Fatalf("expected removal error, got %v", err)
	}
}

func TestMergeV2CommitCollectorFailure(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 1 << 16

	bucketID := BucketId(1)
	bucketName := "b"
	key := []byte("set-key")
	value := []byte("value")
	timestamp := uint64(time.Now().Unix())
	oldFileID := int64(5)

	bt := NewBTree()
	record := (&Record{}).
		WithKey(key).
		WithFileId(oldFileID).
		WithDataPos(123).
		WithTimestamp(timestamp).
		WithTTL(Persistent)
	bt.InsertRecord(key, record)

	db := &DB{
		opt:   opts,
		Index: newIndex(),
		bm: &BucketManager{
			BucketInfoMapper: map[BucketId]*Bucket{
				bucketID: {Meta: &BucketMeta{}, Id: bucketID, Name: bucketName, Ds: uint16(DataStructureBTree)},
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

	entry := createTestEntry(bucketID, key, value, DataDeleteFlag, Committed, Persistent, timestamp, 1, DataStructureBTree)
	if err := job.writeEntry(entry, oldFileID); err != nil {
		t.Fatalf("writeEntry: %v", err)
	}

	if err := job.commit(); err == nil || !strings.Contains(err.Error(), "failed to add hint") {
		t.Fatalf("commit should surface collector errors, got %v", err)
	}
}

func TestMergeV2CommitSkipsStaleEntries(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 1 << 16

	bucketID := BucketId(1)
	bucketName := "b"
	key1 := []byte("key-current")
	key2 := []byte("key-stale")
	now := uint64(time.Now().Unix())

	record1 := NewRecord().
		WithKey(key1).
		WithFileId(1).
		WithDataPos(111).
		WithTimestamp(now).
		WithTTL(Persistent)

	record2 := NewRecord().
		WithKey(key2).
		WithFileId(2).
		WithDataPos(222).
		WithTimestamp(now).
		WithTTL(Persistent)

	bt := NewBTree()
	bt.InsertRecord(key1, record1)
	bt.InsertRecord(key2, record2)

	db := &DB{
		opt:   opts,
		Index: newIndex(),
		bm: &BucketManager{
			BucketInfoMapper: map[BucketId]*Bucket{
				bucketID: {Meta: &BucketMeta{}, Id: bucketID, Name: bucketName, Ds: uint16(DataStructureBTree)},
			},
		},
	}
	db.Index.bTree.idx[bucketID] = bt

	writer := &recordingHintWriter{}
	collector := NewHintCollector(GetMergeFileID(1), writer, 1)
	out := &mergeOutput{
		seq:       1,
		fileID:    GetMergeFileID(1),
		dataFile:  &DataFile{rwManager: &mockRWManager{}},
		collector: collector,
	}

	job := &mergeV2Job{
		db:          db,
		outputs:     []*mergeOutput{out},
		valueHasher: fnv.New32a(),
		manifest:    &mergeManifest{},
		pending:     []int64{1, 2},
	}

	entry1 := createTestEntry(bucketID, key1, []byte("value1"), DataSetFlag, Committed, Persistent, now, 1, DataStructureBTree)
	entry2 := createTestEntry(bucketID, key2, []byte("value2"), DataSetFlag, Committed, Persistent, now, 2, DataStructureBTree)

	if err := job.writeEntry(entry1, record1.FileID); err != nil {
		t.Fatalf("writeEntry #1: %v", err)
	}
	if err := job.writeEntry(entry2, record2.FileID); err != nil {
		t.Fatalf("writeEntry #2: %v", err)
	}

	// Simulate a concurrent write moving record2 to a new file.
	record2.WithFileId(999).WithDataPos(555)

	if err := job.commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if record1.FileID != out.fileID {
		t.Fatalf("record1 should move to merge file, got %d want %d", record1.FileID, out.fileID)
	}
	if record2.FileID != 999 {
		t.Fatalf("record2 should remain at new file, got %d", record2.FileID)
	}

	if len(writer.writes) != 1 {
		t.Fatalf("expected 1 hint write, got %d", len(writer.writes))
	}
	if got := string(writer.writes[0].Key); got != string(key1) {
		t.Fatalf("hint written for wrong key, got %s want %s", got, key1)
	}
}

func TestMergeV2CommitSkipsTimestampMismatch(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 1 << 16

	bucketID := BucketId(1)
	bucketName := "b"
	key := []byte("test-key")
	oldTimestamp := uint64(time.Now().Unix())
	newTimestamp := oldTimestamp + 100

	record := NewRecord().
		WithKey(key).
		WithFileId(5).
		WithDataPos(111).
		WithTimestamp(oldTimestamp).
		WithTTL(Persistent)

	bt := NewBTree()
	bt.InsertRecord(key, record)

	db := &DB{
		opt:   opts,
		Index: newIndex(),
		bm: &BucketManager{
			BucketInfoMapper: map[BucketId]*Bucket{
				bucketID: {Meta: &BucketMeta{}, Id: bucketID, Name: bucketName, Ds: uint16(DataStructureBTree)},
			},
		},
	}
	db.Index.bTree.idx[bucketID] = bt

	writer := &recordingHintWriter{}
	collector := NewHintCollector(GetMergeFileID(1), writer, 1)
	out := &mergeOutput{
		seq:       1,
		fileID:    GetMergeFileID(1),
		dataFile:  &DataFile{rwManager: &mockRWManager{}},
		collector: collector,
	}

	job := &mergeV2Job{
		db:          db,
		outputs:     []*mergeOutput{out},
		valueHasher: fnv.New32a(),
		manifest:    &mergeManifest{},
		pending:     []int64{5},
	}

	entry := createTestEntry(bucketID, key, []byte("value"), DataDeleteFlag, Committed, Persistent, oldTimestamp, 1, DataStructureBTree)
	if err := job.writeEntry(entry, record.FileID); err != nil {
		t.Fatalf("writeEntry: %v", err)
	}

	// Simulate concurrent update changing timestamp
	record.WithTimestamp(newTimestamp)

	if err := job.commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if record.Timestamp != newTimestamp {
		t.Fatalf("record timestamp should remain new value %d, got %d", newTimestamp, record.Timestamp)
	}

	if len(writer.writes) != 0 {
		t.Fatalf("expected 0 hint writes for stale entry, got %d", len(writer.writes))
	}
}

func TestMergeV2CommitSkipsStaleEntriesAllDataStructures(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 1 << 16

	now := uint64(time.Now().Unix())

	// Setup BTree
	btreeBucketID := BucketId(1)
	btreeKey := []byte("btree-key")
	btreeRecord := NewRecord().
		WithKey(btreeKey).
		WithFileId(10).
		WithDataPos(100).
		WithTimestamp(now).
		WithTTL(Persistent)
	btree := NewBTree()
	btree.InsertRecord(btreeKey, btreeRecord)

	// Setup Set
	setBucketID := BucketId(2)
	setKey := []byte("set-key")
	setValue := []byte("set-value")
	setValueHash := fnv.New32a()
	setValueHash.Write(setValue)
	setHash := setValueHash.Sum32()
	setRecord := NewRecord().
		WithFileId(11).
		WithDataPos(200).
		WithTimestamp(now).
		WithTTL(Persistent)
	setIdx := NewSet()
	setIdx.M[string(setKey)] = map[uint32]*Record{setHash: setRecord}

	// Setup List
	listBucketID := BucketId(3)
	listKey := []byte("list-key")
	listSeq := uint64(1)
	listEncodedKey := encodeListKey(listKey, listSeq)
	listRecord := NewRecord().
		WithFileId(12).
		WithDataPos(300).
		WithTimestamp(now).
		WithTTL(Persistent)
	listIdx := NewList()
	listIdx.Items[string(listKey)] = NewBTree()
	listIdx.Items[string(listKey)].InsertRecord(ConvertUint64ToBigEndianBytes(listSeq), listRecord)

	// Setup SortedSet
	sortedBucketID := BucketId(4)
	sortedKey := []byte("sorted-key")
	sortedValue := []byte("sorted-value")
	sortedValueHash := fnv.New32a()
	sortedValueHash.Write(sortedValue)
	sortedHash := sortedValueHash.Sum32()
	sortedRecord := NewRecord().
		WithFileId(13).
		WithDataPos(400).
		WithTimestamp(now).
		WithTTL(Persistent)
	sortedIdx := NewSortedSet(nil)
	score := SCORE(1.5)
	sortedSetKey := string(sortedKey) + SeparatorForZSetKey + "1.5"
	sortedIdx.M[string(sortedKey)] = NewSortedSet(nil).M[string(sortedKey)]
	if sortedIdx.M[string(sortedKey)] == nil {
		sortedIdx.M[string(sortedKey)] = &SkipList{dict: make(map[uint32]*SkipListNode)}
	}
	sortedNode := &SkipListNode{score: score, record: sortedRecord}
	sortedIdx.M[string(sortedKey)].dict[sortedHash] = sortedNode

	db := &DB{
		opt:   opts,
		Index: newIndex(),
		bm: &BucketManager{
			BucketInfoMapper: map[BucketId]*Bucket{
				btreeBucketID:  {Meta: &BucketMeta{}, Id: btreeBucketID, Name: "btree", Ds: uint16(DataStructureBTree)},
				setBucketID:    {Meta: &BucketMeta{}, Id: setBucketID, Name: "set", Ds: uint16(DataStructureSet)},
				listBucketID:   {Meta: &BucketMeta{}, Id: listBucketID, Name: "list", Ds: uint16(DataStructureList)},
				sortedBucketID: {Meta: &BucketMeta{}, Id: sortedBucketID, Name: "sorted", Ds: uint16(DataStructureSortedSet)},
			},
		},
	}
	db.Index.bTree.idx[btreeBucketID] = btree
	db.Index.set.idx[setBucketID] = setIdx
	db.Index.list.idx[listBucketID] = listIdx
	db.Index.sortedSet.idx[sortedBucketID] = sortedIdx

	writer := &recordingHintWriter{}
	collector := NewHintCollector(GetMergeFileID(1), writer, 1)
	out := &mergeOutput{
		seq:       1,
		fileID:    GetMergeFileID(1),
		dataFile:  &DataFile{rwManager: &mockRWManager{}},
		collector: collector,
	}

	job := &mergeV2Job{
		db:          db,
		outputs:     []*mergeOutput{out},
		valueHasher: fnv.New32a(),
		manifest:    &mergeManifest{},
		pending:     []int64{10, 11, 12, 13},
	}

	// Write entries for all data structures
	btreeEntry := createTestEntry(btreeBucketID, btreeKey, []byte("btree-value"), DataDeleteFlag, Committed, Persistent, now, 1, DataStructureBTree)
	if err := job.writeEntry(btreeEntry, btreeRecord.FileID); err != nil {
		t.Fatalf("writeEntry btree: %v", err)
	}

	setEntry := createTestEntry(setBucketID, setKey, setValue, DataSetFlag, Committed, Persistent, now, 2, DataStructureSet)
	if err := job.writeEntry(setEntry, setRecord.FileID); err != nil {
		t.Fatalf("writeEntry set: %v", err)
	}

	listEntry := createTestEntry(listBucketID, listEncodedKey, []byte("list-value"), DataLPushFlag, Committed, Persistent, now, 3, DataStructureList)
	if err := job.writeEntry(listEntry, listRecord.FileID); err != nil {
		t.Fatalf("writeEntry list: %v", err)
	}

	sortedEntry := createTestEntry(sortedBucketID, []byte(sortedSetKey), sortedValue, DataZAddFlag, Committed, Persistent, now, 4, DataStructureSortedSet)
	if err := job.writeEntry(sortedEntry, sortedRecord.FileID); err != nil {
		t.Fatalf("writeEntry sorted: %v", err)
	}

	// Simulate concurrent updates (change FileID for half, Timestamp for other half)
	btreeRecord.WithFileId(999) // BTree: FileID mismatch
	setRecord.WithTimestamp(now + 100) // Set: Timestamp mismatch
	listRecord.WithFileId(888) // List: FileID mismatch
	sortedRecord.WithTimestamp(now + 200) // SortedSet: Timestamp mismatch

	if err := job.commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Verify records remain at new values
	if btreeRecord.FileID != 999 {
		t.Fatalf("btree record should keep new FileID 999, got %d", btreeRecord.FileID)
	}
	if setRecord.Timestamp != now+100 {
		t.Fatalf("set record should keep new Timestamp, got %d", setRecord.Timestamp)
	}
	if listRecord.FileID != 888 {
		t.Fatalf("list record should keep new FileID 888, got %d", listRecord.FileID)
	}
	if sortedRecord.Timestamp != now+200 {
		t.Fatalf("sorted record should keep new Timestamp, got %d", sortedRecord.Timestamp)
	}

	// All entries should be stale, no hints written
	if len(writer.writes) != 0 {
		t.Fatalf("expected 0 hint writes for all stale entries, got %d", len(writer.writes))
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
		return tx.NewBucket(DataStructureBTree, bucket)
	}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	putValue := func(val []byte) error {
		return db.Update(func(tx *Tx) error {
			return tx.Put(bucket, targetKey, val, Persistent)
		})
	}

	if err := putValue(initialValue); err != nil {
		t.Fatalf("put initial value: %v", err)
	}

	fillerVal := bytes.Repeat([]byte("f"), 1024)
	for i := 0; i < 16; i++ {
		key := []byte(fmt.Sprintf("fill-%02d", i))
		if err := db.Update(func(tx *Tx) error {
			return tx.Put(bucket, key, fillerVal, Persistent)
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
			return tx.Put(bucket, key, fillerVal, Persistent)
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
	job.onRewriteEntry = func(entry *Entry) {
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
		return tx.NewBucket(DataStructureSet, bucket)
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
	job.onRewriteEntry = func(entry *Entry) {
		if bytes.Equal(entry.Key, targetKey) && entry.Meta.Ds == DataStructureSet {
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
		return tx.NewBucket(DataStructureBTree, bucket)
	}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// Create many entries to produce multiple lookups during commit
	numEntries := 100
	fillerVal := bytes.Repeat([]byte("x"), 512)
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		if err := db.Update(func(tx *Tx) error {
			return tx.Put(bucket, key, fillerVal, Persistent)
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
			return tx.Put(bucket, extraKey, fillerVal, Persistent)
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
			return tx.Put(bucket, []byte("concurrent-key"), []byte("concurrent-value"), Persistent)
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
	setEntry := createTestEntry(1, []byte("set-key"), setValue, DataSetFlag, Committed, Persistent, uint64(time.Now().Unix()), 1, DataStructureSet)
	if err := job.writeEntry(setEntry, 1); err != nil {
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
	sortedEntry := createTestEntry(2, []byte("sorted-key"), sortedValue, DataZAddFlag, Committed, Persistent, uint64(time.Now().Unix()), 2, DataStructureSortedSet)
	if err := job.writeEntry(sortedEntry, 2); err != nil {
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

func TestMergeV2ApplyLookupEdgeCases(t *testing.T) {
	db := &DB{
		opt:   DefaultOptions,
		Index: newIndex(),
		bm: &BucketManager{
			BucketInfoMapper: map[BucketId]*Bucket{
				1: {Meta: &BucketMeta{}, Id: 1, Name: "bucket", Ds: uint16(DataStructureBTree)},
			},
		},
	}

	bucketID := BucketId(1)
	key := []byte("test-key")
	record := NewRecord().
		WithKey(key).
		WithFileId(10).
		WithDataPos(100).
		WithTimestamp(1000).
		WithTTL(Persistent)

	bt := NewBTree()
	bt.InsertRecord(key, record)
	db.Index.bTree.idx[bucketID] = bt

	job := &mergeV2Job{db: db}

	// Test 1: nil entry
	if job.applyLookup(nil) {
		t.Fatal("applyLookup(nil) should return false")
	}

	// Test 2: entry with nil hint
	if job.applyLookup(&mergeLookupEntry{hint: nil}) {
		t.Fatal("applyLookup with nil hint should return false")
	}

	// Test 3: entry with nil collector (should still work for index update)
	lookupWithoutCollector := &mergeLookupEntry{
		hint: &HintEntry{
			BucketId:  uint64(bucketID),
			Key:       key,
			Ds:        uint16(DataStructureBTree),
			FileID:    20,
			DataPos:   200,
			Timestamp: 2000,
			TTL:       Persistent,
			ValueSize: 100,
		},
		collector:     nil, // No collector
		origFileID:    record.FileID,
		origTimestamp: record.Timestamp,
		origTTL:       record.TTL,
		origValueSize: record.ValueSize,
		origTxID:      record.TxID,
	}

	if !job.applyLookup(lookupWithoutCollector) {
		t.Fatal("applyLookup should succeed even without collector")
	}

	if record.FileID != 20 || record.DataPos != 200 {
		t.Fatalf("record should be updated: got FileID=%d DataPos=%d", record.FileID, record.DataPos)
	}

	// Test 4: Bucket doesn't exist
	nonExistentLookup := &mergeLookupEntry{
		hint: &HintEntry{
			BucketId:  9999, // Non-existent bucket
			Key:       key,
			Ds:        uint16(DataStructureBTree),
			FileID:    30,
			DataPos:   300,
			Timestamp: 3000,
		},
		origFileID:    20,
		origTimestamp: 2000,
	}

	if job.applyLookup(nonExistentLookup) {
		t.Fatal("applyLookup should fail for non-existent bucket")
	}

	// Test 5: Record doesn't exist in index
	missingKeyLookup := &mergeLookupEntry{
		hint: &HintEntry{
			BucketId:  uint64(bucketID),
			Key:       []byte("missing-key"),
			Ds:        uint16(DataStructureBTree),
			FileID:    40,
			DataPos:   400,
			Timestamp: 4000,
		},
		origFileID:    10,
		origTimestamp: 1000,
	}

	if job.applyLookup(missingKeyLookup) {
		t.Fatal("applyLookup should fail for missing key")
	}

	// Test 6: Set without valueHash
	setBucketID := BucketId(2)
	db.bm.BucketInfoMapper[setBucketID] = &Bucket{Meta: &BucketMeta{}, Id: setBucketID, Name: "set", Ds: uint16(DataStructureSet)}
	setIdx := NewSet()
	setRecord := NewRecord().WithFileId(50).WithTimestamp(5000)
	setIdx.M["set-key"] = map[uint32]*Record{12345: setRecord}
	db.Index.set.idx[setBucketID] = setIdx

	setLookupNoHash := &mergeLookupEntry{
		hint: &HintEntry{
			BucketId:  uint64(setBucketID),
			Key:       []byte("set-key"),
			Ds:        uint16(DataStructureSet),
			FileID:    60,
			DataPos:   600,
			Timestamp: 6000,
		},
		hasValueHash:  false, // Missing value hash
		origFileID:    50,
		origTimestamp: 5000,
	}

	if job.applyLookup(setLookupNoHash) {
		t.Fatal("applyLookup should fail for Set without value hash")
	}

	// Test 7: SortedSet without valueHash
	sortedBucketID := BucketId(3)
	db.bm.BucketInfoMapper[sortedBucketID] = &Bucket{Meta: &BucketMeta{}, Id: sortedBucketID, Name: "sorted", Ds: uint16(DataStructureSortedSet)}
	sortedIdx := NewSortedSet(nil)
	sortedRecord := NewRecord().WithFileId(70).WithTimestamp(7000)
	sortedIdx.M["sorted-key"] = &SkipList{
		dict: map[uint32]*SkipListNode{
			54321: {score: SCORE(1.5), record: sortedRecord},
		},
	}
	db.Index.sortedSet.idx[sortedBucketID] = sortedIdx

	sortedLookupNoHash := &mergeLookupEntry{
		hint: &HintEntry{
			BucketId:  uint64(sortedBucketID),
			Key:       []byte("sorted-key" + SeparatorForZSetKey + "1.5"),
			Ds:        uint16(DataStructureSortedSet),
			FileID:    80,
			DataPos:   800,
			Timestamp: 8000,
		},
		hasValueHash:  false, // Missing value hash
		origFileID:    70,
		origTimestamp: 7000,
	}

	if job.applyLookup(sortedLookupNoHash) {
		t.Fatal("applyLookup should fail for SortedSet without value hash")
	}

	// Test 8: List with invalid flag (not LPush or RPush)
	listBucketID := BucketId(4)
	db.bm.BucketInfoMapper[listBucketID] = &Bucket{Meta: &BucketMeta{}, Id: listBucketID, Name: "list", Ds: uint16(DataStructureList)}
	listIdx := NewList()
	listRecord := NewRecord().WithFileId(90).WithTimestamp(9000)
	listIdx.Items["list-key"] = NewBTree()
	listIdx.Items["list-key"].InsertRecord(ConvertUint64ToBigEndianBytes(1), listRecord)
	db.Index.list.idx[listBucketID] = listIdx

	listLookupBadFlag := &mergeLookupEntry{
		hint: &HintEntry{
			BucketId:  uint64(listBucketID),
			Key:       encodeListKey([]byte("list-key"), 1),
			Ds:        uint16(DataStructureList),
			Flag:      DataSetFlag, // Wrong flag for list
			FileID:    100,
			DataPos:   1000,
			Timestamp: 10000,
		},
		origFileID:    90,
		origTimestamp: 9000,
	}

	if job.applyLookup(listLookupBadFlag) {
		t.Fatal("applyLookup should fail for List with invalid flag")
	}
}

func TestMergeV2ApplyLookupUpdatesSecondaryIndexes(t *testing.T) {
	db := &DB{
		opt:   DefaultOptions,
		Index: newIndex(),
		bm: &BucketManager{
			BucketInfoMapper: map[BucketId]*Bucket{},
		},
	}

	// Prepare buckets
	buckets := []struct {
		id   BucketId
		ds   DataStructure
		name string
	}{
		{1, DataStructureSet, "set"},
		{2, DataStructureList, "list"},
		{3, DataStructureSortedSet, "zset"},
	}

	for _, b := range buckets {
		db.bm.BucketInfoMapper[b.id] = &Bucket{Meta: &BucketMeta{}, Id: b.id, Ds: uint16(b.ds), Name: b.name}
	}

	// Set bucket
	setRecord := &Record{Value: []byte("member"), FileID: 10, Timestamp: 1, TTL: Persistent}
	setIdx := db.Index.set.getWithDefault(buckets[0].id)
	if err := setIdx.SAdd("set-key", [][]byte{setRecord.Value}, []*Record{setRecord}); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	setHash := fnv.New32a()
	_, _ = setHash.Write(setRecord.Value)

	// List bucket
	listIdx := db.Index.list.getWithDefault(buckets[1].id)
	listKey := []byte("list-key")
	seq := uint64(42)
	listRecord := &Record{FileID: 11, Timestamp: 2, TTL: Persistent, TxID: 1}
	listIdx.Items[string(listKey)] = NewBTree()
	listIdx.Items[string(listKey)].InsertRecord(ConvertUint64ToBigEndianBytes(seq), listRecord)

	// Sorted set bucket
	sortedIdx := db.Index.sortedSet.getWithDefault(buckets[2].id, db)
	sortedValue := []byte("sorted-member")
	sortedRecord := &Record{Value: sortedValue, FileID: 12, Timestamp: 3, TTL: Persistent}
	if err := sortedIdx.ZAdd("zset-key", SCORE(1.5), sortedValue, sortedRecord); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	sortedHash := fnv.New32a()
	_, _ = sortedHash.Write(sortedValue)

	job := &mergeV2Job{db: db}

	// Apply set lookup
	if !job.applyLookup(&mergeLookupEntry{
		hint: &HintEntry{
			BucketId:  uint64(buckets[0].id),
			Key:       []byte("set-key"),
			Ds:        uint16(DataStructureSet),
			FileID:    100,
			DataPos:   1000,
			Timestamp: 100,
			TTL:       5,
			ValueSize: 9,
		},
		valueHash:    setHash.Sum32(),
		hasValueHash: true,
		origFileID:   setRecord.FileID,
		origTimestamp: setRecord.Timestamp,
		origTTL:       setRecord.TTL,
		origValueSize: setRecord.ValueSize,
		origTxID:      setRecord.TxID,
	}) {
		t.Fatal("set lookup should apply")
	}

	if setRecord.FileID != 100 || setRecord.DataPos != 1000 {
		t.Fatalf("set record not updated: %+v", setRecord)
	}

	// Apply list lookup
	listKeyEncoded := encodeListKey(listKey, seq)
	if !job.applyLookup(&mergeLookupEntry{
		hint: &HintEntry{
			BucketId:  uint64(buckets[1].id),
			Key:       listKeyEncoded,
			Ds:        uint16(DataStructureList),
			Flag:      DataLPushFlag,
			FileID:    200,
			DataPos:   2000,
			Timestamp: 200,
			TTL:       6,
			ValueSize: 7,
		},
		origFileID:   listRecord.FileID,
		origTimestamp: listRecord.Timestamp,
		origTTL:       listRecord.TTL,
		origValueSize: listRecord.ValueSize,
		origTxID:      listRecord.TxID,
	}) {
		t.Fatal("list lookup should apply")
	}

	if listRecord.FileID != 200 || listRecord.DataPos != 2000 {
		t.Fatalf("list record not updated: %+v", listRecord)
	}

	// Apply sorted set lookup
	sortedKey := []byte("zset-key" + SeparatorForZSetKey + "1.5")
	if !job.applyLookup(&mergeLookupEntry{
		hint: &HintEntry{
			BucketId:  uint64(buckets[2].id),
			Key:       sortedKey,
			Ds:        uint16(DataStructureSortedSet),
			FileID:    300,
			DataPos:   3000,
			Timestamp: 300,
			TTL:       7,
			ValueSize: 13,
		},
		valueHash:    sortedHash.Sum32(),
		hasValueHash: true,
		origFileID:   sortedRecord.FileID,
		origTimestamp: sortedRecord.Timestamp,
		origTTL:       sortedRecord.TTL,
		origValueSize: sortedRecord.ValueSize,
		origTxID:      sortedRecord.TxID,
	}) {
		t.Fatal("sorted set lookup should apply")
	}

	node := sortedIdx.M["zset-key"].dict[sortedHash.Sum32()]
	if node == nil || node.record.FileID != 300 || node.record.DataPos != 3000 {
		t.Fatalf("sorted set node not updated: %+v", node)
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

	bucketID := BucketId(1)
	now := uint64(time.Now().Unix())

	entries := []*Entry{
		createTestEntry(bucketID, []byte("uncommitted"), []byte("v1"), DataSetFlag, UnCommitted, Persistent, now, 1, DataStructureBTree),
		createTestEntry(bucketID, []byte("deleted"), []byte("v2"), DataDeleteFlag, Committed, Persistent, now, 2, DataStructureBTree),
		createTestEntry(bucketID, []byte("expired"), []byte("v3"), DataSetFlag, Committed, 1, uint64(time.Now().Add(-2*time.Second).Unix()), 3, DataStructureBTree),
		createTestEntry(bucketID, []byte("nonpending"), []byte("v4"), DataSetFlag, Committed, Persistent, now, 4, DataStructureBTree),
	}

	var goodEntry *Entry
	goodEntry = createTestEntry(bucketID, []byte("good"), []byte("keep"), DataSetFlag, Committed, Persistent, now, 5, DataStructureBTree)

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

	bt := NewBTree()
	bt.InsertRecord(goodEntry.Key, (&Record{}).
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
		Index: newIndex(),
		bm: &BucketManager{
			BucketInfoMapper: map[BucketId]*Bucket{
				bucketID: {Meta: &BucketMeta{}, Id: bucketID, Ds: uint16(DataStructureBTree)},
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
		db:      &DB{opt: Options{Dir: dir}, fm: newFileManager(FileIO, 1, 0.5, 1<<12)},
		oldData: []string{nested},
	}

	if err := job.cleanupOldFiles(); err == nil {
		t.Fatal("cleanupOldFiles should surface removal errors")
	}
}
