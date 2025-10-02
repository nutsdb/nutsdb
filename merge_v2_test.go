package nutsdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
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

func TestEnumerateDataFileIDs(t *testing.T) {
	dir := t.TempDir()

	// Create some test files
	createFile := func(name string) {
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
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
		f.Close()
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
