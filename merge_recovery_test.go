package nutsdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoverMergeManifestWritingState(t *testing.T) {
	dir := t.TempDir()
	createFile := func(name string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644))
	}

	createFile("merge_0.dat")
	createFile("merge_0.hint")
	createFile("merge_1.dat")
	createFile("merge_1.hint")

	manifest := &mergeManifest{
		Status:            manifestStatusWriting,
		MergeSeqMax:       1,
		PendingOldFileIDs: []int64{GetMergeFileID(1), 0},
	}

	require.NoError(t, writeMergeManifest(dir, manifest))
	opts := DefaultOptions
	opts.Dir = dir
	db := &DB{opt: opts}

	require.NoError(t, db.recoverMergeManifest())

	manifestPath := filepath.Join(dir, mergeManifestFileName)
	_, err := os.Stat(manifestPath)
	require.Truef(t, os.IsNotExist(err), "manifest file should be removed")

	_, err = os.Stat(filepath.Join(dir, "merge_0.dat"))
	require.Truef(t, os.IsNotExist(err), "merge_0.dat should be removed")
	_, err = os.Stat(filepath.Join(dir, "merge_0.hint"))
	require.Truef(t, os.IsNotExist(err), "merge_0.hint should be removed")

	_, err = os.Stat(filepath.Join(dir, "merge_1.dat"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, "merge_1.hint"))
	require.NoError(t, err)
}

func TestRecoverMergeManifestCommittedState(t *testing.T) {
	dir := t.TempDir()
	createFile := func(name string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644))
	}

	createFile("0.dat")
	createFile("0.hint")
	createFile("1.dat")

	manifest := &mergeManifest{
		Status:            manifestStatusCommitted,
		PendingOldFileIDs: []int64{0, 1},
	}

	require.NoError(t, writeMergeManifest(dir, manifest))
	opts := DefaultOptions
	opts.Dir = dir
	db := &DB{opt: opts}

	require.NoError(t, db.recoverMergeManifest())

	manifestPath := filepath.Join(dir, mergeManifestFileName)
	_, err := os.Stat(manifestPath)
	require.Truef(t, os.IsNotExist(err), "manifest file should be removed")

	_, err = os.Stat(filepath.Join(dir, "0.dat"))
	require.Truef(t, os.IsNotExist(err), "0.dat should be removed")
	_, err = os.Stat(filepath.Join(dir, "0.hint"))
	require.Truef(t, os.IsNotExist(err), "0.hint should be removed")

	_, err = os.Stat(filepath.Join(dir, "1.dat"))
	require.Truef(t, os.IsNotExist(err), "1.dat should be removed")
	_, err = os.Stat(filepath.Join(dir, "1.hint"))
	require.Truef(t, os.IsNotExist(err), "1.hint should be removed")
}

func TestRecoverMergeManifestUnknownStatus(t *testing.T) {
	dir := t.TempDir()
	manifest := &mergeManifest{
		Status:            mergeManifestStatus("unexpected"),
		PendingOldFileIDs: []int64{0},
	}

	require.NoError(t, writeMergeManifest(dir, manifest))
	opts := DefaultOptions
	opts.Dir = dir
	db := &DB{opt: opts}

	err := db.recoverMergeManifest()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown merge manifest status")

	manifestPath := filepath.Join(dir, mergeManifestFileName)
	_, statErr := os.Stat(manifestPath)
	require.NoError(t, statErr)
}

func TestRecoverMergeManifestWritingStatus(t *testing.T) {
	dir := t.TempDir()

	keepID := GetMergeFileID(0)
	manifest := &mergeManifest{
		Status:            manifestStatusWriting,
		PendingOldFileIDs: []int64{keepID, 0},
	}
	if err := writeMergeManifest(dir, manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	mustTouch := func(name string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("touch %s: %v", name, err)
		}
	}

	mustTouch("merge_0.dat")
	mustTouch("merge_0.hint")
	mustTouch("merge_1.dat")
	mustTouch("merge_1.hint")

	db := &DB{opt: Options{Dir: dir}}
	if err := db.recoverMergeManifest(); err != nil {
		t.Fatalf("recoverMergeManifest: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "merge_0.dat")); err != nil {
		t.Fatalf("merge_0.dat should be preserved: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "merge_1.dat")); !os.IsNotExist(err) {
		t.Fatalf("merge_1.dat should be purged, err=%v", err)
	}
	if manifest, err := loadMergeManifest(dir); err != nil || manifest != nil {
		t.Fatalf("manifest should be removed, got %v %v", manifest, err)
	}
}

func TestRecoverMergeManifestCommittedStatus(t *testing.T) {
	dir := t.TempDir()

	mustTouch := func(name string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("touch %s: %v", name, err)
		}
	}

	mustTouch("0.dat")
	mustTouch("0.hint")

	manifest := &mergeManifest{
		Status:            manifestStatusCommitted,
		PendingOldFileIDs: []int64{0, 1},
	}
	if err := writeMergeManifest(dir, manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	db := &DB{opt: Options{Dir: dir}}
	if err := db.recoverMergeManifest(); err != nil {
		t.Fatalf("recover committed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "0.dat")); !os.IsNotExist(err) {
		t.Fatalf("0.dat should be removed, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "0.hint")); !os.IsNotExist(err) {
		t.Fatalf("0.hint should be removed, err=%v", err)
	}
	if manifest, err := loadMergeManifest(dir); err != nil || manifest != nil {
		t.Fatalf("manifest should be removed, got %v %v", manifest, err)
	}
}

func TestRecoverMergeManifestUnknownStatusFails(t *testing.T) {
	dir := t.TempDir()
	manifest := &mergeManifest{Status: "unknown"}
	if err := writeMergeManifest(dir, manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	db := &DB{opt: Options{Dir: dir}}
	if err := db.recoverMergeManifest(); err == nil {
		t.Fatal("recoverMergeManifest should fail on unknown status")
	}
}
