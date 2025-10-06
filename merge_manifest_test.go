package nutsdb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadMergeManifestHandlesReadAndDecodeErrors(t *testing.T) {
	dir := t.TempDir()

	manifestPath := filepath.Join(dir, mergeManifestFileName)
	if err := os.Mkdir(manifestPath, 0o755); err != nil {
		t.Fatalf("mkdir manifest path: %v", err)
	}

	if _, err := loadMergeManifest(dir); err == nil {
		t.Fatal("loadMergeManifest should fail when manifest path is a directory")
	}

	if err := os.Remove(manifestPath); err != nil {
		t.Fatalf("remove directory: %v", err)
	}

	if err := os.WriteFile(manifestPath, []byte("not-json"), 0o644); err != nil {
		t.Fatalf("write corrupt manifest: %v", err)
	}

	if _, err := loadMergeManifest(dir); err == nil {
		t.Fatal("loadMergeManifest should fail on corrupt JSON")
	}
}

func TestWriteMergeManifestPropagatesWriteAndRenameErrors(t *testing.T) {
	dir := t.TempDir()

	// Force the tmp write to fail by pre-creating a directory with the tmp suffix name.
	tmpPath := filepath.Join(dir, mergeManifestFileName+mergeManifestTempFileSuffix)
	if err := os.Mkdir(tmpPath, 0o755); err != nil {
		t.Fatalf("mkdir tmp path: %v", err)
	}

	manifest := &mergeManifest{Status: manifestStatusWriting}
	if err := writeMergeManifest(dir, manifest); err == nil {
		t.Fatal("writeMergeManifest should fail when tmp path is not writable")
	}

	if err := os.Remove(tmpPath); err != nil {
		t.Fatalf("cleanup tmp dir: %v", err)
	}

	// Create the real manifest path as a directory so rename fails.
	manifestPath := filepath.Join(dir, mergeManifestFileName)
	if err := os.Mkdir(manifestPath, 0o755); err != nil {
		t.Fatalf("mkdir real manifest path: %v", err)
	}

	if err := writeMergeManifest(dir, manifest); err == nil {
		t.Fatal("writeMergeManifest should fail when rename target is a directory")
	}
}

func TestRemoveMergeManifestIgnoresMissingFile(t *testing.T) {
	dir := t.TempDir()
	if err := removeMergeManifest(dir); err != nil {
		t.Fatalf("removeMergeManifest should ignore missing file: %v", err)
	}
}
