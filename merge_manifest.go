package nutsdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type mergeManifestStatus string

const (
	mergeManifestFileName       = "merge_manifest.json"
	mergeManifestTempFileSuffix = ".tmp"

	manifestStatusWriting   mergeManifestStatus = "writing"
	manifestStatusCommitted mergeManifestStatus = "committed"
)

type mergeManifest struct {
	Status            mergeManifestStatus `json:"status"`
	MergeSeqMax       int                 `json:"mergeSeqMax"`
	PendingOldFileIDs []int64             `json:"pendingOldFileIDs"`
}

func loadMergeManifest(dir string) (*mergeManifest, error) {
	path := filepath.Join(dir, mergeManifestFileName)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read merge manifest: %w", err)
	}
	var manifest mergeManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("decode merge manifest: %w", err)
	}
	return &manifest, nil
}

func writeMergeManifest(dir string, manifest *mergeManifest) error {
	data, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("encode merge manifest: %w", err)
	}
	manifestPath := filepath.Join(dir, mergeManifestFileName)
	tmpPath := manifestPath + mergeManifestTempFileSuffix
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write manifest tmp: %w", err)
	}
	if err := os.Rename(tmpPath, manifestPath); err != nil {
		return fmt.Errorf("rename manifest tmp: %w", err)
	}
	return nil
}

func removeMergeManifest(dir string) error {
	path := filepath.Join(dir, mergeManifestFileName)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove merge manifest: %w", err)
	}
	return nil
}
