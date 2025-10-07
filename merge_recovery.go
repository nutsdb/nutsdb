package nutsdb

import (
	"errors"
	"fmt"
	"os"
)

func (db *DB) recoverMergeManifest() error {
	manifest, err := loadMergeManifest(db.opt.Dir)
	if err != nil {
		return err
	}
	if manifest == nil {
		return nil
	}

	switch manifest.Status {
	case manifestStatusWriting:
		keep := make(map[int64]struct{}, len(manifest.PendingOldFileIDs))
		for _, fid := range manifest.PendingOldFileIDs {
			if !IsMergeFile(fid) {
				continue
			}
			keep[fid] = struct{}{}
		}
		if err := purgeMergeFiles(db.opt.Dir, keep); err != nil {
			return fmt.Errorf("purge stale merge files: %w", err)
		}
		if err := removeMergeManifest(db.opt.Dir); err != nil {
			return err
		}
	case manifestStatusCommitted:
		for _, fid := range manifest.PendingOldFileIDs {
			if err := os.Remove(getDataPath(fid, db.opt.Dir)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove old data file %d: %w", fid, err)
			}
			if err := os.Remove(getHintPath(fid, db.opt.Dir)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove old hint file %d: %w", fid, err)
			}
		}
		if err := removeMergeManifest(db.opt.Dir); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown merge manifest status %q", manifest.Status)
	}

	return nil
}
