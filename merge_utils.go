package nutsdb

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const MergeFileIDBase int64 = math.MinInt64

func GetMergeFileID(seq int) int64 {
	return MergeFileIDBase + int64(seq)
}

func IsMergeFile(fileID int64) bool {
	return fileID < 0
}

func GetMergeSeq(fileID int64) int {
	return int(fileID - MergeFileIDBase)
}

func mergeFilePrefix() string {
	return "merge_"
}

func mergeDataFileName(seq int) string {
	return fmt.Sprintf("%s%d%s", mergeFilePrefix(), seq, DataSuffix)
}

func mergeHintFileName(seq int) string {
	return fmt.Sprintf("%s%d%s", mergeFilePrefix(), seq, HintSuffix)
}

func getMergeDataPath(dir string, seq int) string {
	return filepath.Join(dir, mergeDataFileName(seq))
}

func getMergeHintPath(dir string, seq int) string {
	return filepath.Join(dir, mergeHintFileName(seq))
}

func parseMergeSeq(name string) (int, bool) {
	if !strings.HasPrefix(name, mergeFilePrefix()) {
		return 0, false
	}
	suffix := strings.TrimPrefix(name, mergeFilePrefix())
	idx := strings.Index(suffix, ".")
	if idx > 0 {
		suffix = suffix[:idx]
	}
	seq, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, false
	}
	return seq, true
}

func enumerateDataFileIDs(dir string) (userIDs []int64, mergeIDs []int64, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, DataSuffix) {
			continue
		}
		base := strings.TrimSuffix(name, DataSuffix)
		if seq, ok := parseMergeSeq(base); ok {
			mergeIDs = append(mergeIDs, GetMergeFileID(seq))
			continue
		}
		id, err := strconv.ParseInt(base, 10, 64)
		if err != nil {
			continue
		}
		userIDs = append(userIDs, id)
	}
	sort.Slice(userIDs, func(i, j int) bool { return userIDs[i] < userIDs[j] })
	sort.Slice(mergeIDs, func(i, j int) bool { return mergeIDs[i] < mergeIDs[j] })
	return userIDs, mergeIDs, nil
}

func purgeMergeFiles(dir string, keep map[int64]struct{}) error {
	_, mergeIDs, err := enumerateDataFileIDs(dir)
	if err != nil {
		return err
	}
	for _, fid := range mergeIDs {
		if keep != nil {
			if _, ok := keep[fid]; ok {
				continue
			}
		}
		if err := os.Remove(getDataPath(fid, dir)); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.Remove(getHintPath(fid, dir)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
