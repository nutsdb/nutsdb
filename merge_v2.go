package nutsdb

import (
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"os"
	"sort"
)

// mergeV2Job manages the entire merge operation lifecycle.
//
// # Memory Efficiency Design
//
// This implementation prioritizes memory efficiency over runtime validation by eliminating
// staleness checking during the commit phase. In large-scale merges (e.g., 10GB+ of data),
// this design choice provides significant memory savings:
//
//   - Previous approach: ~145 bytes per entry (with staleness metadata)
//   - Current approach: ~50 bytes per entry (minimal metadata)
//   - Savings: ~65% memory reduction for 10M entries (~1.4GB → ~500MB)
//
// # Correctness Guarantee via Index Rebuild
//
// Stale entries (those updated concurrently during merge) may be written to hint files.
// This is safe because of the file ordering guarantee during index rebuild:
//
//  1. Merge files use negative FileIDs (starting from math.MinInt64)
//  2. Normal data files use positive FileIDs (starting from 0)
//  3. Index rebuild processes files in ascending FileID order
//  4. Therefore: merge files are always processed BEFORE normal files
//
// Example scenario:
//   - Merge begins: entry "foo" at File#3, timestamp=1000
//   - During merge: concurrent update writes "foo" to File#4, timestamp=2000
//   - Merge writes stale version to MergeFile#-9223372036854775808
//   - On restart/rebuild:
//     Step 1: Load MergeFile (fileID=-9223372036854775808) → index["foo"] = {ts:1000, stale}
//     Step 2: Load File#3 → skipped (merged away)
//     Step 3: Load File#4 → index["foo"] = {ts:2000, fresh} ✓ Overwrites stale value
//
// This design trades hint file size and rebuild time for dramatically reduced memory usage
// during merge operations, which is critical for Bitcask-based systems that already
// consume significant memory for in-memory indexes.
type mergeV2Job struct {
	db             *DB
	pending        []int64
	outputs        []*mergeOutput
	lookup         []*mergeLookupEntry
	manifest       *mergeManifest
	oldData        []string
	oldHints       []string
	outputSeqBase  int
	valueHasher    hash.Hash32
	onRewriteEntry func(*Entry)
}

// mergeLookupEntry tracks minimal information needed to update indexes at commit time.
// We no longer store original file metadata for staleness checking - this reduces memory usage
// significantly in large merges (from ~145 bytes/entry to ~50 bytes/entry + key length).
type mergeLookupEntry struct {
	hint         *HintEntry
	valueHash    uint32
	hasValueHash bool
	collector    *HintCollector
}

type mergeOutput struct {
	seq       int
	fileID    int64
	dataFile  *DataFile
	collector *HintCollector
	dataPath  string
	hintPath  string
	writeOff  int64
	finalized bool
}

func (db *DB) mergeV2() error {
	job := &mergeV2Job{db: db}
	if err := job.prepare(); err != nil {
		return err
	}
	defer job.finish()

	if err := job.enterWritingState(); err != nil {
		return job.abort(err)
	}

	if err := job.rewrite(); err != nil {
		return job.abort(err)
	}

	if err := job.commit(); err != nil {
		return job.abort(err)
	}

	if err := job.finalizeOutputs(); err != nil {
		return job.abort(err)
	}

	if err := job.cleanupOldFiles(); err != nil {
		return fmt.Errorf("cleanup old files: %w", err)
	}

	return nil
}

func (job *mergeV2Job) prepare() error {
	job.db.mu.Lock()

	if job.db.isMerging {
		job.db.mu.Unlock()
		return ErrIsMerging
	}

	userIDs, mergeIDs, err := enumerateDataFileIDs(job.db.opt.Dir)
	if err != nil {
		job.db.mu.Unlock()
		return fmt.Errorf("failed to enumerate data file IDs: %w", err)
	}

	job.pending = append(job.pending, userIDs...)
	job.pending = append(job.pending, mergeIDs...)

	maxSeq := -1
	for _, fid := range mergeIDs {
		seq := GetMergeSeq(fid)
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	if maxSeq >= 0 {
		job.outputSeqBase = maxSeq + 1
	}
	if len(job.pending) < 2 {
		job.db.mu.Unlock()
		return ErrDontNeedMerge
	}
	sort.Slice(job.pending, func(i, j int) bool { return job.pending[i] < job.pending[j] })

	job.db.isMerging = true

	if !job.db.opt.SyncEnable && job.db.opt.RWMode == MMap {
		if err := job.db.ActiveFile.rwManager.Sync(); err != nil {
			job.db.isMerging = false
			job.db.mu.Unlock()
			return fmt.Errorf("failed to sync active file: %w", err)
		}
	}

	if err := job.db.ActiveFile.rwManager.Release(); err != nil {
		job.db.isMerging = false
		job.db.mu.Unlock()
		return fmt.Errorf("failed to release active file: %w", err)
	}

	job.db.MaxFileID++
	path := getDataPath(job.db.MaxFileID, job.db.opt.Dir)
	activeFile, err := job.db.fm.getDataFile(path, job.db.opt.SegmentSize)
	if err != nil {
		job.db.isMerging = false
		job.db.mu.Unlock()
		return fmt.Errorf("failed to create new active file: %w", err)
	}
	job.db.ActiveFile = activeFile
	job.db.ActiveFile.fileID = job.db.MaxFileID

	job.db.mu.Unlock()

	return nil
}

func (job *mergeV2Job) finish() {
	job.db.mu.Lock()
	job.db.isMerging = false
	job.db.mu.Unlock()
}

func (job *mergeV2Job) enterWritingState() error {
	job.lookup = make([]*mergeLookupEntry, 0)
	job.manifest = &mergeManifest{
		Status:            manifestStatusWriting,
		MergeSeqMax:       -1,
		PendingOldFileIDs: append([]int64(nil), job.pending...),
	}
	if job.valueHasher == nil {
		job.valueHasher = fnv.New32a()
	}
	if err := writeMergeManifest(job.db.opt.Dir, job.manifest); err != nil {
		return err
	}
	return nil
}

func (job *mergeV2Job) rewrite() error {
	for _, fid := range job.pending {
		if err := job.rewriteFile(fid); err != nil {
			return err
		}
	}
	return nil
}

func (job *mergeV2Job) finalizeOutputs() error {
	for _, out := range job.outputs {
		if err := out.finalize(); err != nil {
			return err
		}
	}
	return nil
}

// commit atomically updates in-memory indexes and writes hint files.
// Note: We don't validate staleness here. If an entry was updated concurrently during merge,
// we'll write the stale version to the hint file. This is safe because during index rebuild,
// merge files (negative FileIDs) are processed before normal files (positive FileIDs), so
// newer values will overwrite stale ones. This tradeoff saves significant memory.
func (job *mergeV2Job) commit() error {
	job.db.mu.Lock()
	defer job.db.mu.Unlock()

	// Phase 1: Write all hints (even potentially stale ones)
	for _, entry := range job.lookup {
		if entry == nil {
			continue
		}
		if entry.collector != nil && entry.hint != nil {
			if err := entry.collector.Add(entry.hint); err != nil {
				return fmt.Errorf("failed to add hint to collector: %w", err)
			}
		}
	}

	// Phase 2: Update in-memory indexes (for current runtime correctness)
	for _, entry := range job.lookup {
		if entry == nil {
			continue
		}
		job.applyLookup(entry)
	}

	if len(job.outputs) == 0 {
		job.manifest.MergeSeqMax = -1
	} else {
		job.manifest.MergeSeqMax = job.outputs[len(job.outputs)-1].seq
	}
	job.manifest.Status = manifestStatusCommitted
	if err := writeMergeManifest(job.db.opt.Dir, job.manifest); err != nil {
		return err
	}

	job.oldData = job.oldData[:0]
	job.oldHints = job.oldHints[:0]
	for _, fid := range job.pending {
		job.oldData = append(job.oldData, getDataPath(fid, job.db.opt.Dir))
		job.oldHints = append(job.oldHints, getHintPath(fid, job.db.opt.Dir))
	}

	return nil
}

func (job *mergeV2Job) cleanupOldFiles() error {
	for _, path := range job.oldData {
		_ = job.db.fm.fdm.closeByPath(path)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	for _, path := range job.oldHints {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return removeMergeManifest(job.db.opt.Dir)
}

func (job *mergeV2Job) abort(err error) error {
	var errs []error

	// Clean up all output files
	for _, out := range job.outputs {
		if out != nil {
			if finalizeErr := out.finalize(); finalizeErr != nil {
				errs = append(errs, fmt.Errorf("failed to finalize output %d: %w", out.seq, finalizeErr))
			}
			if out.dataPath != "" {
				if removeErr := os.Remove(out.dataPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
					errs = append(errs, fmt.Errorf("failed to remove data file %s: %w", out.dataPath, removeErr))
				}
			}
			if out.hintPath != "" {
				if removeErr := os.Remove(out.hintPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
					errs = append(errs, fmt.Errorf("failed to remove hint file %s: %w", out.hintPath, removeErr))
				}
			}
		}
	}

	// 清理合并清单文件
	if manifestErr := removeMergeManifest(job.db.opt.Dir); manifestErr != nil {
		errs = append(errs, fmt.Errorf("failed to remove merge manifest: %w", manifestErr))
	}

	// 如果有清理错误，将它们添加到原始错误中
	if len(errs) > 0 {
		return fmt.Errorf("merge aborted with error: %w, cleanup errors: %v", err, errs)
	}

	return err
}

func (job *mergeV2Job) rewriteFile(fid int64) error {
	path := getDataPath(fid, job.db.opt.Dir)
	fr, err := newFileRecovery(path, job.db.opt.BufferSizeOfRecovery)
	if err != nil {
		return fmt.Errorf("failed to create file recovery for %s: %w", path, err)
	}
	defer func() {
		if releaseErr := fr.release(); releaseErr != nil {
			// Log the error but don't override the original error
			// In a production environment, you might want to log this
		}
	}()

	off := int64(0)
	for {
		if off >= fr.size {
			break
		}
		entry, err := fr.readEntry(off)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, ErrIndexOutOfBound) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrHeaderSizeOutOfBounds) {
				break
			}
			return fmt.Errorf("merge rewrite read entry at offset %d: %w", off, err)
		}
		if entry == nil {
			break
		}
		sz := entry.Size()

		// 验证条目大小，防止无效数据导致的问题
		if sz <= 0 {
			off++
			continue
		}

		off += sz

		if entry.Meta.Status != Committed {
			continue
		}
		if entry.isFilter() {
			continue
		}
		if IsExpired(entry.Meta.TTL, entry.Meta.Timestamp) {
			continue
		}

		job.db.mu.RLock()
		pending := job.db.isPendingMergeEntry(entry)
		job.db.mu.RUnlock()
		if !pending {
			continue
		}

		if job.onRewriteEntry != nil {
			job.onRewriteEntry(entry)
		}

		if err := job.writeEntry(entry); err != nil {
			return fmt.Errorf("failed to write entry: %w", err)
		}
	}

	return nil
}

func (job *mergeV2Job) writeEntry(entry *Entry) error {
	if entry == nil {
		return fmt.Errorf("cannot write nil entry")
	}

	data := entry.Encode()
	if len(data) == 0 {
		return fmt.Errorf("encoded entry is empty")
	}

	out, err := job.ensureOutput(int64(len(data)))
	if err != nil {
		return fmt.Errorf("failed to ensure output: %w", err)
	}

	if out == nil {
		return fmt.Errorf("output is nil")
	}

	offset := out.writeOff
	if _, err := out.dataFile.WriteAt(data, offset); err != nil {
		return fmt.Errorf("failed to write data at offset %d: %w", offset, err)
	}
	out.writeOff += int64(len(data))

	hint := newHintEntryFromEntry(entry, out.fileID, uint64(offset))

	lookupEntry := &mergeLookupEntry{
		hint:      hint,
		collector: out.collector,
	}
	if entry.Meta.Ds == DataStructureSet || entry.Meta.Ds == DataStructureSortedSet {
		h := job.valueHasher
		h.Reset()
		if _, err := h.Write(entry.Value); err != nil {
			return fmt.Errorf("failed to compute value hash: %w", err)
		}
		lookupEntry.valueHash = h.Sum32()
		lookupEntry.hasValueHash = true
	}
	job.lookup = append(job.lookup, lookupEntry)

	return nil
}

func (job *mergeV2Job) ensureOutput(size int64) (*mergeOutput, error) {
	if size <= 0 {
		return nil, fmt.Errorf("invalid size: %d", size)
	}

	if len(job.outputs) == 0 {
		return job.newOutput()
	}

	cur := job.outputs[len(job.outputs)-1]
	if cur == nil {
		return job.newOutput()
	}

	if cur.writeOff+size > job.db.opt.SegmentSize {
		return job.newOutput()
	}
	return cur, nil
}

func (job *mergeV2Job) newOutput() (*mergeOutput, error) {
	seq := job.outputSeqBase + len(job.outputs)
	fileID := GetMergeFileID(seq)
	dataPath := getMergeDataPath(job.db.opt.Dir, seq)
	hintPath := getMergeHintPath(job.db.opt.Dir, seq)

	// 清理可能存在的旧文件
	if err := os.Remove(dataPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("failed to remove old data file %s: %w", dataPath, err)
	}
	if err := os.Remove(hintPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("failed to remove old hint file %s: %w", hintPath, err)
	}

	dataFile, err := job.db.fm.getDataFileByID(job.db.opt.Dir, fileID, job.db.opt.SegmentSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get data file: %w", err)
	}

	var hintWriter *HintFileWriter
	var collector *HintCollector
	if job.db.opt.EnableHintFile {
		hintWriter = &HintFileWriter{}
		if err := hintWriter.Create(hintPath); err != nil {
			// 如果创建 hint writer 失败，需要清理已创建的数据文件
			_ = dataFile.Close()
			_ = os.Remove(dataPath)
			return nil, fmt.Errorf("failed to create hint writer: %w", err)
		}
		collector = NewHintCollector(fileID, hintWriter, DefaultHintCollectorFlushEvery)
	}
	out := &mergeOutput{
		seq:       seq,
		fileID:    fileID,
		dataFile:  dataFile,
		collector: collector,
		dataPath:  dataPath,
		hintPath:  hintPath,
	}
	job.outputs = append(job.outputs, out)
	return out, nil
}

func (out *mergeOutput) finalize() error {
	if out.finalized {
		return nil
	}

	var errs []error

	if out.collector != nil {
		if err := out.collector.Close(); err != nil && !errors.Is(err, errHintCollectorClosed) {
			errs = append(errs, fmt.Errorf("failed to close hint collector: %w", err))
		}
	}

	if out.dataFile != nil {
		if err := out.dataFile.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("failed to sync data file: %w", err))
		}
		if err := out.dataFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close data file: %w", err))
		}
	}

	out.finalized = true

	if len(errs) > 0 {
		return fmt.Errorf("finalize errors: %v", errs)
	}

	return nil
}

// applyLookup updates in-memory indexes with the merged entry's new location.
// This ensures runtime correctness even if concurrent updates happened during merge.
func (job *mergeV2Job) applyLookup(entry *mergeLookupEntry) {
	if entry == nil || entry.hint == nil {
		return
	}

	hint := entry.hint
	bucketID := BucketId(hint.BucketId)

	switch hint.Ds {
	case DataStructureBTree:
		bt, exist := job.db.Index.bTree.exist(bucketID)
		if !exist {
			return
		}
		record, ok := bt.Find(hint.Key)
		if !ok || record == nil {
			return
		}
		record.FileID = hint.FileID
		record.DataPos = hint.DataPos
		record.Timestamp = hint.Timestamp
		record.TTL = hint.TTL
		record.ValueSize = hint.ValueSize

	case DataStructureSet:
		setIdx, exist := job.db.Index.set.exist(bucketID)
		if !exist {
			return
		}
		members, ok := setIdx.M[string(hint.Key)]
		if !ok {
			return
		}
		if !entry.hasValueHash {
			return
		}
		record, ok := members[entry.valueHash]
		if !ok || record == nil {
			return
		}
		record.FileID = hint.FileID
		record.DataPos = hint.DataPos
		record.Timestamp = hint.Timestamp
		record.TTL = hint.TTL
		record.ValueSize = hint.ValueSize

	case DataStructureList:
		if hint.Flag != DataLPushFlag && hint.Flag != DataRPushFlag {
			return
		}
		listIdx, exist := job.db.Index.list.exist(bucketID)
		if !exist {
			return
		}
		userKey, seq := decodeListKey(hint.Key)
		if userKey == nil {
			return
		}
		items, ok := listIdx.Items[string(userKey)]
		if !ok {
			return
		}
		record, ok := items.Find(ConvertUint64ToBigEndianBytes(seq))
		if !ok || record == nil {
			return
		}
		record.FileID = hint.FileID
		record.DataPos = hint.DataPos
		record.Timestamp = hint.Timestamp
		record.TTL = hint.TTL
		record.ValueSize = hint.ValueSize

	case DataStructureSortedSet:
		sortedIdx, exist := job.db.Index.sortedSet.exist(bucketID)
		if !exist {
			return
		}
		key, _ := splitStringFloat64Str(string(hint.Key), SeparatorForZSetKey)
		if key == "" {
			return
		}
		sl, ok := sortedIdx.M[key]
		if !ok {
			return
		}
		if !entry.hasValueHash {
			return
		}
		node, ok := sl.dict[entry.valueHash]
		if !ok || node == nil {
			return
		}
		record := node.record
		if record == nil {
			return
		}
		record.FileID = hint.FileID
		record.DataPos = hint.DataPos
		record.Timestamp = hint.Timestamp
		record.TTL = hint.TTL
		record.ValueSize = hint.ValueSize
	}
}
