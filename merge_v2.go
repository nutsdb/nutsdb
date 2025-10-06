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

type mergeLookupEntry struct {
	hint          *HintEntry
	valueHash     uint32
	hasValueHash  bool
	collector     *HintCollector
	origFileID    int64
	origTimestamp uint64
	origTTL       uint32
	origTxID      uint64
	origValueSize uint32
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

func (job *mergeV2Job) commit() error {
	job.db.mu.Lock()
	defer job.db.mu.Unlock()

	for _, entry := range job.lookup {
		if entry == nil {
			continue
		}
		applied := job.applyLookup(entry)
		if !applied {
			continue
		}
		if entry.collector != nil && entry.hint != nil {
			if err := entry.collector.Add(entry.hint); err != nil {
				return fmt.Errorf("failed to add hint to collector: %w", err)
			}
		}
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

		if err := job.writeEntry(entry, fid); err != nil {
			return fmt.Errorf("failed to write entry: %w", err)
		}
	}

	return nil
}

func (job *mergeV2Job) writeEntry(entry *Entry, srcFileID int64) error {
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
		hint:          hint,
		collector:     out.collector,
		origFileID:    srcFileID,
		origTimestamp: entry.Meta.Timestamp,
		origTTL:       entry.Meta.TTL,
		origTxID:      entry.Meta.TxID,
		origValueSize: entry.Meta.ValueSize,
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

func (job *mergeV2Job) applyLookup(entry *mergeLookupEntry) bool {
	if entry == nil || entry.hint == nil {
		return false
	}

	hint := entry.hint
	bucketID := BucketId(hint.BucketId)
	matchRecord := func(record *Record) bool {
		if record == nil {
			return false
		}
		if record.FileID != entry.origFileID {
			return false
		}
		if record.Timestamp != entry.origTimestamp {
			return false
		}
		return true
	}

	switch hint.Ds {
	case DataStructureBTree:
		bt, exist := job.db.Index.bTree.exist(bucketID)
		if !exist {
			return false
		}
		record, ok := bt.Find(hint.Key)
		if !ok || record == nil {
			return false
		}
		if !matchRecord(record) {
			return false
		}
		record.FileID = hint.FileID
		record.DataPos = hint.DataPos
		record.Timestamp = hint.Timestamp
		record.TTL = hint.TTL
		record.ValueSize = hint.ValueSize
		return true

	case DataStructureSet:
		setIdx, exist := job.db.Index.set.exist(bucketID)
		if !exist {
			return false
		}
		members, ok := setIdx.M[string(hint.Key)]
		if !ok {
			return false
		}
		if !entry.hasValueHash {
			return false
		}
		record, ok := members[entry.valueHash]
		if !ok || record == nil {
			return false
		}
		if !matchRecord(record) {
			return false
		}
		record.FileID = hint.FileID
		record.DataPos = hint.DataPos
		record.Timestamp = hint.Timestamp
		record.TTL = hint.TTL
		record.ValueSize = hint.ValueSize
		return true

	case DataStructureList:
		if hint.Flag != DataLPushFlag && hint.Flag != DataRPushFlag {
			return false
		}
		listIdx, exist := job.db.Index.list.exist(bucketID)
		if !exist {
			return false
		}
		userKey, seq := decodeListKey(hint.Key)
		if userKey == nil {
			return false
		}
		items, ok := listIdx.Items[string(userKey)]
		if !ok {
			return false
		}
		record, ok := items.Find(ConvertUint64ToBigEndianBytes(seq))
		if !ok || record == nil {
			return false
		}
		if !matchRecord(record) {
			return false
		}
		record.FileID = hint.FileID
		record.DataPos = hint.DataPos
		record.Timestamp = hint.Timestamp
		record.TTL = hint.TTL
		record.ValueSize = hint.ValueSize
		return true

	case DataStructureSortedSet:
		sortedIdx, exist := job.db.Index.sortedSet.exist(bucketID)
		if !exist {
			return false
		}
		key, _ := splitStringFloat64Str(string(hint.Key), SeparatorForZSetKey)
		if key == "" {
			return false
		}
		sl, ok := sortedIdx.M[key]
		if !ok {
			return false
		}
		if !entry.hasValueHash {
			return false
		}
		node, ok := sl.dict[entry.valueHash]
		if !ok || node == nil {
			return false
		}
		record := node.record
		if record == nil {
			return false
		}
		if !matchRecord(record) {
			return false
		}
		record.FileID = hint.FileID
		record.DataPos = hint.DataPos
		record.Timestamp = hint.Timestamp
		record.TTL = hint.TTL
		record.ValueSize = hint.ValueSize
		return true
	}

	return false
}
