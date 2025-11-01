package nutsdb

import (
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"os"
	"sort"

	"github.com/nutsdb/nutsdb/internal/data"
	"github.com/nutsdb/nutsdb/internal/utils"
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
	hint         *HintEntry     // Hint entry containing key and location metadata
	valueHash    uint32         // Hash of the value for Set/SortedSet duplicate detection
	hasValueHash bool           // Indicates if valueHash is valid
	collector    *HintCollector // Hint file collector for writing hints
}

// mergeOutput represents a single merge output file with its associated hint file.
// Each merge operation may produce multiple output files to respect segment size limits.
type mergeOutput struct {
	seq       int            // Sequence number within this merge operation
	fileID    int64          // File ID (negative for merge files)
	dataFile  *DataFile      // Output data file handle
	collector *HintCollector // Hint file collector for this output
	dataPath  string         // Path to the data file
	hintPath  string         // Path to the hint file
	writeOff  int64          // Current write offset in the data file
	finalized bool           // Whether this output has been finalized
}

// mergeV2 executes the complete merge operation using the V2 algorithm.
// This is the main entry point for the merge process, orchestrating all phases.
func (db *DB) mergeV2() error {
	job := &mergeV2Job{db: db}

	// Prepare merge job - validate state and enumerate files
	if err := job.prepare(); err != nil {
		return err
	}
	defer job.finish()

	// Enter writing state - prepare for merge operations
	if err := job.enterWritingState(); err != nil {
		return job.abort(err)
	}

	// Rewrite phase - process all pending files and create merge outputs
	if err := job.rewrite(); err != nil {
		return job.abort(err)
	}

	// Commit phase - update indexes and write hint files
	if err := job.commit(); err != nil {
		return job.abort(err)
	}

	// Finalize outputs - ensure all data is persisted
	if err := job.finalizeOutputs(); err != nil {
		return job.abort(err)
	}

	// Clean up old files to reclaim disk space
	if err := job.cleanupOldFiles(); err != nil {
		return fmt.Errorf("cleanup old files: %w", err)
	}

	return nil
}

// prepare initializes the merge job by validating state, enumerating files, and setting up the database.
// It ensures the database is ready for merge and creates a new active file for ongoing writes.
func (job *mergeV2Job) prepare() error {
	job.db.mu.Lock()

	// Prevent concurrent merges
	if job.db.isMerging {
		job.db.mu.Unlock()
		return ErrIsMerging
	}

	// Enumerate all data files (both user and merge files)
	userIDs, mergeIDs, err := enumerateDataFileIDs(job.db.opt.Dir)
	if err != nil {
		job.db.mu.Unlock()
		return fmt.Errorf("failed to enumerate data file IDs: %w", err)
	}

	// Collect all pending files for merging
	job.pending = append(job.pending, userIDs...)
	job.pending = append(job.pending, mergeIDs...)

	// Determine next merge sequence number based on existing merge files
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

	// Skip merge if there are fewer than 2 files
	if len(job.pending) < 2 {
		job.db.mu.Unlock()
		return ErrDontNeedMerge
	}

	// Sort files by ID for consistent processing order
	sort.Slice(job.pending, func(i, j int) bool { return job.pending[i] < job.pending[j] })

	// Mark database as merging
	job.db.isMerging = true

	// Sync active file if using mmap without sync
	if !job.db.opt.SyncEnable && job.db.opt.RWMode == MMap {
		if err := job.db.ActiveFile.rwManager.Sync(); err != nil {
			job.db.isMerging = false
			job.db.mu.Unlock()
			return fmt.Errorf("failed to sync active file: %w", err)
		}
	}

	// Release current active file for merge processing
	if err := job.db.ActiveFile.rwManager.Release(); err != nil {
		job.db.isMerging = false
		job.db.mu.Unlock()
		return fmt.Errorf("failed to release active file: %w", err)
	}

	// Create new active file for writes during merge
	job.db.MaxFileID++
	path := getDataPath(job.db.MaxFileID, job.db.opt.Dir)
	activeFile, err := job.db.fm.GetDataFile(path, job.db.opt.SegmentSize)
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

// finish cleans up the merge job by resetting the merging state.
// Always called via defer to ensure cleanup even if merge fails.
func (job *mergeV2Job) finish() {
	job.db.mu.Lock()
	job.db.isMerging = false
	job.db.mu.Unlock()
}

// enterWritingState initializes the merge job for writing entries.
// Creates the merge manifest and prepares necessary data structures.
func (job *mergeV2Job) enterWritingState() error {
	// Initialize lookup entries for tracking merged entries
	job.lookup = make([]*mergeLookupEntry, 0)

	// Create merge manifest to track merge progress
	job.manifest = &mergeManifest{
		Status:            manifestStatusWriting,
		MergeSeqMax:       -1,
		PendingOldFileIDs: append([]int64(nil), job.pending...),
	}

	// Initialize value hasher for Set/SortedSet duplicate detection
	if job.valueHasher == nil {
		job.valueHasher = fnv.New32a()
	}

	// Write initial manifest to enable recovery
	if err := writeMergeManifest(job.db.opt.Dir, job.manifest); err != nil {
		return err
	}
	return nil
}

// rewrite processes all pending files and rewrites their valid entries to merge outputs.
// This is the main phase where data compaction happens.
func (job *mergeV2Job) rewrite() error {
	for _, fid := range job.pending {
		if err := job.rewriteFile(fid); err != nil {
			return err
		}
	}
	return nil
}

// finalizeOutputs ensures all output files are properly closed and synced to disk.
// This must be called after all entries have been written.
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

	// Phase 1: Write all hints to hint files (even potentially stale ones)
	// This ensures hints are available for fast index recovery
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
	// This ensures the database continues to work correctly after merge
	for _, entry := range job.lookup {
		if entry == nil {
			continue
		}
		job.applyLookup(entry)
	}

	// Update merge manifest with completion status
	if len(job.outputs) == 0 {
		job.manifest.MergeSeqMax = -1
	} else {
		job.manifest.MergeSeqMax = job.outputs[len(job.outputs)-1].seq
	}
	job.manifest.Status = manifestStatusCommitted
	if err := writeMergeManifest(job.db.opt.Dir, job.manifest); err != nil {
		return err
	}

	// Prepare list of old files for cleanup
	job.oldData = job.oldData[:0]
	job.oldHints = job.oldHints[:0]
	for _, fid := range job.pending {
		job.oldData = append(job.oldData, getDataPath(fid, job.db.opt.Dir))
		job.oldHints = append(job.oldHints, getHintPath(fid, job.db.opt.Dir))
	}

	return nil
}

// cleanupOldFiles removes the old data and hint files that were merged, as well as the manifest file.
// This is called after a successful merge to reclaim disk space.
func (job *mergeV2Job) cleanupOldFiles() error {
	// Close and remove old data files
	for _, path := range job.oldData {
		_ = job.db.fm.fdm.CloseByPath(path)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	// Remove old hint files
	for _, path := range job.oldHints {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	// Remove the merge manifest file
	return removeMergeManifest(job.db.opt.Dir)
}

// abort cleans up all created files when a merge fails and returns the original error.
// It ensures no partial merge state is left behind and combines any cleanup errors.
func (job *mergeV2Job) abort(err error) error {
	var errs []error

	// Clean up all output files created during the failed merge
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

	// Clean up merge manifest file
	if manifestErr := removeMergeManifest(job.db.opt.Dir); manifestErr != nil {
		errs = append(errs, fmt.Errorf("failed to remove merge manifest: %w", manifestErr))
	}

	// If there are cleanup errors, add them to the original error
	if len(errs) > 0 {
		return fmt.Errorf("merge aborted with error: %w, cleanup errors: %v", err, errs)
	}

	return err
}

// rewriteFile processes a single data file during merge, rewriting valid entries to new merge files.
// It reads entries sequentially, filters out invalid/expired entries, and rewrites remaining entries.
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

		// Validate entry size to prevent issues with invalid data
		if sz <= 0 {
			off++
			continue
		}

		off += sz

		// Skip entries that are not committed
		if entry.Meta.Status != Committed {
			continue
		}
		// Skip filter entries
		if entry.isFilter() {
			continue
		}
		// Skip expired entries
		if data.IsExpired(entry.Meta.TTL, entry.Meta.Timestamp) {
			continue
		}

		// Check if entry is still pending merge (not overwritten by newer version)
		job.db.mu.RLock()
		pending := job.db.isPendingMergeEntry(entry)
		job.db.mu.RUnlock()
		if !pending {
			continue
		}

		// Allow custom processing of entries during rewrite
		if job.onRewriteEntry != nil {
			job.onRewriteEntry(entry)
		}

		if err := job.writeEntry(entry); err != nil {
			return fmt.Errorf("failed to write entry: %w", err)
		}
	}

	return nil
}

// writeEntry writes an entry to the appropriate merge output file and creates the corresponding hint entry.
// It also calculates value hashes for Set and SortedSet data structures to support duplicate detection.
func (job *mergeV2Job) writeEntry(entry *Entry) error {
	if entry == nil {
		return fmt.Errorf("cannot write nil entry")
	}

	data := entry.Encode()
	if len(data) == 0 {
		return fmt.Errorf("encoded entry is empty")
	}

	// Get or create appropriate output file for this entry size
	out, err := job.ensureOutput(int64(len(data)))
	if err != nil {
		return fmt.Errorf("failed to ensure output: %w", err)
	}

	if out == nil {
		return fmt.Errorf("output is nil")
	}

	// Write the encoded entry data to the output file
	offset := out.writeOff
	if _, err := out.dataFile.WriteAt(data, offset); err != nil {
		return fmt.Errorf("failed to write data at offset %d: %w", offset, err)
	}
	out.writeOff += int64(len(data))

	// Create hint entry for fast index lookup
	hint := newHintEntryFromEntry(entry, out.fileID, uint64(offset))

	lookupEntry := &mergeLookupEntry{
		hint:      hint,
		collector: out.collector,
	}

	// For Set and SortedSet, compute value hash to handle duplicate detection
	if entry.Meta.Ds == DataStructureSet || entry.Meta.Ds == DataStructureSortedSet {
		h := job.valueHasher
		h.Reset()
		if _, err := h.Write(entry.Value); err != nil {
			return fmt.Errorf("failed to compute value hash: %w", err)
		}
		lookupEntry.valueHash = h.Sum32()
		lookupEntry.hasValueHash = true
	}

	// Store lookup entry for later index update during commit phase
	job.lookup = append(job.lookup, lookupEntry)

	return nil
}

// ensureOutput returns the appropriate output file for writing entries of the given size.
// If no output exists or the current output would exceed segment size, creates a new output.
func (job *mergeV2Job) ensureOutput(size int64) (*mergeOutput, error) {
	if size <= 0 {
		return nil, fmt.Errorf("invalid size: %d", size)
	}

	// If no outputs exist yet, create the first one
	if len(job.outputs) == 0 {
		return job.newOutput()
	}

	// Get the current output file
	cur := job.outputs[len(job.outputs)-1]
	if cur == nil {
		return job.newOutput()
	}

	// Check if adding this entry would exceed the segment size limit
	if cur.writeOff+size > job.db.opt.SegmentSize {
		return job.newOutput()
	}

	return cur, nil
}

// newOutput creates a new merge output file with associated hint file collector.
// It generates unique file IDs using merge sequence numbers and cleans up any existing files.
func (job *mergeV2Job) newOutput() (*mergeOutput, error) {
	seq := job.outputSeqBase + len(job.outputs)
	fileID := GetMergeFileID(seq)
	dataPath := getMergeDataPath(job.db.opt.Dir, seq)
	hintPath := getMergeHintPath(job.db.opt.Dir, seq)

	// Clean up any existing files from previous failed merges
	if err := os.Remove(dataPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("failed to remove old data file %s: %w", dataPath, err)
	}
	if err := os.Remove(hintPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("failed to remove old hint file %s: %w", hintPath, err)
	}

	// Create the data file with merge-specific file ID
	dataFile, err := job.db.fm.GetDataFileByID(job.db.opt.Dir, fileID, job.db.opt.SegmentSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get data file: %w", err)
	}

	// Create hint file writer and collector if hint files are enabled
	var hintWriter *HintFileWriter
	var collector *HintCollector
	if job.db.opt.EnableHintFile {
		hintWriter = &HintFileWriter{}
		if err := hintWriter.Create(hintPath); err != nil {
			// If hint writer creation fails, clean up the created data file
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

// finalize properly closes and syncs the output files, ensuring data is persisted to disk.
// This method can be called multiple times safely (idempotent operation).
func (out *mergeOutput) finalize() error {
	if out.finalized {
		return nil
	}

	var errs []error

	// Close hint collector and flush any pending hints
	if out.collector != nil {
		if err := out.collector.Close(); err != nil && !errors.Is(err, errHintCollectorClosed) {
			errs = append(errs, fmt.Errorf("failed to close hint collector: %w", err))
		}
	}

	// Sync data file to ensure all writes are persisted to disk
	if out.dataFile != nil {
		if err := out.dataFile.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("failed to sync data file: %w", err))
		}
		// Close the data file
		if err := out.dataFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close data file: %w", err))
		}
	}

	// Mark as finalized to prevent double-finalization
	out.finalized = true

	// Return any errors that occurred during finalization
	if len(errs) > 0 {
		return fmt.Errorf("finalize errors: %v", errs)
	}

	return nil
}

// updateRecordWithHintIfNewer updates a record with hint data only if the hint is newer or same timestamp.
// This prevents overwriting newer entries that were written after merge started.
func updateRecordWithHintIfNewer(record *data.Record, hint *HintEntry) bool {
	// Only update if our hint is newer or same timestamp (don't overwrite newer data)
	if record.Timestamp <= hint.Timestamp {
		record.FileID = hint.FileID
		record.DataPos = hint.DataPos
		record.Timestamp = hint.Timestamp
		record.TTL = hint.TTL
		record.ValueSize = hint.ValueSize
		return true
	}
	return false
}

// applyLookup updates in-memory indexes with the merged entry's new location.
// This ensures runtime correctness even if concurrent updates happened during merge.
// The function handles all supported data structures: BTree, Set, List, and SortedSet.
//
// IMPORTANT: We check timestamps to avoid overwriting newer entries that were written
// after the merge started. The initial check happens in rewriteFile, but the index mutex
// is released before commit, so newer entries might exist in the index now.
func (job *mergeV2Job) applyLookup(entry *mergeLookupEntry) {
	if entry == nil || entry.hint == nil {
		return
	}

	hint := entry.hint
	bucketID := BucketId(hint.BucketId)

	switch hint.Ds {
	case DataStructureBTree:
		// Update BTree index with new file location if hint is newer or same age
		bt, exist := job.db.Index.bTree.exist(bucketID)
		if !exist {
			return
		}
		record, ok := bt.Find(hint.Key)
		if !ok || record == nil {
			return
		}
		updateRecordWithHintIfNewer(record, hint)

	case DataStructureSet:
		// Update Set index using value hash for duplicate detection
		setIdx, exist := job.db.Index.set.exist(bucketID)
		if !exist {
			return
		}
		members, ok := setIdx.M[string(hint.Key)]
		if !ok {
			return
		}
		// Value hash is required for Set to identify the specific member
		if !entry.hasValueHash {
			return
		}
		record, ok := members[entry.valueHash]
		if !ok || record == nil {
			return
		}
		updateRecordWithHintIfNewer(record, hint)

	case DataStructureList:
		// Update List index entries (only push operations are merged)
		if hint.Flag != DataLPushFlag && hint.Flag != DataRPushFlag {
			return
		}
		listIdx, exist := job.db.Index.list.exist(bucketID)
		if !exist {
			return
		}
		// Decode list key to extract user key and sequence number
		userKey, seq := decodeListKey(hint.Key)
		if userKey == nil {
			return
		}
		items, ok := listIdx.Items[string(userKey)]
		if !ok {
			return
		}
		// Find the specific list item by sequence number
		record, ok := items.Find(utils.ConvertUint64ToBigEndianBytes(seq))
		if !ok || record == nil {
			return
		}
		updateRecordWithHintIfNewer(record, hint)

	case DataStructureSortedSet:
		// Update SortedSet index using both key and value hash
		sortedIdx, exist := job.db.Index.sortedSet.exist(bucketID)
		if !exist {
			return
		}
		// Extract member key from the encoded key
		key, _ := splitStringFloat64Str(string(hint.Key), SeparatorForZSetKey)
		if key == "" {
			return
		}
		sl, ok := sortedIdx.M[key]
		if !ok {
			return
		}
		// Value hash is required for SortedSet to identify the specific member
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
		updateRecordWithHintIfNewer(record, hint)
	}
}
