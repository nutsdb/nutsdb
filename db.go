package nutsdb

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/xujiajun/utils/filesystem"
	"github.com/xujiajun/utils/strconv2"
)

var (
	// ErrDBClosed is returned when db is closed
	ErrDBClosed = errors.New("db is closed")

	// ErrBucket is returned when bucket is not in the HintIdx
	ErrBucket = errors.New("err bucket")
)

const (
	//flag
	DataDeleteFlag uint16 = 0
	DataSetFlag    uint16 = 1
	//status
	UnCommitted uint16 = 0
	Committed   uint16 = 1

	Persistent  uint32 = 0
	ScanNoLimit        = -1
)

type (
	// DB represents a collection of buckets that persist on disk.
	DB struct {
		opt        Options   // the database options
		HintIdx    BPTreeIdx // Hint Index
		ActiveFile *DataFile
		MaxFileId  int64
		mu         sync.RWMutex
		KeyCount   int // total key number ,include expired, deleted, repeated.
		closed     bool
		isMerging  bool
	}

	// BPTreeIdx is the B+ tree index
	BPTreeIdx map[string]*BPTree

	Entries map[string]*Entry
)

// Open returns a newly initialized DB object.
func Open(opt Options) (*DB, error) {
	db := &DB{
		HintIdx:   make(BPTreeIdx),
		MaxFileId: 0,
		opt:       opt,
		KeyCount:  0,
		closed:    false,
	}

	if ok := filesystem.PathIsExist(db.opt.Dir); !ok {
		if err := os.Mkdir(db.opt.Dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	if err := db.buildIndexes(); err != nil {
		return nil, fmt.Errorf("db.buildIndexes error: %s", err)
	}

	return db, nil
}

// Update executes a function within a managed read/write transaction.
func (db *DB) Update(fn func(tx *Tx) error) error {
	return db.managed(true, fn)
}

// View executes a function within a managed read-only transaction.
func (db *DB) View(fn func(tx *Tx) error) error {
	return db.managed(false, fn)
}

// managed calls a block of code that is fully contained in a transaction.
func (db *DB) managed(writable bool, fn func(tx *Tx) error) (err error) {
	var tx *Tx
	tx, err = db.Begin(writable)
	if err != nil {
		return
	}

	if err = fn(tx); err != nil {
		err = tx.Rollback()
		return
	}

	if err = tx.Commit(); err != nil {
		err = tx.Rollback()
		return
	}

	return
}

// GetValidKeyCount returns the number of the key which not expired and not deleted.
func (db *DB) GetValidKeyCount(bucket string) (keyCount int, err error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if _, ok := db.HintIdx[bucket]; ok {
		keyCount = db.HintIdx[bucket].validKeyCount
		return
	}

	return 0, ErrBucket
}

// getDataPath returns the data path at given fid.
func (db *DB) getDataPath(fId int64) string {
	return db.opt.Dir + "/" + strconv2.Int64ToStr(fId) + DataSuffix
}

// Merge removes dirty data and reduce data redundancy,following these steps:
//
// 1. Filter delete or expired entry.
//
// 2. Write entry to activeFile if the key not exist，if exist miss this write operation.
//
// 3. Filter the entry which is committed.
//
// 4. At last remove the merged files.
//
// Caveat: Merge is Called means starting multiple write transactions, and it
// will effect the other write request. so execute it at the appropriate time.
func (db *DB) Merge() error {
	var (
		off                           int64
		dataFileIds, pendingMergeFIds []int
		pendingMergeEntries           []*Entry
	)

	db.isMerging = true

	_, dataFileIds = db.getMaxFileIdAndFileIds()
	pendingMergeFIds = dataFileIds

	for _, pendingMergeFId := range pendingMergeFIds {
		off = 0
		fId := int64(pendingMergeFId)
		f, err := NewDataFile(db.getDataPath(fId), db.opt.SegmentSize)
		if err != nil {
			db.isMerging = false
			return err
		}
		readEntriesNum := 0
		pendingMergeEntries = []*Entry{}

		for {
			if entry, err := f.ReadAt(int(off)); err == nil {
				if entry == nil {
					break
				}
				readEntriesNum++
				if entry.Meta.Flag == DataDeleteFlag || IsExpired(entry.Meta.TTL, entry.Meta.timestamp) {
					off += entry.Size()
					continue
				}

				if off >= db.opt.SegmentSize {
					break
				}

				if r, err := db.HintIdx[string(entry.Meta.bucket)].Find(entry.Key); err == nil {
					if r.H.meta.timestamp == entry.Meta.timestamp {
						pendingMergeEntries = append(pendingMergeEntries, entry)
					}
				}
				off += entry.Size()
			} else {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("when merge operation build hintIndex readAt err: %s", err)
			}
		}

		//rewrite to active file
		tx, err := db.Begin(true)
		if err != nil {
			db.isMerging = false
			return err
		}

		for _, e := range pendingMergeEntries {
			err := tx.put(string(e.Meta.bucket), e.Key, e.Value, e.Meta.TTL, e.Meta.Flag, e.Meta.timestamp)
			if err != nil {
				tx.Rollback()
				db.isMerging = false
				return err
			}
		}
		tx.Commit()
		//remove old file
		if err := os.Remove(db.getDataPath(int64(pendingMergeFId))); err != nil {
			db.isMerging = false
			return fmt.Errorf("when merge err: %s", err)
		}
	}

	return nil
}

// Close releases all db resources.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDBClosed
	}

	db.closed = true

	db.ActiveFile = nil

	db.HintIdx = nil

	return nil
}

// setActiveFile sets the ActiveFile (DataFile object).
func (db *DB) setActiveFile() (err error) {
	filepath := db.getDataPath(db.MaxFileId)
	db.ActiveFile, err = NewDataFile(filepath, db.opt.SegmentSize)
	if err != nil {
		return
	}

	db.ActiveFile.fileId = db.MaxFileId

	return nil
}

// getMaxFileIdAndFileIds returns max fileId and fileIds.
func (db *DB) getMaxFileIdAndFileIds() (maxFileId int64, dataFileIds []int) {
	files, _ := ioutil.ReadDir(db.opt.Dir)
	if len(files) == 0 {
		return 0, nil
	}

	maxFileId = 0

	for _, f := range files {
		id := f.Name()
		fileSuffix := path.Ext(path.Base(id))
		if fileSuffix != DataSuffix {
			continue
		}

		id = strings.TrimSuffix(id, DataSuffix)
		idVal, _ := strconv2.StrToInt(id)
		dataFileIds = append(dataFileIds, idVal)
	}

	sort.Sort(sort.IntSlice(dataFileIds))
	maxFileId = int64(dataFileIds[len(dataFileIds)-1])

	return
}

// getActiveFileWriteOff returns the write offset of activeFile.
func (db *DB) getActiveFileWriteOff() (off int64, err error) {
	off = 0
	for {
		if item, err := db.ActiveFile.ReadAt(int(off)); err == nil {
			if item == nil {
				break
			}

			off += item.Size()
			//set ActiveFileActualSize
			db.ActiveFile.ActualSize = off

		} else {
			if err == io.EOF {
				break
			}

			return -1, fmt.Errorf("when build activeDataIndex readAt err: %s", err)
		}
	}

	return
}

// buildHintIdx builds the Hint Indexes.
func (db *DB) buildHintIdx(dataFileIds []int) error {
	var (
		off                int64
		unconfirmedRecords []*Record
		committedTxIds     map[uint64]struct{}
		e                  *Entry
	)

	committedTxIds = make(map[uint64]struct{})

	for _, dataId := range dataFileIds {
		off = 0
		fId := int64(dataId)
		f, err := NewDataFile(db.getDataPath(fId), db.opt.SegmentSize)

		if err != nil {
			return err
		}

		for {
			if entry, err := f.ReadAt(int(off)); err == nil {
				if entry == nil {
					break
				}

				e = nil
				if db.opt.EntryIdxMode == HintAndRAMIdxMode {
					e = &Entry{
						Key:   entry.Key,
						Value: entry.Value,
						Meta:  entry.Meta,
					}
				}

				if entry.Meta.status == Committed {
					committedTxIds[entry.Meta.txId] = struct{}{}
				}

				unconfirmedRecords = append(unconfirmedRecords, &Record{
					H: &Hint{
						key:     entry.Key,
						fileId:  fId,
						meta:    entry.Meta,
						dataPos: uint64(off),
					},
					E: e,
				})

				off += entry.Size()

				if off >= db.opt.SegmentSize {
					break
				}
			} else {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("when build hintIndex readAt err: %s", err)
			}
		}
	}

	if len(unconfirmedRecords) > 0 {
		for _, r := range unconfirmedRecords {
			if _, ok := committedTxIds[r.H.meta.txId]; ok {
				bucket := string(r.H.meta.bucket)
				if _, ok := db.HintIdx[bucket]; !ok {
					db.HintIdx[bucket] = NewTree()
				}
				r.H.meta.status = Committed
				if err := db.HintIdx[bucket].Insert(r.H.key, r.E, r.H, CountFlagEnabled); err != nil {
					return fmt.Errorf("when build hintIndex insert index err: %s", err)
				}
				db.KeyCount++
			}
		}
	}

	return nil
}

// buildIndexes builds indexes when db initialize resource.
func (db *DB) buildIndexes() (err error) {
	var (
		maxFileId   int64
		dataFileIds []int
	)

	maxFileId, dataFileIds = db.getMaxFileIdAndFileIds()

	//init db.ActiveFile
	db.MaxFileId = maxFileId

	//set ActiveFile
	if err = db.setActiveFile(); err != nil {
		return
	}

	if dataFileIds == nil && maxFileId == 0 {
		return
	}

	if db.ActiveFile.writeOff, err = db.getActiveFileWriteOff(); err != nil {
		return
	}

	// build hint index
	return db.buildHintIdx(dataFileIds)
}
