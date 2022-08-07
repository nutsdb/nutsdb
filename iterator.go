package nutsdb

import "fmt"

type Iterator struct {
	tx *Tx

	current *Node
	i       int

	bucket string

	entry *Entry
}

func newIterator(tx *Tx, bucket string) *Iterator {
	return &Iterator{
		tx:     tx,
		bucket: bucket,
	}
}

func (it *Iterator) SetNext() (bool, error) {
	if err := it.tx.checkTxIsClosed(); err != nil {
		return false, err
	}

	if it.current == nil && (it.tx.db.opt.EntryIdxMode == HintKeyAndRAMIdxMode ||
		it.tx.db.opt.EntryIdxMode == HintKeyValAndRAMIdxMode) {
		if index, ok := it.tx.db.BPTreeIdx[it.bucket]; ok {
			it.Seek(index.FirstKey)
		}
	}

	if it.i >= it.current.KeysNum {
		it.current, _ = it.current.pointers[order-1].(*Node)
		if it.current == nil {
			return false, nil
		}
		it.i = 0
	}

	pointer := it.current.pointers[it.i]
	record := pointer.(*Record)
	it.i++

	if record.H.Meta.Flag == DataDeleteFlag || record.IsExpired() {
		return it.SetNext()
	}

	if it.tx.db.opt.EntryIdxMode == HintKeyAndRAMIdxMode {
		path := it.tx.db.getDataPath(record.H.FileID)
		df, err := it.tx.db.fm.getDataFile(path, it.tx.db.opt.SegmentSize)
		if err != nil {
			return false, err
		}

		if item, err := df.ReadAt(int(record.H.DataPos)); err == nil {
			err = df.rwManager.Release()
			if err != nil {
				return false, err
			}

			it.entry = item
			return true, nil
		} else {
			err := df.rwManager.Release()
			if err != nil {
				return false, err
			}
			return false, fmt.Errorf("HintIdx r.Hi.dataPos %d, err %s", record.H.DataPos, err)
		}
	}

	if it.tx.db.opt.EntryIdxMode == HintKeyValAndRAMIdxMode {
		it.entry = record.E
		return true, nil
	}

	return false, nil
}

func (it *Iterator) Seek(key []byte) {
	it.current = it.tx.db.BPTreeIdx[it.bucket].FindLeaf(key)

	for it.i = 0; it.i < it.current.KeysNum && compare(it.current.Keys[it.i], key) < 0; {
		it.i++
	}
}

func (it *Iterator) Entry() *Entry {
	return it.entry
}
