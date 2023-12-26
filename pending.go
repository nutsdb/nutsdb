package nutsdb

// pendingBucketList the uncommitted bucket changes in this Tx
type pendingBucketList map[Ds]map[BucketName]*Bucket

// pendingEntriesInBTree means the changes Entries in DataStructureBTree in the Tx
type pendingEntriesInBTree map[BucketName]map[string]*Entry

// pendingEntryList the uncommitted Entry changes in this Tx
type pendingEntryList struct {
	entriesInBTree pendingEntriesInBTree
	entries        map[Ds]map[BucketName][]*Entry
	size           int
}

func NewPendingEntriesList() *pendingEntryList {
	pending := &pendingEntryList{
		entriesInBTree: map[BucketName]map[string]*Entry{},
		entries:        map[Ds]map[BucketName][]*Entry{},
		size:           0,
	}
	return pending
}

func (pending *pendingEntryList) submitEntry(ds Ds, bucket string, e *Entry) {
	switch ds {
	case DataStructureBTree:
		if _, exist := pending.entriesInBTree[bucket]; !exist {
			pending.entriesInBTree[bucket] = map[string]*Entry{}
		}
		pending.entriesInBTree[bucket][string(e.Key)] = e
	default:
		if _, exist := pending.entries[ds]; !exist {
			pending.entries[ds] = map[BucketName][]*Entry{}
		}
		entries := pending.entries[ds][bucket]
		entries = append(entries, e)
		pending.entries[ds][bucket] = entries
	}
	pending.size++
}

// rangeBucket input a range handler function f and call it with every bucket in pendingBucketList
func (p pendingBucketList) rangeBucket(f func(bucket *Bucket) error) error {
	for _, bucketsInDs := range p {
		for _, bucket := range bucketsInDs {
			err := f(bucket)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// toList collect all the entries in pendingEntryList to a list.
func (pending *pendingEntryList) toList() []*Entry {
	list := make([]*Entry, pending.size)
	var i int
	for _, entriesInBucket := range pending.entriesInBTree {
		for _, entry := range entriesInBucket {
			list[i] = entry
			i++
		}
	}
	for _, entriesInDS := range pending.entries {
		for _, entries := range entriesInDS {
			for _, entry := range entries {
				list[i] = entry
				i++
			}
		}
	}
	return list
}
