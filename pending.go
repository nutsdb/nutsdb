package nutsdb

// pendingBucketList the uncommitted bucket changes in this Tx
type pendingBucketList map[Ds]map[BucketName]*Bucket

// pendingEntryList the uncommitted Entry changes in this Tx
type pendingEntryList struct {
	entries map[Ds]map[BucketName][]*Entry
	size    int
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
func (ens *pendingEntryList) toList() []*Entry {
	list := make([]*Entry, ens.size)
	var i int
	for _, entriesInDS := range ens.entries {
		for _, entries := range entriesInDS {
			for _, entry := range entries {
				list[i] = entry
				i++
			}
		}
	}
	return list
}
