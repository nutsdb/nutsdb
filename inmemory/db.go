package inmemory

type (
	Options struct {
		// ShardsCount represents Number of cache shards.
		ShardsCount uint64
	}

	// DB indicates that all data is stored in memory.
	// If the system restarts or crashes, all data will be lost.
	DB struct {
		opts   Options
		shards []*ShardDB
		Hasher Hasher
	}
)

// DefaultOptions default options
var DefaultOptions = Options{
	ShardsCount: 256,
}

// Open returns a newly initialized in memory DB object.
func Open(opts Options) (*DB, error) {
	db := &DB{
		opts:   opts,
		Hasher: newDefaultHasher(),
		shards: make([]*ShardDB, opts.ShardsCount),
	}

	for i := 0; i < int(opts.ShardsCount); i++ {
		db.shards[i] = InitShardDB()
	}

	return db, nil
}

// GetShard Get sharded db according to bucket as hashedKey
func (db *DB) GetShard(bucket string) (shardDB *ShardDB) {
	hashedKey := db.Hasher.Sum64(bucket)
	idx := hashedKey % db.opts.ShardsCount
	return db.shards[idx]
}

// Managed read and write operations of sharded db based on bucket as hashedKey
func (db *DB) Managed(bucket string, writable bool, fn func(shardDB *ShardDB) error) error {
	shardDB := db.GetShard(bucket)
	shardDB.Lock(writable)
	if err := fn(shardDB); err != nil {
		shardDB.Unlock(writable)
		return err
	}
	shardDB.Unlock(writable)
	return nil
}
