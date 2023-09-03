# Comparison

### BoltDB

BoltDB is similar to NutsDB, both use B+tree and support transaction. However, Bolt uses a B+tree internally and only a single file, and NutsDB is based on bitcask model with multiple log files. NutsDB supports TTL and many data structures, but BoltDB does not support them .

### LevelDB/RocksDB

LevelDB and RocksDB are based on a log-structured merge-tree (LSM tree).An LSM tree optimizes random writes by using a write ahead log and multi-tiered, sorted files called SSTables. LevelDB does not have transactions. It supports batch writing of key/value pairs and it supports read snapshots but it will not give you the ability to do a compare-and-swap operation safely. NutsDB supports many data structures, but RocksDB does not support them.

### Badger

Badger is based in LSM tree with value log. It designed for SSDs. It also supports transaction and TTL. But in my benchmark its write performance is not as good as i thought. In addition, NutsDB supports data structures such as list、set、sorted set, but Badger does not support them.
