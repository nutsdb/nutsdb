# 与其他类似产品的对比

## BoltDB

BoltDB和NutsDB很相似都是内嵌型的key-value数据库，同时支持事务。Bolt基于B+tree引擎模型，只有一个文件，NutsDB基于bitcask引擎模型，会生成多个文件。当然他们都支持范围扫描和前缀扫描这两个实用的特性。

## LevelDB, RocksDB

LevelDB 和 RocksDB 都是基于LSM tree模型。不支持bucket。 其中RocksDB目前还没看到golang实现的版本。

## Badger

Badger也是基于LSM tree模型。但是写性能没有我想象中高。不支持bucket。

另外，以上数据库均不支持多种数据结构如list、set、sorted set，而NutsDB从0.2.0版本开始支持这些数据结构。
