## v0.1.0 （2019-2-28）
* Support Put/Get/Delete Operations
* Support TTL
* Support Range/Prefix Scanning
* Support Merge Operation
* Support BackUp Operation
* Support Bucket

## v0.2.0 （2019-3-05）
* Support list
* Support set
* Support sorted set
* Add tests for list,set,sorted set
* Add examples for list,set,sorted set
* Fix error when batch put operations
* Update README && CHANGELOG

## v0.3.0（2019-3-11）
* Support persistence
* Discard mmap package
* Discard EntryIdxMode options: HintAndRAMIdxMode and HintAndMemoryMapIdxMode
* Add new EntryIdxMode options: HintKeyValAndRAMIdxMode and HintKeyAndRAMIdxMode
* Fix when fn is nil
* Update README && CHANGELOG

## v0.4.0（2019-3-15）
* Support mmap loading file
* Add rwmanager interface
* Add new options: RWMode, SyncEnable and StartFileLoadingMode
* Fix tx bug when a tx commits
* Clean up some codes
* Update README && CHANGELOG