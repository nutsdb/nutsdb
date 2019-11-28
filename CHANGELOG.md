## v0.1.0 （2019-2-28）
* [New feature] Support Put/Get/Delete Operations
* [New feature] Support TTL
* [New feature] Support Range/Prefix Scanning
* [New feature] Support Merge Operation
* [New feature] Support BackUp Operation
* [New feature] Support Bucket

## v0.2.0 （2019-3-05）
* [New feature] Support list
* [New feature] Support set
* [New feature] Support sorted set
* [Bug Fix] Fix error when batch put operations
* [Change] Update README && CHANGELOG

## v0.3.0（2019-3-11）
* [New feature] Support persistence
* [Bug Fix] Fix when fn is nil
* [Change] Discard mmap package
* [Change] Discard EntryIdxMode options: HintAndRAMIdxMode and HintAndMemoryMapIdxMode
* [Change] Add new EntryIdxMode options: HintKeyValAndRAMIdxMode and HintKeyAndRAMIdxMode

## v0.4.0（2019-3-15）
* [New feature] Support mmap loading file
* [Bug Fix] Fix tx bug when a tx commits
* [Change] Add rwmanager interface
* [Change] Add new options: RWMode, SyncEnable and StartFileLoadingMode
* [Change] Clean up some codes
* [Change] Update README && CHANGELOG

## v0.5.0 (2019-11-28)
* [New feature] Support EntryIdxMode: HintBPTSparseIdxMode
* [New feature] Support GetAll() function for all models
* [Bug Fix] Fix error too many open files in system
* [Bug Fix] Fix constant 2147483648 overflows int
* [Bug Fix] Fix when the number of files waiting to be merged not at least 2
* [Bug Fix] Fix data pollution when executing the merge method
* [Change] Modify Records type && Entries type
* [Change] Refactor for tx Commit function
* [Change] Update Iterating over keys about README
* [Change] Fix some grammatical mistakes about README
* [Change] Rename variable for func ReadBPTreeRootIdxAt
* [Change] Add issue templates
* [Change] Update README && CHANGELOG