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

## v0.6.0 (2021-03-21)
* [New Feature] Add PrefixSearchScan() with regexp search ability（#53）
* [New Feature] Allow put with timestamp (#88 )
* [Bug Fix] Fix ZMembers bug (#58 )
* [Bug Fix] Repeated key merge fix (#83 )
* [Bug Fix] The LRem implementation is not consistent with the description (#92 )
* [Refactor] Improve buildBPTreeRootIdxes file reading (#67)
* [Docs] Update README && CHANGELOG

## v0.7.0 (2022-03-06)
* [New Feature] support im memory db (#109)
* [New Feature] Add backup with tar+gz (#111)
* [New Feature] Add IterateBuckets() and DeleteBucket()
* [Refactor] refactor error (#112)
* [Bug Fix] Windows The process cannot access the file because it is being used by another process. (#110)
* [Docs] Update README && CHANGELOG

## v0.7.1 (2022-03-06)
* [Bug Fix] Delete buckets without persistence.(#115)

## v0.8.0 (2022-04-01)
* [Perf] optimize tx commit for batch write (#132)
* [Bug Fix] fix: open file by variable (#118)
* [Bug Fix] fix close file before error check（#122）
* [Bug Fix] fix rwManager.Close order（#133）
* [Bug Fix] fix last entry status error （#134）
* [Bug Fix] fix: read ErrIndexOutOfBound err
* [CI] add unit-test action （#120）
* [Chore] add constant ErrKeyNotFound and ErrKeyNotExist (#125)
* [Chore] chore: remove unnecessary conversion  (#126)
* [Chore] chore(ds/list): replace for-loop with append  (#127)
* [Chore] add push check for lint, typo  (#131)
* [Style] style: fix typo and ineffectual assignment  (#130)

## v0.9.0 (2022-06-17)
* [Bug Fix] close file before error check &remove redundant judgments （#137） @xujiajun
* [Bug Fix] update golang.org/x/sys to support go1.18 build （#139）@ag9920
* [Bug Fix] when use merge, error: The process cannot access the file because it is being used by another process (#166) @xujiajun
* [Bug Fix] fix code example. (#143) @gphper
* [Bug Fix] merge error after delete bucket (#153) @xujiajun
* [Perf] add fd cache(#164) @elliotchenzichang
* [Perf] optimize sadd function inserting duplicate data leads to datafile growth (#146) @gphper
* [Refactor] rewrite managed to support panic rollback （#136）@ag9920
* [Refactor] errors: optimize error management (#163) @xpzouying
* [Test] Update testcase: use testify test tools (#138) @xpzouying
* [Test] change list and set test with table driven test and testify (#145） @bigdaronlee163
* [Test] refactor db_test for string use testify (#147) @Rand01ph
* [Test] add [bucket_meat/entry] unit test (#148) @gphper
* [Test] update bptree unittest (#149) @xpzouying
* [Test] Update tx bptree testcase (#155) @xpzouying
* [Test] complete zset tests with testify (#151) @bigdaronlee163
* [Test] optimization tx_bucket_test and bucket_meta_test  (#156) @gphper
* [Test] test:complete tx_zset tests with testify (#162) @bigdaronlee163
* [Chore] remove unused member (#157) @xpzouying
* [Style]  format code comments etc. (#140) @moyrne
