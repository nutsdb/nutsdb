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

## v0.10.0（2022-08-13）
* [Bug Fix]List data structure with count parameter negative, lack of boundary judgment (#183) @andrewhzy
* [New Feature] add LRemByIndex (#174) @Nailcui
* [New Feature] add LKeys SKeys ZKeys API (#175) @Nailcui
* [New Feature] add Iterator API (HintKeyAndRAMIdxMode and HintKeyValAndRAMIdxMode)(#191) @zeina1i
* [Refactor] graceful options parameters (#185) @Nailcui
* [Test] Add rwmanager fileio test (#170) @zeina1i
* [Test] Improve code coverage about list  (#183) @andrewhzy
* [Test] Test coverage improvement for inmemory  (#187) @andrewhzy
* [Docs] A few corrections in ReadME file (#171) @kwakubiney

## v0.11.0（2022-10-31）
* [Bug Fix] In BPTSparse when combination of bucket and key is repeated (#207) @ShiMaRing
* [Bug Fix] MInInt function compatible with 32-bit operating systems (#208) @xujiajun
* [Bug Fix] Index EOF issue#213 (#214) @xujiajun
* [Perf] Optimize concurrent read performance (#205) @elliotchenzichang
* [Perf] Use biobuf optimaze startspeed (#212) @elliotchenzichang
* [New Feature] Support reverse iterator (EntryIdxMode: HintKeyAndRAMIdxMode and HintKeyValAndRAMIdxMode) (#202) @zeina1i
* [New Feature] Add support for IterateBuckets regularized matching (#198) @Nailcui
* [New Feature] list all key of bucket in memory mode (#206) @Nailcui
* [New Feature] Add PrefixScan in memory mode  (#211) @Nailcui
* [Refactor] make default options to be created in a factory method (#196) @elliotchenzichang
* [Refactor] use size constant value (#204) @elliotchenzichang
* [Chore] add iterator example (#209) @xujiajun
* [Chore] remove option StartFileLoadingMode (#218) @xujiajun

## v0.11.1（2022-11-13）
* [Bug Fix] avoid nil of it.current (#233) @mindon
* [Bug Fix] it.current may be nil when options.Reverse is false (#234) @xujiajun
* [Refactor] changing the lock to be one of property of the structure can make the code more readable.(#228) @elliotchenzichang
* [New Feature] add buffer size of recovery reader as param (#230) @elliotchenzichang

## v0.12.0（2023-02-26）
* [New Feature] feat:support ttl function for list (feat:support ttl function for list #263) @xuyukeviki
* [Bug Fix] fix: panic: db.buildIndexes error: unexpected EOF issue (panic: db.buildIndexes error: unexpected EOF #244) @xujiajun
* [Bug Fix] issue NewIterator with Reverse=true stop at 28 records #250 :: Andrew :: bug fixed (issue #250 :: Andrew :: bug fixed #266) @andrewhzy
* [Bug Fix] Fix issues LIST "start or end error" after deleting the last item from list  #280: LIST "start or end error" after deleting the last item Fix issues #280: LIST "start or end error" after deleting the last item #282 @ShawnHXH
* [Performance] Use file recovery in merge (Use file recovery in merge #259) @elliotchenzichang
* [Performance] perf(read): reduce one read I/O perf(read): reduce one read I/O #271 @lugosix
* [Refactor] add options function of BufferSizeOfRecovery (add options function of BufferSizeOfRecovery #236) @elliotchenzichang
* [Refactor] add fd release logic to file recovery reader (add fd release logic to file recovery reader #242 ) @elliotchenzichang
* [Refactor] rebuild parse meta func (rebuild parse meta func #243 ) @elliotchenzichang
* [Refactor] change the xujiajun/nutsdb -> nutsdb/nutsdb change the xujiajun/nutsdb -> nutsdb/nutsdb #281 @elliotchenzichang
* [Refactor] Update doc Update doc #285 @elliotchenzichang
* [Refactor] Fix verify logic Fix verify logic #286 @elliotchenzichang
* [Refactor] fix: issue (Error description problem about IsPrefixSearchScan #288) fix: issue (#288) #289 @CodePrometheus
* [Refactor] docs: fix typo( docs: fix typo #252) @icpd
* [Refactor] fix a typo in code fix a typo #291 @elliotchenzichang
* [UnitTest] Adding a test of readEntry() and refacting readEntry() (Adding a test of readEntry() and refacting readEntry() #237 ) @wangxuanni
* [UnitTest] test coverage improvement (fix: issue (#213) #214 ) @andrewhzy
* [UnitTest] adding a test for IsKeyEmpty func in github.com/xujiajun/nutsdb/errors.go:26 (adding a test for IsKeyEmpty func in github.com/xujiajun/nutsdb/errors.go:26: #265) @lyr-2000
* [UnitTest] Add test for SetKeyPosMap in nutsdb/bptree.go (Add test for SetKeyPosMap in nutsdb/bptree.go #268) @ShawnHXH
* [UnitTest] Add test for ToBinary in bptree.go Add test for ToBinary in bptree.go #272 @ShawnHXH
* [UnitTest] Add test for bucket in errors.go Add test for bucket in errors.go #278 @ShawnHXH
* [UnitTest] add test for rwmanger_mmap.Release add test for rwmanger_mmap.Release #283 @vegetabledogdog
* [UnitTest] test: add tests for IsDBClosed, IsPrefixScan and IsPrefixSearchScan test: add tests for IsDBClosed, IsPrefixScan and IsPrefixSearchScan #290 @CodePrometheus