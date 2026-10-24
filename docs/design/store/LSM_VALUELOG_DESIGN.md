# DESIGN: LSM + ValueLog（路径 C）

> 本文档是 **唯一存储引擎** 的总体设计。  
> 选型：**LSM-Tree 管 key / 版本 / 索引；现有 `fileio.Store`（`.seg`）作为 ValueLog 存 value（及墓碑元数据可选）**。  
> 相关文档：[SST_DESIGN.md](./SST_DESIGN.md)、[MANIFEST_DESIGN.md](./MANIFEST_DESIGN.md)、[WAL_DESIGN.md](./WAL_DESIGN.md)、[STORE_MGR_DESIGN.md](./STORE_MGR_DESIGN.md)、[STORAGE_IO.md](../fileio/STORAGE_IO.md)。  
> 索引：[../README.md](../README.md)。

---

## 0. 决策与范围

### 0.1 已确认选型

| 项 | 选择 |
|----|------|
| 架构 | **LSM + ValueLog（路径 C）**（唯一引擎） |
| ValueLog | 复用 [fileio.Store](../fileio/STORAGE_IO.md)：`.seg` Append / Read / Seal / Delete |
| 索引层 | MemTable + Immutable + 多层 SST；SST 存 **`key → ValueRef`**，默认不内嵌大 value |
| 恢复 | **WAL + MANIFEST + SST** |

### 0.2 目标

| 目标 | 说明 |
|------|------|
| 内存可控 | 运行期不必维护「全量 key → Location」地图；仅 MemTable + 块缓存 / 表缓存 |
| 写吞吐 | ValueLog 顺序写 value；LSM 顺序写 WAL / flush SST |
| 点查正确 | 多层查找拿到最新 `ValueRef` → ValueLog 一跳读 value |
| 空间回收 | SST compaction 回收过期 key/版本；ValueLog GC 回收无引用 value 空洞 |
| 职责清晰 | `fileio` 仍只做字节持久化；LSM 控制面在 `internal/store` |

### 0.3 非目标（首版）

- 跨进程共享同一数据目录写
- 完整事务隔离级别 / MVCC 多快照（可先单版本 + tombstone）
- 与旧版 nutsdb / Bitcask 盘格式兼容
- 分布式复制协议
- HintFile 或全量内存 Location 索引
---

## 1. 总体架构

```text
┌─────────────────────────────────────────────────────────────┐
│                 StoreManager (LSM + ValueLog)                │
│                                                             │
│   Put/Delete ──► WAL ──► MemTable                           │
│                      │         │                            │
│                      │         ▼ flush                      │
│                      │    Immutable MemTable(s)             │
│                      │         │                            │
│                      │         ▼                            │
│                      │      SST L0 → L1 → … Ln              │
│                      │      (key → ValueRef, sorted)        │
│                      │         ▲                            │
│                      │    MANIFEST / VersionSet             │
│                                                             │
│   Get: MemTable → Imm → SST levels → ValueRef               │
│                         │                                   │
│                         ▼                                   │
│              ValueLog = fileio.Store (*.seg)                │
│              Location{FileID,Offset,Length} → value bytes   │
└─────────────────────────────────────────────────────────────┘
```

| 组件 | 职责 | 文档 |
|------|------|------|
| **ValueLog** | 追加存 Put value（及可选 Delete 旁路记录）；按 `Location` 点读 | [STORAGE_IO.md](../fileio/STORAGE_IO.md) |
| **WAL** | 保证 MemTable 崩溃可恢复 | [WAL_DESIGN.md](./WAL_DESIGN.md) |
| **MemTable** | 可变有序表：`key → ValueRef \| Tombstone \| Inline` | 本文 §3 |
| **SST** | 不可变有序表文件；点查 / 范围扫 | [SST_DESIGN.md](./SST_DESIGN.md) |
| **VersionSet + MANIFEST** | 当前有效 SST 集合、level、compaction 原子切换 | [MANIFEST_DESIGN.md](./MANIFEST_DESIGN.md) |
| **Compaction** | 合并 SST，丢掉被覆盖的 key / tombstone（按策略） | 本文 §6 |
| **ValueLog GC** | 重写仍被引用的 value，更新引用，删旧 `.seg` | 本文 §7 |

---

## 2. 寻址与 ValueRef

ValueLog 仍使用 fileio 契约：

```go
// fileio.Location — ValueLog 内绝对地址
type Location struct {
    FileID uint32
    Offset uint64
    Length uint32
}
```

LSM 索引层使用：

```go
type ValueKind uint8

const (
    ValueKindLocation ValueKind = 1 // 大 value / 默认：指向 ValueLog
    ValueKindInline   ValueKind = 2 // 小 value 内嵌于 MemTable/SST
    ValueKindTombstone ValueKind = 3
)

type ValueRef struct {
    Kind ValueKind
    // Kind==Location
    Loc fileio.Location
    // Kind==Inline（可选；阈值 ValueInlineThreshold）
    Inline []byte
    // 公共元数据（也可只放 ValueLog payload / SST 旁路字段）
    Timestamp uint64 // Unix 秒
    TTL       uint32 // 0 = Persistent
}
```

**不变量**：

```text
ValueKindLocation ⇒ Loc 指向 ValueLog 中一条完整、CRC 合法的 Put record
ValueKindTombstone ⇒ 该 key 在本版本视为删除；Get 返回 ErrKeyNotFound
ValueKindInline ⇒ 不访问 ValueLog；Inline 即用户 value 字节
```

小 value 阈值（建议默认 `256` 或 `1024` 字节，可配）：低于阈值可 **只写 WAL+MemTable/SST，不写 ValueLog**，降低读放大。

---

## 3. MemTable

### 3.1 结构

- 有序：`MemTree[memValue]`（默认实现 `RBTree`；`memValue = ValueRef + Seq`，见 `mem_tree.go` / `rb_tree.go` / `memtable.go`）
- 容量：按 **估算字节数** 触发 freeze（默认如 64MiB）
- 冻结后变为 Immutable，进入 flush 队列；同时分配新的可变 MemTable

### 3.2 与全量内存索引的差异

| | 全量 Location 地图（已废弃） | LSM MemTable |
|--|------------------------------|--------------|
| 覆盖范围 | **全量**最新 key | **仅未 flush** 的写入 |
| value | 仅 `Location` | `ValueRef`（Location / Inline / Tombstone） |
| 恢复 | 扫段 / Hint 重建全表 | WAL replay 到 MemTable |

---

## 4. 写路径

```text
Put(key, record):
  1. 校验 key / record；规范化 Timestamp
  2. 持写锁（短临界区，见 §6.3）：
     a. 若 len(value) >= InlineThreshold:
          payload = encodePutPayload(key, record)
          loc = valueLog.Append*(payload, RecordPut)
          ref = ValueRef{Kind: Location, Loc: loc, Timestamp, TTL}
        else:
          ref = ValueRef{Kind: Inline, Inline: value, Timestamp, TTL}
     b. wal.Append(PutOp, key, ref)
     c. mem.Put(key, ref)
     d. 若 mem 超限值 → freeze 当前 mem 进入 imm 队列，换新 mem，并通知 flush 协程
  3. 释放写锁后 return（不等待 SST flush / compaction 完成）

Delete(key):
  1. 写锁内：wal.Append(DeleteOp, key, Tombstone) → mem.Put(tombstone) → 限值检查同 Put
  2. 不写 ValueLog（tombstone 只在 LSM 侧）
```

**顺序要求（崩溃安全）**：

```text
ValueLog Append 成功（若需要）→ WAL 记录含最终 ValueRef → MemTable 可见
```

- ValueLog 已写但 WAL/Mem 未提交：重启后该 value 成孤儿，由 ValueLog GC 回收（可接受）。  
- **禁止** MemTable 先可见而 WAL 未 durable（按 SyncMode）。  
- Put/Delete **成功返回**只保证：在当前 `WALSyncMode` / ValueLog `SyncMode` 下，该条已进入 WAL（及必要的 ValueLog），**不**保证已进入 SST。

BatchPut / BatchDelete：同一写锁下顺序执行；批末按 SyncMode `wal.Sync()` / `valueLog.Sync()`；同样不在批内同步等待 flush。

## 5. 读路径

```text
Get(key):
  1. 查 MemTable；命中则 resolve(ref)
  2. 从新到旧查 Immutable MemTables
  3. 按 Version 查 SST：L0（可能多文件、可能重叠）→ L1 → … Ln
  4. 未命中 → ErrKeyNotFound

resolve(ref):
  if Tombstone or TTL expired → ErrKeyNotFound
  if Inline → 组装 core.Record
  if Location → valueLog.Read(loc) → decodePutPayload → core.Record
```

Iterate：对 MemTable + Imm + 各 level SST 做 **多路归并**（同 key 取最新序列号 / 最新 level 规则），再 `resolve`。

点查优化：SST Bloom Filter、块缓存、表缓存（见 SST_DESIGN）。

---

## 6. Flush 与 Compaction

### 6.1 Flush（Immutable MemTable → L0）

由 **后台 flush 协程**执行（与 Put 解耦）：

```text
Flush(imm):
  1. （无写锁）将 imm 按 key 有序写出 SST.tmp → fsync → rename
  2. 短写锁：
       MANIFEST.LogAndApply( add L0 file,
                             LastSequence/LogNumber = imm.maxSeq,
                             NextFileNumber … )
       从 imm 队列移除该 imm
  3. WAL 回收（见下）；然后通知 compaction 检查
```

L0 文件允许 key range **重叠**。允许多个 imm 排队；**按冻结顺序 FIFO** flush。

**WAL 回收（异步 flush 约束）**：

- 禁止在仍有并发 Put 追加同一 WAL 时对整个 `wal/` 做 `RemoveAll`。  
- 正确做法：`LogNumber = 已 flush 的最大 sequence`；`Replay` 跳过 `seq <= LogNumber` 的记录（已实现语义）。  
- 段级物理删除：仅删除「全部 record 的 seq 均 ≤ LogNumber」的已 seal WAL 段；首版可延后，允许 WAL 临时膨胀，Close 时再统一处理。  
- 旧实现「每次 flush 后 `RemoveAll(wal/)`」**仅适用于同步 flush 且持写锁**；异步模型下废弃。

### 6.2 Compaction（建议首版：Leveled）

| Level | 目标 | 说明 |
|-------|------|------|
| L0 | 文件数限值触发 | 与 L1 重叠文件一并合并 |
| L1…Ln | 层总大小 ≈ `base * multiplier^(n)` | 层内文件 key range **不重叠** |

合并输出：

- 同 key 只保留 **最新** `ValueRef`（或 tombstone）
- 可丢弃：已被更新覆盖的旧 Location 引用（**不**立刻删 ValueLog 数据）
- tombstone：可在到达底层且无更旧数据时丢弃（标准 LSM 墓碑回收规则）
- 输出 key 必须有序（实现用 `sort.Slice` / 等价 O(n log n)，禁止 O(n²) 演示排序）

Compaction **只重写 SST**，默认 **不重写 ValueLog**（把 value 搬家交给 §7）。

由 **后台 compaction 协程**执行；flush 完成后或 L0/层大小超限时调度。首版 **单线程** compaction，避免并行合并复杂度。

### 6.3 并发与锁模型（必须）

目标：Put 延迟不包含 SST 写盘与 compaction；Get 不被长时间 flush/compact 阻塞。

```text
┌───────────── 写锁（短）──────────────┐
│ Put/Delete: vlog? → WAL → mem       │
│ freeze: mem → imm 队列，signal flush │
│ LogAndApply / 摘除 imm / 更新读者表  │
└─────────────────────────────────────┘

┌───────────── 读锁 / Version 快照 ────┐
│ Get / 部分 Iterate：mem + imm + SST  │
│ Version 不可变；Apply 后读者换新视图 │
└─────────────────────────────────────┘

┌───────────── 后台（无持写锁做 I/O）──┐
│ flush worker: 写 SST 文件            │
│ compact worker: 读多 SST、写新 SST   │
│ 仅在 Apply / 删旧文件时回到短写锁    │
└─────────────────────────────────────┘
```

| 操作 | 锁 | 说明 |
|------|-----|------|
| Put / Delete / Batch* | 写锁 | 仅覆盖内存与 WAL/vlog 追加；**禁止**在锁内写 SST / 跑完整 compaction |
| freeze + 入 imm 队列 | 写锁 | O(1) 指针切换 |
| Flush 写 SST | **无写锁** | 只读 imm（已冻结） |
| Flush 的 LogAndApply | 写锁 | 短 |
| Compaction 读输入 SST、写输出 | **无写锁** | 基于 Apply 前拿到的 FileMeta 列表 / Version 快照 |
| Compaction 的 LogAndApply + 删旧 SST | 写锁 | 短；删文件可在锁外（Apply 后） |
| Get | 读锁（或等价：拷贝 mem/imm 指针 + `Version` 快照） | 不与 flush I/O 互斥等待 |

**Close**：

1. 置 `closing`，拒绝新写；  
2. 停止并 `Wait` flush / compaction 协程；  
3. 同步 flush 剩余 mem（可在写锁内或停写后单线程执行）；  
4. `wal.Sync` + `vlog.Sync` + Close。

**错误处理**：后台 flush/compaction 失败不得默默丢数据；应记录日志，保留 imm / 旧 Version，并支持重试。Put 已返回成功的键仍以 WAL+imm/mem 为准，重启可恢复。

## 7. ValueLog GC

SST compaction 不会释放 `.seg` 空间。需要独立 GC：

```text
ValueLogGC:
  1. 选一批 candidate sealed segments（低存活率 / 最老）
  2. 扫描 Version + MemTable，构建「仍被引用的 Location 集合」
     （或：扫 candidate 段内每条 record，询问「该 key 的最新 ValueRef 是否仍指向此 Loc」）
  3. 对仍存活的 Put：Append 到新 active ValueLog 段 → newLoc
  4. 产出「重定位表」oldLoc → newLoc（或直接写新 SST 补丁）
  5. 写新 SST / 或对受影响 key 批量更新引用并 flush
  6. MANIFEST Apply 后 DeleteSegment(old FileIDs)
```

首版可简化为：

- **仅 GC 已无任何 SST/Mem 引用的整段**（段级回收），实现简单、空间回收偏保守；  
- 后续再做段内重写。

指标：`vlog_live_ratio`、`vlog_gc_bytes_rewritten`、`vlog_obsolete_segments`。

---

## 8. 启动恢复

```text
OpenLSM(opts):
  1. 打开 ValueLog = fileio.Open（恢复 .seg used_bytes / active）
  2. 加载 MANIFEST → 当前 Version（SST 集合）
  3. 打开 / 校验各 SST（可懒加载 + 校验 footer）
  4. 找最新 WAL，replay → MemTable
  5. 不建全量 memIndex；索引来自 SST + MemTable
  6. 后台启动 compaction /（可选）ValueLog GC
```

崩溃窗口：

| 落点 | 结果 |
|------|------|
| ValueLog 已写，WAL 未写 | 孤儿 value；GC 回收 |
| WAL 已写，Mem 未写 | replay 恢复 |
| SST 已写，MANIFEST 未 Apply | 下次打开忽略该 SST 临时文件 |
| MANIFEST 已 Apply，旧 SST 未删 | 下次打开以 MANIFEST 为准，后台删孤儿文件 |

---

## 9. 目录布局

```text
<data_dir>/
  CURRENT                 # 指向当前 MANIFEST 文件名（可选，或固定 MANIFEST）
  MANIFEST-<seq>          # 见 MANIFEST_DESIGN
  wal/
    <walID>.wal           # 或复用 fileio 独立子目录
  sst/
    <fileNumber>.sst
  vlog/                   # ValueLog
    <fileID>.seg
```

ValueLog 使用子目录 `vlog/`，避免与 SST/WAL 混名。`.seg` 仅作为 ValueLog（及可选 WAL 后端）。

---

## 10. Options（建议）

```go
type LSMOptions struct {
    Dir string

    // ValueLog
    ValueLog fileio.Options // Dir 可指向 Dir/vlog

    // MemTable / Flush
    MemTableSize       int64 // bytes estimate
    MaxImmutableFlush  int

    // Inline
    ValueInlineThreshold int // default 256

    // Levels
    L0FileNumCompactionTrigger int
    LevelSizeMultiplier        int // default 10
    LevelBaseSize              int64

    // WAL
    WALSyncMode fileio.SyncMode

    // GC
    ValueLogGCEnabled   bool
    ValueLogGCLiveRatio float64 // e.g. 0.3

    // Cache
    BlockCacheSize int
    TableCacheSize int
}
```

`OpenStoreManager(opts LSMOptions) (StoreManager, error)` — 唯一打开入口。

---

## 11. 对外 API

对外实现 [`StoreManager`](../../../internal/store/store_manager.go)。

内部可增加运维 API（首版可不导出）：

```go
type LSMAdmin interface {
    CompactRange(start, limit []byte) error
    TriggerValueLogGC() error
    Stats() LSMStats
}
```

---

## 12. 实施里程碑

| 阶段 | 内容 | 依赖 |
|------|------|------|
| **L0** | 本文档 + SST/MANIFEST/WAL 定稿 | — |
| **L1** | WAL + MemTable + ValueLog Put/Get（无 SST，重启靠 WAL） | fileio |
| **L2** | Flush → L0 SST；MANIFEST；重启加载 SST | L1 |
| **L3** | Leveled compaction；Iterate 多路归并 | L2 |
| **L4** | Bloom / block cache；TTL；Batch | L3 |
| **L5** | ValueLog 段级 GC → 段内重写 GC | L3+ |

---

## 13. 测试要点

1. Put → Close → Open → Get（WAL + ValueLog）  
2. 超 MemTable 阈值后 flush，杀进程，仅靠 SST+WAL 恢复  
3. 同 key 多次 Put，compaction 后只留最新；旧 Location 可被 GC  
4. Delete tombstone 在各 level 可见性  
5. Inline 与 Location 混用  
6. MANIFEST Apply 中断：无半应用 Version  
7. ValueLog 孤儿段可被 GC 删除  

---

## 14. 总结

1. **唯一引擎**：LSM 管索引与版本；`fileio.Store` 专任 **ValueLog**。  
2. 写：ValueLog（大 value）→ WAL → MemTable；读：LSM 找 `ValueRef` → ValueLog 点读。  
3. Compaction 回收索引与版本；ValueLog GC 回收 value 空间。
