# DESIGN: LSM + ValueLog（路径 C）

> 本文档是 **唯一存储引擎** 的总体设计。  
> 选型：**LSM-Tree 管 key / 版本 / 索引；现有 `fileio.Store`（`.seg`）作为 ValueLog 存 value（及墓碑元数据可选）**。  
> 相关文档：[SST_DESIGN.md](./SST_DESIGN.md)、[MANIFEST_DESIGN.md](./MANIFEST_DESIGN.md)、[WAL_DESIGN.md](./WAL_DESIGN.md)、[STORE_MGR_DESIGN.md](./STORE_MGR_DESIGN.md)、[fileio/DESIGN.md](../fileio/DESIGN.md)。

---

## 0. 决策与范围

### 0.1 已确认选型

| 项 | 选择 |
|----|------|
| 架构 | **LSM + ValueLog（路径 C）**（唯一引擎） |
| ValueLog | 复用 [fileio.Store](../fileio/DESIGN.md)：`.seg` Append / Read / Seal / Delete |
| 索引层 | MemTable + Immutable + 多层 SST；SST 存 **`key → ValueRef`**，默认不内嵌大 value |
| 恢复 | **WAL + MANIFEST + SST** |
| 已移除 | Bitcask / HintFile / `EngineMode` 多引擎分派 |

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
| **ValueLog** | 追加存 Put value（及可选 Delete 旁路记录）；按 `Location` 点读 | [fileio/DESIGN.md](../fileio/DESIGN.md) |
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

- 有序：skiplist 或复用 / 改造现有 `RBTree`（value 改为 `ValueRef`）
- 容量：按 **条目数** 或 **估算字节数** 触发 freeze（默认如 64MiB 估算）
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
  2. 若 len(value) >= InlineThreshold:
       payload = encodePutPayload(key, record)   // 与 DiskStore entry 格式对齐，便于 ValueLog 复用
       loc = valueLog.Append*(payload, RecordPut)
       ref = ValueRef{Kind: Location, Loc: loc, Timestamp, TTL}
     else:
       ref = ValueRef{Kind: Inline, Inline: value, Timestamp, TTL}
  3. wal.Append(PutOp, key, ref)                 // 见 WAL_DESIGN；须先于或与 MemTable 同事务语义
  4. mem.Put(key, ref)
  5. 若 mem 超阈值 → freeze → 异步 FlushToL0
  6. return

Delete(key):
  1. wal.Append(DeleteOp, key, Tombstone)
  2. mem.Put(key, Tombstone)                     // 或专用 Delete
  3. 可选：不写 ValueLog（推荐）；tombstone 只在 LSM 侧可见
  4. 阈值检查同 Put
```

**顺序要求（崩溃安全）**：

```text
ValueLog Append 成功（若需要）→ WAL 记录含最终 ValueRef → MemTable 可见
```

- ValueLog 已写但 WAL/Mem 未提交：重启后该 value 成孤儿，由 ValueLog GC 回收（可接受）。  
- **禁止** MemTable 先可见而 WAL 未 durable（按 SyncMode）。

BatchPut / BatchDelete：同一写锁下顺序执行；批末按 SyncMode `wal.Sync()` / `valueLog.Sync()`。

---

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

### 6.1 Flush（MemTable → L0）

```text
Flush(imm):
  1. 将 imm 中条目按 key 排序写出 SST（已有序则直接写）
  2. fsync SST
  3. MANIFEST：LogAndApply( add L0 file, remove imm 引用 )
  4. 可截断 / 归档对应 WAL 前缀（见 WAL_DESIGN）
```

L0 文件允许 key range **重叠**。

### 6.2 Compaction（建议首版：Leveled）

| Level | 目标 | 说明 |
|-------|------|------|
| L0 | 文件数阈值触发 | 与 L1 重叠文件一并合并 |
| L1…Ln | 层总大小 ≈ `base * multiplier^(n)` | 层内文件 key range **不重叠** |

合并输出：

- 同 key 只保留 **最新** `ValueRef`（或 tombstone）
- 可丢弃：已被更新覆盖的旧 Location 引用（**不**立刻删 ValueLog 数据）
- tombstone：可在到达底层且无更旧数据时丢弃（标准 LSM 墓碑回收规则）

Compaction **只重写 SST**，默认 **不重写 ValueLog**（把 value 搬家交给 §7）。

### 6.3 并发

- 单 compaction 线程首版即可；与 Get 通过 Version 不可变快照共存  
- 写路径只碰 MemTable/WAL/ValueLog；Apply Version 时短临界区

---

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

## 9. 已移除的模型

以下不再支持，也无兼容路径：

| 已移除 | 原用途 |
|--------|--------|
| HintFile (`.hint`) | Bitcask 启动加速 |
| 全量 `key → Location` MemStore 作为主索引 | Bitcask 运行期索引 |
| `EngineMode` / Bitcask DiskStore | 多引擎分派 |

`.seg` **仅**作为 ValueLog（及可选 WAL 后端）。

---

## 10. 目录布局

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

ValueLog 使用子目录 `vlog/`，避免与 SST/WAL 混名。

---

## 11. Options（建议）

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

## 12. 对外 API

对外实现 [StoreManager](./store_manager.go)。

内部可增加运维 API（首版可不导出）：

```go
type LSMAdmin interface {
    CompactRange(start, limit []byte) error
    TriggerValueLogGC() error
    Stats() LSMStats
}
```

---

## 13. 实施里程碑

| 阶段 | 内容 | 依赖 |
|------|------|------|
| **L0** | 本文档 + SST/MANIFEST/WAL 定稿；移除 Bitcask/Hint | — |
| **L1** | WAL + MemTable + ValueLog Put/Get（无 SST，重启靠 WAL） | fileio |
| **L2** | Flush → L0 SST；MANIFEST；重启加载 SST | L1 |
| **L3** | Leveled compaction；Iterate 多路归并 | L2 |
| **L4** | Bloom / block cache；TTL；Batch | L3 |
| **L5** | ValueLog 段级 GC → 段内重写 GC | L3+ |

---

## 14. 测试要点

1. Put → Close → Open → Get（WAL + ValueLog）  
2. 超 MemTable 阈值后 flush，杀进程，仅靠 SST+WAL 恢复  
3. 同 key 多次 Put，compaction 后只留最新；旧 Location 可被 GC  
4. Delete tombstone 在各 level 可见性  
5. Inline 与 Location 混用  
6. MANIFEST Apply 中断：无半应用 Version  
7. ValueLog 孤儿段可被 GC 删除  

---

## 15. 总结

1. **唯一引擎**：LSM 管索引与版本；`fileio.Store` 专任 **ValueLog**。  
2. **已移除** HintFile / Bitcask / 全量内存 Location 主索引。  
3. 写：ValueLog（大 value）→ WAL → MemTable；读：LSM 找 `ValueRef` → ValueLog 点读。  
4. Compaction 回收索引与版本；ValueLog GC 回收 value 空间。
