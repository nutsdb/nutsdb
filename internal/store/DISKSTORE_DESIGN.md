# DESIGN: DiskStore（持久化 KV StoreManager）

> 本文档描述 `internal/store` 包中 **DiskStore** 的设计。  
> DiskStore 实现 [StoreManager](./store_manager.go) API，并通过 [fileio.Store](../fileio/store.go) / [HintManager](../fileio/HINTFILE_DESIGN.md) 完成磁盘持久化与启动加速。

---

## 0. 需求（已确认）

1. 按 `StoreManager` / `BatchAPI` 实现对应的 **disk_store**。  
2. 通过 **fileio** 的 API 与持久化存储交互（`.seg` 读写、seal、删段等）。  
3. 启动恢复应使用 **HintFile**（见 [HINTFILE_DESIGN.md](../fileio/HINTFILE_DESIGN.md)），损坏时可降级扫描 `.seg`。  
4. **不需要**与旧版 nutsdb `Entry` 格式兼容：DiskStore payload 为 next 代独立编码，不做读写旧盘格式、不做迁移适配层。

`StoreManager` 契约：

```go
type StoreManager interface {
    Get(ctx context.Context, key []byte) (*core.Record, error)
    Put(ctx context.Context, key []byte, value *core.Record) error
    Delete(ctx context.Context, key []byte) error
    Iterate(ctx context.Context, callback func(key []byte, value *core.Record) bool) error
    Close() error
    BatchAPI
}
```

---

## 1. 背景与目标

`fileio.Store` 只做字节级 append / 点查，**不理解 KV**。  
`MemStore` 提供进程内有序 KV，但**不落盘**。  
DiskStore 位于二者之上：把 `core.Record` 编成 payload 写入 `.seg`，用内存索引保存 `key → Location`，并用 `.hint` 加速重启恢复。

### 1.1 目标

| 目标 | 说明 |
|------|------|
| API 对齐 | 完整实现 `StoreManager` + `BatchAPI` |
| 语义对齐 MemStore | Put upsert、Get 未命中 `ErrKeyNotFound`、Iterate 按 key 升序 |
| 持久化 | 所有变更经 `fileio.Store.Append`；崩溃后可恢复到 durable 前缀 |
| 快速恢复 | sealed 段优先 `HintManager.LoadAll`；active / 损坏 hint 走 `Store.Iterate` |
| 职责清晰 | DiskStore 管 KV 语义与索引；fileio 管段文件与 I/O |

### 1.2 非目标（首版）

- 多 bucket 路由 / 命名空间（注释中的 bucket 由上层 key 约定或后续扩展）
- Compaction / GC 策略实现（仅预留钩子：`DeleteSegment` + hint 删除）
- 跨进程并发写同一数据目录
- 完整事务 / WAL 以外的隔离级别

---

## 2. 总体架构

```text
┌──────────────────────────────────────────────────────┐
│                   Upper Engine                        │
│            (DB / Tx / Bucket / TTL 策略)              │
└──────────────────────────┬───────────────────────────┘
                           │ StoreManager
┌──────────────────────────▼───────────────────────────┐
│                     DiskStore                         │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐  │
│  │  memIndex   │  │ entry codec  │  │ Options     │  │
│  │ key→Location│  │ Put/Delete   │  │ Sync/TTL…   │  │
│  └──────┬──────┘  └──────┬───────┘  └─────────────┘  │
│         │                │                            │
│         ▼                ▼                            │
│  fileio.Store ◄──► fileio.HintManager                 │
│   (*.seg)              (*.hint)                       │
└──────────────────────────┬───────────────────────────┘
                           ▼
                        Disk / FS
```

| 组件 | 职责 |
|------|------|
| `DiskStore` | 实现 `StoreManager`；维护 memIndex；编解码 entry；协调 seal/hint/recovery |
| `memIndex` | 运行期最新可见 `key → fileio.Location`（有序，供 Iterate） |
| `entry codec` | `core.Record` ↔ Store payload；供 Hint `decodeKey` 使用 |
| `fileio.Store` | `.seg` Append/Read/Iterate/Seal/DeleteSegment/Close |
| `HintManager` | seal 后 `BuildFromSegment`；启动 `LoadAll`；删段时 `Delete` |

**不变量**：

```text
memIndex[key] = loc  ⇒ loc 指向的 record 为该 key 的最新 Put（且未随后 Delete）
HintKeys(F) 与 SegKeys(F) 按 HINTFILE §1.3 完全一致
API 入参 key 与 payload 内编码的 key 字节完全一致
```

---

## 3. 内存索引（memIndex）

### 3.1 结构

推荐直接复用现有 `MemStore` / `RBTree`，但 value 改为 `fileio.Location`（或薄包装）：

```go
// 逻辑视图
memIndex: ordered map[key] → Location
```

| 方案 | 优点 | 缺点 |
|------|------|------|
| **A. `RBTree[Location]`（推荐）** | 与 MemStore Iterate 同序；Put/Delete O(log n) | 需小改或新 Typed tree |
| B. `map[string]Location` + 排序 Iterate | 实现简单 | Iterate 需每次收集排序 |
| C. MemStore 存完整 `*core.Record` | Get 免读盘 | 内存大；与「索引只存 Location」冲突 |

首版采用 **A**：索引只存 `Location`，Get 时再 `Store.Read`。

### 3.2 并发

- DiskStore 全局 `sync.RWMutex`（或等价单写者）  
- 写路径（Put/Delete/Batch*/Close/Recovery apply）持写锁  
- Get / BatchGet / Iterate 持读锁；Iterate 回调期间可读盘（注意避免长时间占锁：可先拷贝 key/loc 列表再释放锁读盘）

### 3.3 与 MemStore 语义对照

| 操作 | MemStore | DiskStore |
|------|----------|-----------|
| Put | upsert `*Record` | upsert `Location`，并 Append Put record |
| Get | 返回内存 `*Record` | 查 loc → Read → decode；未命中 `ErrKeyNotFound` |
| Delete | `(rec, bool)` | Append Delete；更新索引；未命中仍可写墓碑或直接成功（见 §5） |
| Iterate | 有序，callback 返 false 停止 | 有序遍历 memIndex，Read+decode 后回调 |

---

## 4. Entry Payload 格式（核心新增）

> **格式立场**：本节定义为 DiskStore / fileio next 代专用 payload，**不兼容**旧版 nutsdb `Entry`（无 uvarint 链、无 BucketId/Flag/TxID 等遗留字段，也不提供双向转换）。旧数据需由上层离线迁移工具重写，不在本层处理。

`fileio` record 信封为：

```text
[crc32c(4) | payload_len(4) | type(1) | reserved(3) | payload]
```

DiskStore **只定义 payload**（Little-Endian）。

### 4.1 Put payload（`RecordPut`）

```text
┌──────────────── Put Payload ────────────────┐
│ key_len(4, uint32)                          │
│ value_len(4, uint32)                        │
│ timestamp(8, uint64)                        │
│ ttl(4, uint32)                              │   // 0 = Persistent
│ key(key_len)                                │
│ value(value_len)                            │
└─────────────────────────────────────────────┘
```

固定头：`4+4+8+4 = 20`；`payload_len = 20 + key_len + value_len`。

### 4.2 Delete payload（`RecordDelete`）

```text
┌────────────── Delete Payload ───────────────┐
│ key_len(4, uint32)                          │
│ key(key_len)                                │
└─────────────────────────────────────────────┘
```

`key_len` 必须 > 0（满足 Hint `key_len > 0`）。

### 4.3 Meta（可选）

内部 checkpoint 等使用 `RecordMeta`；`decodeKey` 返回 `fileio.ErrHintSkipMeta`，**不进 hint / 不进 memIndex**。

### 4.4 编解码 API（建议）

```go
func encodePutPayload(key []byte, rec *core.Record) ([]byte, error)
func encodeDeletePayload(key []byte) ([]byte, error)

func decodePutPayload(payload []byte) (key []byte, rec *core.Record, err error)
func decodeDeletePayload(payload []byte) (key []byte, err error)

// 供 HintManager.BuildFromSegment / Verify / Recovery 使用
func decodeKey(payload []byte, typ fileio.RecordType) (key []byte, err error)
```

`decodeKey` 规则：

| typ | 行为 |
|-----|------|
| `RecordPut` | 解析 Put 头，返回 key 切片（拷贝） |
| `RecordDelete` | 解析 Delete 头，返回 key |
| `RecordMeta` | `return nil, fileio.ErrHintSkipMeta` |
| 其他 | 返回错误 |

### 4.5 校验

| 规则 | 错误 |
|------|------|
| API `key` 为空 | `ErrKeyEmpty` |
| Put 时 `value == nil` | `ErrInvalidRecord`（新增）或视为空 Record |
| Put 时 `value.Key` 非空且与 API key 不一致 | `ErrKeyMismatch`（新增，推荐强制一致或忽略 value.Key 以 API key 为准） |
| `key_len` / `value_len` 溢出或与切片不符 | `ErrCorruptEntry` |
| 整条 fileio record 超 `MaxRecordSize` | `fileio.ErrRecordTooLarge` |

**推荐策略**：写入时以 API `key` 为准写入 payload；`core.Record.Key` 若为空则填入 API key；若非空则必须 `bytes.Equal`。

---

## 5. 操作语义

### 5.1 Get

```text
Get(ctx, key):
  1. 若 closed → ErrDiskStoreClosed
  2. 若 len(key)==0 → ErrKeyEmpty
  3. loc, ok = memIndex.Get(key); 若 !ok → ErrKeyNotFound
  4. payload, typ = store.Read(loc)
  5. 若 typ != RecordPut → ErrKeyNotFound（防御）
  6. _, rec = decodePutPayload(payload)
  7. 若启用读时 TTL 且已过期 → 可选惰性删索引并 ErrKeyNotFound
  8. return rec
```

### 5.2 Put

```text
Put(ctx, key, value):
  1. 校验 key / value / ctx
  2. 规范化 Record：Key=key；Timestamp 若为 0 可填 now（可配）
  3. payload = encodePutPayload(key, value)
  4. loc = store.Append(payload, RecordPut)   // 或按 SyncMode 用 AppendSync
  5. memIndex.Put(key, loc)
  6. onSegmentMaybeSealed()                   // 见 §8
  7. return nil
```

### 5.3 Delete

对齐「删除语义成功」而非 MemStore 的 `(bool)`：

```text
Delete(ctx, key):
  1. 校验
  2. 若 memIndex 无此 key：
       默认：仍 Append 墓碑（幂等删除，便于跨副本）或直接 return nil
       首版推荐：return nil（不写盘），与「键本就不存在」一致、减少膨胀
  3. 若存在：
       payload = encodeDeletePayload(key)
       loc = store.Append(payload, RecordDelete)
       memIndex.Delete(key)
       onSegmentMaybeSealed()
  4. return nil
```

> 配置项 `DeleteWritesTombstoneIfMissing bool`（默认 false）可切换为「缺失也写墓碑」。

### 5.4 Iterate

```text
Iterate(ctx, callback):
  持读锁收集 []{key, loc}（已按 key 有序）
  释放锁后逐条：
    Read + decodePut；跳过损坏/过期（可配）
    若 !callback(key, rec) → stop
```

不扫描磁盘上的历史版本；**只暴露 memIndex 中的最新可见 key**（与 MemStore 一致）。

### 5.5 BatchAPI

| API | 行为 |
|-----|------|
| `BatchPut` | 同一写锁下顺序 Append 多条 Put，更新 memIndex；结束时按 SyncMode 决定是否 `store.Sync()` 一次（group commit） |
| `BatchDelete` | 对存在的 key 写墓碑并删索引；可选末尾一次 Sync |
| `BatchGet` | 对每个 key 执行 Get 语义；缺失项 `Value=nil` 或整批跳过——**推荐**：返回与 keys 等长切片，缺失 `Value=nil` 且不报错 |

任一中途失败：已写入的 Append 不回滚（append-only）；返回错误；调用方需知部分可能已落盘。可在文档标明「尽力而为批处理」。

### 5.6 Close

```text
Close:
  1. 写锁；若已 closed return nil
  2. store.Sync()（best-effort）
  3. 若 active 刚 seal 触发的 hint 未完成，尽力 BuildFromSegment
  4. store.Close()
  5. 标记 closed；清空 memIndex 引用
```

### 5.7 Context

- 在获取锁前 / 批处理循环间隙检查 `ctx.Done()`  
- 已进入 `fileio` 的单次 I/O 不强制可取消（首版）  
- 取消时返回 `ctx.Err()`

---

## 6. TTL 策略

`core.Record.TTL`：`0` 表示 `Persistent`（永不过期）。

| 模式 | 行为 | 首版 |
|------|------|------|
| **Read-time expire** | Get/Iterate 时用 `Timestamp+TTL` 与 now 比较；过期视为不存在，可选异步补墓碑 | **默认启用** |
| Write-time ignore | 只存储字段，不过滤过期 | 可关 |

过期判定（秒级，与常见 KV 一致）：

```text
if TTL == 0: not expired
else if nowUnix > Timestamp + uint64(TTL): expired
```

> `Timestamp` 单位约定为 **Unix 秒**（若现有代码为纳秒需在实现时统一并写死；推荐秒，简单）。实现时在文档/常量中固定一种。

---

## 7. 启动恢复

```text
Open(opts) (*DiskStore, error):
  1. fileio.Open(opts.FileIO) → store
  2. hintMgr = NewHintManager(opts.Dir)
  3. memIndex = empty ordered map
  4. Recovery:
       a. ids = list segments (via store 内部或目录扫描)
       b. activeID = store 当前 active（需 DiskStore 能获知；见 §7.1）
       c. for id in sort(ids):
            if id == activeID: continue
            rd, err = hintMgr.OpenReader(id)
            if err != nil:
                store.Iterate(id, applyRecord)      // fallback
            else:
                rd.Iterate(applyHintEntry)
                rd.Close()
       d. if activeID != 0:
            store.Iterate(activeID, applyRecord)
  5. return DiskStore
```

### 7.1 如何得知 active FileID

`fileio.Store` 接口当前未暴露 active ID。可选：

| 方案 | 说明 |
|------|------|
| **A. 扩展 Store 只读 API**（推荐） | 增加 `ActiveFileID() uint32` / `ListFileIDs() []uint32` |
| B. DiskStore 自行扫目录 | 解析 `*.seg`；合法 footer → sealed；无 footer 的最大 ID → active |
| C. Recovery 全部 Iterate | 正确但慢；hint 仍加速 sealed |

首版推荐 **A**：在 `fileio.Store` 增加最小元数据查询，避免 DiskStore 重复解析 header/footer。

### 7.2 apply 规则

```text
applyHintEntry(e):
  if e.Type == RecordDelete: memIndex.Delete(e.Key)
  else if e.Type == RecordPut: memIndex.Put(e.Key, e.Loc)

applyRecord(loc, typ, payload):
  key, err = decodeKey(payload, typ)
  if ErrHintSkipMeta: return nil
  if typ == RecordDelete: memIndex.Delete(key)
  else if typ == RecordPut: memIndex.Put(key, loc)
```

按 FileID 升序、段内顺序覆盖 → 与全量扫盘结果一致。

---

## 8. Seal 与 Hint 集成

```text
onSegmentMaybeSealed / after Append that triggers rotate:
  // fileio 在 Append 内部可能已 seal 旧段并创建新 active
  // DiskStore 需要感知「哪些 FileID 刚 seal」

推荐：fileio 提供回调或返回值，例如：
  Append(...) (loc Location, sealed []uint32, err error)
或 DiskStore 在 Append 前后记录 activeID，变化则旧 ID 已 seal。
```

对每个新 seal 的 `fileID`：

```text
hintMgr.BuildFromSegment(store, fileID, decodeKey)
// BuildFromSegment 内部已 Verify；失败仅打日志，不影响在线服务
```

删段（compaction 上层调用时）：

```text
store.DeleteSegment(fileID)
hintMgr.Delete(fileID)   // 忽略 ErrHintNotFound
```

---

## 9. Sync 与耐久性

映射 `fileio.Options.SyncMode`：

| SyncMode | Put/Delete | Batch* |
|----------|------------|--------|
| `SyncNoSync` | `Append` | 多 `Append`，不主动 Sync |
| `SyncBatch` | `Append`（fileio 内按字节阈值 Sync） | 批末可额外 `store.Sync()` |
| `SyncEveryWrite` | `AppendSync` | 每条 Sync 或批末一次 Sync（推荐批末一次，语义为「整批 durable」） |

崩溃后：仅 `durable_offset` 前的 Location 有效；memIndex 经 Recovery 重建后自然不含未 sync 数据。

---

## 10. Options 与构造

```go
type DiskStoreOptions struct {
    Dir string

    // 透传 / 覆盖 fileio.Options
    SegmentSize     uint64
    WriteBufferSize int
    MaxRecordSize   uint64
    SyncMode        fileio.SyncMode
    MaxOpenSegments int
    ReadBackend     fileio.ReadBackend

    // DiskStore 自身
    EnableReadTTL              bool // default true
    DeleteWritesTombstoneIfMissing bool // default false
    AutoFillTimestamp          bool // default true, Timestamp==0 时填 Unix 秒
}

func OpenDiskStore(opts DiskStoreOptions) (StoreManager, error)
```

目录结构与 fileio 相同：

```text
<data_dir>/0000000001.seg
<data_dir>/0000000001.hint
...
```

---

## 11. 错误

| 错误 | 含义 |
|------|------|
| `ErrKeyNotFound` | 已有：键不存在或读时过期 |
| `ErrKeyEmpty` | 已有：空 key |
| `ErrDiskStoreClosed` | 新增：已 Close |
| `ErrInvalidRecord` | 新增：nil Record 等 |
| `ErrKeyMismatch` | 新增：Record.Key 与 API key 不一致 |
| `ErrCorruptEntry` | 新增：payload 解析失败 |
| `context.Canceled` / `DeadlineExceeded` | ctx 取消 |
| 透传 `fileio.*` | 如 `ErrRecordTooLarge`、`ErrCorrupt` |

---

## 12. 建议代码布局

```text
internal/store/
  store_manager.go      # 接口（已有）
  mem_store.go          # 内存实现（已有）
  errors.go             # 错误（扩展）
  entry.go              # Put/Delete payload 编解码 + decodeKey
  disk_store.go         # DiskStore 实现 StoreManager
  disk_store_recover.go # Open / Recovery（可合并）
  disk_store_test.go
  DISKSTORE_DESIGN.md   # 本文档
```

---

## 13. 测试计划

1. **语义对等**：同一操作序列下，MemStore 与 DiskStore 的 Get/Iterate 可见结果一致（忽略持久化细节）。  
2. **持久化**：Put → Close → Open → Get 命中；Delete 后重启仍不存在。  
3. **Hint**：seal 多段后目录出现 `*.hint`；重启不扫 sealed 全量仍正确；故意损坏 hint 后仍正确（fallback）。  
4. **Key 一致**：`BuildFromSegment` + `VerifyHintMatchesSegment` 在 DiskStore seal 路径上通过。  
5. **Batch**：BatchPut 后 BatchGet；中途超大 record 失败时行为符合文档。  
6. **TTL**：过期键 Get 返回 `ErrKeyNotFound`。  
7. **并发**：多读单写 smoke（可选）。

---

## 14. 实施里程碑

1. **D1**：`entry.go` 编解码 + 单测；扩展 `errors.go`  
2. **D2**：`DiskStore` Put/Get/Delete/Close + memIndex（无 hint）  
3. **D3**：Open Recovery（目录扫 / `ActiveFileID` + Iterate）  
4. **D4**：Seal 后 `HintManager.BuildFromSegment`；LoadAll 加速恢复  
5. **D5**：Iterate + BatchAPI + SyncMode / TTL  
6. **D6**（可选）：compaction 钩子、`ActiveFileID` 正式并入 fileio 接口、mmap sealed  

---

## 15. 总结

1. DiskStore 是 **StoreManager 的磁盘实现**，组合 `fileio.Store` + `HintManager` + 有序 memIndex。  
2. **Entry payload** 在 store 层定义（Put/Delete），保证 Hint `decodeKey` 与 `.seg` 内 key 字节一致；**不兼容**旧版 nutsdb Entry。  
3. 运行期读路径：`memIndex → Store.Read → decode`；写路径：`encode → Append → 更新索引 → seal/hint`。  
4. 恢复：sealed 用 hint，active/损坏用 Iterate；按 FileID 覆盖得到最新视图。  
5. 首版不做多 bucket / compaction；TTL 默认读时过滤。

实现时以本文档与 [DESIGN.md](../fileio/DESIGN.md)、[HINTFILE_DESIGN.md](../fileio/HINTFILE_DESIGN.md) 为准；若需扩展 `fileio.Store` 元数据 API，应同步更新 fileio 设计。
