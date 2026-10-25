# DESIGN: 用户 API（Control Plane）

> 面向最终用户的公开 API。本版优先从 **nutsdb v1** 迁移 **Bucket + KV（BTree）** 表面；List / Set / SortedSet 等后续再开。  
> 存储引擎仍为 LSM + ValueLog（见 [store/LSM_VALUELOG_DESIGN.md](../store/LSM_VALUELOG_DESIGN.md)）。  
> 索引：[../README.md](../README.md)。

**状态：U0 已实现（根包 `DB`/`Tx`/`Bucket` + `buckets.meta`）；§4.2–4.4 仍为暂不做。**

---

## 0. 目标与非目标

### 0.1 目标

| 目标 | 说明 |
|------|------|
| 兼容心智 | `Open` → `Update`/`View` → `tx.GetBucket(ds, name).Put/Get/...`；相对 v1 增加 `ctx`，KV 挂到 Bucket 句柄 |
| Bucket 命名空间 | 同名 key 可存在于不同 bucket；bucket 须先创建再写 KV |
| 事务外壳 | 读写均在 `Tx` 内；`Update` 可写，`View` 只读；`ctx` 可取消 |
| 落到引擎 | Commit 时把变更落到 `StoreManager`（或等价批量接口） |

### 0.2 非目标（本版）

- List / Set / SortedSet API（v1 的 `RPush`/`SAdd`/`ZAdd` 等）
- Watch、HTTP、完整 BackupTarGZ 运维面（可后续补）
- 与 v1 盘格式二进制兼容
- 完整 Serializable Snapshot Isolation 证明（首版可用「单写者 + Commit 落盘」简化）

---

## 1. 分层

```text
┌─────────────────────────────────────────┐
│  用户代码                                 │
│  Open / Update|View / Tx / Bucket.*      │  ← 本文档
└───────────────────┬─────────────────────┘
                    │ Commit：编码 bucket+key → Put/Delete
                    ▼
┌─────────────────────────────────────────┐
│  StoreManager（LSM + ValueLog）          │  ← docs/design/store/*
└─────────────────────────────────────────┘
```

- **根包 `github.com/nutsdb/nutsdb`**：导出 `DB` / `Tx` / `Bucket` / `Options` / 错误与常量。  
- **`internal/store`**：不直接暴露给外部模块；由根包 facade 持有。  
- KV 读写走 **`tx.GetBucket(ds, name).*`**，不再在 `Tx` 上带 `bucket` 参数。
---

## 2. 打开与生命周期（对齐 v1）

```go
package nutsdb

func Open(options Options, ops ...Option) (*DB, error)

// ctx 贯穿整段事务：等写锁、fn 执行、Commit 均可被取消。
// fn 内收到同一（或派生）ctx，便于长扫描 / 批量写中途 abort。
func (db *DB) Update(ctx context.Context, fn func(ctx context.Context, tx *Tx) error) error  // 读写
func (db *DB) View(ctx context.Context, fn func(ctx context.Context, tx *Tx) error) error    // 只读
func (db *DB) Close(ctx context.Context) error
```

相对 v1：`Update`/`View`/`Close` 均带 `context.Context`（取消 / deadline）。用法：

```go
db, err := nutsdb.Open(nutsdb.DefaultOptions, nutsdb.WithDir("/tmp/nutsdb-next"))
if err != nil { ... }
defer db.Close(context.Background())

ctx := context.Background()
_ = db.Update(ctx, func(ctx context.Context, tx *nutsdb.Tx) error {
    if err := tx.NewKVBucket("bucket1"); err != nil {
        return err
    }
    return tx.GetBucket(nutsdb.DataStructureBTree, "bucket1").Put([]byte("k"), []byte("v"), 0)
})

_ = db.View(ctx, func(ctx context.Context, tx *nutsdb.Tx) error {
    select {
    case <-ctx.Done():
        return ctx.Err()
    default:
    }
    v, err := tx.GetBucket(nutsdb.DataStructureBTree, "bucket1").Get([]byte("k"))
    _ = v
    return err
})
```
### 2.1 Options（首版最小集）

从 v1 收敛；未列字段可后续加：

| 字段 / Option | 含义 | 备注 |
|---------------|------|------|
| `Dir` / `WithDir` | 数据目录 | 必填 |
| `Sync` 相关 | 映射到引擎 `WALSyncMode` / ValueLog `SyncMode` | 命名可再议 |
| `EntryIdxMode` 等 | 暂不暴露或忽略 | next 无 Hint RAM 模式 |

`DefaultOptions` + 函数式 `Option` 保留，降低迁移成本。

---

## 3. Bucket 管理与句柄

v1 中 bucket 与 **数据结构类型 `ds`** 绑定：同名 bucket 的 BTree 与 List 是不同命名空间。  
**本版只落地 KV（BTree）**；`ds` 仍保留在管理 API 里，便于以后扩展。

### 3.1 创建 / 删除 / 列举（在 `Tx` 上）

```go
const (
    DataStructureBTree     uint16 = ... // KV
    DataStructureList      uint16 = ... // 本版：创建可返回 ErrNotSupported
    DataStructureSet       uint16 = ...
    DataStructureSortedSet uint16 = ...
)

// 推荐
func (tx *Tx) NewKVBucket(name string) error
// 等价于 NewBucket(DataStructureBTree, name)

func (tx *Tx) NewBucket(ds uint16, name string) error
func (tx *Tx) DeleteBucket(ds uint16, bucket string) error
func (tx *Tx) ExistBucket(ds uint16, bucket string) bool
func (tx *Tx) IterateBuckets(ds uint16, pattern string, f func(bucket string) bool) error
```

### 3.2 取得句柄（链式入口）

```go
// GetBucket 返回绑定到本 Tx + ds + 名称的句柄（始终非 nil）。
// 未创建时后续 Put/Get 等返回 ErrBucketNotFound；
// 本版非 BTree 的 ds 上调用 KV 方法返回 ErrNotSupported。
func (tx *Tx) GetBucket(ds uint16, name string) *Bucket
```

推荐写法：

```go
tx.GetBucket(nutsdb.DataStructureBTree, "bucket1").Put([]byte("k"), []byte("v"), 0)
v, err := tx.GetBucket(nutsdb.DataStructureBTree, "bucket2").Get([]byte("k"))
```

同一 Tx 内可多次 `GetBucket`；句柄只借用 Tx，**不可**跨事务或在 `fn` 返回后继续使用。

语义：

| 行为 | 说明 |
|------|------|
| 创建前必须 `New*` | 未创建时 `GetBucket(ds, name).Put/Get/...` → `ErrBucketNotFound` |
| 重复创建 | `ErrBucketAlreadyExist` |
| 删除不存在 | `ErrBucketNotFound` |
| `IterateBuckets` | `pattern` 对齐 v1；首版可只支持 `"*"` |

**已定**：bucket 元数据存 **单独 meta 文件**（不进 LSM / ValueLog）。

- 路径：数据目录下独立文件（如 `BUCKETS` / `buckets.meta`，具体名实现时定）。  
- 内容：`(ds, name) → bucketId` 等；Open 时加载进内存 `BucketManager`。  
- 变更：`NewBucket` / `DeleteBucket` 进 Tx pending；**Commit 时先落引擎数据，再原子更新 meta 文件**（写临时文件 + rename；细节可另开小节）。  
- 不做：把 `__bucket__/...` 当普通 KV 写进 LSM。

---

## 4. KV API（挂在 `*Bucket` 上）

相对 v1：`tx.Put(bucket, key, ...)` → **`tx.GetBucket(ds, bucket).Put(key, ...)`**。  
以下均在 `*Bucket` 上；**仅对 `DataStructureBTree` 有效**（其它 ds 的方法返回 `ErrNotSupported`）。

### 4.1 核心（P0，首版必须）

```go
func (b *Bucket) Put(key, value []byte, ttl uint32) error
func (b *Bucket) Get(key []byte) (value []byte, err error)
func (b *Bucket) Delete(key []byte) error
func (b *Bucket) Has(key []byte) (bool, error)

func (b *Bucket) Name() string
```

| 约定 | v1 | next 初稿 |
|------|----|-----------|
| `ttl == 0` | 永不过期 | 同 |
| `ttl > 0` | 秒级 TTL | 同；落盘用引擎 `Timestamp`+`TTL` |
| 空 key | 错误 | 同（`ErrKeyEmpty`） |
| Get 未找到 | 错误 | 统一为 `ErrKeyNotFound`（见 §6） |
| bucket 不存在 | — | `ErrBucketNotFound` |

### 4.2 常用扩展（暂不做）

以下 v1 能力本版 **不暴露、不实现**（需要时再开）：

```go
// 暂不做
// PutIfNotExists / PutIfExists / GetSet / ValueLen / GetTTL / Persist
```

### 4.3 扫描与批量（暂不做）

以下 v1 能力本版 **不暴露、不实现**（需要时再开）：

```go
// 暂不做
// GetAll / GetKeys / GetValues / GetMaxKey / GetMinKey
// RangeScan / RangeScanEntries / PrefixScan / PrefixSearchScan / PrefixScanEntries
// MSet / MGet
```

### 4.4 值运算类（暂不做）

以下 v1 能力本版 **不暴露、不实现**（需要时再开）：

```go
// 暂不做
// PutWithTimestamp / Append / GetRange
// Incr / Decr / IncrBy / DecrBy / GetBit / SetBit
```

---

## 5. 与引擎的键映射（建议）

用户 API：`(bucket, key)` → 引擎单键：

```text
engineKey = encode(bucketId or bucketName) || userKey
```

| 方案 | 优点 | 缺点 |
|------|------|------|
| **bucketId（v1 风格）** | 改名友好、紧凑 | 需稳定 id 分配与元数据 |
| **bucketName 前缀** | 实现简单 | 改名贵、前缀膨胀 |

初稿建议：**bucketId（uint64/u32）+ userKey**，与 v1 `Bucket.Id` 心智一致；元数据见 §3。

Delete / TTL / tombstone：直接调用 `StoreManager.Delete` / `Put`（带 TTL）。

---

## 6. 错误（建议统一名，兼容别名）

| next 建议名 | v1 常见名 | 场景 |
|-------------|-----------|------|
| `ErrDBClosed` | `ErrDBClosed` | 已关闭 |
| `ErrTxClosed` / `ErrTxNotWritable` | 同 | 事务状态 |
| `ErrBucketNotFound` | `ErrBucketNotFound` / `ErrNotFoundBucket` | 未创建或已删 |
| `ErrBucketAlreadyExist` | 同 | 重复 `NewBucket` |
| `ErrKeyNotFound` | 各处 Key not found | Get/扫描 |
| `ErrKeyEmpty` | 同 | 空 key |
| `ErrNotSupported` | — | List/Set/ZSet 等本版未实现 |

可对旧名提供 `var ErrNotFoundBucket = ErrBucketNotFound` 以减轻迁移摩擦（是否保留请改）。

---

## 7. 事务语义（首版简化）

```text
Update(ctx, fn):
  若 ctx 已取消 → 直接返回 ctx.Err()
  begin writable Tx（等待写锁时可响应 ctx）
  fn(ctx, tx)     // 变更进 pending（内存）；fn 应检查 ctx
  若 fn 返回 err → Rollback，不写引擎
  否则 Commit（写入过程可检查 ctx；已部分写入时的策略见下）
  → 成功则对调用方可见

View(ctx, fn):
  begin read-only Tx
  fn(ctx, tx)     // 只读；写操作返回 ErrTxNotWritable
```

取消约定（首版）：

| 阶段 | 行为 |
|------|------|
| 等写锁 / `fn` 返回前 | 取消 → Rollback，无引擎副作用 |
| Commit 进行中 | 尽量尽快停；若已有部分引擎写，首版可 **继续写完再返回 `ctx.Err()`**，或 **返回错误且依赖上层重试**（实现时二选一并写清） |
| `fn` 自行 `return ctx.Err()` | 与普通错误相同 → Rollback |

首版约束（可改）：

1. **进程内单写事务**（与 v1 类似的写互斥）；多 `View` 可并发（依赖引擎读锁）。  
2. Commit：**同一临界区**内顺序 `Put`/`Delete`（或未来 `Batch*`），批末按 Options sync。  
3. 读自己的未提交写：Tx 内 pending 优先于引擎（v1 同款）。  
4. `fn` / Commit 中应响应 `ctx.Done()`（扫描类 API 恢复后再强调周期检查）。
---

## 8. 里程碑建议

| 阶段 | 内容 |
|------|------|
| **U0** | `Open`、带 `ctx` 的 `Close`/`Update`/`View`、`NewKVBucket`/`GetBucket`、`DeleteBucket`/`ExistBucket`、`Bucket.Put/Get/Delete/Has`（含 `Put` 的 TTL） |
| **U1** | `IterateBuckets`、错误别名与文档示例 |
| **U2** | §4.2 / §4.3 / §4.4 按需恢复；List/Set/ZSet（另开设计） |

---

## 9. 待你拍板

1. ~~bucket 元数据存哪？~~ → **独立 meta 文件** `buckets.meta`（见 §3）  
2. ~~引擎键？~~ → **bucketId（u64 big-endian）|| userKey**  
3. 错误名是否保留 v1 别名？ → 已保留 `ErrNotFoundBucket`  
4. `Options` 要暴露哪些 sync/memtable 旋钮？（仍待定；首版仅 `Dir`）  
5. ~~P2 值运算 API？~~ → **暂不做**（见 §4.4）

---

## 10. 参考

- v1：`Open` / `DB.Update|View`（`db.go`），`Tx` bucket（`tx_bucket.go`），KV（`tx_btree.go`）  
- 官方指南：Buckets、KV pair  
- 引擎：`StoreManager`（[STORE_MGR_DESIGN.md](../store/STORE_MGR_DESIGN.md)）
