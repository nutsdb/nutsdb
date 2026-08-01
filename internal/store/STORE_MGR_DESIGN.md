# DESIGN: StoreMgr（LSM StoreManager 入口）

> StoreMgr 是对外 [`StoreManager`](./store_manager.go) 的实现入口。  
> **唯一引擎：LSM + ValueLog（路径 C）**。  
> 主设计：[LSM_VALUELOG_DESIGN.md](./LSM_VALUELOG_DESIGN.md)；组件：[WAL_DESIGN.md](./WAL_DESIGN.md)、[SST_DESIGN.md](./SST_DESIGN.md)、[MANIFEST_DESIGN.md](./MANIFEST_DESIGN.md)、[fileio/DESIGN.md](../fileio/DESIGN.md)。

---

## 0. 需求（已确认）

1. 实现 `StoreManager` / `BatchAPI` 全套 API。  
2. **`OpenStoreManager(opts LSMOptions)`** 打开 LSM + ValueLog 引擎（无多引擎分派）。  
3. 写路径耐久顺序：ValueLog（如需）→ WAL → MemTable（见 LSM / WAL 设计）。  
4. 读路径：MemTable → Immutable → SST levels → `resolve(ValueRef)` → ValueLog 点读。

---

## 1. 总体架构

```text
                 OpenStoreManager(LSMOptions)
                              │
                              ▼
                        lsmStoreMgr
           ┌─────────┬────────┼────────┬──────────┐
           ▼         ▼        ▼        ▼          ▼
       MemTable     WAL   VersionSet  ValueLog   Compactor
           │                  SST         │
           │               (MANIFEST)     │
           └──────────────────────────────┘
                    fileio.Store (vlog/)
```

```go
func OpenStoreManager(opts LSMOptions) (StoreManager, error)
```

---

## 2. 组件

```go
type lsmStoreMgr struct {
    opts     LSMOptions
    vlog     fileio.Store
    wal      WAL
    mem      MemTable
    imms     []*MemTable
    versions VersionSet
    mu       sync.RWMutex
    closed   bool
}
```

细节协议以 [LSM_VALUELOG_DESIGN.md](./LSM_VALUELOG_DESIGN.md) 为准。

---

## 3. 写 / 读（摘要）

```text
Put:
  可选 vlog.Append → ValueRef
  wal.Append(ref) → mem.Put(ref)
  超限 → freeze + Flush → MANIFEST.LogAndApply

Delete:
  wal.Append(tombstone) → mem.Put(tombstone)

Get:
  MemTable → Imm → SST levels → resolve(ValueRef)
```

---

## 4. Open / Close

```text
Open:
  1. 初始化目录布局（vlog/ wal/ sst/ MANIFEST）
  2. fileio.Open(vlog) + OpenWAL + RecoverVersionSet
  3. ReplayWAL → MemTable
  4. 启动 flush / compaction 后台（可配）

Close:
  1. 停后台；可选 flush MemTable
  2. wal.Sync + vlog.Sync + Close
```

---

## 5. ValueLog Entry Payload

Put 写入 ValueLog 的 payload（Little-Endian），与 `entry.go` 一致：

```text
key_len(4) | value_len(4) | timestamp(8) | ttl(4) | key | value
```

Delete 默认 **不写** ValueLog，仅 WAL / MemTable / SST tombstone。

---

## 6. 错误

| 情况 | 行为 |
|------|------|
| WAL / vlog Append 失败 | 返回错误；MemTable 不发布该键 |
| 已 closed | `ErrStoreClosed` |
| 未实现阶段 | `ErrNotImplemented`（实现完成前） |
| ctx 取消 | 锁前/批间隙返回 `ctx.Err()` |

---

## 7. 建议代码布局

```text
internal/store/
  DESIGN_INDEX.md
  LSM_VALUELOG_DESIGN.md
  SST_DESIGN.md / MANIFEST_DESIGN.md / WAL_DESIGN.md
  STORE_MGR_DESIGN.md     # 本文
  store_manager.go        # 接口
  lsm_options.go          # LSMOptions + OpenStoreManager
  lsm_store_mgr.go        # 实现（待建）
  entry.go                # ValueLog Put payload
  mem_store.go            # 可复用为 MemTable 有序结构
```

---

## 8. 测试要点

1. Put/Get/Delete/Iterate + Persist/Reopen（WAL + SST）  
2. Flush 后砍 WAL 仍可 Get  
3. Compaction 后旧 SST 删除与读一致  
4. ValueLog GC 后 Location 更新正确  

---

## 9. 里程碑

| 阶段 | 内容 |
|------|------|
| **S0** | 文档定稿；移除 Bitcask / HintFile |
| **S1** | WAL + MemTable + ValueLog Get/Put |
| **S2** | Flush L0 + MANIFEST |
| **S3** | Compaction + Iterate 归并 |
| **S4** | ValueLog GC |

---

## 10. 总结

1. **唯一入口** `OpenStoreManager` → LSM + ValueLog。  
2. 不再提供 Bitcask / HintFile / `EngineMode` 分派。  
3. Value 大块在 `fileio.Store`；索引在 MemTable + SST。
