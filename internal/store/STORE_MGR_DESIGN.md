# DESIGN: StoreMgr（MemStore + DiskStore 协调层）

> StoreMgr 是对外的 [`StoreManager`](./store_manager.go) 实现。  
> 它协调 **MemStore**（`key → Location` 内存索引）与 **DiskStore/fileio**（持久化），保证写路径上两边一致；**磁盘更新失败时回滚内存索引**。

相关文档：[DISKSTORE_DESIGN.md](./DISKSTORE_DESIGN.md)、[fileio/DESIGN.md](../fileio/DESIGN.md)、[HINTFILE_DESIGN.md](../fileio/HINTFILE_DESIGN.md)。

---

## 0. 需求（已确认）

1. 实现 `StoreManager` / `BatchAPI` 全套 API。  
2. **设计原则**：`mem_store` 与 `disk_store` **同时更新**；若 **disk 更新失败**，必须 **revert mem_store** 上对应操作，使索引回到写之前的状态。  
3. 持久化语义、Entry 格式、Hint/Recovery 遵循 DiskStore 设计；本层只规定「谁先谁后、如何回滚」。

---

## 1. 背景与目标

| 组件 | 角色 |
|------|------|
| `MemStore` | 有序内存索引：`key → fileio.Location` |
| Disk / fileio | Append/Read `.seg`，Hint 恢复 |
| **StoreMgr** | 实现 `StoreManager`；编排双写与回滚 |

### 1.1 目标

- API 对齐 `StoreManager`
- 写成功 ⇒ mem 与 disk 对同一 key 指向一致的最新 Location（或 Delete 后均无该 key）
- **disk 失败 ⇒ mem 与写前一致**（不出现「索引已变、盘上无对应成功写入」的窗口被提交）
- 读路径：mem 查 Location → disk `Read` → decode `core.Record`

### 1.2 非目标

- 跨进程共享同一 MemStore
- 分布式事务 / 两阶段提交到外部系统
- disk 成功但进程在更新 mem 前崩溃的补偿（重启靠 Recovery 重建 mem，见 §6）

---

## 2. 总体架构

```text
                 StoreManager (StoreMgr)
                           │
           ┌───────────────┴───────────────┐
           ▼                               ▼
     MemStore                         Disk Engine
  key → Location                 fileio.Store + Hint
  (RBTree)                       entry codec / recover
```

```go
type storeMgr struct {
    opts    DiskStoreOptions
    mem     MemStore           // 内存索引
    store   fileio.Store       // 段文件
    hintMgr fileio.HintManager
    mu      sync.RWMutex
    closed  bool
}
```

对外构造：

```go
func OpenStoreManager(opts DiskStoreOptions) (StoreManager, error)
```

`OpenDiskStore` 可作为兼容别名，内部转到 `OpenStoreManager`。

---

## 3. 双写协议（核心）

因 `MemStore` 存的是 **Location**，合法 Location **只能在 disk Append 成功后**获得，故采用：

```text
Disk-first, then Mem；Mem 提交失败则 Revert Mem
```

### 3.1 Put

```text
Put(key, record):
  1. 校验；规范化 Record（Timestamp 等）
  2. snapshot:
       oldLoc, hadOld = mem.Get(key)   // hadOld=false 表示原先不存在
  3. payload = encodePutPayload(...)
  4. loc = disk.Append*(payload, RecordPut)
       若失败 → return err          // mem 未改，无需 revert
  5. err = mem.Put(key, loc)
       若失败 → revertMemPut(key, oldLoc, hadOld); return err
  6. seal/hint 副作用（失败只打日志，不回滚已提交的 mem/disk）
  7. return nil
```

`revertMemPut`：

```text
if hadOld: mem.Put(key, oldLoc)
else:      mem.Delete(key)
```

### 3.2 Delete

```text
Delete(key):
  1. snapshot: oldLoc, hadOld = mem.Get(key)
  2. 若不存在且 !DeleteWritesTombstoneIfMissing → return nil
  3. disk.Append*(encodeDeletePayload(key), RecordDelete)
       若失败 → return err          // mem 未改
  4. mem.Delete(key)
       // Delete 对 MemStore 几乎不失败；若扩展实现失败：
       //   revert: mem.Put(key, oldLoc) when hadOld
  5. seal/hint 副作用
  6. return nil
```

### 3.3 BatchPut / BatchDelete

- 同一把写锁下按条执行与单条相同的 disk→mem 协议。  
- **中途失败**：已成功条目保持提交（append-only 无法回滚 disk）；**当前失败条目**若已 Append 成功但 mem 失败，对该 key 做 revert。  
- 批末按 SyncMode 调用 `store.Sync()`（与 DiskStore 设计一致）。

### 3.4 不变量

```text
写路径返回成功 ⇒ mem[key] 与 disk 上该 key 最新可见记录一致
写路径因 disk 错误返回失败 ⇒ mem[key] 与调用前一致
写路径因 mem 错误返回失败 ⇒ mem[key] 与调用前一致（已 revert）；disk 可能多一条孤儿 record（可接受，Recovery/compaction 可忽略）
```

---

## 4. 读路径

| API | 行为 |
|-----|------|
| Get | mem.Get → disk.Read → decode；TTL 过滤；未命中 `ErrKeyNotFound` |
| Iterate | 拷贝 mem 有序 (key,loc) → 逐条 Read/decode/回调 |
| BatchGet | 对每个 key 调 Get；缺失 `Value=nil` |

读不修改 mem（惰性删过期索引为首版可选，默认不改索引）。

---

## 5. 与 DiskStore 设计的关系

| 能力 | 归属 |
|------|------|
| Entry 编解码、TTL、SyncMode、Hint、Recovery | 同 [DISKSTORE_DESIGN.md](./DISKSTORE_DESIGN.md) |
| 双写顺序与 mem 回滚 | **本设计（StoreMgr）强制** |
| `MemStore` 数据结构 | [mem_store.go](./mem_store.go) |

StoreMgr 内部复用 DiskStore 的引擎逻辑（fileio / hint / recover / entry），对外只暴露一个 `StoreManager`。

---

## 6. 启动与关闭

### Open

```text
OpenStoreManager(opts):
  1. fileio.Open + NewHintManager + NewMemStore
  2. Recovery（hint 优先，active Iterate）写入 mem
  3. return StoreMgr
```

Recovery 直接构建 mem，不走 Put 双写（盘上已是真相）。

### Close

```text
Sync → SealIfNeeded（若 rotate 则 BuildHint）→ Close fileio → 标记 closed
```

---

## 7. 错误

沿用 store / fileio 错误；另：

| 情况 | 行为 |
|------|------|
| disk Append 失败 | 返回原错误；mem 不变 |
| mem.Put 失败 | revert mem；返回该错误 |
| ctx 取消 | 锁前/批间隙检查；返回 `ctx.Err()` |

---

## 8. 建议代码布局

```text
internal/store/
  STORE_MGR_DESIGN.md   # 本文档
  store_manager.go      # 接口
  store_mgr.go          # StoreMgr + OpenStoreManager
  disk_store.go         # 引擎：recover / append / hint / options
  mem_store.go
  entry.go
  store_mgr_test.go
```

---

## 9. 测试要点

1. Put/Get/Delete/Iterate/Batch 与 DiskStore 语义一致  
2. **Revert**：注入会失败的 `MemStore.Put`，断言 disk Append 后 mem 仍为旧 Location（或仍不存在）  
3. disk Append 失败时 mem 完全不变  
4. Persist + Reopen 后与关闭前可见集合一致  
5. seal 后生成 `.hint`，重启可恢复  

---

## 10. 里程碑

1. **S1**：本文档定稿  
2. **S2**：`OpenStoreManager` + 写路径 disk→mem + revert  
3. **S3**：与现有 disk_store 测试对齐；增加 revert 单测  
4. **S4**：`OpenDiskStore` 委托 `OpenStoreManager`（兼容）  

---

## 11. 总结

1. StoreMgr 是唯一对外的 `StoreManager` 实现入口。  
2. **Disk Append 成功后再提交 MemStore**；Mem 提交失败则 **revert 到 snapshot**。  
3. Disk 失败时 Mem 从未修改，自然满足「失败不脏读索引」。  
4. 崩溃恢复不依赖双写原子性，而依赖 `.seg` + `.hint` 重建 MemStore。
