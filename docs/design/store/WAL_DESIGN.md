# DESIGN: WAL（Write-Ahead Log）

> 保护 **MemTable** 在 flush 到 SST 之前的崩溃安全。  
> ValueLog（`fileio.Store`）保护的是 **value 字节**；WAL 保护的是 **索引侧更新**（key → ValueRef / tombstone）。  
> 总览见 [LSM_VALUELOG_DESIGN.md](./LSM_VALUELOG_DESIGN.md)。

---

## 0. 目标与非目标

### 0.1 目标

| 目标 | 说明 |
|------|------|
| 耐久 | 按 SyncMode，在 Put/Delete 返回成功前 WAL 达到约定耐久点 |
| 可重放 | 启动时按序 replay 到 MemTable |
| 可回收 | SST flush 并 MANIFEST Apply 后，截断或删除已持久化前缀 |
| 实现简单 | 首版可用独立长度前缀日志；或 **复用 fileio.Store 作为 WAL 后端**（推荐评估） |

### 0.2 非目标

- 替代 ValueLog（大 value 仍进 `.seg`）
- 替代 MANIFEST
- 多写者并行写同一 WAL（单写者）
- HintFile / 全量内存索引恢复

---

## 1. 两种实现选型

| 方案 | 做法 | 优点 | 缺点 |
|------|------|------|------|
| **A. 复用 fileio.Store** | `wal/` 目录单独一个 Store；record type=`RecordMeta` 或专用 `RecordWAL` | 复用 CRC/seal/恢复扫描 | WAL 与 ValueLog 段格式相同，需严格分子目录 |
| **B. 专用 WAL 文件** | `wal/<id>.log` 简单 append | 格式更瘦 | 再实现一套 sync/恢复 |

**推荐首版：方案 A**——`fileio.Options{Dir: data/wal}`，与 `data/vlog` 分离；语义清晰。

若采用方案 A，须扩展或约定 `RecordType`：

```go
// fileio 可新增（或上层只用 RecordMeta + 自描述 payload）
RecordWAL RecordType = ... // 可选
```

payload 由本设计定义，fileio 不解析。

---

## 2. 目录

```text
<data_dir>/wal/
  0000000001.seg    # 若用 fileio
  0000000002.seg
```

或：

```text
<data_dir>/wal/
  000001.log
  000002.log
```

`Version.LogNumber` / `MinLogNumber`：小于等于已 flush 序号的 WAL 段可删。

---

## 3. 记录格式（payload）

```text
┌──────────── WAL Record Payload ─────────────┐
│ op(1)          // 1=Put, 2=Delete           │
│ sequence(8)    // 全局单调                   │
│ key_len(4)                                  │
│ key(key_len)                                │
│ value_ref ...  // 同 SST ValueRef 编码       │
└─────────────────────────────────────────────┘
```

Delete：`op=2`，`ValueRef.Kind=Tombstone`（可不带 inline/loc）。

Put 且 value 在 ValueLog：

```text
写顺序：
  1) valueLog.Append → loc
  2) wal.Append(Put, key, ValueRef{Location: loc, ...})
  3) mem.Put
```

Put 且 Inline：

```text
  1) wal.Append(Put, key, ValueRef{Inline: ...})
  2) mem.Put
  // 无 ValueLog 写
```

---

## 4. Sync 策略

与 fileio `SyncMode` 对齐：

| 模式 | WAL 行为 |
|------|----------|
| `NoSync` | 不主动 fsync（仅测试/可丢数据场景） |
| `BatchSync` | 按字节/时间 group commit（默认） |
| `EveryWrite` | 每条 `AppendSync` |

`StoreManager.Put` 成功返回 ⇒ 在该模式下 WAL（及若有的 ValueLog）已达对应耐久语义。

---

## 5. 写路径集成

见 [LSM_VALUELOG_DESIGN §4](./LSM_VALUELOG_DESIGN.md)。摘要：

```text
禁止：MemTable 对调用方可见，但对应 WAL 记录未按 SyncMode 耐久
允许：ValueLog 多孤儿；WAL 未写成功则 Put 返回错误且 Mem 不变
```

---

## 6. Replay

```text
ReplayWAL(wal, mem):
  按 FileID / 记录序 Iterate
  对每条：
    decode → mem.Put(key, ref) 或 tombstone
  更新 LastSequence
```

注意：

- 只 replay **尚未被 SST 覆盖** 的 WAL（`log_number > Version.LogNumber`）
- 重复 replay 与 MemTable 语义一致（后者覆盖前者）

---

## 7. 回收

```text
Flush 完成且 MANIFEST.LogAndApply 成功后：
  Version.LogNumber = 本 flush 对应的 wal 序号
  删除 / 回收所有 fileID 或 log 序号 ≤ LogNumber 的 WAL 段
```

进行中的 Immutable flush 所覆盖的写入必须已全部进入该 SST，才能推进 LogNumber。

---

## 8. API（建议）

```go
type WAL interface {
    Append(op WALOp, key []byte, ref ValueRef, seq uint64) error
    AppendSync(op WALOp, key []byte, ref ValueRef, seq uint64) error
    Sync() error
    Iterate(fn func(op WALOp, key []byte, ref ValueRef, seq uint64) error) error
    Close() error
}

func OpenWAL(opts WALOptions) (WAL, error)
```

---

## 9. 与「数据即日志」模型的差异

| | 数据段即主日志（已废弃） | 本 WAL |
|--|--------------------------|--------|
| 恢复材料 | 扫 `.seg` / Hint | WAL + SST + MANIFEST |
| 全量索引 | 重启建满内存地图 | 只建 MemTable（未 flush 部分） |
| 日志内容 | key+value 同段 | 索引侧变更；大 value 在 ValueLog |

---

## 10. 测试要点

1. Put Inline 后杀进程，replay 后 Get 成功  
2. Put Location：ValueLog+WAL 都写后杀进程，恢复正确  
3. ValueLog 成功、WAL 失败：重启无该 key；ValueLog 孤儿可 GC  
4. Flush 后删 WAL，重启仅靠 SST  
5. BatchSync 下崩溃：只丢未 sync 尾部

---

## 11. 里程碑

1. **W1**：方案 A + Put/Delete 记录 + Replay  
2. **W2**：与 Flush / LogNumber 回收对接  
3. **W3**：group commit 延迟打满指标

---

## 12. 总结

WAL 是 MemTable 的耐久影子；ValueLog 是大 value 的耐久载体。二者目录分离、职责分离；回收点由 MANIFEST 的 `LogNumber` 驱动。
