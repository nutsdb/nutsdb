# DESIGN: MANIFEST / VersionSet

> 记录 LSM 当前有效 SST 集合与层级，保证 flush / compaction 的 **原子可见性**。  
> 总览见 [LSM_VALUELOG_DESIGN.md](./LSM_VALUELOG_DESIGN.md)；SST 格式见 [SST_DESIGN.md](./SST_DESIGN.md)。

---

## 0. 目标与非目标

### 0.1 目标

| 目标 | 说明 |
|------|------|
| 原子切换 | 读者看到的 Version 要么是 Apply 前，要么是 Apply 后，无中间态 |
| 可恢复 | 崩溃后能重建最新 Version |
| 并发读 | Get/Iterate 持有 Version 不可变快照（引用计数），与 compaction 并存 |
| 简洁 | 首版可用「追加日志式 MANIFEST」；不必上 RocksDB 全套复杂度 |

### 0.2 非目标

- 跨目录分布式共识
- 在 MANIFEST 中存储 ValueLog 段列表（ValueLog 由 `fileio` 目录扫描恢复；GC 删段另议）
- 替代 WAL（MemTable 恢复仍靠 WAL）

---

## 1. 核心概念

```go
type FileMeta struct {
    FileNumber uint64
    Level      int
    Size       uint64
    Smallest   []byte
    Largest    []byte
}

type Version struct {
    Files      [][]FileMeta // Files[level]
    // 可选：sequence / log number
    LastSequence   uint64
    LogNumber      uint64 // 可回收的 WAL 下界相关
    NextFileNumber uint64
}

type VersionSet struct {
    mu          sync.Mutex
    current     *Version
    // 历史 Version 通过引用计数在迭代器释放后回收
}
```

**不变量**：

```text
L1…Ln：同层 FileMeta 按 Smallest 排序，且 key range 不重叠
L0：允许重叠
MANIFEST 中列出的每个 FileNumber 必须对应完整可读的 .sst
```

---

## 2. 目录与 CURRENT

```text
<data_dir>/
  CURRENT                 # 文本一行：MANIFEST-000123
  MANIFEST-000123         # 当前日志
  MANIFEST-000100         # 旧文件，compact 后可删
  sst/...
```

`CURRENT` 内容示例：

```text
MANIFEST-000123\n
```

更新 `CURRENT`：写 `CURRENT.tmp` → fsync → rename →（可选）目录 fsync。

---

## 3. MANIFEST 记录格式

MANIFEST 为 **只追加** 的记录流（可用简单长度前缀，不必用 fileio segment）。

### 3.1 记录头

```text
crc32c(4)
payload_len(u32)
type(u8)
payload(payload_len)
```

### 3.2 记录类型

| type | 名称 | 含义 |
|------|------|------|
| 1 | `VersionEdit` | 一次 Apply 的增量（主类型） |
| 2 | `NewFile` | （可内嵌进 VersionEdit） |
| 3 | `CompactPointer` | 可选，记录各 level 压缩进度 |
| 4 | `Snapshot` | 可选：全量 Version 快照，便于截断旧日志 |

### 3.3 VersionEdit payload（逻辑字段）

```text
next_file_number(u64)
last_sequence(u64)
log_number(u64)                 // 可选
deleted_files: repeated (level:u32, file_number:u64)
added_files: repeated FileMeta编码
```

`FileMeta` 编码：

```text
level(u32)
file_number(u64)
size(u64)
smallest_len(u32) | smallest
largest_len(u32) | largest
```

一次 flush 或 compaction = **一条** `VersionEdit`（可含多个 add/delete）。

---

## 4. LogAndApply 流程

```text
LogAndApply(edit VersionEdit):
  1. 持 VersionSet 锁
  2. 在 current 上模拟 apply，得到 next Version；校验不变量（L1+ 不重叠等）
  3. 编码 VersionEdit，追加到 MANIFEST，fsync
  4. current = next；更新引用计数
  5. 释放锁
  6. 异步删除 edit.deleted_files 对应 .sst（须确认无旧 Version / Iterator 引用）
```

读者：

```text
GetVersion() (*Version, guard)
  // guard.Release() 减少引用；引用为 0 且非 current 时可删物理文件
```

---

## 5. 启动加载

```text
RecoverVersionSet(dir):
  1. 读 CURRENT → manifestPath
  2. 顺序扫描 MANIFEST 记录，从空 Version 起 apply 每条 VersionEdit
     （若存在 Snapshot 记录，可从最近 Snapshot 起跳）
  3. 校验列出的每个 SST 文件存在且 Footer 合法（可懒校验）
  4. current = 最终 Version
  5. NextFileNumber = max(edit) + 1
```

损坏策略：

- 单条 CRC 失败：截断至上一条完整记录（类似 WAL），**丢弃尾部脏写**
- `CURRENT` 丢失：尝试选最大编号 MANIFEST（需文档约定；首版可要求 CURRENT 必须存在）

---

## 6. MANIFEST 压缩（Rewrite）

当 MANIFEST 过大：

```text
1. 将 current Version 写成 Snapshot 记录到新 MANIFEST-<seq>
2. fsync
3. 更新 CURRENT 指向新文件
4. 删除旧 MANIFEST
```

与 RocksDB `WriteSnapshot` / manifest rewrite 同思路。

---

## 7. 与 WAL / ValueLog 的边界

| 状态 | 归属 |
|------|------|
| 哪些 SST 可读 | **MANIFEST** |
| MemTable 未 flush 内容 | **WAL** |
| value 字节 | **ValueLog `.seg`** |
| 哪些 `.seg` 仍被引用 | 由 Version+Mem 推导；GC 时计算，**不必**每段写进 MANIFEST（首版） |

可选增强：在 VersionEdit 中增加 `obsolete_vlog_file_ids`，与 GC 原子绑定——L5 再做。

---

## 8. API（建议）

```go
type VersionEdit struct {
    Added   []FileMeta
    Deleted []struct{ Level int; FileNumber uint64 }
    // NextFileNumber, LastSequence, LogNumber ...
}

type VersionSet interface {
    Current() *Version
    LogAndApply(edit *VersionEdit) error
    NewFileNumber() uint64
    Close() error
}

func RecoverVersionSet(dir string) (*versionSet, error)
```

---

## 9. 测试要点

1. Flush：Add L0 → Apply → 崩溃 → 重启仍见该 SST  
2. Compaction：Add 新文件 + Delete 旧文件 同 edit；崩溃在 fsync 前 → 仍见旧 Version  
3. Iterator 持旧 Version 时，compaction 删文件不得真删直至 Release  
4. MANIFEST 尾部损坏截断恢复  
5. Rewrite 后 CURRENT 切换，旧文件可删

---

## 10. 里程碑

1. **M1**：仅 VersionEdit 追加 + CURRENT；无 Snapshot rewrite  
2. **M2**：引用计数删 SST  
3. **M3**：MANIFEST rewrite  
4. **M4**：与 compaction 调度器对接

---

## 11. 总结

MANIFEST 是 LSM 的 **版本真相源**：用追加 `VersionEdit` + fsync 实现原子切换；读者通过不可变 `Version` 快照与写/压缩并发。ValueLog 段生命周期不由此文档首版强制登记，由 GC 扫描引用决定。
