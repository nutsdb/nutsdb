# DESIGN: KV 存储交互层（Storage I/O Layer）

> 本文档描述一套面向 KV 存储系统的**磁盘交互层**设计方案。  
> 目标优先保证：**顺序写吞吐**与**随机读延迟**。  
>
> **引擎定位**：在 [LSM + ValueLog](../store/LSM_VALUELOG_DESIGN.md) 中，本层的 `Store`（`.seg`）担任 **ValueLog**（及可选 **WAL 后端**）；  
> **不**在本层实现 MemTable / SST / Compaction / HintFile。  
> 相关：[LSM_VALUELOG_DESIGN.md](../store/LSM_VALUELOG_DESIGN.md)。

---

## 0. 寻址模型约定（全文前提）

上层索引与交互层之间只传递稳定地址：

```go
type Location struct {
    FileID uint32
    Offset uint64 // 段内绝对字节偏移（指向 record 起始）
    Length uint32 // 整条 record 编码长度（含 header，单位：字节）
}
```

### 0.1 字段位宽

| 字段 | 类型 | 理由 |
|------|------|------|
| `FileID` | `uint32` | 段数量通常远小于 2^32 |
| `Offset` | `uint64` | 与 `pread`/`pwrite` 偏移语义对齐；单段可 > 4GiB |
| `Length` | `uint32` | 与盘上 `payload_len` 同宽；单条 record 受 `MaxRecordSize` 约束，无需 uint64 |

**盘上 payload 长度**使用 `uint32`（见 §3.3），并强制：

```text
MaxRecordSize <= min(4GiB - 1, SegmentSize - header_size - footer_size)  // 且 MaxRecordSize 可放入 uint32
Location.Length = record_header_size + payload_len                       // uint32
```

即：**`Offset` 用 uint64 对齐文件偏移；`Length` / 盘上 `payload_len` 用 uint32，并有明确上限**。  
内存索引若需更紧凑，可在上层对 `Offset/Length` 做变长编码或按 `SegmentSize` 收窄存储，但不改变本层契约。

### 0.2 派生关系

```text
record_end = Location.Offset + uint64(Location.Length)
```

### 0.3 可见性（必须遵守）

| 场景 | 可读上界 | 说明 |
|------|----------|------|
| 同进程，`Append` 已返回 | `written_offset` | 已进入 kernel/page cache 的前缀 |
| 同进程，`AppendSync` 已返回 | `durable_offset` | 该条及之前已 fsync |
| 进程崩溃并恢复后 | `durable_offset` | 未 Sync 的 Location **不得**假定仍可读 |
| 跨进程共享数据目录 | 仅 `durable_offset` 前可读 | 未定义对未 sync 段的跨进程读 |

不变量：

```text
header_size <= durable_offset <= written_offset <= used_bytes <= capacity - footer_size
```

空段初始值：

```text
used_bytes = written_offset = durable_offset = header_size
```

---

## 1. 背景与目标

KV 引擎（WiscKey / **LSM-ValueLog**）的典型 I/O 形态是：

- **写路径**：几乎只有 append（顺序写）
- **读路径**：根据上层持有的 `Location` 做点查（随机读）
- **删改**：逻辑删除 / 新版本追加；物理回收由上层 GC / compaction 负责

因此交互层应做成**为上述访问模式特化的段式存储引擎**，而不是通用 POSIX 封装。

### 1.0 在 LSM + ValueLog 中的角色

| 角色 | 目录建议 | 说明 |
|------|----------|------|
| **ValueLog（主）** | `<data_dir>/vlog/` | Put 的大 value 经 `Append` 落盘；LSM 的 `ValueRef` 持有返回的 `Location` |
| **WAL 后端（可选）** | `<data_dir>/wal/` | 另一 `Store` 实例，payload 为 WAL 记录；见 [WAL_DESIGN.md](../store/WAL_DESIGN.md) |

**不变量（对本层的约束）**：

```text
Location 一经 Append 返回且段未 Delete，地址稳定可读（sealed 后尤其如此）
本层不解释 payload 内的 key；不维护 key → Location
ValueLog GC / 删段由上层在确认无引用后调用 DeleteSegment
```

### 1.1 目标

| 目标 | 说明 |
|------|------|
| 顺序写高效 | 单 active segment 追加；用户态聚合缓冲；可控 fsync |
| 随机读高效 | 按 `Offset`/`Length` 一次 I/O 读完；fd/mmap 缓存 |
| 地址稳定 | sealed 段只读，`Location` 长期有效 |
| 崩溃可恢复 | 以 `durable_offset` 为准截断脏尾 |
| 职责清晰 | 只做字节持久化与定位；不做 key 索引 / 事务 / TTL / SST |

### 1.2 非目标

- 不提供文件内随机写 / 原地更新
- 不实现 SQL / 二级索引 / 事务协议
- 不在本层做跨文件 GC 策略（只提供扫描与删段）
- 不保证跨进程并发写同一 active segment
- 不实现 LSM 的 MemTable / SST / MANIFEST / HintFile（见 `internal/store`）

---

## 2. 总体架构

```text
┌──────────────────────────────────────────────────────────┐
│  Upper: LSM StoreManager                                 │
│  MemTable / SST / MANIFEST / WAL / ValueLog GC           │
└────────────────────────────┬─────────────────────────────┘
                             │ Store.Append / Store.Read (Location)
┌────────────────────────────▼─────────────────────────────┐
│              Store (门面) = ValueLog 或 WAL 后端           │
│         ┌──────────┴──────────┐                          │
│         ▼                     ▼                          │
│   SegmentManager          Appender/Reader                │
│   (生命周期/FD LRU)        (顺序写/随机读)                  │
│         │                     │                          │
│         └──────────┬──────────┘                          │
│                    ▼                                     │
│            Segment Backend                               │
│         FileIO / MMap(Sealed)                            │
└────────────────────┬─────────────────────────────────────┘
                     ▼
                  Disk / FS
```

调用关系：上层只依赖 `Store`；`Store` 内部组合 `SegmentManager` + `Appender` + `Reader`。  
同一进程可打开 **两个** `Store` 实例（`vlog/` 与 `wal/`），彼此 FileID 空间独立。

| 对象 | 职责 |
|------|------|
| `Store` | 对外唯一入口 |
| `Segment` | 固定容量文件；`capacity`/`used_bytes` 为 uint64 |
| `ActiveSegment` | 唯一可写段 |
| `SealedSegment` | 只读段 |
| `SegmentManager` | fileID 分配、seal/rotate、fd LRU、删除 |
| `Appender` | 编码、写缓冲、Sync、返回 Location |
| `Reader` | 按 Location 点查；按 fileID 顺序迭代 |

---

## 3. 磁盘布局与格式常数

### 3.1 常数（实现必须一致）

| 名称 | 值 | 说明 |
|------|----|------|
| 字节序 | **Little-Endian** | 所有多字节整数 |
| `header_size` | **4096** | Segment header 区大小（含 padding） |
| `footer_size` | **4096** | Segment footer 区大小（含 padding） |
| `record_header_size` | **12** | `crc32(4) + payload_len(4) + type(1) + reserved(3)` |
| CRC | **CRC-32C (Castagnoli)** | 硬件加速友好 |
| 默认 `SegmentSize` | `256 << 20` | 256MiB；可配，建议 ≤ 1GiB |
| 默认 `WriteBufferSize` | `1 << 20` | 1MiB |
| `MaxRecordSize` | `min(4<<20, usable)` | 默认 4MiB；可配，且必须 ≤ 可用区 |

可用区：

```text
usable = SegmentSize - header_size - footer_size
```

### 3.2 Segment 文件

```text
<data_dir>/<fileID>.seg
```

`fileID` 在文件名中为 **10 位十进制、前导零补齐**（与 `uint32` 十进制最大宽度一致），例如：

```text
<data_dir>/0000000001.seg
<data_dir>/0000000042.seg
```

可选分片目录（启用 shard 时）：

```text
<data_dir>/shard-<id>/<fileID>.seg
```

示例：`data/shard-0/0000000001.seg`。

`FileID` **全局唯一**（跨 shard 不复用进行中的 ID；obsolete 删除后可复用，见 §6.2）。
内存与 API 中仍使用 `uint32` 数值；仅磁盘文件名使用零填充形式，便于目录排序与对齐。

预分配：优先 `fallocate(capacity)` 硬分配；不支持时降级 `Truncate(capacity)`，并在文档/日志中标记可能稀疏。

布局：

```text
[0, header_size)                         Segment Header
[header_size, capacity - footer_size)    Record Stream + free space
[capacity - footer_size, capacity)       Segment Footer（seal 后有效）
```

**Footer 固定写在文件末尾窗口** `[capacity - footer_size, capacity)`，不紧跟 `used_bytes`。  
这样 seal 不必移动数据；未 seal 时 footer 区视为 reserved，不参与追加。

#### Header（前缀有效字段 + zero padding 到 4096）

```text
magic(4) = 0x4E444247          // "NDBG" 示例
version(2)
flags(2)
capacity(8, uint64)
created_at(8, unix nanos)
header_crc(4)                  // 覆盖 magic..created_at
padding...
```

#### Footer（seal 时写入；前缀有效字段 + padding 到 4096）

```text
seal_magic(4)
record_count(8, uint64)
used_bytes(8, uint64)          // 必须满足 header_size <= used_bytes <= capacity - footer_size
footer_crc(4)                  // 覆盖 seal_magic..used_bytes
padding...
```

### 3.3 Record 编码

```text
┌────────────── Record ─────────────────────────────┐
│ crc32c(4)                                         │
│ payload_len(4, uint32)                            │
│ type(1)                                           │
│ reserved(3)                                       │
│ payload(payload_len)                              │
└───────────────────────────────────────────────────┘

record_header_size = 12
Location.Offset    = record 起始偏移
Location.Length    = 12 + payload_len   // uint32
```

CRC 覆盖：`payload_len | type | reserved | payload`（**不含** crc 字段自身）。

校验规则：

- `payload_len == 0` 合法（例如纯 delete meta）
- `Location.Length == 0` **非法**
- `Read` 时必须验证 `Location.Length == 12 + payload_len`
- `payload_len > MaxRecordSize` → `ErrRecordTooLarge`

本层 CRC 保证 **I/O 完整性**；上层 entry 可另有业务校验，允许并存。

---

## 4. 写路径（顺序写优化）

### 4.1 Append 流程

```text
Append(payload, type)
  1. n = len(payload); length = 12 + n
  2. if length > MaxRecordSize || length > usable: return ErrRecordTooLarge
  3. if used_bytes + length > capacity - footer_size:
        SealActive(); create new Active; retry once
  4. offset = used_bytes
  5. encode record
  6. if length > WriteBufferSize:
        flush buffer (if any); pwrite record directly  // 旁路缓冲
     else:
        append to write buffer; flush if full
  7. used_bytes += length
  8. 若本次已 pwrite：written_offset = used_bytes（或推进到已写出位置）
  9. return Location{FileID, offset, length}
```

### 4.2 写缓冲

- 默认聚合多条小 record，满 `WriteBufferSize` 或 `Sync` 时 `pwrite`
- **大 record（`length > WriteBufferSize`）旁路 buffer**，连续写完，避免强制放大缓冲
- 可选 4KiB 对齐填充（默认关闭；若开启需计入 Length/CRC 策略并单独规定，首版不做 padding）

### 4.3 写并发

- Active 段 **单写者**（mutex 或单队列）
- 上层可 group commit：多个逻辑写共用一次 `Sync`

```go
Append(payload []byte, typ RecordType) (Location, error)       // 不等待 durable
AppendSync(payload []byte, typ RecordType) (Location, error)   // Append + Sync
Sync() error                                                   // durable_offset = written_offset
```

### 4.4 Sync 策略

| 模式 | 行为 |
|------|------|
| `NoSync` | 不主动 fsync |
| `BatchSync` | 按时间 / 按写入字节阈值 fsync（默认） |
| `EveryWrite` | 每次 `AppendSync` 都 fsync |

优先 `fdatasync`（若平台支持）。

---

## 5. 读路径（随机读优化）

### 5.1 点查

```go
Read(loc Location) (payload []byte, typ RecordType, err error)
ReadInto(loc Location, buf []byte) (payload []byte, typ RecordType, err error)
```

- `Read`：始终拷贝，调用方拥有返回切片  
- `ReadInto`：优先写入 `buf`，足够则零分配；不足则分配

步骤：

1. 取 segment handle（fd LRU）
2. 边界检查：`Length > 0`、无溢出、`Offset >= header_size`、`Offset+uint64(Length) <= written_offset`（恢复后段用 `used_bytes`）
3. 一次 `pread` 读 `Length` 字节
4. 校验 CRC / `payload_len` / Length 一致性
5. 返回 payload

失败错误：

| 错误 | 含义 |
|------|------|
| `ErrInvalidLocation` | 字段非法或越界 |
| `ErrCorrupt` | CRC/长度不一致 |
| `ErrSegmentNotFound` | fileID 不存在 |
| `ErrStaleLocation` | 指向已删除/obsolete 段 |

### 5.2 扫描（compaction / 恢复辅助）

```go
Iterate(fileID uint32, fn func(loc Location, typ RecordType, payload []byte) error) error
```

顺序扫 sealed/active 前缀，后端可启用 readahead；与点查路径分离。

### 5.3 Backend

| Backend | Active 写 | Sealed 读 |
|---------|-----------|-----------|
| FileIO | 默认 | 默认 |
| MMap | 禁止 | 可选热点 sealed |

### 5.4 FD LRU

- `MaxOpenSegments` 默认 256
- 按最近访问淘汰；mmap 段淘汰前 `munmap`
- 目标：热 `FileID` 命中，避免 open/close 成为随机读瓶颈

---

## 6. Segment 生命周期

```text
create(preallocate)
  -> active(append)
  -> seal(sync + write footer at tail)
  -> immutable Read/Iterate
  -> obsolete -> Delete
```

| 状态 | 写 | 读 |
|------|----|-----|
| Active | Append | `Offset+Length <= written_offset` |
| Sealed | 否 | 任意完整 Location |
| Obsolete | 否 | 否 |

### 6.1 Seal

1. flush write buffer  
2. `Sync`（`durable_offset = written_offset = used_bytes`）  
3. 在 `capacity - footer_size` 写入 footer  
4. 标记 sealed，切换新 active  

### 6.2 FileID 分配

- 启动时扫描现有 `max(FileID)`，新段用 `max+1`
- 磁盘文件名固定为 `%010d`（10 位前导零）；解析时要求恰好 10 位数字
- `uint32` 耗尽前应报警；允许复用 **已 Delete 的 obsolete** ID（不得复用仍被索引引用的 ID）
- 删除段文件前，上层须保证无引用（compaction 完成）

---

## 7. 崩溃恢复

1. 扫描目录，解析 header（magic/version/crc/`capacity`）
2. 若 footer `seal_magic`+crc 合法：**信任** `footer.used_bytes`，段为 sealed
3. 否则视为未 seal（或 seal 中崩溃）：
   - 从 `offset = header_size` 扫描 record
   - `next = offset + 12 + payload_len`
   - 越界 / CRC 失败 / `payload_len` 非法则停止
   - `used_bytes =` 最后一条完整 record 末尾
   - `written_offset = durable_offset = used_bytes`
4. 选择未 seal 的最大 FileID 作为 active；若无则新建
5. 不完整脏尾保留在 free space 即可（逻辑上不可见）；可选用 zero-fill 擦除（非必须）

---

## 8. 效率决策摘要

**顺序写快**：单 active、批量 syscall、预分配、group commit。  
**随机读快**：`Location` 一跳 `pread`、fd LRU、sealed 不可变。  

放弃：文件内随机写；本层 key 映射；无上限的盘上 uint64 `payload_len`（改为 uint32 + `MaxRecordSize`）。

---

## 9. 分片并行写（可选）

```text
shard = hash(key) % N
each shard: ActiveSegment + Appender
Location 不含 ShardID（FileID 全局唯一即可）
目录: data/shard-<n>/<fileID>.seg   // fileID 为 10 位前导零，如 0000000001.seg
```

恢复与 compaction 按 shard 独立；跨 shard 无共享 active。

---

## 10. 对外 API

```go
package fileio

type Location struct {
    FileID uint32
    Offset uint64
    Length uint32
}

type RecordType uint8

const (
    RecordPut RecordType = iota + 1
    RecordDelete
    RecordMeta
)

type SyncMode int
type ReadBackend int

type Store interface {
    Append(payload []byte, typ RecordType) (Location, error)
    AppendSync(payload []byte, typ RecordType) (Location, error)
    Sync() error

    Read(loc Location) (payload []byte, typ RecordType, err error)
    ReadInto(loc Location, buf []byte) (payload []byte, typ RecordType, err error)
    Iterate(fileID uint32, fn func(loc Location, typ RecordType, payload []byte) error) error

    SealIfNeeded() error
    DeleteSegment(fileID uint32) error
    Close() error
}

type Options struct {
    Dir             string
    SegmentSize     uint64 // default 256MiB
    WriteBufferSize int    // default 1MiB
    MaxRecordSize   uint64 // default 4MiB
    SyncMode        SyncMode
    MaxOpenSegments int
    ReadBackend     ReadBackend
}
```

错误变量（示例）：`ErrRecordTooLarge`、`ErrInvalidLocation`、`ErrCorrupt`、`ErrSegmentNotFound`、`ErrStaleLocation`、`ErrSegmentFull`（内部 rotate 用，对上可隐藏为自动 seal）。

建议指标：`append_bytes`、`fsync_latency`、`read_iops`、`fd_cache_hit`、`recovery_scan_bytes`。

上层（**LSM + ValueLog**）：

```text
Put(大 value):
  loc = vlog.Append(encode(entry))
  wal.Append(key → ValueRef{Location: loc})
  memTable.Put(key, ref)
  // flush/compaction 后 key→ref 进入 SST；Get 时 SST/Mem 取 ref 再 vlog.Read(loc)

Put(小 value / Inline):
  wal.Append(key → ValueRef{Inline: value})  // 可不写 vlog
  memTable.Put(...)
```

详见 [LSM_VALUELOG_DESIGN.md](../store/LSM_VALUELOG_DESIGN.md)。

---

## 11. 与长度前缀流式布局对比

| 方案 | 顺序写 | 随机读 |
|------|--------|--------|
| 仅 `ContentSize+Content`，索引无 Length | 好 | 易两跳 I/O |
| **本方案：Append-only + 12B header + Location(Length)** | 好 | **一跳 I/O** |

---

## 12. 实施里程碑

1. **M1**：FileIO + Append/写缓冲/大 record 旁路 + `Read`/`ReadInto`  
2. **M2**：4096 header/footer、seal/rotate、启动恢复  
3. **M3**：fd LRU + BatchSync/group commit  
4. **M4**：Sealed mmap（可选）+ `Iterate`  
5. **M5**：删段接口 + 分片目录（可选）  

---

## 13. 总结

1. **契约**：`Location{FileID, Offset:u64, Length:u32}`；盘上单条 `payload_len` 为 u32 且受 `MaxRecordSize` 约束；文件名中 `FileID` 为 10 位前导零。  
2. **布局**：`header_size=footer_size=4096`；footer 固定在文件尾；LE + CRC-32C。  
3. **写**：单 active 顺序追加；小包聚合、大包旁路；Sync 策略可配。  
4. **读**：一跳点查；可见性区分进程内与崩溃后。  
5. **恢复**：信任合法 footer；否则扫描未 seal 段重建 `used_bytes`。  
6. **定位**：本层作为 **ValueLog**（及可选 WAL 后端）；LSM 索引在 `internal/store`。

交互层只把顺序写与随机读做对；索引、事务、SST compaction、ValueLog GC 策略留在上层。
