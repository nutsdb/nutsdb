# DESIGN: SST（Sorted String Table）

> LSM 路径下不可变有序表文件格式。  
> SST 存 **`key → ValueRef`**（默认指向 ValueLog 的 `fileio.Location`）。  
> 总览见 [LSM_VALUELOG_DESIGN.md](./LSM_VALUELOG_DESIGN.md)。

---

## 0. 目标与非目标

### 0.1 目标

| 目标 | 说明 |
|------|------|
| 有序 | 文件内 key 严格升序（`bytes.Compare`） |
| 点查快 | 分区索引 + 可选 Bloom；通常 1～2 次磁盘读定位 Data Block |
| 范围扫 | 顺序读 Data Block，便于 compaction / Iterate |
| 崩溃安全 | 完整 footer 才可被 MANIFEST 引用；临时文件用 `.sst.tmp` |
| 与 ValueLog 解耦 | value 主体在 `.seg`；SST 只存引用或小 inline |

### 0.2 非目标

- 已删除的历史格式（无迁移）
- 文件内更新 / 追加新 key（SST 只读；更新靠新文件 + Version）
- 跨文件去重（由 compaction 负责）

---

## 1. 文件命名

```text
<data_dir>/sst/<fileNumber>.sst
```

- `fileNumber`：全局单调递增 `uint64`（MANIFEST 分配），文件名建议 `%020d.sst` 或 `%016x.sst`
- 写入：`sst/<fileNumber>.sst.tmp` → fsync → rename → 目录 fsync（若平台需要）

---

## 2. 整体布局

```text
┌─────────────────────────────────────────┐
│ Data Block 0                            │
│ Data Block 1                            │
│ ...                                     │
│ Data Block N-1                          │
├─────────────────────────────────────────┤
│ Meta Block（可选：属性、统计）            │
├─────────────────────────────────────────┤
│ Filter Block（可选：Bloom）              │
├─────────────────────────────────────────┤
│ Index Block                             │
├─────────────────────────────────────────┤
│ Footer（固定长度）                       │
└─────────────────────────────────────────┘
```

所有多字节整数：**Little-Endian**。  
块校验：每块可选尾部 CRC-32C（与 fileio 一致，硬件友好）。

---

## 3. 条目编码（Data Block 内）

每条逻辑记录：

```text
┌──────────── SST Entry ──────────────────┐
│ shared_key_len(varint)                  │  // 与前一条 key 前缀共享长度（前缀压缩）
│ unshared_key_len(varint)                │
│ unshared_key(bytes)                     │
│ value_ref_len(varint)                   │
│ value_ref(bytes)                        │  // 见 §3.1
│ sequence(uint64)                        │  // 可选；首版可用 Timestamp+写入序简化
│ kind(uint8)                             │  // 冗余于 ValueRef.Kind，便于扫描
└─────────────────────────────────────────┘
```

首版若想降低复杂度：**可先不做前缀压缩**，改为：

```text
key_len(u32) | key | value_ref_bytes | sequence(u64) | kind(u8)
```

里程碑 L2 用定长头；L4 再加前缀压缩与 restart points。

### 3.1 ValueRef 盘上编码

```text
kind(1)
timestamp(8)
ttl(4)
switch kind:
  Location:
    file_id(4) | offset(8) | length(4)     // = fileio.Location，16B
  Inline:
    inline_len(u32) | inline_bytes
  Tombstone:
    (无额外字段)
```

`kind` 与 [LSM_VALUELOG_DESIGN §2](./LSM_VALUELOG_DESIGN.md) 一致。

### 3.2 Restart Points（若启用前缀压缩）

每 `R` 条（默认 16）强制 `shared_key_len=0`，Index / 块内二分依赖 restart 数组。

---

## 4. Data Block

```text
[ entries... ][ restart_array ][ restart_count(u32) ][ block_crc(u4) ]
```

- 建议目标块大小：`4KiB` 或 `16KiB`（可配 `SSTBlockSize`）
- 点查：在块内对 restart 二分，再线性扫

---

## 5. Index Block

每条指向一个 Data Block：

```text
separator_key_len(u32) | separator_key | block_offset(u64) | block_length(u32)
```

`separator_key`：该 Data Block 内最大 key（或最短分隔键）。  
点查：对 Index 二分 → 读对应 Data Block。

---

## 6. Filter Block（Bloom）

- 按 SST 全表或按 Data Block 建 Bloom（首版：**全表一个 Bloom** 即可）
- 假阳率目标：约 1%（`bits_per_key ≈ 10`）
- Get 前先查 Bloom；未命中可跳过该 SST（L0 多文件时收益大）

无 Filter 时仍正确，仅更慢。

---

## 7. Footer（固定 48B，示例）

```text
filter_offset(8)
filter_length(u32)
index_offset(8)
index_length(u32)
meta_offset(8)
meta_length(u32)
magic(4) = 0x53535431          // "SST1"
format_version(2)
flags(2)                       // bit0: has_filter, bit1: prefix_compress, ...
footer_crc(4)                  // 覆盖 footer 内除自身外字段
```

打开 SST：读文件末尾 Footer → 校验 magic/crc → 按需 mmap 或缓存 Index/Filter。

---

## 8. 读写 API（建议）

```go
package store

type SSTWriter interface {
    Add(key []byte, ref ValueRef, seq uint64) error // key 必须单调不减
    Finish() (meta SSTFileMeta, err error)          // 写 filter/index/footer 并原子发布
    Abandon() error
}

type SSTReader interface {
    Get(key []byte) (ref ValueRef, seq uint64, ok bool, err error)
    NewIterator(start, limit []byte) Iterator
    FileNumber() uint64
    SmallestKey() []byte
    LargestKey() []byte
    Close() error
}

type SSTFileMeta struct {
    FileNumber uint64
    Size       uint64
    Smallest   []byte
    Largest    []byte
    // Level 由 Version 持有，不必写入文件；也可冗余写入 Meta Block
}
```

`SSTBuilder` 在 Flush / Compaction 中使用：输入已按 key 排好序的迭代器。

---

## 9. 与 ValueLog 的分工

| | ValueLog (`.seg`) | SST |
|--|-------------------|-----|
| 顺序 | 写入序 append | **按 key 排序** |
| 内容 | value（及 Put payload） | ValueRef（Location / Inline / Tombstone） |
| 用途 | 点读 value 字节 | LSM 主索引 |

---

## 10. 缓存

| 缓存 | 内容 |
|------|------|
| TableCache | 打开的 `SSTReader`（fd / mmap） |
| BlockCache | Data Block 字节（按 `fileNumber+offset` 键） |

淘汰：LRU；大小由 `LSMOptions` 配置。

---

## 11. 校验与错误

| 错误 | 含义 |
|------|------|
| `ErrSSTCorrupt` | CRC / magic / 长度不一致 |
| `ErrSSTNotFound` | MANIFEST 引用但文件缺失 |
| `ErrSSTKeyOrder` | Writer 收到乱序 key |

损坏的 SST：打开失败 → 若 MANIFEST 仍引用则拒绝启动或标记只读修复（首版：**Open 失败**）。

---

## 12. 里程碑

1. **S1**：无压缩定长条目 + Index + Footer；无 Bloom  
2. **S2**：Bloom + Table/Block cache  
3. **S3**：前缀压缩 + restart points  
4. **S4**：与 compaction 迭代器对接（多路归并输出 Writer）

---

## 13. 总结

SST 是路径 C 的 **有序索引文件**：存 key 与 ValueRef，value 大块仍在 ValueLog。  
完整性靠 Footer + 块 CRC；生命周期由 MANIFEST 管理。
