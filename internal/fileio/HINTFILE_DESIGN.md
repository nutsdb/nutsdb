# DESIGN: HintFile（Key → Location 持久化索引）

> 本文档描述建立在 [Store / Location](./DESIGN.md) 之上的 **HintFile** 设计。  
> HintFile 用于加速启动恢复：用紧凑的 `key → Location` 映射重建内存索引，避免对每个 `.seg` 做全量 `Iterate`。

---

## 0. 需求（已确认）

1. HintFile 描述一种数据结构，**存储所有 key 对应的 `Location` 数据**。  
2. `Location` 定义见 [DESIGN.md §0](./DESIGN.md#0-寻址模型约定全文前提)：

```go
type Location struct {
    FileID uint32
    Offset uint64 // 段内绝对字节偏移（指向 record 起始）
    Length uint32 // 整条 record 编码长度（含 header）
}
```

3. HintFile 文件后缀为 **`.hint`**。  
4. **同 `FileID` 的 `.hint` 中 key 内容，必须与对应 `.seg` 内 KV 记录的 key 内容完全一致**（字节、顺序、条数；详见 §1.3）。

---

## 1. 背景与目标

`fileio.Store` 只负责字节持久化与按 `Location` 点查，**不做 key 索引**。上层典型路径为：

```text
Put:  loc = Store.Append(encode(entry)); memIndex[key] = loc
Get:  payload, _ = Store.Read(memIndex[key])
```

进程重启后，`memIndex` 丢失。若仅扫描全部 `.seg`（`Store.Iterate`），恢复代价与数据量成正比。  
Bitcask 类系统用 **HintFile** 保存每个 sealed 段内「最新可见」的 `key → Location`，启动时顺序读 `.hint` 即可重建索引。

### 1.1 目标

| 目标 | 说明 |
|------|------|
| 加速恢复 | 有合法 `.hint` 时，跳过对应 `.seg` 的全量扫描 |
| 契约对齐 | 条目中的地址必须是完整 `Location{FileID,Offset,Length}`，保证一跳 `Store.Read` |
| **Key 一致** | 同 `FileID` 下，`.hint` 中的 key 与对应 `.seg` 内 KV 记录的 key **字节完全一致**（见 §1.3） |
| 崩溃安全 | 损坏 / 缺失的 hint **可降级**：回退到 `Store.Iterate(fileID)` |
| 职责清晰 | HintFile 只存索引映射；不存 value；不做事务 / TTL 语义裁决（可选存元数据供上层过滤） |

### 1.2 非目标

- 不替代内存索引（运行期仍以 memIndex 为准）
- 不保证跨进程并发写同一 `.hint`
- 不在 HintFile 内实现 compaction 策略（只提供读写与按段删除）
- 不兼容旧版 nutsdb uvarint HintEntry（本设计为 next 代独立格式）

### 1.3 硬性不变量：同 FileID 的 Key 一致性

对任意 `FileID = F`，在 `.hint` 被标记为 **complete**（合法 footer）时，必须满足：

```text
令 SegKeys(F) = Store.Iterate(F) 得到的、属于「KV 语义」的记录序列
               每条为 (offset 序, key_bytes, record_type, Location)

令 HintKeys(F) = 读取 F.hint 得到的 HintEntry 序列
               每条为 (文件序, key_bytes, record_type, Location)

则必须：
  1. len(HintKeys) == len(SegKeys)
  2. 对所有 i：HintKeys[i].key 与 SegKeys[i].key 字节完全相等
  3. 对所有 i：HintKeys[i].Loc == SegKeys[i].Location
  4. 对所有 i：HintKeys[i].Type == SegKeys[i].record_type
  5. HintKeys 与 SegKeys 按段内 Offset 升序一一对应（禁止乱序、禁止丢条、禁止多条）
```

「KV 语义」记录指 payload 中携带业务 key 的 `RecordPut` / `RecordDelete`（以及上层约定的同类 entry）。  
纯内部 `RecordMeta`（无业务 key）**不进入** hint，也不计入上述序列。

推论：

| 禁止 | 原因 |
|------|------|
| 段内同 key 只保留最后一次 | 会丢掉中间版本的 key 出现次数，与 `.seg` 不一致 |
| 用全局 memIndex「该 FileID 的最新子集」生成 hint | memIndex 只有最新值，缺少段内历史 Put/Delete |
| `keep=false` 跳过本应有 key 的 KV 记录 | 造成 hint 缺 key |
| 手工/compaction 写入与 `.seg` 不符的 key | 破坏恢复正确性前提 |

**唯一合法构建路径（默认）**：`BuildFromSegment`——对 sealed `.seg` 做 `Iterate`，按序从每条 KV payload 抽出 **与写入时相同的 key 字节**，写入对应 HintEntry。  
若提供更快路径（例如 seal 前旁路缓冲），实现必须能证明与 `BuildFromSegment` 结果逐条一致，否则不得 `Finish`。

验证（实现应提供，至少用于测试 / 可选启动校验）：

```text
VerifyHintMatchesSegment(store, fileID, decodeKey) error
  // 双指针扫描 .hint 与 .seg，任一 key/Location/type 不一致 → ErrHintSegKeyMismatch
```

---

## 2. 总体架构

```text
┌─────────────────────────────────────────────────┐
│              Upper KV Engine                     │
│   MemIndex[key] = Location                       │
│   Put / Delete / Compaction / Recovery           │
└───────────────┬─────────────────┬───────────────┘
                │                 │
                ▼                 ▼
        fileio.Store         HintManager
     (*.seg Append/Read)   (*.hint Writer/Reader)
                │                 │
                └────────┬────────┘
                         ▼
                      Disk / FS
```

| 对象 | 职责 |
|------|------|
| `Store` | `.seg` 顺序写 / 随机读 / seal / Iterate / DeleteSegment |
| `HintManager` | 按 `FileID` 管理 `.hint` 的创建、追加、seal、加载、删除 |
| `HintWriter` | 向某个 `FileID` 对应的 hint 顺序追加 `HintEntry` |
| `HintReader` | 顺序扫描 hint，回调重建 memIndex |
| 上层引擎 | 决定何时写 hint、如何解析 payload、TTL/墓碑覆盖规则 |

**不变量**：`.hint` 中每条 `Location.FileID` 必须等于该 hint 文件所属的 `FileID`（自描述校验用）。

---

## 3. 命名与生命周期

### 3.1 文件命名

与 segment 一一对应：

```text
<data_dir>/<fileID>.seg
<data_dir>/<fileID>.hint
```

`fileID` 在文件名中为 **10 位十进制、前导零补齐**（与 `.seg` 相同），例如：

```text
<data_dir>/0000000001.seg
<data_dir>/0000000001.hint
```

| 规则 | 说明 |
|------|------|
| 后缀 | 固定 `.hint` |
| `fileID` | 10 位前导零十进制，与 `.seg` 相同（如 `0000000001.hint` ↔ `0000000001.seg`） |
| 分片目录 | 若启用 shard，hint 与 seg 同目录：`shard-<id>/<fileID>.hint` |

### 3.2 与 Segment 状态对应

| Segment 状态 | Hint 状态 | 说明 |
|--------------|-----------|------|
| Active | **无持久 hint** 或仅内存缓冲 | 运行期索引在 memIndex；崩溃后对该段 `Iterate` 重建 |
| Sealed | **必须尝试写完整 hint** | seal 完成后（或 compaction 产出 sealed 段时）落盘并 fsync |
| Obsolete | 与 `.seg` 一并删除 | `DeleteSegment` 后删除同 ID 的 `.hint` |

推荐策略（默认）：

```text
1. Active 段写入期间：仅更新 memIndex，不写磁盘 hint
2. Store seal 某 FileID 成功后：必须 HintManager.BuildFromSegment（从 .seg Iterate 抽出 key）
3. 写出完成后可选 VerifyHintMatchesSegment；再 rename/fsync，标记 hint 完整
4. 删除段时同步删除 hint
```

> 可选优化（非首版必须）：active 段旁路写 `*.hint.tmp` 做增量 checkpoint，但 **Finish 前必须与 `.seg` 的 KV key 序列对齐校验**；首版为简单起见，**仅在 seal / compaction 完成时用 BuildFromSegment 生成完整 hint**。

### 3.3 生命周期图

```text
create .seg (active)
  -> Append records + update memIndex
  -> seal .seg
  -> BuildFromSegment: Iterate .seg → 逐条写出与 payload 中 key 字节完全一致的 HintEntry
  -> (optional) VerifyHintMatchesSegment
  -> sealed: .seg + .hint 只读，且 Key 序列一致
  -> compaction may rewrite to new FileID（新 hint 同样必须与新 .seg 一致）
  -> DeleteSegment(oldID) + remove oldID.hint
```

---

## 4. 磁盘布局与格式常数

### 4.1 常数

| 名称 | 值 | 说明 |
|------|----|------|
| 字节序 | **Little-Endian** | 与 Store 一致 |
| 后缀 | `.hint` | 需求固定 |
| `hint_header_size` | **64** | 固定头 + padding |
| `hint_footer_size` | **64** | seal 后写在文件尾 |
| CRC | **CRC-32C (Castagnoli)** | 与 Store record 一致 |
| magic | `0x4E444248` | `"NDBH"`（NutsDB Hint） |
| version | `1` | 格式版本 |

布局：

```text
[0, header_size)                         Hint Header
[header_size, file_size - footer_size)   HintEntry stream
[file_size - footer_size, file_size)     Hint Footer（完整写出后有效）
```

与 `.seg` 不同：hint **不预分配固定容量**，随条目增长；footer 写在最终文件末尾。

### 4.2 Header（64B）

```text
magic(4) = 0x4E444248
version(2)
flags(2)           // bit0: sealed/complete
file_id(4)         // 本 hint 对应的 FileID
created_at(8)      // unix nanos
reserved(12)
header_crc(4)      // 覆盖 magic..reserved
padding...         // 至 64B
```

### 4.3 Footer（64B，完整 hint 才有）

```text
seal_magic(4) = 0x48465452   // "HFTR" Hint FooTeR
entry_count(8)
payload_bytes(8)             // header 之后、footer 之前的字节数
footer_crc(4)                // 覆盖 seal_magic..payload_bytes
padding...                   // 至 64B
```

**完整性判定**：

1. header magic/version/crc 合法，且 `file_id` 与文件名一致  
2. footer 合法，且 `header_size + payload_bytes + footer_size == file_size`  
3. 扫描过程中每条 entry CRC 通过，实际条数 == `entry_count`

任一失败 → 视为 **corrupt / incomplete**，恢复时丢弃该 hint，回退 `Store.Iterate`。

### 4.4 HintEntry 编码

每条 entry 描述「一个 key 在本段中的一条记录地址」：

```text
┌──────────────── HintEntry ────────────────┐
│ crc32c(4)                                 │  // 覆盖 crc 之后全部字段
│ key_len(4, uint32)                        │
│ loc.FileID(4, uint32)                     │  // 必须 == 本文件 FileID
│ loc.Offset(8, uint64)                     │
│ loc.Length(4, uint32)                     │
│ record_type(1)                            │  // 对齐 Store.RecordType
│ reserved(3)                               │
│ key(key_len)                              │
└───────────────────────────────────────────┘
```

固定头长度（不含 key）：

```text
hint_entry_header_size = 4+4+4+8+4+1+3 = 28
entry_size = 28 + key_len
```

CRC 覆盖：`key_len | FileID | Offset | Length | type | reserved | key`（**不含** crc 自身）。

校验规则：

| 规则 | 行为 |
|------|------|
| `key_len == 0` | **非法**（本层要求 key 非空） |
| `key_len` 过大（如 > 1MiB） | 拒绝 / 视为损坏 |
| `Location.Length == 0` | **非法** |
| `loc.FileID != hint.file_id` | **非法** |
| `record_type` 未知 | 可保留原样交给上层，或记警告后仍加载 Location |

**为何带 `record_type`**：恢复时上层需区分 Put / Delete 墓碑，否则仅有 Location 还需再读 `.seg` 才能知道语义。  
**为何不强制带 TTL/Timestamp**：这些属于上层 entry payload；需要时可在 flags 扩展 optional trailer（见 §4.5）。首版默认 **不写 TTL**，过期判断在 `Store.Read` 解码后进行。

### 4.5 可选扩展（flags）

`flags` 预留：

| bit | 含义 |
|-----|------|
| 0 | complete（与 footer 同时出现） |
| 1 | entries 携带 optional meta（timestamp+TTL，+12B） |

若 bit1 置位，entry 在 `reserved` 后、`key` 前增加：

```text
timestamp(8) + ttl(4)
```

并计入 CRC。首版实现可忽略 bit1（恒为 0）。

---

## 5. 语义：与 `.seg` 一一对应（含同 key 多条）

HintFile 是对应 sealed `.seg` 的 **KV key 投影**，不是 memIndex 的去重快照。

| 场景 | 处理（必须） |
|------|----------------|
| 段内多次 Put 同一 key | hint **保留每一次**；key 字节与各次 payload 中的 key 完全一致 |
| Put 后 Delete | Delete 对应一条 `RecordDelete` HintEntry，key 与墓碑记录中的 key 一致 |
| 跨段 | 不同 `FileID` 的 hint 独立；**全局 memIndex** 加载时按 FileID 升序、段内顺序覆盖 |

**禁止**「只保留段内最后一次」——那会使 `HintKeys` 与 `SegKeys` 长度/内容不一致，违反 §1.3。

**加载顺序（必须）**：

```text
按 FileID 升序加载各合法 .hint
对同一 key：后出现的 Location 覆盖先出现的（段内多版本 + 跨段覆盖）
最后对仍无 hint 的 active / corrupt 段执行 Iterate，同样按时间序覆盖
```

因为 hint 与 `.seg` 的 key 序列一致，用 hint 恢复 memIndex 的结果与扫描 `.seg` 抽取 key 的结果相同（在 decodeKey 一致的前提下）。

---

## 6. 对外 API（建议）

```go
package fileio

const HintSuffix = ".hint"

type HintEntry struct {
    Key  []byte
    Loc  Location
    Type RecordType
}

type HintWriter interface {
    // Append writes one hint entry. Key must be non-empty.
    Append(entry HintEntry) error
    // Sync flushes buffered data to durable storage.
    Sync() error
    // Finish writes footer, fsyncs, and marks the hint complete.
    Finish() error
    // Close aborts an unfinished writer without a valid footer
    // (recovery will ignore this file), or no-ops after Finish.
    Close() error
}

type HintReader interface {
    // Iterate calls fn for each valid entry in file order.
    Iterate(fn func(HintEntry) error) error
    // Close releases the underlying file.
    Close() error
}

type HintManager interface {
    // CreateWriter creates/truncates <fileID>.hint for writing.
    CreateWriter(fileID uint32) (HintWriter, error)
    // OpenReader opens a complete hint; returns error if missing/corrupt.
    OpenReader(fileID uint32) (HintReader, error)
    // BuildFromSegment is the canonical way to create a complete hint.
    // It MUST Iterate the sealed segment in offset order and, for every KV record,
    // append a HintEntry whose Key bytes are exactly those returned by decodeKey
    // for that record (see §1.3). Skipping KV records or rewriting keys is forbidden.
    BuildFromSegment(store Store, fileID uint32, decodeKey func(payload []byte, typ RecordType) (key []byte, err error)) error
    // VerifyHintMatchesSegment checks §1.3: hint keys/locations/types match the segment.
    VerifyHintMatchesSegment(store Store, fileID uint32, decodeKey func(payload []byte, typ RecordType) (key []byte, err error)) error
    // Delete removes <fileID>.hint if present.
    Delete(fileID uint32) error
    // LoadAll loads all complete hints in FileID order into apply(entry).
    LoadAll(apply func(HintEntry) error) error
}
```

说明：

- `decodeKey` 由上层提供：从 Store payload 解析出业务 key；返回的 key 字节必须与当初写入 `.seg` 时 payload 内的 key **完全一致**（含相等长度与每一位）。  
- `decodeKey` 对非 KV 的 `RecordMeta` 应返回明确错误或由约定的 sentinel 表示「不进入 hint」；实现上建议：`decodeKey` 返回 `(nil, ErrHintSkipMeta)` 仅用于无 key 的 meta，**不得**用于跳过 Put/Delete。  
- **禁止**用 memIndex 子集直接 `Finish` 出正式 `.hint`（除非能证明与 `BuildFromSegment` 逐条一致并通过 `VerifyHintMatchesSegment`）。  
- `BuildFromSegment` 是 seal 后的默认且推荐路径。

### 6.1 写缓冲

- 默认用户态缓冲（如 64KiB～1MiB），`Append` 聚合，`Sync`/`Finish` 时写出  
- 单条 `key_len` 过大时旁路缓冲（与 Store 大 record 策略类似）

### 6.2 原子发布

推荐写出流程：

```text
1. 写到 <fileID>.hint.tmp          // 例如 0000000001.hint.tmp
2. Finish: 写 footer + fsync
3. rename(<fileID>.hint.tmp -> <fileID>.hint)
4. fsync 目录项（若平台支持）
```

崩溃落在 rename 前：旧 hint 保留或无 hint → 均可安全降级。  
**禁止**在无 footer 的半成品上直接覆盖正式 `.hint` 而不用临时文件。

---

## 7. 启动恢复流程

```text
Recovery(dir):
  1. Store.Open(dir)                         // 恢复 .seg 元数据 / active
  2. ids = listSegmentIDs()
  3. hinted = {}
  4. for id in sort(ids):
       if id == active.FileID: continue      // active 永不信任磁盘 hint
       rd, err = HintManager.OpenReader(id)
       if err != nil:
           // missing/corrupt → fallback
           Store.Iterate(id, decode_and_apply_to_memIndex)
       else:
           rd.Iterate(apply_to_memIndex)     // 后者覆盖前者
           hinted[id] = true
           rd.Close()
  5. if active != nil:
       Store.Iterate(active.FileID, apply)   // 必须扫 active
  6. 完成 → 服务读写
```

`apply_to_memIndex` 伪代码：

```text
apply(entry):
  if entry.Type == RecordDelete:
      memIndex.Delete(entry.Key)   // 或存墓碑，由上层决定
  else:
      memIndex[entry.Key] = entry.Loc
```

指标建议：`hint_load_entries`、`hint_fallback_segments`、`recovery_scan_bytes`。

---

## 8. 与 Seal / Compaction / 删段的集成

### 8.1 Seal

```text
Store seal fileID success
  -> HintManager.BuildFromSegment(store, fileID, decodeKey)   // 唯一默认路径
  -> HintManager.VerifyHintMatchesSegment(...)                 // 推荐；失败则删 tmp、不发布
```

失败策略：记录日志；**.seg 仍可用**，不发布 `.hint`，下次恢复对该段 `Iterate`。  
**不得**在 Verify 失败时仍 rename 为正式 `.hint`。

### 8.2 Compaction

```text
1. 读 live keys → Append 到新 active/sealed segment（得到新 Location）
2. BuildFromSegment(新 FileID) 生成与新 .seg 的 KV key 完全一致的 .hint
3. VerifyHintMatchesSegment(新 FileID)
4. 切换 memIndex 指向新 Location
5. Store.DeleteSegment(oldID)
6. HintManager.Delete(oldID)
```

新段的 hint 必须反映 **新 `.seg` 内实际写入的每一条 KV**（含顺序与 key 字节），而不是旧 hint 的简单拷贝或 memIndex 去重视图。

顺序要求：**先保证新 seg+hint 与 memIndex 一致，再删旧文件**，避免窗口期内既无 hint 又丢 seg。

### 8.3 DeleteSegment

`Store.DeleteSegment` 不自动删 hint（保持 Store 职责单一）。上层或 `HintManager` 封装：

```text
DeleteSegmentPair(fileID):
  Store.DeleteSegment(fileID)
  HintManager.Delete(fileID)
```

---

## 9. 并发与可见性

| 场景 | 规则 |
|------|------|
| 写 hint | 单写者（每 FileID 一个 Writer） |
| 读 hint | 仅打开 **Finish 完成** 的文件；读写不交叉 |
| 与 Store | Hint 中 Location 必须指向已 durable 的 sealed 段记录 |
| 崩溃后 | 无 footer / CRC 失败 → 忽略 hint |

不变量：

```text
合法 complete hint(F)
  ⇒ HintKeys(F) 与 SegKeys(F) 按 §1.3 完全一致（key 字节 / Location / type / 顺序 / 条数）
  ⇒ 其中每个 Location 在对应 sealed .seg 内可读
hint 缺失/损坏/Verify 失败
  ⇒ 不妨碍正确性，只影响恢复速度（回退 Iterate）
```

---

## 10. 错误

| 错误 | 含义 |
|------|------|
| `ErrHintNotFound` | 对应 `.hint` 不存在 |
| `ErrHintCorrupt` | header/footer/entry CRC 或长度不一致 |
| `ErrHintIncomplete` | 无合法 footer（崩溃半成品） |
| `ErrHintInvalidEntry` | key 空、Length=0、FileID 不匹配等 |
| `ErrHintSegKeyMismatch` | Verify 发现 hint 与 `.seg` 的 key/Location/type/条数不一致 |
| `ErrHintClosed` | Writer/Reader 已关闭 |

`OpenReader` 对 corrupt/incomplete 应返回错误，由 `LoadAll` / Recovery 捕获并 fallback。

---

## 11. 与旧版 HintFile 的差异

| 项 | Legacy | 本设计 |
|----|--------|--------|
| 地址 | `FileID int64` + `DataPos`（无 Length） | `Location{FileID u32, Offset u64, Length u32}` |
| 元数据 | BucketId/Flag/Status/Ds/TTL… | 默认仅 `RecordType`；TTL 可选扩展 |
| 编码 | 全 uvarint | 定长头 + key 变长，扫描更快 |
| 完整性 | 弱 / 依赖读尽 | header + per-entry CRC + footer |
| 发布 | 直接写 | `.tmp` + rename |
| 兼容 | — | **不兼容**，新 magic `NDBH` |

---

## 12. 实施里程碑

1. **H1**：常量、`HintEntry` 编解码、单测（CRC / 非法条目）  
2. **H2**：`HintWriter` / `HintReader` + tmp rename + footer  
3. **H3**：`BuildFromSegment` + `VerifyHintMatchesSegment` + `LoadAll`（含 key 一致性单测）  
4. **H4**：接入上层 Recovery / Seal / Compaction 删对  
5. **H5**（可选）：active checkpoint（仍须通过 Verify）、optional TTL meta、mmap 只读 hint  

---

## 13. 总结

1. **需求落实**：`.hint` 持久化 **key → Location**；`Location` 完全遵循 Store 契约。  
2. **一文件一段**：`<fileID>.hint` ↔ `<fileID>.seg`（文件名均为 10 位前导零）；active 不落盘，seal 后生成完整 hint。  
3. **Key 一致**：同 FileID 下，hint 的 KV key 序列与 `.seg` **字节级、顺序级、条数级完全一致**（§1.3）；禁止 memIndex 去重快照冒充 hint。  
4. **格式**：64B header + CRC 保护的 entry 流 + 64B footer；损坏可降级扫描 `.seg`。  
5. **恢复**：按 FileID 升序加载 hint，后者覆盖前者；active / 失败段走 `Iterate`。  
6. **边界**：HintFile 是索引加速层，不存 value，不替代 memIndex，不侵入 Store 字节语义。

上层只需在 Seal/Compaction/Recovery 三处与 `HintManager` 协作，即可在正确性不变的前提下显著缩短启动时间。
