# internal/store 设计文档索引

> **唯一存储引擎：LSM + ValueLog（路径 C）**。

## 阅读顺序

| 顺序 | 文档 | 内容 |
|------|------|------|
| 1 | [LSM_VALUELOG_DESIGN.md](./LSM_VALUELOG_DESIGN.md) | **总览**：架构、写/读、flush、compaction、ValueLog GC、目录、里程碑 |
| 2 | [WAL_DESIGN.md](./WAL_DESIGN.md) | MemTable 耐久与 replay |
| 3 | [SST_DESIGN.md](./SST_DESIGN.md) | 有序表格式（key → ValueRef） |
| 4 | [MANIFEST_DESIGN.md](./MANIFEST_DESIGN.md) | VersionSet / 原子切换 |
| 5 | [STORE_MGR_DESIGN.md](./STORE_MGR_DESIGN.md) | `OpenStoreManager` 边界 |
| 6 | [../fileio/DESIGN.md](../fileio/DESIGN.md) | ValueLog（及可选 WAL 后端）段式 I/O |

## 一句话

```text
MemTable/SST(key→ValueRef) + WAL + MANIFEST + fileio.Store(vlog)
```
