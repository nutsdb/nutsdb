# 设计文档索引

> 本目录集中存放 nutsdb next 代存储相关设计文档。  
> 代码实现位于 `internal/fileio`、`internal/store`。

## 目录结构

```text
docs/design/
├── README.md                 # 本索引
├── controlplane/             # 面向用户的公开 API
│   └── USER_INTERFACE.md
├── fileio/                   # 磁盘 I/O 层（ValueLog / WAL 后端）
│   └── STORAGE_IO.md
└── store/                    # LSM + ValueLog 引擎
    ├── LSM_VALUELOG_DESIGN.md   # 总览（建议首读）
    ├── WAL_DESIGN.md
    ├── SST_DESIGN.md
    ├── MANIFEST_DESIGN.md
    └── STORE_MGR_DESIGN.md
```

## 阅读顺序

| 顺序 | 文档 | 内容 |
|------|------|------|
| 0 | [controlplane/USER_INTERFACE.md](./controlplane/USER_INTERFACE.md) | 用户 API：`Open`/`Tx`、Bucket、KV（对齐 v1） |
| 1 | [store/LSM_VALUELOG_DESIGN.md](./store/LSM_VALUELOG_DESIGN.md) | **引擎总览**：架构、写/读、flush、compaction、ValueLog GC |
| 2 | [store/WAL_DESIGN.md](./store/WAL_DESIGN.md) | MemTable 耐久与 replay |
| 3 | [store/SST_DESIGN.md](./store/SST_DESIGN.md) | 有序表格式（key → ValueRef） |
| 4 | [store/MANIFEST_DESIGN.md](./store/MANIFEST_DESIGN.md) | VersionSet / 原子切换 |
| 5 | [store/STORE_MGR_DESIGN.md](./store/STORE_MGR_DESIGN.md) | `OpenStoreManager` 边界 |
| 6 | [fileio/STORAGE_IO.md](./fileio/STORAGE_IO.md) | ValueLog（及可选 WAL 后端）段式 I/O |

## 分类说明

| 分类 | 路径 | 对应代码 |
|------|------|----------|
| **用户面** | `controlplane/` | 根包 `nutsdb`（待实现 facade） |
| **I/O 层** | `fileio/` | [`internal/fileio`](../../internal/fileio) |
| **引擎层** | `store/` | [`internal/store`](../../internal/store) |
## 一句话

```text
MemTable/SST(key→ValueRef) + WAL + MANIFEST + fileio.Store(vlog)
```
