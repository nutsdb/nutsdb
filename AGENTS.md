# AGENTS.md

本文件为 AI 编码助手（Claude Code、Codex、Cursor、Gemini CLI 等）提供本仓库的工作指引。  
当本文件与设计文档（`docs/design/`）冲突时，**以设计文档与代码实现为准**；若存在 `README.md` / `CONTRIBUTING.md`，人类文档优先于本文件中的流程约定。

**工具无关原则：** 使用或新增 AI 能力、仓库级 rules / 约定时，尽量少出现与特定开发工具强绑定的路径、配置或术语（例如某 IDE 专属目录、扩展或产品名）。约定应优先写在本文件、`docs/`、CI 与通用配置（如 `codecov.yml`）中，使任意助手与贡献者都能遵循。

## Project Overview

`github.com/nutsdb/nutsdb` 是 NutsDB 的下一代嵌入式 KV 存储引擎（Go 库），采用 **LSM + ValueLog** 架构：

- **LSM**（`internal/store`）：MemTable / WAL / SST / MANIFEST，管 key、版本与索引；SST 存 `key → ValueRef`
- **ValueLog**（`internal/fileio`）：段式 `.seg` 顺序写 value，提供可复用的磁盘 I/O

模块路径：`github.com/nutsdb/nutsdb`，Go 版本见 `go.mod`（当前 `go 1.24`）。  
对外打开入口：`store.OpenStoreManager(opts LSMOptions)`，实现 `StoreManager` / `BatchAPI`。

一句话：

```text
MemTable/SST(key→ValueRef) + WAL + MANIFEST + fileio.Store(vlog)
```

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│              store.StoreManager (LSM + ValueLog)             │
│                                                             │
│   Put/Delete ──► WAL ──► MemTable                           │
│                      │         │                            │
│                      │         ▼ flush                      │
│                      │    Immutable MemTable(s)             │
│                      │         │                            │
│                      │         ▼                            │
│                      │      SST L0 → … Ln                   │
│                      │      (key → ValueRef + Filter/Bloom) │
│                      │         ▲                            │
│                      │    MANIFEST / VersionSet             │
│                                                             │
│   Get: Mem → Imm → SST(+Bloom) → ValueRef                   │
│                         │                                   │
│                         ▼                                   │
│              ValueLog = fileio.Store (*.seg)                │
└─────────────────────────────────────────────────────────────┘
```

数据目录（由 `LSMOptions.Dir` 派生，见 `internal/store/paths.go`）：

| 子目录 | 内容 |
|--------|------|
| `vlog/` | ValueLog（`fileio.Store`，`*.seg`） |
| `wal/`  | WAL（复用 `fileio.Store`） |
| `sst/`  | SST 文件（`*.sst`） |
| 根目录  | `CURRENT` / `MANIFEST-*` |

## Repository Map

```text
.
├── AGENTS.md                 # 本文件
├── logger.go                 # 用户侧日志配置入口（SetLogger / SetLevel）
├── go.mod / go.sum
├── docs/
│   └── design/               # 设计文档（权威）
│       ├── README.md         # 索引与阅读顺序
│       ├── fileio/
│       │   └── STORAGE_IO.md
│       └── store/
│           ├── LSM_VALUELOG_DESIGN.md   # 总览（建议首读）
│           ├── WAL_DESIGN.md
│           ├── SST_DESIGN.md
│           ├── MANIFEST_DESIGN.md
│           └── STORE_MGR_DESIGN.md
└── internal/                 # 实现；外部模块不应依赖
    ├── core/                 # Record 等共享领域类型
    ├── fileio/               # 段式磁盘 I/O（ValueLog / WAL 后端）
    ├── store/                # LSM 引擎 + StoreManager
    ├── logger/               # 可插拔分级日志（供 internal 调用）
    ├── utils/                # LRU 等通用工具
    └── testutils/            # 测试辅助
```

### 包职责与导入边界

| 包 | 职责 | 可依赖 |
|----|------|--------|
| `internal/core` | `Record` 等核心类型 | 标准库 |
| `internal/fileio` | Segment / Append / Read / Seal / Sync；**不感知 LSM** | `logger`、`utils` |
| `internal/store` | `StoreManager`、MemTable、WAL、SST、Filter/Bloom、MANIFEST、compaction | `core`、`fileio`、`logger`、`utils` |
| `internal/logger` | 全局可配置 Logger + `Infof`/`Warnf` 等 | 标准库 |
| `internal/utils` | LRU 等通用工具 | 标准库 |
| `internal/testutils` | 测试专用 | 按需 |

规则：

1. **`fileio` 不得 import `store`**（单向依赖）。
2. 新增引擎能力优先落在 `store`；纯字节持久化落在 `fileio`。
3. Filter 通过 `FilterPolicy` / `Filter` / `FilterBuilder` 接口扩展（默认 Bloom：`nutsdb.BuiltinBloomV1`）。
4. 日志：用户通过根包 `nutsdb.SetLogger` / `SetLevel` 配置；`internal/*` 只调用 `internal/logger`（`Infof` 等），禁止依赖根包。默认 `log.Default()` + `LevelInfo`；可用 `Nop()` 关闭。不引入第三方日志框架。
5. 改存储格式或恢复语义前，先读并对齐 `docs/design/` 对应文档。

### `internal/store` 主要文件

| 文件 | 职责 |
|------|------|
| `store_manager.go` | `StoreManager` / `BatchAPI` 接口 |
| `lsm_options.go` / `lsm_store.go` | 选项、打开与读写主路径 |
| `memtable.go` / `mem_tree.go` / `rb_tree.go` | 内存表 |
| `wal.go` | WAL Append / Replay |
| `sst.go` | SST Writer/Reader（含 Filter Block） |
| `filter.go` / `bloom_filter.go` | 可插拔 Filter + 默认 Bloom |
| `manifest.go` | CURRENT / MANIFEST / VersionSet |
| `compaction.go` | L0→Ln compaction |
| `value_ref.go` / `entry.go` | ValueRef 与 payload 编解码 |
| `paths.go` | 子目录名与文件权限常量 |

## Build & Test Commands

Go **1.24.x**（与 CI 一致）。无需 Makefile。

```bash
# 构建
go build ./...

# 全部测试（含竞态；CI 同款）
go test -race ./...

# 单包 / 单测
go test -race ./internal/store/
go test -run TestLSM_PutGetDelete ./internal/store/

# StoreManager 性能（默认 MemTable 较大，点查多半打内存；测 Bloom/SST 需缩小 MemTable 或 Reopen）
go test ./internal/store/ -bench=BenchmarkStoreManager -benchmem -count=1

# 覆盖率
go test -coverprofile=coverage.out ./... && go tool cover -html=coverage.out

# 格式化与静态检查
gofmt -s -w .
go vet ./...
```

CI：`.github/workflows/ci.yml`（`go test -race`，Linux / Windows）。

## Code Style & Conventions

- 格式化：所有 `.go` 必须通过 `gofmt -s`。
- 命名：Effective Go / Go Code Review Comments；导出标识符需以名称开头的 godoc。
- 错误：`fmt.Errorf("context: %w", err)` 包装；库代码禁止随意 `panic`。
- 接口：小而聚焦；Filter 等扩展点放在使用方（`store`）以接口注入。
- 常量：协议 magic、文件后缀、目录名、footer 偏移等抽成命名常量（见 `fileio/const.go`、`store/sst.go`、`store/paths.go`）；缓冲区初始容量等实现细节字面量可保留。
- 依赖：最小化；新增依赖前核对许可证、维护状态与 `go.mod` Go 版本。禁止在库中引入 CLI / 重型应用层框架。
- 公共 API：破坏性变更需明确说明；`internal/` 可更自由，但仍应避免无必要的盘格式不兼容。

## Testing Guidelines

- 标准库 `testing` + 已采用的 `testify/require`（或 `assert`）。
- 表格驱动为默认；独立用例可用 `t.Parallel()`。
- 导出行为至少覆盖一条测试；修 bug 必须带复现测试。
- 存储相关：优先测 Put/Get/Delete、flush/reopen、Batch、TTL、compaction 与 Filter 短路。
- **Codecov 总语句覆盖率必须 ≥ 70%**（权威配置见仓库根目录 `codecov.yml`；上传流程见 `.github/workflows/go.yml`）。本地核对：

```bash
go test -coverprofile=_coverage.out -covermode=atomic ./...
grep -v github.com/nutsdb/nutsdb/examples _coverage.out | grep -v testutils > coverage.out
go tool cover -func=coverage.out | grep total:
```

- 新增代码优先补对应测试；优先抬高薄弱包的覆盖，避免靠扩大 ignore 列表抬高数字。
- 不要声称未经测试的代码可用；改完至少跑受影响包的 `go test`，合并前尽量 `go test -race ./...`。

## Documentation

- 设计决策与格式：写在 `docs/design/`（索引见 `docs/design/README.md`）。
- 包内 `README.md` 仅作指向设计文档的指针，避免重复维护。
- 导出 API 保持 godoc；非显而易见的取舍同步更新对应 DESIGN 文档。

## Safety & Restrictions

- 未经用户明确请求，不要 `git commit` / `git push` / 开 PR。
- 不要触碰密钥、`.env`、`*.key`、`*.crt` 或 `.gitignore` 覆盖的文件。
- 不要凭记忆断言第三方 API；以本仓库代码、`pkg.go.dev` 或模块缓存为准。
- `force-push`、`git reset --hard`、改写历史等破坏性操作必须先询问。

## Commit & PR Conventions

- PR 标题：Conventional Commits，例如 `fix(store): skip SST get on bloom miss`。
- Commit 需签名：`git commit -s`。
- PR 描述包含：动机、影响范围（是否改盘格式 / 恢复路径）、测试情况、破坏性变更。

## Dependency Verification

引入或升级依赖时：

1. 确认其 `go` 版本与本仓库兼容。
2. `go mod tidy`，检查 `go.sum` 变更合理。
3. 若明显扩大模块图，在 PR 中说明理由。
