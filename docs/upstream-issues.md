# 上游反馈问题单（trpc-group/trpc-agent-go）

> 状态更新（2026-08-31）：此前围绕官方模块提交的 issue/PR 均已由官方解决并合并。本文件保留历史复现、影响分析和规避记录，作为方案设计的依赖验证证据，不再表示这些问题处于待提交状态。
> 用途：整理官方问题的可复现证据、修复前影响和本项目的规避决策。
> 来源：Phase 1.5 隔离消费者、真实 Redis 7 / PostgreSQL 16 验证（见 `docs/stage1.5-storage-spike.md`）。
> 发布版本基线：根模块 `v1.11.2`；`session/postgres v1.11.0`；`session/mysql v1.11.2`；`memory/redis v1.11.0`；`storage/redis v1.11.0`；`go-redis v9.11.0`。
> 历史源码复核：截至 2026-08-25，官方 `main` 网页基线为 `bcd0a5679624b85810dd6e047eb63ec8867a05f4`，以下问题在提交反馈时均可复现；后续修复已合并。
> 语言：issue 标题建议英文以便检索，正文可使用中文；官方 `CONTRIBUTING.md` 只强制 PR 标题和正文使用英文。
> 原则：只反馈官方发布模块的真实缺陷；平台 `RedisBackend`、Docker 权限差异和本项目未启用 SQL 等平台侧内容不纳入上游问题本身。

---

## Issue 1：PostgreSQL Session Summary 时间戳/时区语义不一致

- **建议标题**：`session/postgres: summary freshness filter drops existing summaries in non-UTC time zones`
- **模块与版本**：`trpc.group/trpc-go/trpc-agent-go/session/postgres v1.11.0`；最新 `main` 仍存在相同逻辑。
- **已验证环境**：Go 1.21.13、Windows/amd64 Go 客户端、`Asia/Shanghai` 本地时区、Docker `postgres:16-alpine`。当前证据不能表述为“Windows 与 Linux 客户端均复现”；Linux 是数据库容器环境。
- **问题现象**：
  - Session 使用 `time.Now()` 生成 `created_at`，写入 `TIMESTAMP WITHOUT TIME ZONE`。pgx 对该类型编码时会丢弃 `time.Time` 的时区，只保留墙钟时间。
  - Summary 的 `UpdatedAt` 来自 Summary boundary cutoff；框架会将 boundary 时间归一化为 UTC，再将它作为 `session_summaries.updated_at` 写入同样的无时区列。
  - 在 `Asia/Shanghai` 下，同一实际时间可能分别写成约 `17:00` 和 `09:00`，相差约 8 小时。
  - `GetSessionSummaryText` 使用 `AND updated_at >= $6`，其中 `$6 = sess.CreatedAt`，因此会过滤掉数据库中实际存在的 Summary。
- **复现步骤**：
  1. 将 Go 进程本地时区设为 `Asia/Shanghai`，连接真实 PostgreSQL 16。
  2. 使用 `session/postgres v1.11.0` 创建 Session、追加事件并强制生成 Summary。
  3. 使用第二个 Service/连接重新读取 Session，再调用 `GetSessionSummaryText`。
  4. 直接查询 `session_summaries` 可确认 Summary 行存在；将读取视图的 `CreatedAt` 置零后也可以读到该 Summary。
- **实际结果**：默认 freshness 过滤返回空；实测 `summary.updated_at` 早于重新加载的 `session.created_at` 约 8 小时。
- **期望结果**：Session 和 Summary 的数据库时间使用一致、可跨时区比较的语义，已有 Summary 不因进程本地时区被误判为旧数据。
- **影响**：非 UTC 部署下，持久化 Summary 可能无法恢复，导致更多原始事件被回放、token 成本增加；当事件窗口受限时，较早的摘要上下文也可能不可用。不能直接概括为所有 Agent 上下文都会丢失。
- **当前规避**：本项目未将 `session/postgres` 作为生产后端。
- **修复方向约束**：
  - 不应只写“把 `sum.UpdatedAt` 转为 UTC”，因为 Summary boundary 已经是 UTC。
  - `TIMESTAMPTZ` 能从根本上表达绝对时间，但涉及既有 schema 验证和数据迁移，兼容性风险较高。
  - 保留 `TIMESTAMP WITHOUT TIME ZONE` 时，必须让 Session、Event、Summary 和查询参数采用同一墙钟约定；简单恢复全局 `.UTC()` 曾在历史提交 `a5ef5a20` 中被明确撤销，不能未经回归直接恢复。
  - issue 应先让维护者确认兼容策略，再在 PR 中选择最小方案。
- **源码位置**：`session/postgres/schema.sql`、`session/postgres/init.go`、`session/postgres/service.go`、`session/postgres/summary.go`、根模块 `session/internal/summary/summary.go` 与 `session/session.go`。
- **当前状态**：对应 issue/PR 已由官方修复并合并。本项目不使用该 SQL Session 作为首版生产后端，历史规避仍然有效。

---

## Issue 2：MySQL Session `%w` 日志格式导致包级 vet/test 失败

- **建议标题**：`session/mysql: ErrorfContext uses %w and fails go vet`
- **模块与版本**：`trpc.group/trpc-go/trpc-agent-go/session/mysql v1.11.2`；最新 `main` 仍有两处相同格式串。
- **已验证环境**：Phase 1.5 使用 Go 1.21.13 验证发布模块；2026-08-25 使用 Go 1.26.4 再次复核最新源码和发布包。该问题与 MySQL 服务版本无关。
- **问题现象**：异步 Session Event 和 Track Event 持久化 worker 各有一处：

  ```go
  log.ErrorfContext(ctx, "async persist event failed: %w", err)
  ```

  `%w` 只适用于 `fmt.Errorf` 的错误包装语义，不适用于该日志格式 API。Go printf analyzer 因此报告错误。
- **准确复现命令**：在包含该依赖的模块中直接测试发布包：

  ```text
  go test trpc.group/trpc-go/trpc-agent-go/session/mysql
  ```

- **实际结果**：

  ```text
  service.go:865:36: ErrorfContext does not support error-wrapping directive %w
  service.go:897:36: ErrorfContext does not support error-wrapping directive %w
  ```

- **口径修正**：普通消费者仅执行自己模块的 `go test ./...` 时，Go 不会自动 vet 未被测试的依赖包；本项目隔离消费者的 `go test ./...` 实际通过。因此不能写成“任何消费者默认 `go test ./...` 都失败”。在官方子模块内执行 `go test ./...`、执行 `go vet ./...`，或显式测试该依赖包时会失败。
- **期望结果**：`session/mysql` 自身及显式包级测试默认通过 `go vet`；日志使用 `%v`、`%s` 或结构化错误字段。
- **影响**：官方子模块质量检查、直接依赖包检查及覆盖依赖的 CI 会被阻断；运行时错误日志也不应使用包装动词。
- **当前规避**：只在显式测试该依赖包时使用 `-vet=off`；这不是适合长期保留的方案。
- **建议修复方向**：将两处 `%w` 改为 `%v`，补充或保留能够覆盖两条异步错误日志路径的测试，并运行包级 vet。
- **源码位置**：`session/mysql/service.go` 的两个异步持久化 worker。
- **当前状态**：对应 issue/PR 已由官方修复并合并。该问题保留在此处用于说明 Phase 1.5 的依赖核验过程。

---

## Issue 3：Redis Memory 容量检查非原子（并发 Add 可超限）

- **建议标题**：`memory/redis: enforce memory limit atomically during concurrent AddMemory calls`
- **模块与版本**：`trpc.group/trpc-go/trpc-agent-go/memory/redis v1.11.0`；最新 `main` 仍存在相同 check-then-act 实现。
- **已验证环境**：Go 1.21.13、miniredis 与真实 Redis 7 已完成常规 Memory 契约验证。现有自动化只覆盖顺序超限拒绝，尚未保存一个专门断言并发最终容量的运行复现。
- **问题现象**：`AddMemory` 在 `memoryLimit > 0` 时先执行 `HLen`，通过检查后再单独执行 `HSet`。多个并发调用可以同时观察到相同的未满容量，然后分别写入不同 Memory ID。
- **源码级并发序列**：

  ```text
  goroutine A: HLEN = limit - 1
  goroutine B: HLEN = limit - 1
  goroutine A: HSET memory-A
  goroutine B: HSET memory-B
  final HLEN = limit + 1
  ```

- **实际结果口径**：源码已经可以确定容量判断不是原子的；提交 issue 时应明确“存在可超限竞态”。在 PR 前补充真实 Redis 或稳定可控的并发回归测试，不要声称现有 Phase 1.5 测试已经实际观测到超限。
- **期望结果**：判断目标 Memory ID 是否已存在、检查新条目容量和执行 `HSET` 必须组成一个原子操作；并发结束后的不同 Memory 数不得超过 limit。
- **影响**：默认配置也会受影响，因为官方 `DefaultMemoryLimit` 为 `1000`；该问题不只发生在显式设置小容量的租户。强配额和容量保护不能依赖当前实现。
- **当前规避**：本项目生产显式使用 `WithMemoryLimit(0)`，不把官方 limit 当作强配额。
- **建议修复方向**：沿用本模块 `luaUpdateMemory` 的模式增加 Add Lua 脚本，原子执行 `HEXISTS` / `HLEN` / `HSET`，并区分“已有 ID 的幂等覆盖”“新条目成功”“容量已满”等结果。
- **源码位置**：`memory/redis/service.go` 的 `AddMemory`；可参考同文件的 `luaUpdateMemory`。
- **当前状态**：对应 issue/PR 已由官方修复并合并。本项目仍显式关闭强容量限制，避免把后端 limit 当作租户配额系统。

---

## 次级候选结论

### Candidate A：`memory/redis` 构造失败未关闭 client

- 源码层面成立：`NewService` 创建 client 后执行 `Ping`，失败时未调用 `Close`。
- 影响窗口较窄；建议先增加可观测的资源生命周期复现，再决定是否提交独立 issue。
- 不与 Memory 容量 PR 合并，避免一个 PR 同时处理并发语义和生命周期。

### Candidate B：`session/redis` 同时间戳 Event 不保证插入顺序

- 候选问题较强：现有测试描述明确声称同时间戳按插入顺序返回，但测试 ID `event_a`、`event_b` 的字典序恰好与插入顺序一致。
- ZSet 同 score 时按 member 字典序排列；HashIdx 的 member 是 Event ID，旧 ZSet 的 member 是 Event JSON，都不能表达通用插入顺序。
- 建议用逆字典序 ID 写最小复现后，作为第 4 个独立 issue；不并入本轮三个主 PR。

### Candidate C：Runner 持久化失败后仍可能返回响应

- 当前测试 `TestEmitRunnerCompletion_AppendErrorStillEmits` 明确要求 AppendEvent 失败后继续发出 completion，说明这是现有设计行为。
- 不宜直接作为 bug；如需严格持久化确认，应单独提交 proposal/API 行为讨论。

---

## 查重与拆分建议（截至 2026-08-25）

- 已检索上游 issue 的 `postgres`、`timezone`、`summary`、`ErrorfContext`、`%w`、`memory limit`、`HLen`、`redis memory` 等关键词，未发现三个主问题的直接重复项。
- 相关但不重复：#2388 针对 `session/pgvector` 使用 `localtime` 时区名导致 RDS 查询失败，已由 #2399 修复；它与 PostgreSQL Summary freshness 是不同模块、不同根因。
- 三个主问题属于三个独立 Go module，代码所有权、测试命令和风险不同，应分别提交三个 issue 和三个 PR，不做堆叠 PR 或巨型 PR。
- 建议顺序：MySQL（低风险） -> Redis Memory（并发语义） -> PostgreSQL（持久化兼容决策）。issue 可以同时创建，PR 按该顺序推进。

## 三个独立 PR 的可行性

| PR | 建议英文标题 | 预计范围 | 风险 | 必要验证 |
| --- | --- | --- | --- | --- |
| `session/mysql` | `session/mysql: fix async persistence log formatting` | `session/mysql/service.go` 与相关测试 | 低 | 子模块 `go test ./...`、`go vet ./...`，确认两条异步错误路径 |
| `memory/redis` | `memory/redis: enforce memory limit atomically` | `memory/redis/service.go` 与并发回归测试 | 中低 | 子模块普通/race 测试、limit 边界、同 ID 幂等、真实 Redis 7 |
| `session/postgres` | `session/postgres: preserve summary freshness across time zones` | PostgreSQL adapter、schema/时间语义测试；是否迁移 schema 待 issue 决策 | 中高 | 子模块普通/race 测试、UTC 与 UTC+8、真实 PostgreSQL 16、旧 schema 兼容 |

模块独立并不意味着可以只跑仓库根目录测试。根目录 `go test ./...` 不会覆盖这三个子模块；每个 PR 必须进入对应目录运行测试，完成后再按仓库脚本执行全模块回归。

## 历史 Git 工作流记录（已完成）

> 以下内容记录问题提交阶段的分支建议；相关 issue/PR 已完成并合并，不是当前待办。

目标仓库：`D:\project\OpensourceTencent\trpc-agent-go-multi-tenant-workspace`。

当前只读核验结果：

- 当前分支为 `feat/multi-tenant-node-deployment-workspace`，HEAD `a2e8ec2225f5e03f82bbcfde17c8bc0054cdeebb`。
- 工作区存在已修改的交接摘要和多个未跟踪计划/记录文件，不是干净修复基线。
- 本地 `upstream/main` 为 `2b8fe39d...`，仍落后于 2026-08-25 官方网页显示的 `bcd0a567...`；后续必须先 `git fetch upstream` 再确定基线。
- `main` 已由 `D:\project\OpensourceTencent\trpc-agent-go` worktree 占用。

因此不建议在当前脏工作区直接 `git pull` 后连续切换三个修复分支。推荐流程是：

1. 先单独保存或提交当前多租户文档工作，保证不会混入上游修复。
2. 执行 `git fetch upstream`，三个分支都从同一个最新 `upstream/main` 创建，不从当前 feature 分支派生。
3. 使用三个独立 worktree，或在一个完全干净的 worktree 中按顺序完成三条互不依赖的分支：

   - `codex/fix-postgres-summary-timezone`
   - `codex/fix-mysql-log-format`
   - `codex/fix-redis-memory-limit`

4. 每条分支只修改所属子模块及必要的根模块测试/文档；分别推送到 fork 的 `origin`，分别向 `trpc-group/trpc-agent-go:main` 创建 PR。
5. PR 描述使用英文并引用对应 issue，例如 `Fixes #...`；不让第二、第三个 PR 基于第一个 PR 的分支。

结论：三个问题按独立子模块拆分后已完成官方修复并合并。MySQL、Redis 和 PostgreSQL 的修复边界保持独立；本项目无需再维护对应的本地补丁分支。
