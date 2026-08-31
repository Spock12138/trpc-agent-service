# Phase 2 执行记录

## 2026-08-28：Phase 2 收口启动

- 已确认按 A -> B -> D 执行，方案 C 暂缓，不暂存、不提交、不推送，也不开始 Phase 3。
- 宿主用户级 `PHASE2_MODEL_KEY` 已确认存在但未读取或输出；Docker Desktop 27.2.0 已启动，6379/8080 端口空闲。
- 执行前检测到现有 `maxkey-mysql` 容器正在运行；本轮只管理专用 `trpc-phase2-model-smoke-redis`，不修改其他容器。
- 已复核死字段、两段死校验、阶段编号冲突、示例配置和未提交工作区，未发现新的重叠修改。
- 已删除 `ChannelBinding.CredentialRef` 和两段死校验，并增加 Binding `credential_ref` 被严格 JSON 拒绝的回归用例；config/tenant 定向测试通过。
- 已按逻辑 Plan 将后续阶段固定为 Phase 3 可靠消息、Phase 4 双 Worker 与共享状态、Phase 5 Telegram/企业微信与 Web UI；有效文档禁用冲突文本扫描为零。
- 已创建不含秘密的 `configs/phase2.smoke.json`，两个租户均使用 `deepseek-v4-flash` 与 `https://api.deepseek.com`；`git check-ignore` 确认该文件被忽略。
- 工作区隔离缓存下 `go test ./...`、`go test -race ./...`、`go build ./...`、`go vet ./...` 和 `git diff --check` 全部通过，允许进入真实模型 Smoke。
- 专用 `trpc-phase2-model-smoke-redis` 使用 `redis:7-alpine` 启动并返回 `PONG`；`/healthz=ok`、`/readyz=ready`。
- `demo-memory` 与 `demo-redis` 均通过真实 `deepseek-v4-flash` 返回非空 request/session/text；文本长度分别为 38/34，相同外部用户/会话的两个 Session ID 不同，Redis 专用前缀产生 4 个键。
- 已停止 Smoke 服务并删除专用 Redis 容器、2 个临时日志和临时可执行文件；三个临时用户级变量已清除，用户设置的 `PHASE2_MODEL_KEY` 保留，现有容器未修改。
- 最终核对确认 6379/8080 均未监听、专用容器不存在、`maxkey-mysql` 保持运行；Smoke 文件仍被忽略，禁用编号文本为零，实际模型 Key 在项目文本文件中的精确匹配数为 0。
- 本轮未暂存、提交或推送任何文件，Phase 3 未开始；方案 C 继续等待用户单独审阅。

## 2026-08-27：Phase 2 启动

- 已读取 `planning-with-files` 技能要求并保留原有 Phase 0/1/1.5 工作记录。
- 已确认 `feature/phase1.5-storage-spike@fb3cfa3` 与远端一致，创建 `codex/phase2-multi-tenant-runner`。
- 已确认旧 Phase 2 分支停在 Phase 1，未重置、删除或覆盖该分支。
- 已锁定只读 JSON、环境变量兼容、显式配置版本、严格 readiness、启动时凭据校验和存储冷切换方案。
- 已开始生成 `多租户与节点部署--phase2的plan.md`；生产代码尚未修改。
- 已完成计划文档、`task_plan.md`、`findings.md` 和本进度记录的落盘核验；Phase 2 计划文件状态为“已通过，执行中”。
- 已实现领域 Catalog/Repository、严格配置加载器、统一消息类型、BackendProvider 初版和 RunnerCache 空闲 LRU。
- 首次定向测试中 storage/config/tenant/message 通过；agent setup 因默认用户级 Go 构建缓存访问被拒绝，后续改用已忽略的工作区缓存。
- 工作区缓存下新增包全部通过；完成 RunnerRegistry 和多租户 Runtime 初版后，全量编译仅剩 executor 旧白盒测试访问已删除的单 Backend/旧 Runner 工厂签名。
- Repository 契约测试通过；配置严格校验测试首次因第二版本 JSON 夹具未插入而误判，已定位为测试构造问题。
- 已完成严格 JSON/凭据/模型 URL、可信绑定、BackendProvider、Redis/InMemory、RunnerRegistry、空闲 LRU、两租户隔离、严格 readiness、恢复和版本切换测试。
- 静态审查将 readiness 从仅活动版本扩大到启用 AgentApp 的所有声明版本，并禁止模型 URL 通过 userinfo/query/fragment 携带秘密；定向回归通过。
- 当前工具链下 `go test ./...`、`go test -race ./...`、`go vet ./...` 通过；`go build ./...` 返回 0，但写全局 module stat cache 出现权限警告，需用工作区隔离 GOPATH 重跑取得干净证据。
- 隔离 GOPATH/GOMODCACHE 后当前工具链 build 干净通过。
- Go 1.21.13 首次命令仅 version 使用绝对工具链，test/build 误落回 Go 1.26.4 并报告版本不匹配；这是验证命令错误，需统一绝对 `go.exe` 重跑。
- Docker 宿主服务检查为 stopped，提权 `docker ps` 也确认引擎 named pipe 不存在；已获准启动 Docker Desktop，等待真实 Redis 验收。
- 最终评审代码的 race/build/vet 与 Go 1.21.13 test/build 再次通过。
- 最终真实 Redis Smoke 首次在测试执行前编译 `fmt` 失败且无源码诊断；按 Windows 资源波动处理，改用已成功缓存和单并行重跑。
- 最终真实 Redis Phase 2/legacy Smoke 单并行重跑通过，专用容器停止并自动删除。
- 入口复核发现 `start.sh` 未传 `serve` 导致服务立即退出；已修正并支持透传 `-addr`。
- 最终验收矩阵复核补充跨租户 Memory 直接隔离，以及 `v1 InMemory -> v2 Redis` 空 Session 冷切换测试。
- 最终普通全量、executor race、vet 和 Go 1.21.13 全量测试均再次通过；此前最终全量 build/race/vet、Go 1.21.13 build 和模块校验结果保持通过。
- 当前 Windows 没有可用 WSL/Git Bash，`start.sh` 仅完成 `serve "$@"` 静态核对，未执行 Bash 启停闭环；该限制已写入阶段结论。
- Phase 2 本地实现与验收完成，未暂存、提交或推送；等待用户审阅。

# 阶段 0 执行记录

## 2026-08-25：Phase 1.5 完成

- 隔离建立 Redis、MySQL、PostgreSQL 三个远程发布物消费者；固定 root `v1.11.2`、Redis/SQL 子模块矩阵和 Go 1.21.13，确认 `GOWORK=off`、无消费者 `replace`。
- Redis miniredis 与 Docker Redis 7 契约通过，包含 Session/Memory 完整主路径、Summary、工具策略、Lua、24 路并发、跨实例、停止/恢复和 Close 生命周期。
- PostgreSQL 16 真实 schema、Session/Memory、并发、跨连接和重建契约通过；记录 Summary 时区 freshness 缺陷。MySQL 只运行静态/接口/sqlmock，未连接现有 MySQL。
- 选择 B 并接入生产 `RedisBackend`：构造不联网、Ready 可恢复、独立 client、官方版本化前缀、HashIdx 同步 Session、无限 Memory、工具全关、幂等并发 Close。
- 补强生命周期：部分构造返回 `(service, error)` 时全部释放；Ready 在构造前后尊重取消；Runtime 关闭持写锁等待活跃 Handle 排空；显式 Runner 存储错误会再次健康探测并映射依赖不可用。
- 生产 `go.mod` 保持 root `v1.11.2`，加入 Redis 子模块 `v1.11.0` 和 `go-redis v9.11.0`；SQL 子模块未进入生产依赖。
- 删除 Phase 1 自研快照 Service、测试和 `ErrUnsupported`；不迁移演示数据、不双读、不保留 fallback。
- 真实服务验证 Redis 停止前 `healthz/readyz=200/200`，停止后 `200/503` 且消息 `503`，原端口恢复后无需重启 `readyz=200`。
- 当前工具链完成生产 `go test ./...`、`go test -race ./...`、`go build ./...`、`go vet ./...`；Go 1.21.13 完成生产及 Redis/MySQL/PostgreSQL 消费者 test/build。
- 收尾复跑真实阶段容器：Redis/PostgreSQL 真实契约及 Redis/MySQL/PostgreSQL Spike 普通/race 均通过；Redis Memory Spike 的时间排序脆弱断言已改为按关键词核验，`-count=30` 通过。
- 收尾清理仅删除 `trpc-stage15-redis` 与 `trpc-stage15-postgres`；现有 `maxkey-mysql` 等用户容器未改动。
- 最终代码版本再次通过 `go test ./...`、`go test -race ./...`、`go build ./...`、`go vet ./...` 与 `go mod verify`；Go 1.21.13 使用隔离 `GOROOT` 后生产 test/build 也通过。
- 结果写入 `docs/stage1.5-storage-spike.md`，README、Phase 1 Spike、逻辑 Plan、交接摘要和工作记忆同步更新。

## 2026-08-25：Phase 1.5 启动

- 已读取 `planning-with-files` 技能要求并保存审阅通过的 Phase 1.5 计划。
- 已确认代码基线为 `113e4bf`，当前分支仍指向该提交；已有未跟踪文档和密钥笔记继续保护，不使用 `git add -A`。
- 已完成当前源码、官方多模块源码、发布版本、构造器、默认配置、client ownership、key schema 和现有测试基线的只读核对。
- 已将 `.stage15-spike/` 与 `.stage15-cache/` 加入忽略规则；生产依赖尚未修改。

## 2026-08-24

- 已读取 `planning-with-files` 技能要求。
- 已开始阶段 0 调研。
- 已发现 Git 命令需要使用命令级 `safe.directory` 参数，未修改全局配置。
- 已完成服务仓库骨架与旧计划的初步盘点；事实已写入 `findings.md`。
- 已核对 Go module proxy：tRPC-Agent-Go 最新稳定标签为 `v1.11.2`，module path 为 `trpc.group/trpc-go/trpc-agent-go`，Go 版本为 1.21。
- 已完成服务仓库 `go test ./...` 和 `go build ./...` 基线验证，均通过但当前没有测试文件。
- 已完成临时框架导入编译验证，使用临时缓存目录后通过。
- 已完成 GitHub 页面和 Go module 元数据的 Telegram/企业微信 SDK 初步调研；GitHub REST API 连接关闭，搜索页触发 secondary rate limit。
- 已重新通过 Go module proxy 核对完整版本列表和 `v1.11.2` 元数据，并通过 GitHub 远端 tag 交叉验证 tRPC-Agent-Go、Telegram 首选候选和企业微信主要候选。
- 已创建 `docs/stage0-baseline.md`、`docs/stage0-im-sdk-matrix.md`、`docs/stage0-runner-options.md`、`docs/stage0-architecture.md`、`docs/stage0-acceptance.md`。
- 已明确区分已验证事实、阶段 0 推荐和待用户审阅决定；未执行 `go get`，未修改生产代码或服务仓库 `go.mod`。
- 已回写《多租户与节点部署--交接摘要.md》，把下一窗口入口改为审阅 Runner、IM SDK、企业微信协议、群聊作用域和最小交付边界。
- 最终验证：使用可写临时 Go 缓存执行 `go test ./...` 和 `go build ./...` 均通过；所有包仍无测试文件。
- 最终静态扫描：服务 `go.mod` 未变化，生产 Go 源码和依赖文件无 OpenClaw 直接依赖/import；文档中仅保留排除依赖和设计参考说明。
- 已按绝对路径验证并删除 `.stage0-compile` 临时模块及缓存目录。
- 阶段 0 调查和文档产出已完成，进入阶段 1 仍等待用户审阅并确认关键方向。

## 2026-08-24：阶段 0 五项决策已确认

- 已确认 Runner 采用每 Worker 按 `tenant_id + agent_app_id + config_version` 的有界本地缓存，Session/Memory 使用共享后端。
- 已确认 Telegram 首选 `github.com/go-telegram/bot v1.23.0`，备选 `telegram-bot-api/v5 v5.5.1`。
- 已确认企业微信首版按内部自建应用验证，`silenceper/wechat/v2 v2.1.14` 先做协议匹配 Spike，真实联调后置。
- 已确认群聊使用群主体作为 Runner user scope，真实 actor 仅用于权限审计，首版不承诺个人 Memory。
- 已确认最小真实交付和轻量 Web UI 边界；生产扩展以设计文档为主。
- 已更新五份阶段 0 文档、`findings.md` 和本记录；本轮没有引入 SDK、没有修改 `go.mod`、没有修改功能代码。
- 阶段 0 正式关闭；阶段 1 开工前仍需执行依赖/Runner Spike 并生成详细实施规格。
- 已完成阶段 0 文档一致性复核：旧的“待用户确认”表述仅保留在历史记录或技术待验证项中，未再作为当前方案状态。

## 2026-08-24：Phase 1 实现与验收

- 已完成 tRPC-Agent-Go `v1.11.2`、Telegram `v1.23.0`、企业微信 `v2.1.14` 和 RunnerCache Spike，结论写入 `docs/stage1-spike.md`。
- 已真实验证 `deepseek-v4-flash` 可使用 `https://api.deepseek.com` 与 `https://api.deepseek.com/v1`；真实 Key 只从宿主环境读取，未输出或写入仓库。
- 已实现配置 fail closed、Redis URL 规范化、HMAC 身份派生、16/32 位 hex request/trace ID、有界 RunnerCache、单进程 executor 和三个 HTTP 接口。
- [历史记录，已由 Phase 1.5 纠正] 当时只检查 tRPC-Agent-Go 根模块下载目录，误判不存在 `session/redis`、`memory/redis` 或 `storage/redis`；该阶段使用的平台快照适配器后来已删除，生产现采用官方 Redis 子模块与 `RedisBackend` 薄包装。
- 已实现 Mock 模型成功、Session 复用、流式 Event 拼接、空响应、上游 4xx/5xx、超时、Redis 故障和 HTTP 完整状态码测试。
- 已完成真实 HTTP Handler -> executor -> Runner -> Redis -> Mock 模型端到端测试。
- 已完成 `go test ./...`、`go test -race ./...`、`go build ./...` 和 `go vet ./...`；Go 1.21.13 下全量测试通过。
- 宿主 Docker Desktop `27.2.0` 正常，`trpc-redis` 使用 `redis:7-alpine` 且 `PING=PONG`；真实 Redis 下 Session/Memory 跨 runtime Smoke Test 通过。
- 默认 Codex 沙箱无法读取 Docker 配置和 named pipe，宿主执行正常；该差异不是 Windows 或 Docker 权限故障。
- `.stage1-cache/`、`.stage1-spike/` 和明文 Key 本地笔记均已加入 `.gitignore`；未使用 `git add -A`，未暂存或提交文件。

## 2026-08-24：Phase 1 审阅通过，准备进入 Phase 2

- 外部审阅确认 Phase 1 已完成，可进入 Phase 2；完成范围是带边界的单进程真实 Agent 最小闭环。
- 再次确认 `miniredis` 只是纯内存模拟服务端，不落盘；自动化测试保留它以获得稳定性，但不把它当作真实 Redis 验收。
- 使用 Docker 中运行的 `trpc-redis` 完成真实验证：宿主端 `PONG`、服务 `/readyz=200`、真实消息处理、Session/Memory 写入和跨 runtime 读取均通过。
- 真实演示数据使用 `phase1-live:*` 前缀，验证后已清理；容器保持运行。Redis 当前未挂载持久化卷，持久化部署留到后续阶段。
- Docker 权限策略确认：默认保持 workspace-write；需要 Docker 时只对明确的单条 Docker 命令临时提权，仅访问 Docker，不修改其他位置。
- 当前没有开始 Phase 2 生产代码；下一步先生成和审阅 Phase 2 详细计划。
