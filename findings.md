# Phase 2 已确认决策与基线

## 2026-08-28 Phase 2 收口事实

- `ChannelBinding` 不包含 `credential_ref`；严格 JSON 会拒绝该字段，模型和 Redis Profile 的 `credential_ref` 契约保持不变。
- 后续编号已统一为 Phase 3 Gateway/Worker 消息可靠性、Phase 4 双 Worker 与共享状态、Phase 5 Telegram/企业微信与 Web UI。
- 使用 `deepseek-v4-flash`、`https://api.deepseek.com` 和临时 Redis 7 完成真实目录双租户 Smoke；`/healthz`、`/readyz`、`demo-memory`、`demo-redis` 均成功。
- 相同外部用户和会话在两个 Binding 下返回不同 Session ID，Redis 专用前缀产生 4 个键；模型 Key 全程只从宿主用户环境注入，未写入配置或输出。

## 2026-08-27 Phase 2 开工事实

- Phase 1.5 基线提交为 `fb3cfa3097e37ed5176f0dd9e8e79bd064038def`，本地与 `origin/feature/phase1.5-storage-spike` 一致。
- 旧本地 `feature/phase2-multi-tenant-runner` 仍停在 Phase 1 提交 `113e4bf`，本轮从正确基线创建 `codex/phase2-multi-tenant-runner`，不改写旧分支。
- `tenant`、`channels` 仍是占位包；现有 Runtime 只接受固定 `demo-binding/tenant-demo/assistant/v1` 和一个 RedisBackend。
- 现有 RunnerCache 已验证 singleflight、Lease、TTL、Drain 和 Close，但容量满时只拒绝请求，尚未实现空闲项 LRU 淘汰。
- 配置采用严格只读 JSON + 旧环境变量回退；HTTP 路径和请求/响应保持兼容；ConfigVersion 采用显式不可变版本号。
- `/readyz` 严格检查所有启用 Profile；具体消息只检查选中 Profile，防止无关租户后端直接污染执行路由。
- 所有启用配置引用的凭据启动时校验；StorageProfile 切换为显式冷切换，不迁移、不双读、不 fallback。
- Phase 2 包边界固定为：`tenant` 持有领域模型与只读 Repository，`config` 持有 JSON wire schema、旧环境变量转换和 CredentialResolver，避免包循环。
- `config.Config` 暂时保留 Phase 1 扁平字段以兼容既有测试/调用；有 Catalog 时以 Catalog 为准，无 Catalog 时转换成内置 legacy Catalog。
- 官方 InMemory Session/Memory 均提供无错误构造器和 `Close`；可由统一 BackendProvider 与 RedisBackend 一样持有生命周期。
- Phase 1 executor/Smoke 白盒测试通过 `runtime.backend` 直接读 Session/Memory；多后端后应改为测试 helper 按 `demo` Binding 解析活动 ConfigVersion/Profile，再从 Provider 取得选中 Backend。
- miniredis v2.35.0 提供 `Restart() error`，可在同一地址验证 Provider/RedisBackend 故障后无需重启 Runtime 的恢复语义。
- 现有 Cache 测试覆盖 singleflight、容量、创建超时、Drain 和实例隔离，但没有容量满时空闲 LRU；Phase 2 需新增明确关闭最旧空闲 Runner 的断言。
- 静态审查发现严格 readiness 初版只收集 `active_config_version` 的 Profile；已确认口径要求启用 AgentApp 的所有声明版本所引用 Profile，需扩大集合。
- JSON 模型 `base_url` 是允许的非凭据字段，但必须限制为无 userinfo/query/fragment 的 HTTP(S) URL，避免绕过 `credential_ref` 写入明文秘密。
- BackendProvider 的 Profile 参数只作为 tenant/profile 身份使用；kind、凭据和前缀必须从不可变 Repository 重新读取，防止内部调用者绕过权威配置。
- HMAC 身份继续使用 legacy `binding.ID` 以保持 Session 兼容，因此 Catalog 明确要求 Binding ID 跨渠道全局唯一，避免同一 Agent 下发生渠道间身份碰撞。

# 阶段 0 调研发现

## 2026-08-25 Phase 1.5 已确认事实

- Phase 1.5 最终选择 B：官方 Redis Service + 平台生命周期薄包装；生产代码已使用该方案，旧快照适配器与 `ErrUnsupported` 已删除。
- tRPC-Agent-Go Redis/SQL 实现以独立 Go module 发布；根模块目录不包含它们不等于远程没有官方实现。
- Redis 发布矩阵为根模块 `v1.11.2` 搭配 `session/redis`、`memory/redis`、`storage/redis v1.11.0`；这些子模块自身的 `replace` 在消费者中不生效，必须通过隔离消费者模块验证。
- 官方 Redis 子模块会把 `go-redis/v9` 从服务当前 `v9.7.0` 提升到 `v9.11.0`。
- 官方 Redis Event 顺序基于时间戳/ZSet；相同时间戳不承诺通用插入顺序，契约测试使用单调时间并将并发序列语义留给后续阶段。
- 官方 Redis Session 默认 Legacy compat、异步持久化关闭、TTL 为 0；新部署应使用 HashIdx-only、同步持久化、用户 Session 索引开启。
- 官方 Redis Memory 构造时会主动 Ping，默认提供 Add/Update/Search/Load 工具；生产必须延迟初始化并显式关闭全部 Memory 工具。
- Session、Memory 和健康检查必须分别拥有 client；官方 Service 会关闭自己的 client，不能复用 Phase 1 的共享 client 所有权模型。
- 官方键格式与 Phase 1 SHA256 JSON 快照键不兼容；Phase 1 数据属于可丢弃的演示数据，本阶段采用版本化子前缀，不做双读或迁移。
- 用户已确认 SQL 深度：真实 PostgreSQL 最小契约 + MySQL 静态兼容性核验；Redis 选型后在 Phase 1.5 内完成生产接入。
- SQL Session/Memory 均提供 DSN 构造、`WithSkipDBInit` 和表隔离选项；PostgreSQL 额外支持 schema，MySQL Session 额外声明 `toolchain go1.24.4`，必须用远程发布模块和 Go 1.21 消费者实测。
- Redis miniredis 和真实 Redis 7 契约通过：真实 Lua `EVAL`/`EVALSHA`、两套连接、24 路并发唯一 Event/State、Summary、Memory 跨实例、停止/恢复和无 InMemory fallback 均有证据。
- 生产 `RedisBackend` 构造不联网；`Ready` 可在初始 Redis 故障后重试，Session/Memory/health 独立持有 client，官方键位于 `<REDIS_KEY_PREFIX>:official-v1`，Memory 工具全部关闭且 limit=0。
- 官方 Redis Memory 的强容量检查不是原子的，且构造器在内部 client 创建后 Ping 失败时不会 Close 未返回的 client；生产不启用强配额，后者记录为上游极窄窗口生命周期风险。
- 官方 Memory 构造器的 Ping 使用固定 `context.Background()` 超时；`RedisBackend` 在构造前后检查调用方取消、清理所有已返回的部分 Service，并由 Runtime 生命周期读写闸门保证关闭等待活跃请求。上游调用本身无法被外部 context 中断。
- tRPC-Agent-Go Runner 可能记录 Session 持久化错误后继续输出模型回复；平台只对显式返回的存储错误做健康探测并映射依赖不可用，无法从上游结果通道可靠识别被吞掉的中途持久化失败，留给后续 Inbox/幂等阶段处理。
- PostgreSQL 16 真实契约通过 schema、Session/Memory CRUD、跨连接、并发和重建；`session/postgres v1.11.0` 在 Asia/Shanghai 可复现 Summary `updated_at` 比 Session `created_at` 早约 8 小时，默认 freshness 过滤导致 Summary 不可见。
- MySQL Memory/Storage sqlmock 测试通过；Session sqlmock 在 `-vet=off` 下通过，但发布源码两处 `log.ErrorfContext(..., "%w", ...)` 会让默认包级 vet 失败。
- Go 1.21.13 成功消费所有 Redis/SQL 发布模块；MySQL Session 的 `toolchain go1.24.4` 不会强制依赖它的 Go 1.21 消费者升级。
- 当前和 Go 1.21.13 下生产 test/build 均通过；当前工具链下生产 `go test -race ./...` 与 `go vet ./...` 通过。
- 收尾复跑发现 Redis Memory Spike 原先按时间排序把更新项固定在首位，普通模式偶发失败；改为按关键词查找更新项后，普通/race 与 `-count=30` 均稳定通过。
- 最终生产代码在生命周期补强后再次通过全量 test/race/build/vet 与 `go mod verify`；Go 1.21.13 通过隔离 `GOROOT` 的生产 test/build。

## 当前状态

- 本文件随阶段 0 调研持续更新。
- 结论必须区分“已确认事实”“阶段 1 执行建议”和“阶段 1 技术待验证项”。

## 已确认事实

- `trpc-agent-service` 当前只有骨架：`cmd/trpc-service/main.go` 打印版本/说明，`trpcservice/agent`、`channels`、`config`、`tenant`、`metrics`、`tool`、`web` 等主要是占位包。
- 当前 `go.mod` 只有模块 `github.com/liuzengh/trpc-agent-service` 和 `go 1.21`，尚未引入 tRPC-Agent-Go、IM SDK、Redis 或 SQL 依赖。
- 当前服务仓库没有实际 Gateway、Worker、Runner、IM Adapter、Session/Memory、租户绑定或消息幂等实现。
- 当前目标要求至少两种真实 IM 适配逻辑；Web UI 只能作为本地演示入口，不能替代两种 IM。
- 当前结论是不直接依赖或导入 OpenClaw；OpenClaw 只作为设计参考。
- 旧施工版阶段 0 仍按旧仓库源码调查和“单 Runner + 路由 Wrapper”设计，必须转换为当前独立服务的调查结果。
- 截至 2026-08-24，Go module proxy 返回 `trpc.group/trpc-go/trpc-agent-go` 的最新稳定标签为 `v1.11.2`，发布时间为 `2026-08-20T04:03:23Z`，其 module 声明 `go 1.21`。GitHub 仓库地址是 `github.com/trpc-group/trpc-agent-go`，Go 引用必须使用 module path `trpc.group/trpc-go/trpc-agent-go`。
- 当前服务仓库基线验证：`go test ./...` 通过；`go build ./...` 通过；所有包均显示 `[no test files]`，因此通过只证明骨架可编译，不证明目标功能已实现。
- 临时模块导入真实框架 API 并编译通过：`runner.NewRunnerWithAgentFactory`、`runner.WithSessionService`、`runner.WithMemoryService`、`session/inmemory.NewSessionService`、`memory/inmemory.NewMemoryService` 和 `Runner.Close`。
- `runner.Runner` 是 `Run(ctx, userID, sessionID, model.Message, ...agent.RunOption)` 加 `Close() error`；`runner.NewRunner` 接受固定 `appName` 和默认 Agent；`Run` 支持 `agent.RunOptions.AppName` 逐请求覆盖。
- `session.Service` 是较宽的接口，包含 Session 创建/读取/删除、App/User/Session state、事件追加、Summary、异步 Summary Job 和关闭；`session.Key` 包含 `AppName/UserID/SessionID`。
- `memory.Service` 除读写/搜索/删除/清理外，还包含 `Tools()`、自动 Memory Job 和关闭；`memory.UserKey/Key` 包含 `AppName/UserID`，因此租户隔离不能只靠一个外层字段。
- Telegram SDK 初步候选：`github.com/go-telegram/bot` v1.23.0（module go 1.18，MIT，README 明确支持 Bot API 10.2、长轮询、Webhook、自定义 HTTP Client、middleware、handler 和 workers）；`github.com/go-telegram-bot-api/telegram-bot-api/v5` v5.5.1（module go 1.16，MIT，定位为 API wrapper，支持 polling/webhook）；`github.com/mymmrac/telego` v1.11.2（README 覆盖完整 Bot API、长轮询/Webhook、可替换 HTTP 实现，但 module go 1.25.7，不适合当前 go 1.21 基线）。
- 企业微信候选：`github.com/silenceper/wechat/v2` v2.1.14（module go 1.16，GitHub README 标注 Apache-2.0，包含 `work` 企业微信模块、应用消息、素材、客服和回调/解密相关代码）；`github.com/chanxuehong/wechat` README 明确写“暂时停止维护”，不推荐作为新项目首选。
- GitHub REST API 请求受到连接关闭影响，网页访问可用但 GitHub 搜索页触发 secondary rate limit；SDK 版本和模块元数据优先以 Go module proxy 为准，GitHub 页面只作 README/许可证/能力交叉核对。
- 2026-08-24 再次实际执行 `go list -m -versions trpc.group/trpc-go/trpc-agent-go`，版本列表以 `v1.11.2` 结尾；`go list -m -json ...@v1.11.2` 返回发布时间 `2026-08-20T04:03:23Z` 和 `GoVersion: 1.21`。
- GitHub 远端 tag 可访问：tRPC-Agent-Go `v1.11.2`、go-telegram/bot `v1.23.0`、silenceper/wechat `v2.1.14` 均通过 `git ls-remote` 核对。当前网络足以完成本次调研，REST/搜索限流不是结论阻塞项。
- Runner 新候选方案的推荐 cache key 是 `tenant_id + agent_app_id + config_version`；框架 AppName 建议保持稳定为 `tenant/{tenant_id}/app/{agent_app_id}`，不包含配置版本，以便升级后继续原 Session/Memory 命名空间。
- 两个 Worker 不共享内存 Runner；它们按同一配置版本各自创建等价 Runner，并共享 Session/Memory 后端。Runner 内活跃运行不是可迁移状态，Worker 故障接管还需要 Inbox/任务租约和幂等语义。
- tRPC-Agent-Go Session key 同时包含 UserID 和 SessionID。群聊若每个发送者使用不同 Runner userID，会形成不同 Session；首版建议群聊使用合成群主体作为 runner_user_id，真实 actor_user_id 单独用于权限和审计，群聊 Memory 相应为群级作用域。

## 2026-08-24 已确认的阶段 0 决策

- Runner 采用每个 Worker 按 `tenant_id + agent_app_id + config_version` 的有界本地缓存；两个 Worker 不共享 Runner 对象，共享 Session/Memory 后端。
- Telegram 首选 `github.com/go-telegram/bot v1.23.0`，`telegram-bot-api/v5 v5.5.1` 为备选；依赖引入放到阶段 1。
- 企业微信首版按企业内部自建应用验证，先对 `github.com/silenceper/wechat/v2 v2.1.14` 做协议匹配 Spike；真实账号联调后置，Mock 可用于自动化验收。
- 群聊首版使用群主体作为 `runner_user_id`，Session/Memory 为群级作用域；真实 actor 仅用于权限和审计。
- 最小真实交付和 Web UI 边界按 `docs/stage0-acceptance.md` 第 3-5 节执行：核心链路真实编码，Web UI 仅做聊天演示和模拟绑定，生产扩展以文档为主。

## 阶段 1 执行建议

- Runner 需要先验证 singleflight 创建、容量/TTL、版本排空、共享资源引用和 race。
- Telegram 和企业微信候选需要在独立临时模块用 Go 1.21 编译，并验证可替换 HTTP、错误响应和 shutdown 行为。
- 项目最低 Go 版本继续建议 1.21；引入最终确认依赖后需用 Go 1.21 工具链再次构建验证。

## 阶段 1 待验证问题

- `go-telegram/bot` 与 `silenceper/wechat/v2` 的最终协议覆盖和 Adapter 边界。
- Runner 缓存的容量、TTL、AgentFactory 选择、并发安全和优雅关闭。
- 企业微信负责人环境的真实联调时间和凭据保管方式。
- Redis Streams、Inbox/Outbox 和 Session 锁的具体实现及错误语义。
- tRPC-Agent-Go 版本复核后对 `go.mod` 的最终变更。

## 2026-08-24 Phase 1 已验证事实（其中多模块适配器结论已由 Phase 1.5 纠正）

- `trpc.group/trpc-go/trpc-agent-go v1.11.2` 可在 Go 1.21.13 下编译和运行本阶段 Runner/OpenAI 链路。
- 当时只检查根模块发布目录，记录为没有 `session/redis`、`memory/redis` 或 `storage/redis`；Phase 1.5 已按独立 Go module 重新核对，确认这些官方子模块远程发布且可与根模块组合使用。
- Phase 1 平台快照适配曾覆盖 Session 创建/读取/Event 追加/Session State 和 Memory CRUD/关键词搜索；该实现已在 Phase 1.5 删除，当前生产能力由官方 Redis Service 提供。
- 框架 Runner 只关闭自己创建的 Session Service；通过 `WithSessionService` / `WithMemoryService` 注入的共享 Service 为借用资源，Runner 关闭不会误关它们。
- OpenAI SDK 默认会重试部分 5xx。为固定阶段 1 的 `4xx/5xx -> 502` 契约，当前模型构造显式设置 `MaxRetries=0`；平台级重试应与后续 Inbox/Outbox 和幂等一起设计。
- RunnerCache 已验证同 key 100 并发 singleflight、失败共享、创建超时、pending 容量、版本隔离、runtime 隔离、排空超时、Close 幂等和 race。
- `deepseek-v4-flash` 对无 `/v1` 和带 `/v1` 的 DeepSeek Base URL 都真实调用成功，生产示例采用 `https://api.deepseek.com`。
- Telegram SDK 能覆盖消息映射、自定义 HTTP、429 和生命周期；企业微信 SDK 可复用发送、AES 和签名算法，但内部自建应用普通回调仍需平台薄适配。
- Docker 宿主运行正常；仅默认 Codex 沙箱无权访问 `C:\Users\PC\.docker\config.json` 与 `//./pipe/docker_engine`。宿主核验为 Docker Desktop `27.2.0`、Redis `PONG`。
- 真实 Redis Smoke 已验证 Session 和 Memory 跨 runtime 可见，Redis 停止时 Runtime 返回依赖不可用，不回退本地状态。

## Phase 1 保留限制

- Redis 快照适配只适用于当前单进程最小闭环，跨进程并发写可能丢更新，不能直接用于 Phase 4 双 Worker。
- RunnerCache 最大 32 项等参数是 Phase 1 锁定值；当前只有一个预置 Agent，容量与资源占用未形成真实租户规模数据，后续必须基于指标重新校准。
- 两种 IM 仍只有 SDK/协议 Spike，没有真实 Adapter 或真实账号联调。
- 当前没有 Inbox 去重、队列、SQL、多租户配置仓储、双 Worker、Web UI 或 OpenTelemetry 后端。

## 2026-08-24 Phase 1 审阅与真实 Redis 复核

- Phase 1 已通过外部审阅，可进入 Phase 2；审阅确认其完成边界是单进程最小真实闭环，不是完整生产平台。
- `miniredis` 是纯内存 Redis 服务端模拟，不落盘；`go-redis` 对它仍走 Redis 协议，但它不能覆盖真实 Redis 进程、网络或 Docker 运行特性。
- Docker `trpc-redis` 使用 `redis:7-alpine`，宿主端口 `6379`；真实服务配置 `REDIS_URL=localhost:6379` 可正常连接，`/readyz`、Session/Memory 写入和跨 runtime 读取已实际验证。
- 真实演示数据已在验证后清理；当前容器未挂载持久化卷，因此不能把 Phase 1 Smoke Test 表述为持久化验收。
- `REDIS_URL` 服务入口接受简写并规范化；`REDIS_SMOKE_URL` 测试入口目前要求完整 URI，阶段 2 可统一输入规则。
- Docker 访问采用逐命令临时提权策略。默认沙箱不变；不修改 Codex 全局配置，不开启长期 `danger-full-access`。

## Phase 2 入口问题

- Binding Registry 首版是否采用“接口 + 预置配置实现”，而不是立即引入 SQL；这是当前最保守且不扩大依赖的建议。
- Tenant/AgentApp/ChannelBinding/ConfigVersion 的最小字段是否只包含标识、状态、版本和 `credential_ref`，不保存明文凭据。
- `/api/v1/demo/messages` 是否保留兼容层，并在内部转换为统一消息模型；建议保留，避免 Phase 1 演示回归。
- Phase 2 是否必须加入两个逻辑租户的隔离验收；建议必须加入，即使暂时仍只有一个 Agent 类型。
- Telegram/企业微信 Adapter 是否严格留到 Phase 3；当前既有阶段顺序是 Phase 2 先做平台契约与绑定，Phase 3 再做真实 IM。
