# Phase 2 执行计划

## 2026-08-28 收口任务

- [x] A. 删除 `ChannelBinding.CredentialRef`、两段死校验并补严格 JSON 回归测试
- [x] B. 按逻辑 Plan 统一 Phase 3/4/5 编号、职责和 Phase 2 审阅状态
- [x] D. 创建被忽略的真实模型 Smoke 配置并完成自动化验证
- [x] D. 使用临时 Redis 7 完成 DeepSeek 两租户端到端 Smoke
- [x] 收尾清理服务、临时容器和 Codex 设置的用户级环境变量，保留模型 Key

方案 C 暂缓：本轮不暂存、不提交、不推送，也不开始 Phase 3。

## 当前目标

把 Phase 1.5 的固定单租户 Runtime 升级为基于只读预置目录、可信 ChannelBinding、StorageProfile 和 RunnerRegistry 的多租户执行内核，同时保持现有 CLI/HTTP 契约兼容。

## 当前阶段

- [x] 1. 审阅并锁定 Phase 2 配置契约、接口、失败语义和验收矩阵
- [x] 2. 核验 Phase 1.5 基线并创建 `codex/phase2-multi-tenant-runner`
- [x] 3. 实现领域模型、JSON Loader、凭据解析和 PresetRepository
- [x] 4. 实现统一消息契约、BackendProvider 和 InMemory/Redis Profile
- [x] 5. 实现 RunnerRegistry、LRU 和多租户 Runtime
- [x] 6. 完成配置、隔离、版本切换、HTTP 和真实 Redis 验收
- [x] 7. 回写 README、阶段结果、工作记录和交接摘要

## Phase 2 固定边界

- 配置来源为可选 `PLATFORM_CONFIG_FILE` 指向的只读 JSON；未设置时保持 Phase 1 环境变量兼容。
- JSON 只保存 `env:<NAME>` 凭据引用；所有启用配置的凭据在启动时校验。
- Demo HTTP 请求只提交 `binding_id`，内部派生租户、Agent、版本和 StorageProfile。
- 存储后端只实现官方 InMemory/Redis；Redis 不可用不回退 InMemory。
- 配置版本、模型或 StorageProfile 变化必须生成新版本；后端切换不迁移、不双读。
- 不实现真实 IM、Streams、Inbox/Outbox、双 Worker、SQL Repository、Web UI 或配置热加载。

## Phase 2 执行错误

| 错误 | 尝试 | 处理 |
| --- | --- | --- |
| 定向 `go test` 读取用户级 `go-build` 缓存被沙箱拒绝 | 1 | 改用已忽略的 `.stage15-cache/gocache` 工作区缓存重跑，不扩大权限 |
| Runtime 多后端重构后 executor 白盒测试仍访问单一 `runtime.backend` 和旧 `newRunner` | 1 | 增加测试用绑定后端解析 helper，并按新 ConfigVersion/凭据签名构造 Runner |
| “所有启用版本凭据”测试未触发错误 | 1 | 测试字符串替换未命中紧凑的 `}],` 结束符；修正精确插入点后重跑 |
| `rg` 直接搜索 module cache 的 `miniredis*` 通配目录在 Windows 解析失败 | 1 | 先用 PowerShell 解析实际模块目录，再把明确路径交给 `rg` |
| readiness/模型 URL 修正的大补丁 hunk 边界格式错误，未应用 | 1 | 拆成领域、配置、测试和记录四个小补丁逐一应用 |
| `go build ./...` 成功但尝试写全局 module stat cache 出现 Access denied 警告 | 1 | 使用工作区隔离 GOPATH/GOMODCACHE/GOCACHE 重跑，避免不干净的关闭证据 |
| Go 1.21.13 复验仅 version 使用绝对路径，test/build 误调用全局 Go 1.26.4 | 1 | test/build 同样使用工具链绝对 `go.exe`，保持 GOROOT 与编译器一致 |
| 宿主 `dockerDesktopLinuxEngine` named pipe 不存在 | 1 | 确认 Docker 服务停止且 Desktop 已安装；经批准启动 Docker Desktop 后等待引擎就绪 |
| 最终真实 Redis Smoke 首次构建 `fmt` 仅返回 `compile.exe: exit status 1` | 1 | 测试尚未执行；改用已成功缓存并将 `GOMAXPROCS/GOFLAGS -p` 降为 1 重跑，规避 Windows 并行编译资源波动 |
| `start.sh` 只启动无参数二进制，进程打印版本后退出 | 1 | 改为执行 `trpc-service serve "$@"`，使 README 的 legacy/JSON 启动方式真实可用 |
| `bash -n` 调用到未安装 WSL 的 Windows bash 占位程序 | 1 | 检查 Git for Windows Bash 的明确路径；若不存在则记录本机无法执行 shell 脚本验收 |
| Phase 2 收口大补丁已完成写入但外层工具持续等待无返回 | 1 | 终止等待后逐文件核对；代码、文档、忽略规则和 Smoke 配置均完整应用，无半写入文件 |
| Phase 2 收口验收记录大补丁仅部分应用且外层持续等待 | 1 | 终止后逐文件检查，只对缺失的 Phase 2 Plan 和交接摘要使用小补丁补齐，不重复修改已成功文件 |
| 宿主 `Start-Process` 成功但工具未回显预期 PID/日志路径 | 1 | 未重复启动；通过 8080 端口和可执行路径定位唯一服务进程 PID 1340，确认三个临时 User 变量与模型 Key 均存在 |

# Phase 1.5 执行计划

## 当前目标

验证 tRPC-Agent-Go 官方 Redis/SQL 独立子模块与根模块 `v1.11.2` 的实际兼容性，形成唯一 A/B/C 结论，并在同一阶段完成选定 Redis 方案的生产接入与 Phase 1 回归。

## 当前阶段

- [x] 1. 审阅并锁定 Phase 1.5 范围、依赖矩阵、错误语义和关闭门槛
- [x] 2. 在隔离消费者模块验证 Redis/SQL 发布版本、接口和 Go 1.21
- [x] 3. 完成官方 Redis Session/Memory 的 miniredis 与真实 Redis 契约
- [x] 4. 完成 MySQL 静态核验和真实 PostgreSQL 最小契约
- [x] 5. 形成 A/B/C 结论并接入生产 RedisBackend
- [x] 6. 完成定向、race、build、vet、Go 1.21 与真实服务回归
- [x] 7. 回写结果文档、README、逻辑计划、发现、进度与交接摘要

## Phase 1.5 关闭结论

- 唯一决策为 B：官方 Redis Session/Memory + 平台 `RedisBackend` 生命周期薄包装。
- 旧快照适配器和 `ErrUnsupported` 已删除；没有迁移、双读或 fallback。
- 真实 Redis 7、真实 PostgreSQL 16、MySQL sqlmock、生产全量 test/race/build/vet 和 Go 1.21.13 复验完成。
- SQL 子模块未进入生产依赖；PostgreSQL Summary 时区问题和 MySQL Session vet 问题记录在 `docs/stage1.5-storage-spike.md`。

## Phase 1.5 固定边界

- 成功路径优先为 B：官方 Redis Service 加平台生命周期薄包装；平台不重写 `session.Service` / `memory.Service` CRUD。
- SQL 只验证兼容性：PostgreSQL 必须运行真实最小契约，MySQL 只做发布模块、接口、schema 和 sqlmock/静态核验；两者都不进入生产依赖。
- 不实现 Tenant/AgentApp/ChannelBinding/StorageProfile、RunnerRegistry、IM、Redis Streams、双 Worker 或 SQL 平台仓储。
- Phase 1 快照键不迁移、不双读，不允许新旧实现混合写；成功接入后删除快照适配器且不保留隐式 fallback。
- 若官方 Redis 不能满足兼容性、原子性或 Runner 契约且薄包装无法修复，则选择 C 并阻塞 Phase 1.5，不扩展快照适配器。

## Phase 1.5 执行错误

| 错误 | 尝试 | 处理 |
| --- | --- | --- |
| Windows `rg` 将路径中的 `*_test.go` 识别为非法文件名 | 1 | 改用目录路径并通过 `-g '*_test.go'` 过滤，不再传路径通配符 |
| MySQL 发布包编译测试把 10 个 Memory option 错误断言为 9 个 | 1 | 修正本地测试夹具计数；模块下载和编译本身已成功，复跑确认 |
| Go 1.21.13 工具链下载默认写入只读全局 GOMODCACHE | 1 | 单命令切换到 `.stage15-cache/mod` |
| 工作区缓存下载被全局 `GOSUMDB=off` 阻止校验 | 2 | 第三次命令临时设置 `GOSUMDB=sum.golang.org`，不修改全局配置 |
| sumdb 校验仍尝试写旧用户级 `D:\\go\\gopath\\pkg\\sumdb` | 3 | 改为整条命令隔离 `GOPATH`，使 toolchain、module cache 与 sumdb 全部位于工作区缓存 |
| 首次生产 `go mod tidy` 并行下载触发 Windows 虚拟内存上限 | 1 | 复用已校验的工作区模块缓存并设置 `GOMAXPROCS=1`、`GOFLAGS=-p=1` 后成功 |
| PostgreSQL Event 用例使用空 Response，读取时按框架规则被过滤 | 1 | 改为含真实用户消息的有效 Event，随后事件和并发契约通过 |
| PostgreSQL 索引统计条件漏掉 App/User State 表 | 1 | 改为统计全部 `stage15_%` 专用表；自动建表和至少 22 个索引通过 |
| PostgreSQL Summary 默认跨连接读取为空 | 1 | 直接读取行并定位为 `TIMESTAMP` 本地墙钟与 UTC Event 时间相差 8 小时；保留可复现测试和已知限制 |
| MySQL Session 发布包默认 `go test` 被 `%w` 日志格式 vet 检查阻断 | 1 | 记录上游静态缺陷；`-vet=off` sqlmock 普通/race 均通过，生产不引入该模块 |
| Phase 1.5 容器复验首次使用默认 `GOCACHE`，沙箱无法读取用户级 Go 构建缓存 | 1 | 将 `GOCACHE`、`GOPATH`、`GOMODCACHE` 固定到被忽略的 `.stage15-cache/` 后重跑；该错误发生在测试 setup，不是 Redis 契约失败 |
| Runner 中途 Redis 断开时上游记录持久化错误但仍可能返回模型回复 | 1 | 不伪造 `503` 结论；平台仅对显式返回的存储错误做健康探测，已知限制和后续 Inbox/幂等要求写入结果文档 |
| 显式调用 Go 1.21.13 时继承 Go 1.26 `GOROOT`，标准库缓存报告版本不匹配 | 1 | 将 `GOROOT` 指向 1.21.13 toolchain 根目录并使用独立 `GOCACHE` 后重跑；该错误不是生产源码兼容性失败 |
| Redis Memory Spike 断言按时间排序把更新后的 Entry 固定在首位，普通模式偶发失败 | 1 | 改为按关键词检索更新后的 Entry；普通/race 与 `-count=30` 均通过，属于测试夹具排序假设而非官方服务回归 |

## Phase 1 历史记录

## 目标

交付 `HTTP Demo -> executor -> tRPC-Agent-Go Runner -> Redis Session/Memory -> Agent 回复` 的单进程最小闭环，并完成依赖、RunnerCache、Telegram 和企业微信 Spike。

## 阶段

- [x] 1. 在临时模块验证 tRPC-Agent-Go、DeepSeek、Telegram 和企业微信 SDK
- [x] 2. 锁定 Go 1.21 与生产依赖版本
- [x] 3. 实现配置、身份派生、RunnerCache 和 Redis 最小适配
- [x] 4. 实现 executor、CLI 和 HTTP 三个接口
- [x] 5. 补齐 Mock、错误映射、生命周期、race 和脱敏测试
- [x] 6. 完成真实 Redis、真实 DeepSeek 和 Go 1.21 验证
- [x] 7. 回写 Spike、Phase 1 plan、进度、发现和交接摘要

## 阶段边界

- 不实现真实 IM Adapter、Redis Streams、Inbox/Outbox、SQL、双 Worker、Web UI 或 OpenTelemetry 后端。
- `message_id` 只作为链路字段，不提供幂等。
- 群聊只提供群主体 ID 派生函数，不开放群聊 HTTP。
- Phase 1 Redis 快照适配不冒充完整生产后端；未支持方法显式 fail closed。

## 已处理问题

| 问题 | 处理 |
| --- | --- |
| Phase 1 历史上只检查根模块，误判 tRPC-Agent-Go `v1.11.2` 没有可导入的 Redis Session/Memory 实现 | Phase 1.5 按独立 Go module 重新核对并纠正；生产改用官方 Redis 子模块与 `RedisBackend` 薄包装 |
| 默认 Codex 沙箱无法访问 Docker named pipe | 在宿主上下文核验 Docker；明确不是 Windows/Docker 故障 |
| Go 1.21 toolchain 下载被全局 `GOSUMDB=off` 阻止 | 仅在验证命令中临时启用 `sum.golang.org`，不修改全局配置 |
| 超时 Mock 永久等待服务端请求 context，导致测试自身阻塞 | 改为有界延迟 Mock，保留 504 验证且测试有确定上界 |
| OpenAI SDK 默认重试会让 5xx 在短超时下变成 504 | Phase 1 显式关闭 SDK 自动重试，固定 4xx/5xx -> 502；重试留到可靠性阶段 |

## Phase 1 审阅后状态

- Phase 1 已通过外部审阅，可进入 Phase 2。
- `miniredis` 仅用于自动化测试；Docker Redis 已通过真实服务 `/readyz`、Session/Memory 写入和跨 runtime 读取验证。
- 需要 Docker 时保持 workspace-write，只对明确的单条 Docker 命令临时提权，不开启长期 `danger-full-access`。

## Phase 2 准备任务

- [ ] 确认 Binding Registry 的 Phase 2 存储实现（建议 Repository 接口 + 预置配置实现）
- [ ] 确认 Tenant/AgentApp/ChannelBinding/ConfigVersion 最小字段与凭据引用边界
- [ ] 确认 Demo HTTP 兼容层与统一内部消息模型的迁移方式
- [ ] 编写并审阅 `多租户与节点部署--phase2的plan.md`
- [ ] 实现可信绑定解析、跨租户拒绝和 Runner Registry 选择
- [ ] 完成 Phase 2 定向测试后再进入真实 IM Adapter 阶段
