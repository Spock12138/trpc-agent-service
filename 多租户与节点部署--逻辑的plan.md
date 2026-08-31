# 多租户与节点部署——逻辑 Plan（`trpc-agent-service` 施工版）

## 1. 总体目标与固定方案

目标是把当前代码骨架建设成一条可运行、可演示、可验证的多节点 Agent 链路：

```text
Telegram / 企业微信 / Web 调试入口
  -> Channel Adapter
  -> Gateway 验签与可信租户解析
  -> Redis Streams 任务队列
  -> Worker 领取任务
  -> RunnerRegistry 选择或创建 Runner
  -> tRPC-Agent-Go Agent 执行
  -> Redis 共享 Session / Memory
  -> Redis 回复队列
  -> Gateway 出站 Adapter
  -> 外部 IM 或 Web UI
```

固定采用以下方案：

- 当前仓库 `trpc-agent-service` 是实际开发仓库，tRPC-Agent-Go 作为锁定版本的外部依赖。
- 不直接依赖或包装 OpenClaw API。
- Gateway 和 Worker 使用同一代码仓库的不同启动角色。
- Gateway 负责渠道接入、验签、租户解析、幂等认领和回复发送。
- Worker 只负责任务执行，不保存唯一会话状态。
- Runner 缓存键固定为：

```text
tenant_id + agent_app_id + config_version
```

- 每个 Worker 拥有自己的 Runner 缓存，Runner 不跨进程共享。
- Session 和 Memory 使用 Redis 共享后端，保证跨 Worker 可见。
- Redis Session/Memory 固定复用已通过 Phase 1.5 验证的官方子模块，由平台 `RedisBackend` 负责延迟初始化、健康恢复、命名空间和生命周期；平台不重写 CRUD。
- PostgreSQL 保存租户、Agent 配置、ChannelBinding、审计和发布记录。
- Gateway 与 Worker 使用 Redis Streams Consumer Group。
- 可靠性采用“至少一次投递 + 幂等处理”，不宣称分布式绝对只执行一次。
- 回复统一由 Gateway 出站，Worker 不直接持有 IM 凭据。

## 2. 最小真实交付范围

必须编码并验证：

- 真实 tRPC-Agent-Go Agent/Runner 调用。
- `gateway` 和 `worker` 两个启动角色。
- Redis Streams 任务投递、Consumer Group 领取、确认、租约恢复和重试。
- Redis 共享 Session/Memory。
- Tenant、AgentApp、配置版本和 ChannelBinding。
- 外部消息到统一消息模型的转换。
- Telegram 真实适配器。
- 企业微信自建应用回调适配器，首版支持文本消息、验签、解密、解析和回复。
- Inbox 消息去重和跨租户拒绝。
- 基础工具白名单、IM 用户权限检查、敏感信息脱敏。
- Audit Log、基础指标和完整 Trace 链路。
- 轻量 Web 聊天页面、健康检查和 Docker Compose。
- 同机一个 Gateway、两个 Worker 的故障接管演示。

以设计文档为主：

- SQL Session/Memory 后端。
- 向量库、对象存储和外部 Memory 服务。
- Redis 到 SQL 的在线双写、灰度切换和自动回滚。
- Kubernetes、跨机房容灾和大规模调度。
- 完整预算计费、复杂配额和生产级灰度发布。
- 其他 IM 的全面接入。

## 3. 核心接口与数据边界

平台层定义统一模型：

- `InboundMessage`：渠道、外部账号、平台消息 ID、外部用户、会话、文本/附件、接收时间、Trace ID。
- `OutboundMessage`：渠道、绑定 ID、目标用户/群、消息内容、任务 ID、幂等键。
- `ChannelAdapter`：`Verify`、`DecodeInbound`、`SendOutbound`。
- `ChannelBindingResolver`：根据已验签的渠道账号解析 `tenant_id + agent_app_id + credential_ref`。
- `TaskQueue`：发布、领取、确认、重新认领、失败重试。
- `RunnerRegistry`：按配置版本缓存 Runner，支持并发创建保护、TTL/LRU 淘汰和旧版本排空。
- `SessionStore` / `MemoryStore`：统一读写接口，Redis 为首版实际实现。

核心持久化数据至少包括：

```text
tenant
agent_app
agent_app_config
channel_binding
session
session_event
memory_item
inbox_message
outbox_reply
audit_log
```

Redis 负责队列、Inbox 状态、Session/Memory、Session 锁、租约和临时确认状态；PostgreSQL 负责长期配置、绑定和审计。

## 4. 分阶段施工

### 阶段 0：仓库基线与依赖锁定

- 读取 README、源码、贡献约束和最新上游状态。
- 确认 tRPC-Agent-Go 最新稳定版本、实际 Runner/Session/Memory API 和最低 Go 版本。
- 在 `go.mod` 锁定具体版本。
- 选定 Telegram SDK，并固定企业微信自建应用回调协议范围。
- 输出架构图、消息时序图、数据模型、缺口清单和验收矩阵。

阶段门槛：依赖版本、IM 范围、Redis/PostgreSQL 边界和最小链路必须明确。

### 阶段 1：真实 Agent 最小闭环

- 启动一个真实 Agent/Runner。
- 提供 `/healthz`、`/readyz` 和本地 Demo 入站接口。
- 预置一个租户、一个 Agent App 和一个 ChannelBinding。
- 完成“Demo 消息 -> Gateway -> 一个 Worker -> Agent 回复”的真实链路。
- Session 使用 Redis，不使用进程内状态作为正式实现。

### 阶段 1.5：官方多后端兼容性与 Redis 生产接入（已完成）

- 隔离验证根模块 `v1.11.2` 与 Redis/SQL 独立子模块及 Go 1.21.13。
- 真实 Redis 7 验证 Lua、并发、跨实例、故障与恢复；真实 PostgreSQL 16 验证 schema、Session/Memory 和跨连接契约。
- 选择 B：官方 Redis Service + 平台生命周期薄包装；生产键位于 `<REDIS_KEY_PREFIX>:official-v1`。
- 删除 Phase 1 快照适配器，不迁移、不双读、不保留 fallback；SQL 子模块不进入生产依赖。
- PostgreSQL Summary 时区问题和 MySQL Session vet 问题记录为上游限制，详见 `docs/stage1.5-storage-spike.md`。

### 阶段 2：多租户与 RunnerRegistry

- 实现 Tenant、AgentApp、配置版本和 ChannelBinding 查询。
- 服务端只从绑定关系解析租户，拒绝外部伪造的 `tenant_id`。
- Worker 收到任务后按 `tenant_id + agent_app_id + config_version` 查找或创建 Runner。
- 同一 Worker 复用 Runner；不同 Worker 各自创建本地实例。
- 新配置生成新版本，旧 Runner 等待执行结束后按 TTL/LRU 淘汰。
- 未知租户、未知 Agent、配置冲突和后端不可用全部 fail closed。

### 阶段 3：Gateway/Worker 消息可靠性

- Redis Streams 建立 `agent.tasks` 和 `agent.replies`。
- Worker 使用 Consumer Group 领取任务。
- 任务包含 `task_id、tenant_id、agent_app_id、config_version、session_id、message_id、trace_id、attempt`。
- Inbox 唯一键为：

```text
tenant_id + channel_binding_id + platform_message_id
```

- 实现 `processing/succeeded/failed` 状态、租约超时、重新认领和指数退避重试。
- Worker 产出回复事件后再确认任务；回复单独进入出站队列。
- 回复失败由 Gateway 重试，使用幂等键减少重复发送。

### 阶段 4：双 Worker 与共享状态

- 通过 Redis 锁或版本号保证同一 Session 串行处理。
- 验证两个 Worker 都能领取任务。
- 停止一个 Worker 后，另一个 Worker 能重新认领未完成任务。
- 验证同一 Session 跨 Worker 连续可见。
- 验证重复消息不会重复触发 Agent。
- 明确模型超时、工具失败、Redis 暂时不可用和回复失败的处理结果。

### 阶段 5：Telegram、企业微信与 Web UI

- Telegram 使用真实 SDK 接入统一 Adapter。
- 企业微信实现自建应用回调的签名校验、解密、文本解析和回复。
- Mock 请求必须来自官方协议或固定测试向量。
- Web UI 只提供聊天、预置绑定选择和回复展示；不允许直接提交可信 `tenant_id`。
- 首版只实现文本消息，附件、流式消息和复杂卡片列为后续扩展。

### 阶段 6：治理、审计与观测

- 实现租户/Agent 工具白名单和 IM 用户权限。
- 日志、Trace 和错误信息统一脱敏。
- 记录租户维度的请求量、延迟、错误、Token、工具调用和 IM 投递指标。
- Trace 串联 IM 回调、租户解析、Inbox、Runner、Tool、Session/Memory 和回复。
- Audit Log 至少记录租户、渠道、用户、Session、Agent、工具、决策、延迟、错误、成本和 Trace ID。
- 危险操作二次确认首版只支持一个示例工具，确认状态放在 Redis，使用 TTL 和原子一次性消费。

### 阶段 7：部署与最终验收

- 提供 Compose：Gateway、Worker×2、Redis、PostgreSQL。
- 提供预置配置、启动命令、演示脚本和故障演示步骤。
- 补充容量估算、后端迁移、灰度发布和 Kubernetes 设计文档。
- 完成 README、架构图、时序图、数据模型、风险清单和最终验收记录。

## 5. 测试与验收

自动化测试不得依赖真实 IM 账号、公网回调或外部凭据。

必须覆盖：

- Telegram 与企业微信统一消息契约。
- 企业微信签名、时间戳、nonce、解密失败和非法请求。
- ChannelBinding、伪造租户和跨租户访问拒绝。
- Runner 缓存命中、并发创建、配置版本切换和淘汰。
- Redis Streams 并发认领、重复投递、租约恢复和失败重试。
- Session 锁、跨 Worker 可见性和 Worker 故障接管。
- 工具权限、脱敏、审计和 Trace。
- Compose 启停及 Redis、PostgreSQL 短暂不可用场景。

## 6. PR 边界与默认假设

建议拆分为五组可审查提交：

1. 真实 Agent、统一消息模型和 Demo 闭环。
2. 多租户绑定、配置版本和 RunnerRegistry。
3. Redis Streams、Inbox、重试和双 Worker。
4. Telegram、企业微信、Web UI 和契约测试。
5. 治理、观测、Compose、文档和故障验收。

默认假设：

- 企业微信首版采用自建应用回调，不实现群机器人、微信客服和公众号协议。
- Redis 是首版多节点闭环的强制依赖。
- PostgreSQL 只承担配置、绑定和审计，不阻塞第一条 Agent 主链路。
- 所有新增公共接口先保持平台内部使用，确认有外部消费者后再导出。
- 本计划通过审阅后，再为每个阶段编写源码级规格；本计划本身不直接替代阶段规格。
