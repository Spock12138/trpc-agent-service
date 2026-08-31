# 基于 tRPC-Agent-Go 的多租户节点化 Agent 部署平台

> 方案提交版：v1.0
> 复核日期：2026-08-31
> 代码基线：`b1ae13d`（Phase 2 多租户内核）
> 方案状态：Phase 2 已实现并验收；Phase 3–7 按本文设计实施

## 1. 设计目标与范围

本方案面向多个部门或业务线共享 Agent 能力的场景。平台需要让不同租户独立配置 Agent、模型、工具、IM 账号和数据后端，同时允许 Gateway、Worker 水平扩展。核心目标是：租户身份可信、Worker 尽量无状态、会话和记忆跨节点可见、消息至少一次投递但业务结果幂等、故障可恢复且全链路可审计。

首版采用“真实核心链路优先、生产扩展以设计为主”的范围。必须落地真实 tRPC-Agent-Go Runner、可信 ChannelBinding、Redis 共享 Session/Memory、Redis Streams、双 Worker、Telegram 与企业微信适配器、基础治理和可观测性。SQL 迁移、向量库、对象存储、Kubernetes、复杂计费和完整管理后台先提供设计，不阻塞首版演示。

## 2. 核心设计结论

| 关注点 | 方案 | 原因 |
| --- | --- | --- |
| 租户隔离 | 服务端只从已验签的 `ChannelBinding` 派生 `tenant_id + agent_app_id + config_version` | 不信任请求正文、浏览器参数或 IM 文本中的租户字段 |
| Runner | 每个 Worker 按 `tenant_id + agent_app_id + config_version` 使用有界本地缓存 | Runner 不跨进程共享，避免隐式状态；Session/Memory 放共享后端 |
| Session 路由 | 不使用 sticky session；官方 Redis Session/Memory 作为共享状态 | 任一 Worker 都能处理任务，故障时可由其他 Worker 接管 |
| 消息可靠性 | Redis Streams Consumer Group + 租约 + 至少一次投递 + Inbox 幂等 | 不宣称分布式绝对只执行一次，避免重复副作用 |
| 回复发送 | Worker 只生成统一出站事件，Gateway 负责 IM 投递和重试 | IM 凭据、限流和协议细节留在入口边界 |
| 数据分工 | Redis 负责队列、租约、Inbox、Session/Memory；PostgreSQL 负责配置、绑定和审计 | 热状态与长期配置分离，后续可独立扩展 |
| IM | Telegram + 企业微信自建应用回调；统一 `ChannelAdapter` | 复用成熟 SDK，平台层只负责协议适配和统一消息模型 |

## 3. 系统架构

```mermaid
flowchart LR
    TG[Telegram] --> CA[Channel Adapter]
    WX[企业微信回调] --> CA
    WEB[Web 调试入口] --> CA
    CA --> G[Gateway<br/>验签/解密/Binding/Inbox]
    G --> TS[(Redis Streams<br/>agent.tasks)]
    TS --> W1[Worker A]
    TS --> W2[Worker B]
    W1 --> RR[RunnerRegistry]
    W2 --> RR
    RR --> R[tRPC-Agent-Go Runner]
    R --> PG[Plugin / Guardrail<br/>Tool 白名单与审批]
    R --> SS[(Redis Session / Memory)]
    R --> O[统一 Outbound Event]
    O --> RS[(Redis Streams<br/>agent.replies)]
    RS --> G
    G --> CA
    CA --> TG
    CA --> WX
    G --> DB[(PostgreSQL\n租户/配置/绑定/审计)]
    G -.-> OT[OpenTelemetry<br/>Trace / Metrics]
    W1 -.-> OT
    W2 -.-> OT
```

Gateway 负责渠道协议、可信身份解析、Inbox 认领、任务发布和 Outbox 投递；Worker 只负责领取任务、选择 Runner、消费 Event 和写入统一回复。Admin/配置层发布不可变配置版本，Storage Adapter 根据租户选择 InMemory 或 Redis；生产多节点不把 InMemory 作为共享状态。

## 4. 核心消息时序

```mermaid
sequenceDiagram
    participant U as 企业微信用户
    participant A as WeCom Adapter
    participant G as Gateway
    participant Q as Redis Streams
    participant W as Worker
    participant R as Runner
    participant S as Session/Memory
    participant T as Tool/Guardrail
    participant O as Outbox

    U->>A: 回调消息
    A->>A: 校验 timestamp/nonce/signature，AES 解密
    A->>G: InboundMessage(trace_id)
    G->>G: ChannelBinding 解析租户和配置版本
    G->>G: 原子创建/读取 Inbox
    G->>Q: XADD agent.tasks
    G-->>A: 快速确认回调
    Q-->>W: Consumer Group 领取任务
    W->>R: Acquire(tenant, app, version)
    R->>S: 读取 Session/Memory
    R->>T: 工具白名单与 Guardrail 检查
    T-->>R: 允许、拒绝或需确认
    R->>S: 写入 Event/State/Memory
    R-->>W: Event(progress/final/error)
    W->>O: 持久化回复事件并更新执行状态
    W->>Q: ACK task
    O-->>G: 出站投递任务
    G->>A: SendOutbound + 幂等键
    A-->>U: 文本或降级后的消息
```

同一 `trace_id` 贯穿回调、Binding、Inbox、Worker、Runner、Tool、Session/Memory 和 IM 回复；`request_id` 用于客户端响应和日志检索。Agent 已执行与 IM 已送达是两个状态，投递失败只重试 Outbox，不默认重新执行 Agent。

## 5. 数据模型与后端策略

核心长期实体包括：`tenant`、`agent_app`、`agent_app_config`、`channel_binding`、`session`、`session_event`、`memory_item`、`summary`、`inbox_message`、`outbox_reply`、`audit_log`。关键关系是：Tenant 1:N AgentApp，AgentApp 1:N ConfigVersion，Binding N:1 Tenant/AgentApp，Session 绑定 Tenant/App/用户，Event、Memory、Summary 绑定 Session 或 Runner 作用域。

建议的最小字段如下：

```text
tenant(id, status, audit_policy)
agent_app(tenant_id, id, active_config_version, status)
agent_app_config(tenant_id, app_id, version, model, storage_profile, tool_policy)
channel_binding(id, channel, external_account_id, tenant_id, app_id, status)
session(tenant_id, app_id, runner_user_id, session_id, version, updated_at)
session_event(session_id, sequence, role, payload_ref, occurred_at, trace_id)
memory_item(tenant_id, app_id, runner_user_id, key, content_ref, updated_at)
inbox_message(tenant_id, binding_id, platform_message_id, state, attempt, lease_until)
outbox_reply(delivery_id, inbox_id, sequence, idempotency_key, state, last_error)
audit_log(tenant_id, channel, actor_user_id, session_id, agent_name, tool_name,
          decision, latency, error_type, cost, trace_id, created_at)
```

Redis 使用带版本的租户前缀，保存 Streams、Inbox 状态、租约、Session/Memory 和短期确认状态；PostgreSQL 保存配置、绑定、发布记录和审计。向量库适合 Knowledge 检索索引，对象存储适合附件和大文件，SQL/Redis 不保存长期明文密钥。Redis 迁移到 SQL 时采用导出快照、校验计数、按租户灰度切换和可回滚版本，不做隐式双读；向量库迁移采用重建索引和校验和比对。

Phase 1.5 对官方 Redis、MySQL、PostgreSQL 子模块进行隔离验证时发现的兼容性和并发问题，已分别向上游提交反馈；截至本方案复核日，相关 issue/PR 均已由官方修复并合并。本项目继续使用官方发布模块，不维护本地分叉；历史复现、影响范围和规避记录见 [docs/upstream-issues.md](upstream-issues.md)。

## 6. 幂等、并发与故障恢复

Inbox 唯一键固定为 `tenant_id + channel_binding_id + platform_message_id`，首次认领必须原子完成。任务状态为 `received → enqueued → processing → executed → delivery_pending → completed`，失败进入 `retry_wait` 或 `failed_terminal/dead_letter`。Worker 使用有期限租约，Redis Streams 通过 `XACK`、Pending 查询和 `XAUTOCLAIM` 完成接管。可重试错误包括临时 Redis、模型超时、IM 429/5xx；验签失败、未知 Binding、权限拒绝和非法消息直接终止。

Phase 3 先验证单 Worker 的投递和恢复语义，不承诺 Streams 的全局 Session 顺序；Phase 4 增加 Session 级 Redis 锁或版本条件写入，保证同一 Session 串行处理。Worker 取消时调用 `context.Cancel`，停止产生副作用并排空 Runner Event channel；Runtime 关闭按 RunnerRegistry、后端、健康检查客户端的顺序执行，避免 goroutine 和连接泄漏。

## 7. IM、治理与安全

Telegram 适配器使用 Bot API 长轮询或 Webhook；企业微信首版实现自建应用回调的签名校验、AES 解密、文本解析和回复。两者都转换为统一 `InboundMessage/OutboundMessage`，由 Adapter 处理长度、频率、Markdown/HTML、附件和异步回复差异。单聊以绑定和会话主体生成 HMAC 身份；群聊以群主体作为 `runner_user_id`，真实 `actor_user_id` 只用于权限和审计。

Plugin/Guardrail/Callbacks 提供工具白名单、敏感信息脱敏、IM 用户权限、预算检查和危险操作二次确认。日志、Trace、错误响应和审计统一过滤模型 Key、IM Token、数据库密码、Authorization 和长期附件 URL。OpenTelemetry 记录 IM 回调到回复的同一 Trace，并按租户统计请求量、延迟、错误率、Token、工具耗时、投递成功率和后端延迟。

## 8. 预期效果与时间规划

预期效果是：相同外部用户在不同租户下的 Runner、Session、Memory 完全隔离；Worker 无需 sticky session，增加实例即可扩展；重复平台消息不产生第二个 Inbox 业务结果；Worker 故障后的接管时间受租约和认领周期约束；所有关键决策均可通过 Trace 和 Audit Log 追溯。上述为设计目标，生产容量和延迟需在 Phase 7 压测后定标。

| 阶段 | 预计投入 | 交付与门槛 |
| --- | --- | --- |
| Phase 0–2 | 已完成 | 依赖、Runner、Redis、配置、租户隔离和真实 Smoke |
| Phase 3 | 3–5 个工作日 | Streams、Inbox、租约、重试、ACK 顺序和重启恢复 |
| Phase 4 | 3–5 个工作日 | 双 Worker、Session 串行、故障接管和跨节点可见 |
| Phase 5 | 5–7 个工作日 | Telegram、企业微信协议适配、Web UI 和契约测试 |
| Phase 6 | 4–6 个工作日 | 工具治理、脱敏、审计、指标和 Trace |
| Phase 7 | 3–5 个工作日 | Compose、故障演示、容量估算和最终文档 |

企业微信真实账号联调单独跟踪，不作为本地 Mock 契约验收的前置条件；实现过程中同步更新文档和故障证据。

## 9. 风险与缓解措施

1. Redis Streams 只提供至少一次语义：使用 Inbox/Outbox 幂等和明确 ACK 顺序。
2. 同一 Session 并发写入：Phase 4 使用 Session 锁或版本号，并覆盖故障接管测试。
3. 企业微信凭据或公网回调不可用：先完成可追溯官方协议 Mock，真实联调单独记录。
4. IM 限流、长度和异步回复差异：Adapter 做节流、拆分、聚合和降级。
5. Redis 短暂不可用：严格 readiness，消息不回退本地状态，按策略重试。
6. Runner 缓存过大或旧版本未排空：有界 LRU、TTL、引用计数和 Close 幂等。
7. 工具副作用在重试中重复：工具幂等键、审批状态 TTL 和执行结果持久化。
8. 脱敏遗漏造成凭据泄露：统一脱敏层覆盖日志、Trace、错误和审计，并做回归扫描。
9. OTel 或 SQL 仓储接入返工：先做框架能力核验，Repository 接口与 PostgreSQL 实现分离。
10. Windows 无 Bash 启停环境：用 Docker Desktop Linux 容器完成 Compose 验收并记录可迁移性。

## 10. 当前实现状态与验收入口

当前提交基于 `b1ae13d`，Phase 2 已实现真实 Agent、只读 JSON Catalog、可信 Binding、官方 Redis/InMemory Session/Memory、RunnerRegistry、严格 readiness、版本切换和租户隔离；`go test ./...`、`go test -race ./...`、`go build ./...`、`go vet ./...`、Go 1.21.13 复验、真实 Redis 7 和真实模型 Smoke 均已有记录。

尚未实现的能力是 Redis Streams、Inbox/Outbox、双 Worker、真实 IM Adapter、Web UI、SQL 审计仓储、完整 OTel 和 Compose 故障演示。本文提交用于冻结总体设计和 Phase 3 入口，不将这些后续能力表述为当前已完成。

详细证据：

- [阶段 0 架构与消息契约](stage0-architecture.md)
- [Phase 1.5 存储 Spike](stage1.5-storage-spike.md)
- [Phase 2 验收结论](stage2-multi-tenant.md)
- [Phase 3–7 可行性分析](../多租户与节点部署--后续阶段可行性分析.md)
- [逻辑施工 Plan](../多租户与节点部署--逻辑的plan.md)
