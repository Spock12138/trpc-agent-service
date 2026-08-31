# Phase 1：真实 Agent 最小闭环与入口 Spike（优化执行版）

> 更新日期：2026-08-24
> 审阅状态：已通过
> Spike 实际结果：见 `docs/stage1-spike.md`

## 1. 目标、阶段门与边界

目标链路：

```text
HTTP Demo -> Demo Gateway -> executor -> tRPC-Agent-Go Runner
          -> Redis Session/Memory -> Agent 回复
```

执行顺序固定为：

1. 临时模块完成 tRPC-Agent-Go、DeepSeek、Telegram、企业微信和 RunnerCache Spike。
2. Spike 通过后锁定 `go.mod` 与生产参数。
3. 实现单进程分层 runtime/executor 和 HTTP 闭环。
4. 完成自动化测试、真实 Redis Smoke Test 和真实 DeepSeek Smoke Test。

本阶段不实现真实 IM Adapter、Redis Streams、Inbox/Outbox、双 Worker、SQL、多租户仓储、Web UI 或 OpenTelemetry 后端。

## 2. 已修正的问题

- `REDIS_URL` 接受 `localhost:6379` 简写，配置层规范化为 `redis://localhost:6379/0`。
- RunnerCache 参数是 Spike 初始预设；机制验证通过后才锁定为 Phase 1 参数。
- 单进程组件统一命名为 `executor`，不使用容易与 Phase 4 混淆的“Worker 运行时”。
- Phase 1 不开放群聊 HTTP 链路，只保留群主体派生规则的后续契约。
- `message_id` 仅作链路字段，不提供 Inbox 去重。
- DeepSeek 的无 `/v1` 和带 `/v1` Base URL 都必须真实验证。
- 自动化测试使用本地 Mock；真实 API Key 不作为自动化测试前提。
- tRPC-Agent-Go `v1.11.2` 实际没有 Redis Session/Memory 包，Phase 1 使用平台最小 Redis 适配器，能力边界不得扩大表述。

## 3. 配置契约

| 环境变量 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `MODEL_NAME` | 是 | 无 | 模型名，例如 `deepseek-v4-flash` |
| `MODEL_BASE_URL` | 是 | 无 | 已通过 Spike 的 OpenAI-compatible endpoint |
| `MODEL_API_KEY_ENV` | 是 | 无 | API Key 所在环境变量的名称 |
| `MODEL_REQUEST_TIMEOUT` | 否 | `60s` | 单次模型请求超时 |
| `MODEL_MAX_OUTPUT_TOKENS` | 否 | `1024` | 最大输出 Token |
| `IDENTITY_SECRET` | 是 | 无 | 至少 32 字节随机值，用于 HMAC 派生内部身份 |
| `REDIS_URL` | 是 | 无 | Redis URI 或 `host:port[/db]` 简写 |
| `REDIS_KEY_PREFIX` | 否 | `trpc-agent-service:phase1` | Phase 1 Redis 键前缀 |

规则：

- 缺少必填项、API Key 引用变量不存在或身份密钥不足 32 字节时，进程不启动。
- Key 只从 `MODEL_API_KEY_ENV` 指向的环境变量读取，不进入代码、日志、错误响应、trace 或 Git。
- `REDIS_URL` 只允许 `redis`/`rediss` scheme；Redis 不可用时禁止回退 InMemory。
- 本地真实验证使用 Docker Desktop 的 `redis:7-alpine`，代码不自动创建容器。

## 4. Spike 规格

### 4.1 tRPC-Agent-Go 与模型

- 锁定 `trpc.group/trpc-go/trpc-agent-go v1.11.2`，保持 `go 1.21`。
- 使用 `model/openai`、`llmagent` 和 `runner.NewRunner`。
- Mock 覆盖成功、流式事件、上游错误、空响应和超时。
- Phase 1 关闭 OpenAI SDK 自动重试，使上游 4xx/5xx 确定映射为 502；平台级重试后置到幂等与队列阶段。
- 真实验证 `deepseek-v4-flash` 与两种 Base URL，结果写入 Spike 文档。

### 4.2 Telegram

临时模块验证 `github.com/go-telegram/bot v1.23.0` 的文字、群聊、reply、平台消息 ID、自定义 HTTP Client、429/5xx、Webhook/Polling 和 context shutdown。SDK 不写入生产 `go.mod`，正式 Adapter 后置。

### 4.3 企业微信

临时模块验证 `github.com/silenceper/wechat/v2 v2.1.14` 的内部自建应用能力。输出功能覆盖矩阵、缺口清单和平台薄适配函数；真实普通回调入口覆盖不足时，不把客服模块冒充内部应用回调。

### 4.4 RunnerCache

| 参数 | Phase 1 值 |
| --- | --- |
| 最大缓存项 | 32 |
| 空闲 TTL | 30 分钟 |
| 创建超时 | 10 秒 |
| 排空超时 | 30 秒 |
| 关闭超时 | 5 秒 |

验证同 key 100 并发 singleflight、runtime 间 Runner 实例隔离、版本切换、排空、Close 幂等、共享 Service 所有权、失败关闭和 race。

## 5. 生产运行时

固定预置身份：

```text
binding_id     = demo-binding
tenant_id      = tenant-demo
agent_app_id   = assistant
config_version = v1
app_name       = tenant/tenant-demo/app/assistant
```

RunnerCache key 为 `tenant_id + agent_app_id + config_version`。请求只能提交 `binding_id`，不能提交或覆盖内部租户、Agent 和配置版本。

身份规则：

```text
runner_user_id = "u_" + hex(HMAC-SHA256(secret, binding_id | external_user_id))
session_id     = "s_" + hex(HMAC-SHA256(secret, binding_id | direct | conversation_id))
trace_id       = 16 随机字节编码为 32 位小写 hex
request_id     = 8 随机字节编码为 16 位小写 hex
```

Runner 执行要求：

- executor 持有 RunnerCache 和共享 Session/Memory Service。
- 获取 Runner 时增加活跃引用，执行完成后释放。
- 排空时拒绝新请求；超时后取消 context、排空 Event channel 并关闭 Runner。
- Event channel 必须消费到关闭；按顺序拼接 assistant 内容。
- 空响应返回 `ErrEmptyAgentResponse`，不伪造成功。

Redis 适配范围：

- Phase 1 实现 Session 创建/读取、Event 追加、Session State 和 Memory CRUD/关键词搜索。
- Session 和 Memory 跨 runtime/实例可见。
- 完整 App/User State、Summary、分页和跨进程并发写留到共享后端阶段。

## 6. CLI 与 HTTP 契约

CLI 使用标准库 `flag.NewFlagSet`：

```text
trpc-service serve [-addr :8080]
```

无参数、`-h`、`--help` 保留版本与帮助输出，不引入 cobra。

HTTP：

```http
GET /healthz
GET /readyz
POST /api/v1/demo/messages
```

请求体：

```json
{
  "binding_id": "demo-binding",
  "message_id": "msg-001",
  "external_user_id": "user-001",
  "conversation_id": "conversation-001",
  "text": "你好"
}
```

校验规则：body 最大 64 KiB；ID 最大 256 字节；文本最大 16 KiB 且非空；拒绝未知 JSON 字段。

成功响应：

```json
{
  "request_id": "0123456789abcdef",
  "trace_id": "0123456789abcdef0123456789abcdef",
  "session_id": "s_...",
  "text": "..."
}
```

错误响应统一包含 `code`、`message`、`request_id`、`trace_id`，不包含密钥、上游完整响应或内部堆栈。

## 7. 错误映射与验收

| 场景 | HTTP 状态码 |
| --- | --- |
| 成功 | `200` |
| 缺字段、非法 JSON、未知字段、超长字段、空文本 | `400` |
| 未知 `binding_id` | `404` |
| 配置缺失、Redis 不可用、Runner 排空中 | `503` |
| 模型请求超时 | `504` |
| Agent 错误、上游 4xx/5xx、错误事件、空响应 | `502` |

Phase 1 关闭条件：

- Spike 文档记录依赖、DeepSeek Base URL、Telegram 能力和企业微信缺口。
- `go.mod` 锁定依赖，`go test ./...`、`go test -race ./...`、`go build ./...` 通过。
- 三个 HTTP 接口可运行，错误映射和输入限制有测试。
- Mock 端到端请求成功并复用 Session。
- 真实 Redis Session/Memory Smoke Test 通过，故障时无 InMemory 回退。
- 真实 DeepSeek 请求成功；若外部环境失败，只记录外部阻塞。
- 敏感信息不进入日志、响应、trace 或 Git。
