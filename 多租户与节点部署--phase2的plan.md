# Phase 2：多租户配置、可信绑定与 RunnerRegistry 执行计划

> 更新日期：2026-08-27
> 审阅状态：已通过，本地实现与验收完成
> 代码基线：`feature/phase1.5-storage-spike@fb3cfa3`

## 1. 阶段目标与边界

将当前固定的 `demo-binding -> tenant-demo -> assistant -> v1 -> Redis` 单租户 Runtime 升级为：

```text
统一入站消息
  -> ChannelBinding 可信解析
  -> Tenant / AgentApp / ConfigVersion
  -> StorageProfile 后端选择
  -> RunnerRegistry
  -> tRPC-Agent-Go Runner
  -> 隔离的 Session / Memory
```

本阶段必须完成 Tenant、AgentApp、ChannelBinding、ConfigVersion、StorageProfile、只读 JSON 预置目录、服务端可信绑定解析、InMemory/Redis BackendProvider、RunnerRegistry，以及两租户/两后端/版本切换/跨租户拒绝测试。

本阶段不实现真实 IM、Redis Streams、Inbox/Outbox、Gateway/Worker 拆分、双 Worker、SQL Repository、Web UI、配置热加载或存储迁移。

## 2. 固定配置契约

新增可选环境变量 `PLATFORM_CONFIG_FILE`：

- 已设置时加载指定 JSON，支持绝对路径和工作目录相对路径。
- 未设置时将现有环境变量转换为原来的单租户预置目录，保持启动方式兼容。
- `IDENTITY_SECRET` 始终必填且不少于 32 字节。
- JSON 最大 1 MiB，严格解码并拒绝未知字段、多余 JSON 和不支持的 `schema_version`。
- JSON 只能保存 `env:<ENV_NAME>` 凭据引用，不得保存明文模型 Key 或 Redis URL。
- 所有启用配置引用的凭据在启动时检查；错误和日志不得输出凭据值。
- 目录加载后不可修改；文件变化必须重启进程。

配置结构固定为：

```json
{
  "schema_version": 1,
  "tenants": [{"id": "tenant-a", "enabled": true}],
  "storage_profiles": [{
    "tenant_id": "tenant-a",
    "id": "redis-main-v1",
    "kind": "redis",
    "credential_ref": "env:TENANT_A_REDIS_URL",
    "key_prefix": "trpc-agent-service:phase2"
  }],
  "agent_apps": [{
    "tenant_id": "tenant-a",
    "id": "assistant",
    "enabled": true,
    "active_config_version": "v1"
  }],
  "config_versions": [{
    "tenant_id": "tenant-a",
    "agent_app_id": "assistant",
    "version": "v1",
    "storage_profile_id": "redis-main-v1",
    "instruction": "You are a concise assistant.",
    "model": {
      "name": "model-name",
      "base_url": "https://example.test/v1",
      "credential_ref": "env:TENANT_A_MODEL_KEY",
      "request_timeout": "60s",
      "max_output_tokens": 1024
    }
  }],
  "channel_bindings": [{
    "id": "demo-tenant-a",
    "channel": "demo",
    "external_account_id": "demo-tenant-a",
    "tenant_id": "tenant-a",
    "agent_app_id": "assistant",
    "enabled": true
  }]
}
```

校验规则：

- ID 使用安全、可打印的 ASCII 标识，最长 128 字节。
- 所有领域组合键唯一；Binding 必须指向启用的 Tenant 和 AgentApp。
- `active_config_version` 必须存在；ConfigVersion 与 StorageProfile 必须属于同一租户。
- `kind` 只允许 `inmemory` 和 `redis`；InMemory 禁止 Redis 凭据，Redis 必须具有凭据引用和前缀。
- `app_name` 统一派生为 `tenant/<tenant_id>/app/<agent_app_id>`，不允许文件覆盖。
- 配置、模型、指令或 StorageProfile 变化必须使用新 `version`，不允许原地修改已发布版本。

提供不含秘密的 `configs/phase2.example.json`，包含两个租户和 InMemory/Redis 示例。

## 3. 核心类型与接口

平台内部定义 `Tenant`、`AgentApp`、`ChannelBinding`、`ConfigVersion`、`ModelConfig` 和 `StorageProfile`。Repository 至少提供：

```go
ResolveBinding(ctx, channel, bindingID)
GetTenant(ctx, tenantID)
GetAgentApp(ctx, tenantID, agentAppID)
GetConfigVersion(ctx, tenantID, agentAppID, version)
GetStorageProfile(ctx, tenantID, profileID)
ListActiveStorageProfiles(ctx)
```

`PresetRepository` 在启动时建立只读索引并完成全部引用校验。未知、禁用或冲突对象返回稳定分类错误，不暴露内部配置内容。

统一内部消息包含渠道、Binding、外部消息/用户/会话、正文、request/trace ID 和接收时间；统一回复包含渠道、Binding、request/trace/session ID 和正文。它们只作为平台内部契约，不新增公共 HTTP API。

## 4. BackendProvider 与 RunnerRegistry

BackendProvider：

- 按 `tenant_id + storage_profile_id` 并发安全地创建和缓存后端。
- InMemory 使用官方 `session/inmemory` 和 `memory/inmemory`；Redis 复用 Phase 1.5 `RedisBackend`。
- JSON Redis 前缀派生为 `<base>:tenant:<tenant_id>:profile:<profile_id>:official-v1`；旧环境变量模式保持原 `<REDIS_KEY_PREFIX>:official-v1`。
- 后端创建失败不得回退 InMemory 或其他 Profile。
- `/readyz` 检查全部启用 Profile；消息执行只检查本次路由选中的 Profile。
- 关闭顺序固定为 RunnerRegistry、Session/Memory Backend、health client，Close 幂等且并发安全。

RunnerRegistry 复用 RunnerCache 的 singleflight、Lease、排空和关闭机制，并补齐：

- 按 CacheKey 精确读取不可变 ConfigVersion 和 StorageProfile。
- 同 key 并发只创建一个 Runner；不同租户、Agent 或版本使用不同 Runner。
- 容量满时仅 LRU 淘汰 `refs == 0` 的最久未使用项，不得淘汰活跃 Runner。
- 旧配置不再收到新请求，待引用归零后由 TTL/LRU 淘汰。
- 工厂失败、缓存满、排空或后端不可用全部 fail closed。

请求数据流固定为：

```text
HTTP Demo -> InboundMessage(channel=demo) -> ResolveBinding
  -> Tenant -> AgentApp.active_config_version -> ConfigVersion
  -> StorageProfile -> RunnerRegistry.Acquire -> HMAC identity
  -> Runner.Run -> OutboundMessage -> 原 HTTP Reply
```

客户端不能提交或覆盖 `tenant_id`、`agent_app_id`、`config_version` 或 `storage_profile_id`。

## 5. HTTP、错误与生命周期

继续保留：

```text
GET  /healthz
GET  /readyz
POST /api/v1/demo/messages
```

Demo 请求、响应、大小限制和 ID 格式不变，只增加内部消息转换。

| 场景 | HTTP |
| --- | --- |
| 非法 JSON、未知字段、客户端提交租户字段 | `400` |
| 未知或禁用 Binding | `404` |
| Binding 引用配置异常、Repository 失败 | `503` |
| StorageProfile 不可用、Runner 满或排空 | `503` |
| 模型超时 | `504` |
| 模型、Agent 或空响应错误 | `502` |

错误响应只包含 `code`、`message`、`request_id`、`trace_id`，不得返回内部映射、凭据引用、Redis 地址或上游原始错误。

## 6. 实施顺序

1. 从 Phase 1.5 基线创建 `codex/phase2-multi-tenant-runner`，保留旧 Phase 2 分支和未跟踪资料。
2. 实现领域模型、JSON Loader、环境变量兼容、CredentialResolver 和 PresetRepository。
3. 定义统一消息契约，让 Demo HTTP 层只负责校验和转换。
4. 实现 InMemory Backend、BackendProvider 和按 Profile 管理的 RedisBackend 生命周期。
5. 在 RunnerCache 上实现 RunnerRegistry，移除 Runtime 对固定租户和固定 CacheKey 的依赖。
6. 改造 Runtime 为可信绑定驱动的多租户执行链路，并保持 HTTP/CLI 兼容。
7. 完成单元、race、真实 Redis 和端到端验收，输出 `docs/stage2-multi-tenant.md` 并同步 README 和交接摘要。
8. 只暂存 Phase 2 明确文件，不使用 `git add -A`。

## 7. 测试与关闭门槛

自动化测试覆盖：

- JSON、兼容模式、严格字段、唯一性、引用关系、凭据校验和脱敏。
- 未知/禁用/伪造 Binding 拒绝，HTTP `tenant_id` 仍返回 `400`。
- 两租户相同外部用户和会话下配置、Runner、Session、Memory 隔离。
- InMemory/Redis 选择、Redis 前缀隔离、故障无 fallback 和恢复无需重启。
- Runner singleflight、版本隔离、LRU/TTL、排空和 Close 幂等。
- `v1 -> v2` 后新请求使用新 Runner，活跃旧请求完成后才关闭。
- StorageProfile 变化通过新版本冷切换。
- `/healthz`、`/readyz`、Demo HTTP 和既有错误映射回归。

阶段关闭命令：

```text
go test ./...
go test -race ./...
go build ./...
go vet ./...
```

同时完成 Go 1.21.13 test/build、Mock 模型多租户端到端、真实 Redis 7 跨 Runtime/故障/恢复，以及有凭据时的真实模型目录配置 Smoke。

## 8. 已接受限制

- Demo `binding_id` 是本地模拟渠道选择器，不等于生产身份认证。
- JSON 不热加载；跨部署无法自动阻止同版本号改内容，必须依靠评审和 Git 差异。
- 密钥原地轮换不会自动重建缓存 Runner；确定性切换需新 ConfigVersion 或重启。
- StorageProfile 切换不迁移、不双读、不回退。
- 严格 `/readyz` 会被任一启用 Redis Profile 故障拉低。
- `message_id` 仍不幂等；InMemory 不得作为后续多 Worker 的唯一共享状态。

## 9. 实际关闭结果

- 严格 JSON Catalog、可信 Binding、PresetRepository、BackendProvider、RunnerRegistry、空闲 LRU 和多租户 Runtime 已实现。
- 两租户配置/Runner/Session/Memory 隔离、InMemory/Redis 选择、严格 readiness、故障恢复、版本切换和存储冷切换已自动化验证。
- 当前工具链 `test/race/build/vet`、Go 1.21.13 `test/build`、`go mod verify` 和独立真实 Redis 7 Phase 2/legacy Smoke 均通过。
- 2026-08-28 已使用宿主用户环境凭据完成真实模型目录双租户 Smoke；`deepseek-v4-flash`、InMemory/Redis Binding、严格 readiness 和租户 Session 隔离均通过。
- `start.sh` 已修正为执行 `trpc-service serve "$@"`；当前 Windows 没有可用 WSL/Git Bash，脚本只完成静态命令核对，未执行 Bash 启停闭环。
- 当前分支为 `codex/phase2-multi-tenant-runner`，实现尚未暂存、提交或推送。
