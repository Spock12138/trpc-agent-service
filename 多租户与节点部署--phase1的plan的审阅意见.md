# 阶段 1 plan 审阅意见（2026-08-24）

> 审阅对象：`多租户与节点部署--phase1的plan.md`
> 审阅结论：**通过，可执行**。方向正确、边界清晰、与阶段 0 已确认决策完全一致。
> 本文记录：审阅通过的理由、需在详细规格中补明确的 7 个点、外部前提的落实情况（用户已补充模型与 Redis 信息）。

## 1. 审阅结论

本 plan 可以作为阶段 1 的执行依据，直接推进。执行顺序保持不变：

```text
先做 Spike（依赖 + RunnerCache + 两个 IM SDK）
-> Spike 通过后锁定 go.mod
-> 再写生产代码（单进程分层运行时）
```

## 2. 审阅通过的主要理由

1. **"两道门"结构正确**：先在小黑屋（临时模块）验证所有不确定项，通过后才动主仓库 `go.mod`，把风险挡在主代码之外。
2. **边界干净**：只做"一条消息 -> Agent -> 回复"最小闭环，1 个租户 / 1 个 Agent / 1 个版本 / 单进程；不做双 Worker、真实 IM、队列、SQL。
3. **安全习惯从第一天养成**：不信任请求中的 `tenant_id`、Redis 不可用禁止回退 InMemory（fail closed）、密钥不进日志/错误响应/trace、空响应不伪造成功。
4. **与已确认决策一致**：Runner 缓存键 `tenant + app + version`、tRPC-Agent-Go `v1.11.2`、Go 1.21、Telegram/企业微信 SDK 选择均无偏离。
5. **验收标准具体**：接口、请求/响应 JSON、状态码、测试矩阵均已固定，可对照验收。

## 3. 详细规格需补明确的 7 个点（审阅意见落实）

### 3.1 配置清单表格化（最重要）

plan 只写了"从环境变量读取模型配置"，未列全变量名和必填项。规格中必须给出下表：

| 环境变量 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `MODEL_NAME` | 是 | 无 | 模型名，如 `deepseek-v4-flash` |
| `MODEL_BASE_URL` | 是 | 无 | OpenAI-compatible endpoint，如 `https://api.deepseek.com` |
| `MODEL_API_KEY_ENV` | 是 | 无 | 存放 API Key 的**环境变量名**，如 `DEEPSEEK_API_KEY`（程序只读该变量名，不直接持有 Key） |
| `MODEL_REQUEST_TIMEOUT` | 否 | `60s` | 单次模型请求超时 |
| `MODEL_MAX_OUTPUT_TOKENS` | 否 | `1024` | 最大输出 Token |
| `IDENTITY_SECRET` | 是 | 无 | HMAC 身份密钥，要求 >= 32 字节随机值，用于派生 `runner_user_id` / `session_id` |
| `REDIS_URL` | 是 | 无 | Redis 地址，如 `localhost:6379` |

缺失任一必填项 -> 启动时 fail closed（进程不启动或 `readyz` 返回未就绪）。

### 3.2 RunnerCache 参数是"预设"而非"实测"

- 预设参数：最大 32 项 / 空闲 TTL 30 分钟 / 创建超时 10 秒 / 排空超时 30 秒 / 关闭超时 5 秒。
- 正确流程：**先 Spike 验证合理性，再固定到生产代码**。
- 风险提示：phase 1 只有一个 Agent App，32 容量不会被触顶；Spike 重点验证**机制**（singleflight、版本排空、Close 幂等、共享 Service 不误关闭），参数值以实测为准。

### 3.3 trace_id / request_id 格式提前定

- `trace_id`：**32 位 hex**（W3C traceparent 兼容）。阶段 6 要做 OpenTelemetry 串联，现在格式对齐，避免返工。
- `request_id`：随机 16 位 hex 或 ULID，用于日志、响应和后续幂等关联。

### 3.4 命令行解析方式

- 现有 `main.go` 只有 `-h`。阶段 1 新增子命令：`trpc-service serve`（默认监听 `:8080`，用 flag `-addr` 覆盖）。
- 无参数/`-h` 时保持现有"打印版本"行为不变。
- **用标准库 `flag` 子命令解析即可，不引入 cobra**，避免无谓依赖。

### 3.5 企业微信 Spike 的现实约束

- `silenceper/wechat/v2 v2.1.14` 是否真实覆盖"回调验签 + AES 解密 + 文本解析 + 应用消息发送"**以实测为准**。
- 覆盖不足时按 plan 的兜底：平台层补官方协议薄适配。
- Spike 输出必须包含：功能覆盖矩阵 + 缺口清单 + 需要薄适配的具体函数。

### 3.6 "Worker 运行时"命名歧义

- phase 1 是单进程，"Worker 运行时"是进程内组件，容易与 phase 4 的真实 Worker 进程混淆。
- 建议内部命名统一为 `runtime` 或 `executor`，避免后续阶段歧义。

### 3.7 502 / 504 的错误映射表

实现"模型超时 -> 504、其他模型错误 -> 502"必须对错误分类。规格中给出对照表：

| 场景 | HTTP 状态码 | 说明 |
| --- | --- | --- |
| 成功 | `200` | 返回 `request_id / trace_id / session_id / text` |
| 请求错误（缺字段、超长字段、空文本、非法 JSON、未知字段） | `400` | fail closed |
| 未知 `binding_id` | `404` | 只接受预置 `demo-binding` |
| 未就绪 / 后端不可用（配置缺失、Redis 连不上、Runner 排空中） | `503` | `readyz` 同步反映 |
| 模型超时 | `504` | 由 `MODEL_REQUEST_TIMEOUT` 触发 |
| Agent / 模型其他错误（错误事件、空响应、上游 4xx/5xx） | `502` | 空响应返回 `ErrEmptyAgentResponse`，不伪造成功 |

## 4. 外部前提的落实情况（用户 2026-08-24 补充）

### 4.1 模型配置：已确认可用

- 模型名：`deepseek-v4-flash`
- BASE URL（OpenAI 格式）：`https://api.deepseek.com`
- API Key：已准备（存放于本机环境变量，见 4.2）

**明确指出的问题：**

1. **BASE URL 是否带 `/v1` 需 Spike 实测**。tRPC-Agent-Go 的 `model/openai` 有自己拼接路径的方式；DeepSeek 官方兼容 OpenAI 格式，`https://api.deepseek.com` 与 `https://api.deepseek.com/v1` 都可用，但**以实际调用成功为准**，Spike 时做一次真实请求验证。
2. **模型名必须配置化，不硬编码**。`deepseek-v4-flash` 写入 `MODEL_NAME` 环境变量，最终以 API 实际接受的模型名为准；将来换模型只改配置不改代码。
3. **API Key 绝不进 git / 代码 / 日志**。含 Key 的本地笔记文件保持未跟踪，禁止 `git add -A`（见 4.2）。

### 4.2 API Key 保管要求

- 程序通过 `MODEL_API_KEY_ENV` 指向的环境变量读取 Key，例如：
  ```powershell
  # PowerShell 用户级设置（仅示例，实际 Key 见用户本地笔记）
  [Environment]::SetEnvironmentVariable("DEEPSEEK_API_KEY", "<用户本地 Key>", "User")
  ```
- Key 只存在于环境变量中：不入代码、不入日志、不入错误响应、不入 trace、不入 git。
- 含明文 Key 的 `这是一个伟大的开始.txt` 等本地文件：保持未跟踪，建议加入 `.gitignore`，任何情况下不使用 `git add -A` 批量暂存。

### 4.3 Redis 启动：用 Docker Desktop（已验证可操作）

用户本机已装 Docker Desktop（Engine running）。当前容器列表中没有 Redis，需要新增。推荐命令：

```powershell
docker run -d --name trpc-redis -p 6379:6379 redis:7-alpine
```

- `6379` 是 Redis 默认端口，与现有容器（mysql 3306、zookeeper 2181、presenton 5000）不冲突。
- 验证是否跑起来：
  ```powershell
  docker ps --filter name=trpc-redis
  docker exec trpc-redis redis-cli ping   # 期望返回 PONG
  ```
- 阶段 1 程序中 `REDIS_URL=localhost:6379`。
- 容器停止/删除不影响数据持久化决定：首版用无持久化的默认配置即可，持久化卷可在后续阶段再挂载。

## 5. 遗留风险与待确认

| 项 | 状态 | 说明 |
| --- | --- | --- |
| 真实模型真实调用 | 可执行 | Key 已备，Spike 时做一次真实请求验证 BASE URL 与模型名 |
| Redis 本地运行 | 已解决 | 用 Docker Desktop 的 `redis:7-alpine` 容器，命令见 4.3 |
| 企业微信 SDK 覆盖度 | 待 Spike | 见 3.5 |
| tRPC-Agent-Go 版本复核 | 待执行 | 正式引入前复查 `v1.11.2` release notes |
| Git upstream / 分支 / 安全目录 | 未配置 | 不阻塞阶段 1 本地规格编写，正式贡献前完成 |
