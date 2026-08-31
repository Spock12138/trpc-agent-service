# 阶段 0：仓库基线、范围与依赖调研计划

## 目标与边界

阶段 0 的目标是把“当前项目到底有什么、需要自己实现什么、IM 如何接入、Runner 如何组织”调查清楚，为后续编码提供依据。

本阶段只做只读调查、源码分析、依赖对比和方案验证，不实现生产功能。

明确边界：

- 不直接复用或引入 OpenClaw API。
- OpenClaw 仅作为 Telegram 分层、插件注册、消息转换和重试机制的参考材料。
- IM 重点调研 `Telegram + 企业微信`，但最终 SDK 和企业微信接入类型暂不在本阶段直接锁死。
- 不实现真实 IM Adapter、Gateway、Worker、Redis、SQL、Web UI 或生产 Runner。
- 不修改现有 `go.mod`，不提交功能代码。
- 阶段 0 产出经审阅确认后，才能进入阶段 1 实现。

## 调研工作

### 1. 当前仓库基线

调查内容：

- 阅读当前 `README.md`、题目、现有交接摘要和当前逻辑计划。
- 盘点 `cmd/trpc-service`、`trpcservice/*` 和启动脚本。
- 确认当前已有代码、占位代码和完全缺失的能力。
- 读取当前分支、提交、远端和工作区状态。
- 旧工作区只提取需求、风险和设计思想，不把旧代码结构作为新仓库实现依据。

输出：

- 当前仓库能力清单。
- 当前缺口清单。
- 当前仓库到目标平台的差距说明。
- 当前 Git 和依赖基线记录。

### 2. tRPC-Agent-Go 接入调查

调查内容：

- 核对开发时使用的稳定 tRPC-Agent-Go 版本。
- 阅读 Agent、Runner、Session、Memory、Tool、Filter、Callback 和 Telemetry 的真实创建与调用方式。
- 确认 Runner 的初始化参数、生命周期、并发使用方式和关闭方式。
- 确认 Session/Memory 后端能否独立配置、是否支持共享后端。
- 确认哪些能力适合在 `trpc-agent-service` 平台层封装，哪些能力应直接调用框架 API。
- 在临时目录建立最小依赖验证模块，验证核心 API 能够编译调用；不得为了验证而修改服务仓库的 `go.mod`。

输出：

- tRPC-Agent-Go 可复用 API 清单。
- Agent、Runner、Session、Memory 的创建和调用流程图。
- 当前项目需要新增的平台层职责清单。
- Go 最低版本建议及其依赖依据。

### 3. IM SDK 调研

重点调查 Telegram 和企业微信的 GitHub 成熟 SDK。

每个候选 SDK 统一比较：

- GitHub 维护活跃度和最近版本。
- 许可证是否适合项目使用。
- Go 版本要求。
- 收发文字、图片、文件和事件的支持程度。
- Telegram polling/webhook 或企业微信 callback 的支持情况。
- 验签、解密、限流、重试和错误处理能力。
- 是否支持自定义 HTTP Client，是否便于 Mock。
- 是否容易转换为项目统一入站/出站消息。
- 是否存在明显的 `internal` 限制或强绑定运行时。
- 未来替换 SDK 的成本。

输出：

- Telegram SDK 候选对比表。
- 企业微信 SDK 和协议类型候选对比表。
- 推荐候选、备选候选及不采用理由。
- SDK 与平台自有 Adapter 的边界说明。

本阶段只形成推荐，不直接引入 SDK。最终 SDK 选型需在调研结果审阅后确认。

### 4. Runner 方案比较

对以下两种方案进行源码级和概念级比较：

旧方案：

```text
多个租户请求
    -> 共用一个 Runner
    -> 路由 Wrapper 根据内部路由信息选择 Session/Memory 后端
```

新候选方案：

```text
先确定 tenant_id + agent_app_id
    -> Worker 查找或创建对应 Runner
    -> 使用该 Runner 执行请求
```

比较维度：

- Runner 创建成本和内存占用。
- Agent 配置和工具隔离。
- Session/Memory 后端选择。
- 配置版本更新。
- Worker 进程内缓存方式。
- 两个 Worker 之间的行为。
- 共享 Session/Memory 的要求。
- 错误、重试和故障接管。
- 实现对 tRPC-Agent-Go 框架的侵入程度。
- 代码和测试维护成本。

需要特别记录：

- 旧方案的主要负担是路由 Wrapper 需要完整覆盖 Session/Memory 的能力、可选接口和生命周期，复杂度较高。
- 不把“AppName 可能漏传或错传”作为已经证实的旧方案缺陷。
- 新方案当前只是候选方案，不能在阶段 0 未经确认前直接实现。

输出：

- 两种 Runner 方案对比表。
- 新方案的实例关系图：Worker、Runner、Session、Memory。
- 配置更新和跨 Worker 行为说明。
- 推荐方案及其尚未解决的问题。
- 一个不修改生产代码的最小可行性验证规格。

### 5. 平台主链路和契约边界

阶段 0 需要形成当前项目自己的主链路草案：

```text
Telegram / 企业微信
    -> 项目自己的 IM Adapter
    -> 协议验证与统一入站消息
    -> ChannelBinding
    -> tenant_id + agent_app_id
    -> Inbox 幂等认领
    -> Gateway
    -> Worker
    -> Runner
    -> Session / Memory
    -> 统一出站消息
    -> 对应 IM Adapter
    -> 平台回复
```

需要明确：

- 统一入站消息至少包含渠道、外部账号、平台消息 ID、用户 ID、会话/群组、文本、附件和原始元数据。
- 统一出站消息至少支持文字、附件、回复目标和流式/进度事件。
- 外部请求中的 `tenant_id` 不可信。
- 租户必须通过已验签的渠道账号和 `ChannelBinding` 解析。
- `tenant_id + channel_binding_id + platform_message_id` 作为消息幂等的候选唯一键。
- Channel Adapter 跟随 Gateway。
- Worker 不保存唯一的 Session/Memory 状态。
- 未知绑定、配置冲突、未知 Agent App 和后端不可用时统一 fail closed。

输出：

- 现状流程图。
- 目标主链路图。
- 入站/出站消息契约草案。
- ChannelBinding、Inbox、Session ID 的规则草案。
- Gateway、Worker、Adapter、Runner 的职责边界。

## 阶段 0 产出与审阅门槛

建议形成以下阶段 0 文档：

- `docs/stage0-baseline.md`：仓库现状、框架 API 和缺口。
- `docs/stage0-im-sdk-matrix.md`：Telegram/企业微信 SDK 候选对比。
- `docs/stage0-runner-options.md`：旧方案与新候选方案比较。
- `docs/stage0-architecture.md`：主链路、契约和职责边界。
- `docs/stage0-acceptance.md`：范围、验收标准和后续阶段入口。

阶段 0 完成前必须确认：

- 当前仓库的真实能力和缺口已明确。
- tRPC-Agent-Go 的实际接入方式已验证。
- OpenClaw 已明确排除为直接 API 依赖。
- IM SDK 候选和选型标准已经形成。
- 统一消息、ChannelBinding 和 Inbox 边界已经形成。
- 旧 Runner 和新候选 Runner 方案已能够清楚比较。
- 最小真实交付、非目标和 Web UI 范围已经明确。
- 用户审阅并确认下一阶段采用的 Runner 方案和 IM SDK 方向。

## 验证方式

阶段 0 使用以下验证：

- 当前服务仓库的基础构建和定向测试，记录基线结果。
- 临时目录中的 tRPC-Agent-Go 最小导入和编译验证。
- 对候选 SDK 做独立编译、API 检查和 Mock 可测试性检查。
- 使用静态分析确认服务仓库没有 OpenClaw 直接依赖。
- 不使用真实 IM 凭据、公网 webhook 或线上账号。
- 所有发现的问题记录到阶段 0 缺口和风险清单中。

阶段 0 的最终结果不是“功能已经完成”，而是：

> 后续实现人员无需再猜测项目边界、SDK 方向、消息契约、Runner 责任和验收标准，可以直接按照审阅后的阶段 0 结果进入阶段 1。
