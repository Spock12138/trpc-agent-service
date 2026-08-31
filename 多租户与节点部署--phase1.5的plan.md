# Phase 1.5：官方多后端兼容性验证与 Redis 生产接入

> 日期：2026-08-25
> 状态：已审阅，执行中

## 1. 目标与两道门

Phase 1.5 先在隔离消费者模块验证官方 Redis/SQL 子模块，不修改生产依赖；验证完成后形成唯一 A/B/C 结论，并在同一阶段接入选定 Redis 方案、完成 Phase 1 回归。

成功关闭要求生产运行时已采用通过验证的 Redis 方案。若官方实现存在无法由薄包装解决的契约缺陷，则保留现状并阻塞 Phase 2。

## 2. 依赖与隔离 Spike

- 根模块固定 `trpc-agent-go v1.11.2`。
- Redis 固定 `session/redis`、`memory/redis`、`storage/redis v1.11.0`。
- SQL 固定 `session/mysql v1.11.2`，`session/postgres`、`memory/mysql`、`memory/postgres v1.11.0`。
- Spike 位于被忽略且可清理的 `.stage15-spike/` / `.stage15-cache/`，不使用本地源码 `replace` 或 `go.work`。
- 记录 `go list -m all`、`go mod graph`、最终 storage 子模块和 `go-redis` 版本；在当前工具链与 Go 1.21.13 下验证接口和 Runner 注入。

## 3. Redis 契约

- Session：Create/Get/List/Delete、App/User/Session State、Event 顺序与并发、Summary、TTL、前缀隔离、非法 Key、取消与故障。
- Memory：Add/Read/Search/Update/Delete/Clear、幂等、限制、跨实例、前缀隔离、工具面、取消与故障。
- miniredis 只用于快速测试；独立 Docker Redis 7 是 Lua/EVALSHA、真实连接、并发和故障恢复的最终证据。
- 生产不启用 Summary/异步持久化、Extractor 或 Memory 工具；Memory limit 设为无限，不作为强配额。

## 4. SQL 契约

- MySQL 只做发布模块编译、接口、构造选项、schema、事务、初始化和关闭路径的静态/sqlmock 核验。
- PostgreSQL 使用独立 `postgres:16-alpine` 运行自动建表、Session/Memory 最小 CRUD、Event/State/Summary、跨连接、并发和关闭重建验证。
- SQL 子模块不进入生产 `go.mod`，只形成兼容性与接入成本结论。

## 5. 决策与生产接入

- A：直接构造即可保持延迟启动、ready 恢复、前缀、工具策略和统一关闭。
- B：官方数据契约通过，平台只协调初始化、健康检查、命名空间和生命周期；这是预期成功路径。
- C：官方数据契约或兼容性存在薄包装无法修复的阻塞；保留当前实现并阻塞阶段，不继续扩展快照适配器。

B 路径新增内部 `RedisBackend`，构造不联网，Ready 时初始化并允许 Redis 恢复后重试；Session、Memory、健康检查使用独立 client，关闭顺序为 RunnerCache -> Session -> Memory -> health client。官方键使用 `<REDIS_KEY_PREFIX>:official-v1`，无旧键迁移或 fallback。

## 6. 关闭门槛

- HTTP/CLI/配置/ID/错误映射保持兼容。
- 官方 Redis 定向契约、真实 Redis、真实 PostgreSQL、Mock 模型端到端全部完成。
- `go test ./...`、`go test -race ./...`、`go build ./...`、`go vet ./...` 以及 Go 1.21.13 test/build 通过。
- 输出 `docs/stage1.5-storage-spike.md` 并回写 README、Phase 1 纠正文档、逻辑计划、交接摘要、发现和进度。
- Phase 1.5 关闭后才生成 Phase 2 详细计划。
