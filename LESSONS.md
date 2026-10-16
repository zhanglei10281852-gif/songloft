# 开发经验

保存主仓库与公共宿主契约的证据型经验；执行见 [AI 开发流程](docs/ai-development.md)。后续会话在相关任务前读取适用条目。客户端、插件和设备经验放所属子仓库。

## 记录与升级

- 每条含规则、范围、失败/修复证据、正确示例和独立事件日期；使用完整仓库引用与 commit SHA/链接。
- `Seen N×` 统计独立事件的不同日期，同一事故反复复盘不递增；三天后提出规范升级并标记权威位置。已有铁律、严重问题不等待三次。
- 不编造次数、修复状态或验证结果，与 [英文版](LESSONS.en.md) 同步，敏感信息脱敏。

## 条目

### FLOW-001：关联 Issue 的修复推送后，主动交付待确认的回复草稿

- 范围：主仓库与子仓库的开发交付及 Issue 沟通。
- 失败：2026-10-13，songloft-org/songloft#495 的修复 [songloft-org/songloft-plugin-miot@1cb1b52](https://github.com/songloft-org/songloft-plugin-miot/commit/1cb1b5214612e9fe611e8d8c61bfa47d5fc2f579) 已推送，v2026.10.14 已发布并同步主仓库 [540bf0c](https://github.com/songloft-org/songloft/commit/540bf0c76f87f08ba54dec1573cf9922126ad68c)，交付却未准备并展示 Issue 回复草稿。用户追问后才明确安排回复，造成额外往返。
- 修复：当前未提交的流程更新，在 AGENTS、AI 流程及 dev-flow 中要求推送后主动展示完整草稿、目标 Issue 和具体评论确认动作；已有适用评论授权直接执行。草稿跟随 CI/发布结果更新，评论与关闭分别授权。
- Done right：用户明确要求回复后，发布说明已[发表并回读验证](https://github.com/songloft-org/songloft/issues/495#issuecomment-6147071915)，说明可用版本、实际验证和真机/时长差异的验收边界。新增「主动草稿 → 确认 → 发表」流程的后续实用验证待补，不能把本次补发视为已验证该完整流程。
- Seen 1× (2026-10-13)。状态：按用户明确要求立即纳入 [关联 Issue 回复](docs/ai-development.md#关联-issue-回复)，不等待经验次数门槛。

### TEST-001：临时目录清理失败要核对后台文件写入

- 范围：Go 插件生命周期、异步字节码缓存与测试临时目录。
- 规则：`TempDir RemoveAll: directory not empty` 不能直接归为环境问题或用延时掩盖；检查测试结束和后台写盘的先后顺序。重跑通过只证明该次检查通过，不代表生命周期竞态已修复。
- 失败：2026-10-14，songloft-org/songloft#518 的父指针提交 [`405e8a0`](https://github.com/songloft-org/songloft/commit/405e8a0f2697bac53c3311567fdca025256335df) 在 [CI 首轮](https://github.com/songloft-org/songloft/actions/runs/38526078645/attempts/1) 的 `TestManager_ReloadPlugin` 清理阶段报错。日志先出现管理器关闭，再出现 `bytecode cached`；本批只改客户端指针，未改 Go 源码。
- 调查与处理：`JSService.Load` 异步执行字节码编译及缓存写入，关闭流程未等待该写盘；证据指向后台写盘与临时目录清理竞态。同一 SHA 本地 `CGO_ENABLED=0 go test -tags dev,lite ./internal/jsplugin -run '^TestManager_ReloadPlugin$' -count=30` 通过；失败作业重跑后 Quality 通过。日志在 `/tmp/songloft-518-{parent-ci-failed,reload-repeat}.log`。本批未修复该后台任务生命周期，后续应补确定性回归与退出等待。
- Done right：保留首轮失败、实际 SHA 与重跑结果；若修复生命周期，验证关闭返回后不再写缓存，再清理临时目录，不把单次重跑当成修复证明。
- Seen 1× (2026-10-14)。状态：经验，生命周期修复待处理。

### AUTH-001：refresh token 轮换必须事务内条件消费，会话身份与展示信息分离

- 范围：主仓库认证模块（`internal/services/auth_service.go`、`internal/database` 的 `auth_tokens`）。
- 规则：① 并发安全不能靠「先 `IsRevoked` 再 `Revoke`」的检查-执行序列，必须在 `RunInTx` 内把带守卫（`token_type='refresh' AND revoked_at IS NULL AND expires_at >= ?`）的条件 UPDATE 作为**第一条语句**原子消费，命中 0 行即复用/失效，再读持久状态分类为「已消费/已撤销/已过期/类型错误」；② 旧 refresh 消费与新 access/refresh 写入同一事务共同提交，任何失败整体回滚，未提交的旧 refresh 必须保持可重试；③ 会话身份是登录时生成、持久化并跨轮换稳定的独立列（`session_id`），User-Agent 等 `client_info` 只用于展示，绝不能充当会话键（相同 UA 的独立登录必须是不同会话，登出按会话撤销）；④ 用哨兵错误区分凭证无效（401）与存储故障（500），DB 错误不得被映射成凭证错误。
- 失败：2026-10-14，基线 `6b050ff` 的 `RefreshToken` 为 IsRevoked→Get→Revoke→两次 Create 的非事务序列：同一 refresh 并发刷新可各自通过检查并生成多套仍有效的令牌；Create 失败时旧 refresh 已撤销而新令牌缺失，会话被锁死；刷新后新 JWT 的 `client_id` 被写成旧行的 `client_info`（UA），导致相同 UA 的两个独立会话在登出时按 UA 匹配而互相撤销 refresh。
- 修复：未提交工作树（基线 `6b050ff`）。迁移 `0040_auth_token_session.sql` 增加 `session_id`；`ConsumeRefreshToken`/`RevokeActiveTokensBySession` 固定 SQL；登录与轮换改为单事务提交；哨兵错误 `ErrInvalidRefreshToken`/`ErrRefreshTokenConsumed` 等，handler 按 `errors.Is` 映射 401/500；存量空 `session_id` 行保留旧登出/新生会话链的兼容路径。
- Done right：`TestRefreshToken_ConcurrentSameRefresh`（文件库、24 goroutine 并发恰 1 成功，其余全部 consumed）、`TestRefreshToken_ConsumedStateSurvivesRestart`（重开库+新 AuthService 后已消费仍拒绝、链头可继续轮换）、`TestRefreshToken_RollbackKeepsRefreshRetryable`（事务回滚后旧 refresh 仍可轮换）、`TestLogout_SessionIsolationSameUA`（同 UA 两会话登出隔离）均通过，`go test -race` 通过；handler 层验证已消费返回 401 且提示「已被使用」、关库返回 500。
- Seen 1× (2026-10-14)。状态：经验。

## 条目格式

```text
## 稳定编号：一句话规则
范围：仓库/模块/平台
失败：owner/repo#编号、commit SHA、日期、行为与后果
修复：commit SHA、验证对象与实际结果
Done right：正确示例及证据；没有时写待补
Seen N× (YYYY-MM-DD, YYYY-MM-DD)
状态：经验 / 升级候选 / 已升级（规范位置）
```
