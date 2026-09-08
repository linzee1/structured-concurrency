# LLM Arena: cancellation-deadline-bind

复原一次真实失败会话的现场，用于对比不同 coding agent 在同一任务上的表现。

## 背景（历史真实事件）

1. **用户手改** `CancellationToken.bind()`（提交 `30359e5`，作者 Eric Lin），把旧的
   `addCallback` 实现重写为 `FluentFuture` 链式（transform + catchingAsync）结构。
2. 这份手改**引入了两个隐藏 bug**（fail-fast 不再取消 sibling；手动取消不再取消
   submitCanceller），当时的测试没跑或没暴露。
3. 随后 agent 按契约 handoff 实现「deadline 进 token + task group 接入 bind」——实现中
   撞上 bug（测试红），修改过程**没有找到根因**，最后用「同步块 + 补偿逻辑」
   （storeSubmitCanceller / cancelBoundWork 双锁）绕了过去。结果复杂、low-level，
   用户判定为差设计。
4. 根因后来查明：Guava 组合器语义——`catchingAsync` 的 fallback 在**从输出侧传播的
   取消**下不会被调用（`run()` 的 `isCancelled()` 早退），而 `addCallback` 的
   `onFailure` 会收到 `CancellationException`。修复只需恢复 addCallback 形态。
5. 参照实现存在于 **vformation 仓库主分支历史**：`6dd474a`（deadline + group 接入）、
   `fbfe4f5`（restore addCallback）。**勿拷入本 arena**。

## 现场内容

| 文件 | 说明 |
|---|---|
| `handoff.md` | 接手 agent 的完整交接（任务契约，原版逐字） |
| `snapshot.bundle` | 现场 git 仓库：HEAD = `30359e5`（含 bug 的 bind），**不含**之后任何修复 commit |

复原验证：现场跑 `CancellationTokenTest,CancellationTriggerCartesianTest`
= 21 跑、5 失败、1 错误（bug 在场的红灯）。

## 跑法（让 coding agent pickup）

```bash
git clone llm_arena/cancellation-deadline-bind/snapshot.bundle <workdir>
cd <workdir>
git switch -c <agent>-attempt arena-restore   # bundle 无 HEAD，需显式 checkout
cp ../llm_arena/cancellation-deadline-bind/handoff.md .
# 让 agent 从 handoff.md 开始：先读文件 → 按需求 1/2 实现 → 跑测试 → 更新文档
```

要求 agent：提交自己的工作（commit 留在 `<workdir>`），便于事后 diff 对比。

## 跑其他 coding agent（codex / Claude / 任意 CLI）

推荐用 `run-agent.sh`（自动 clone bundle + 建分支 + 放入 handoff，隔离参考答案）：

```bash
./run-agent.sh <attempt名> <agent命令...>
```

实际例子：

```bash
# codex（openai codex CLI）
./run-agent.sh codex-gpt56-sol codex exec \
    "pick up handoff.md and implement it per the handoff; run the tests; commit your work"

# Claude Code
./run-agent.sh claude-baseline claude --permission-mode default \
    "pick up handoff.md and implement it per the handoff; run the tests; commit your work"

# 不带 agent 命令：只准备 work 目录 + 打印 pickup 提示，自己开终端/IDE 进去跑
./run-agent.sh my-manual-run
```

产物在 `work/<attempt名>/`：agent 的全部 commit 留在里面，评测时：

- 看它自己的 `git log` / `git diff 30359e5..attempt`
- 对照参考答案 `git diff 30359e5..fbfe4f5`（在主仓库跑）
- 跑 `mvn test` 验证行为，`mvn spotless:apply` 后复跑确认没靠格式绕过

> 隔离纪律：agent 的工作目录永远是 bundle clone（`run-agent.sh` 保证）。不要直接在
> 主仓库 clone 里跑 agent——主分支历史含参考答案（`6dd474a..fbfe4f5`），对 agent 是泄题。

## 其他人复现

仓库已公开（`https://github.com/HuaTalk/parallel-in-scope`），其他人只要：

```bash
git clone -b feat/parallel-task-group-builder https://github.com/HuaTalk/parallel-in-scope.git
cd parallel-in-scope/llm_arena/cancellation-deadline-bind
./run-agent.sh my-agent codex exec "pick up handoff.md and implement it; commit your work"
```

需要：git、JDK 11+、Maven（跑测试）、目标 agent 的 CLI。整个 case 自包含在
`llm_arena/cancellation-deadline-bind/` 一个目录里（bundle 内无参考答案，主历史里的
参考答案对评测人可见，对 agent 不可见）。做完把 `work/` 的结果或 README 记录表贴回来即可。

## 评价视角

- **根因发现**：是否识别出「catchingAsync fallback 在取消时不执行」/「allAsList
  失败后 cancel 是 no-op」这两个 Guava 语义，而不是绕过式修复？
- **设计质量**：取消逻辑是否收敛到状态机 + 单一取消出口？是否引入锁/字段/补偿逻辑？
- **契约完成度**：deadline min(parent)、expired-at-bind 同步路径、group 级联语义、
  TIMEOUT 升级 race、归因表是否逐条落实？
- **测试**：是否补了失败场景的回归用例？红灯是否先复现再变绿？

## 每轮记录

| agent | commit(s) | 测试结果 | 根因命中 | 设计备注 |
|---|---|---|---|---|
| codex (gpt-5.6-sol, yolo) | `faf795c` (+657/-234, 15 files) | 410/410 绿（独立复跑确认） | ✗ 未命中 | 保留 catchingAsync 结构，加 cancelWork() 在每取消入口直接 cancel 原始 futures + volatile 字段双保险；取消动作冗余（futureToken 级联 + 手动 cancel 各一遍）；归因含 failedMemberName==null 特判；agent 自曝 stateListener 注册竞态无锁 |
|      |           |          |          |          |
