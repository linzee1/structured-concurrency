# TaskGroup 设计契约：取消、deadline 与归因

> 本文是 TaskGroup 设计契约系列之一（由原《独立并行任务组最终设计契约》按章节拆分）。
> 系列导航：[API 与选项](task-group-api-and-options.md) · [生命周期与状态机](task-group-lifecycle.md) · [提交与 rejection](task-group-submission.md) · [取消与归因](task-group-cancellation.md) · [监听、观测与验收](task-group-observability-and-verification.md)；路由索引见 [design/AGENTS.md](AGENTS.md)。

## 8. 取消、deadline 与 fail-fast

### 8.1 Token 结构

不新增公共 `GroupCancellationToken`。Group 内部复用现有 `CancellationToken`，公共控制入口是 `group.cancel()`：

```text
outer task token（可空）
└─ group token
   ├─ member A token
   ├─ member B token
   └─ member C token
```

成员 deadline 先于 Group deadline 到期时，成员 token 上的 timeout 监听器必须把超时升级为 `groupToken.timeoutCancel()`，使 Group 收敛为 `TIMEOUT` 而不是 fail-fast 的 `FAILED`。

### 8.2 成员主动取消

调用方对成员 future（或成员 token）调用 `cancel()`：

- Group 取消语义与 batch 完全一致（结构化并发）：成员被直接取消即级联取消整个 Group；
- 该成员原因记录为 `MEMBER_CANCELED`；
- 未完成 siblings 通过各自 member token 级联取消，记录 `GROUP_CANCELED`；
- Group completion reason 固定为 `CANCELED`；
- 取消在线程取得执行权前获胜时，用户 callable 不得执行；
- 取消在 RUNNING 后获胜时发出中断请求，但不保证用户代码立即停止。

### 8.3 Fail-fast

任一成员出现 `USER_FAILURE` 或 `SUBMISSION_FAILURE`：

```text
CAS group reason FAILED
record failedMemberName（仅 first winner）
cancel every other unfinished member
wait every frozen public future terminal
publish CLOSED/result/event
```

被传播取消的 siblings 记录 `FAIL_FAST`，不能仅显示为笼统 cancelled。

### 8.4 Deadline

逻辑 deadline 在 `submit()` 时计算；spec 配置耗时不计入 Group timeout：

```text
requestedGroupDeadline = submitStartNanos + resolvedGroupTimeout
groupDeadline = outerBatch == null
        ? requestedGroupDeadline
        : min(requestedGroupDeadline, outerBatch.deadlineNanos())
```

非空组在全部成员准备并注册后、提交循环开始前安排一个物理 timer；空组不分配 timer。若外层 deadline 在 submit 准备期间已经到期，则 Group 固定 `TIMEOUT`，全部已注册成员记录 `TIMEOUT` 并取消，且不得进入用户 callable。

timer 触发时：

- first-wins 固定 `TIMEOUT`；
- 未完成成员取消并记录 `TIMEOUT`；
- timer 必须在 Group 先完成时取消或成为无害 no-op；
- timeout action 使用 `GlobalPar.timeoutScheduler()`，Group 不创建 scheduler。

成员 deadline：

```text
memberDeadline = min(member requested deadline, groupDeadline)
```

（`inheritTimeout()` 的成员解析为 groupDeadline。）

若成员自己的 deadline 先到并导致该成员失败/取消，Group 应固定 `TIMEOUT`，因为结果 API 已明确区分 timeout；不得把它误报成普通 user failure。

deadline 存储在 `CancellationToken` 内部（构造时与 parent 取 min），`bind(List, submitCanceller, timer)`
不再接收 Duration。Group 的 `start()` 按序做三件事：

1. 先给每个成员的公开 future 挂完成 observer，保证后续 bind 触发的取消都被计数；
2. 对 group token 做一次 `bind`（组 deadline + 统一 fail-fast + 全成功检测）；
3. 按条件做成员 bind：**仅当 `memberToken.deadlineNanos() < groupToken.deadlineNanos()`**（即成员
   拥有比组更紧的自己 deadline）时，才对该成员 token `bind` 自己的 future，并注册状态监听器
   （`TIMEOUT` 时调用 `groupToken.timeoutCancel()`，监听器在 CAS 提交之后、取消动作
   之前同步触发）。继承组 deadline 的成员解析出与组完全相同的 deadlineNanos，跳过成员 bind：
   向下传播已由 token 构造期的 parent 监听挂接（group → member `PROPAGATED_CANCELED`），
   成员 future 的取消由上面的 group bind 覆盖，成员 bind 只会为同一时刻多 arm 一个冗余
   timer。未 bind 的成员 token 永停 `RUNNING`（不会到 `SUCCESS`）——这是有意的隐式约束，
   归因改读 group token（见下）。

成员取消原因不由发起取消处手写，而是收敛后读 token state：成员 token `TIMEOUT`
即 `TIMEOUT`；否则 group token 是唯一权威（它在取消成员 futures 之前先提交自己的状态）：
`TIMEOUT`/`FAIL_FAST`/`CANCELED` 分别映射
`TIMEOUT`/`FAIL_FAST`/`GROUP_CANCELED`；`PROPAGATED_CANCELED` 读 `originState()`，祖先为超时
则记 `TIMEOUT`，否则记 `GROUP_CANCELED`；两个 token 都仍是 `RUNNING` 说明没有框架路径碰过
该成员，即用户直消，记 `MEMBER_CANCELED`。

组级 outcome 同样读 group token 推导（`TaskGroupResult.outcome()`）：`TIMEOUT` → `TIMEOUT`；
`FAIL_FAST` → 有失败成员则沿用该成员自己的 outcome（`USER_FAILURE`/`SUBMISSION_FAILURE`），
无失败成员（fail-fast 由成员直消触发）则记 `MEMBER_CANCELED`；`PROPAGATED_CANCELED` 按
`originState()` 归因 `TIMEOUT` 或 `GROUP_CANCELED`；`CANCELED`（用户直接 cancel 组或成员直消
级联）→ `GROUP_CANCELED`；token 仍在 `RUNNING`/`SUCCESS` 时全部成员成功记 `SUCCESS`，否则
`MEMBER_CANCELED`。

嵌套提交的终态不唯一但归因确定：成员 callable 内部的嵌套 batch 继承组 deadline 后自身也会被
bind、arm 自己的 timer，与传播级连同刻竞速，终态可能是 `TIMEOUT`（自己的 timer 先
触发）而非必然 `PROPAGATED_CANCELED`；两种终态经 `originState()` 都归因 TIMEOUT。只有未
bind 的 token 确定为 `PROPAGATED_CANCELED`（只有传播能移动它）。嵌套 batch 自己的超时对外
层只表现为成员失败——谁拥有触发的 deadline，谁报 TIMEOUT。

### 8.5 close

`close()` 不阻塞：

- 空组或所有冻结成员已终态：无副作用；
- 存在未完成成员：等同 `cancel()`；
- 已 `CLOSED`：幂等；
- 不关闭 `GlobalPar` 或任何注册 executor。
