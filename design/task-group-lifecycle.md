# TaskGroup 设计契约：生命周期与状态机

> 本文是 TaskGroup 设计契约系列之一（由原《独立并行任务组最终设计契约》按章节拆分）。
> 系列导航：[API 与选项](task-group-api-and-options.md) · [生命周期与状态机](task-group-lifecycle.md) · [提交与 rejection](task-group-submission.md) · [取消与归因](task-group-cancellation.md) · [监听、观测与验收](task-group-observability-and-verification.md)；路由索引见 [design/AGENTS.md](AGENTS.md)。

## 4. 对象与上下文生命周期

### 4.1 总览

```text
应用生命周期
GlobalPar ────────────────────────────────────────────────────────────

配置生命周期
TaskGroupSpec.Builder ── task* ── build ── TaskGroupSpec（不可变、可复用）

请求/显式协调生命周期
TaskGroup ── created/running ── all terminal ── closed

每个成员对象生命周期
MemberSpec ── added ── frozen ─┐
MemberState ──────────────────────── prepared/submitted/running ── terminal
MultiTaskContext ────────────────────────────────────────────────
TaskExecutionContext ─ created ─ queued ─ run ─ completed/event ─────

动态线程绑定
SubmissionScope                    └─ executor.execute(...) ─┘
TaskExecutionContext.current()                 └─ user callable ─┘

请求级观测（可选）
TaskGraphObservationScope ─────────────────────────────────────────
```

### 4.2 TaskGroup

`TaskGroupSpec` 只保存组级 options 和有序成员定义，是纯数据、可复用对象；外层上下文、
observation 和 deadline 归属在每次 `TaskGroup.submit()` 时按提交线程解析。成员定义
（`MemberSpec`）是 spec 的不可变组成部分，不新增公共 Context。spec 不属于任何物理线程。

`TaskGroup` 本身就是组级运行状态，MUST NOT 再新增 `TaskGroupContext` 或 `CurrentTaskGroupTl`。

它在 `TaskGroup.submit()` 的运行期创建阶段产生，至少持有：

```text
groupId / groupName
startTimeNanos / deadlineNanos
lifecycle
first completion reason
frozen members registry
memberCount / terminalCount
group CancellationToken（内部，不作为公共控制入口）
completion SettableFuture
failedMemberName
TaskGraphObservationScope snapshot（可空）
listener snapshot
```

其中 `first completion reason` 接受 member failure、deadline、group cancel/close、outer cancellation、成员被直接取消。单个成员被调用方直接取消会级联取消整个 Group（见 [取消与归因 §8.2](cancellation.md#82-成员主动取消)）。Group 对象在调用方、成员完成 listener 或 completion future 仍引用它时继续存活；它不属于任何物理线程。

### 4.3 MemberState

每个冻结成员需要一个内部状态记录。推荐作为 `TaskGroup` 的私有/包可见内部类，避免新增公共 Context：

```text
memberName
TaskExecutionContext
公开 ListenableFuture
执行 future/phase
member TaskOutcome（可空，成员终态时赋值）
failure（可空）
```

生命周期从 submit 的全量注册阶段开始，到 Group result 不再被引用为止。成员可能在用户函数开始前取消，此时 MemberState 存在，但 `TaskExecutionContext` 从未安装，且不得伪造 `TaskListener.TaskEvent`。

### 4.4 MultiTaskContext

每个成员创建一个单任务 `MultiTaskContext`：

```text
taskCount = 1
effectiveParallelism = 1
taskName = member MultiTaskOptions.name
executorIdentity / parLabel = member Par 的绑定
```

`memberName` 与 `taskName` 不重复承担同一职责：

- `memberName`：Group 内唯一键和组级结果键；
- `taskName`：现有任务执行、checkpoint 和 TaskListener 的诊断名称。

成员 options 中的 `parallelism` 不产生多个执行实例：成员是单任务，解析时被截断为 1，且
没有任何代码读取它。

### 4.5 TaskExecutionContext

每个成员在 submit 的准备阶段创建一个 `TaskExecutionContext(batch, 0, submitTimeNanos)`。所有成员的 `submitTimeNanos` 使用同一个 submit 提交基准时间，避免提交循环顺序改变组内计时口径。对象从准备阶段存在，但只在 `ScopedCallable.call()` 的用户任务执行阶段安装到普通 ThreadLocal：

```text
install member task
  markStarted
  checkpoint
  user callable
  markEnded
clear current task
  TaskListener callback（显式读取 TaskEvent）
restore previous task
```

这保证 inline/synchronous fallback 下的嵌套恢复：

```text
outer current -> member current -> outer current
```

TaskListener 回调期间 `TaskExecutionContext.current()` MUST 为 null，避免 listener 提交的新任务被误判为已完成成员的结构化子任务。

### 4.6 SubmissionScope

成员每次真正调用目标 executor 的 `execute/submit` 时，必须临时安装成员 Batch：

```java
MultiTaskContext previous = SubmissionScope.install(memberBatch);
try {
    executor.execute(memberFuture);
} finally {
    SubmissionScope.restore(previous);
}
```

它的生命周期只覆盖一次提交调用，不覆盖排队或任务执行。必须保留栈式恢复，以支持拒绝后 inline 执行期间再次嵌套提交的情况。

### 4.7 TaskGraphObservationScope

`submit()` 解析提交线程上同一个 `GlobalPar` 当前有效的 observation；不存在或 owner 不匹配时按 null 处理。成员执行必须使用该解析结果，而不是把 Group membership 写成 TaskGraph edge。

调用方必须让 observation 生命周期覆盖 Group 的所有成员执行。若 observation 已提前关闭，成员不得复活它，后续图记录可以安全忽略。

实现可在创建 `TtlCallable` 快照时短暂 install/restore Group 捕获的 observation，或者为单任务提交 helper 提供显式 observation 参数；MUST NOT 新增 group TTL。

### 4.8 TTL 边界

成员仍使用 `TtlCallable.get(callable, true, true)`，使提交时应用已有的 `TransmittableThreadLocal` 按 TTL 规则捕获和恢复。框架不承诺传播普通 `ThreadLocal` 或自动配置 MDC。排队任务会持有 TTL 快照，这是 TTL 本身的 retention 语义。

## 5. 结构化 parent、取消 parent 与 deadline 上限必须解耦

现有 `MultiTaskContext.resolve()` 把三件事都从 `parent MultiTaskContext` 推导：

1. TaskGraph/嵌套结构 parent；
2. cancellation token parent；
3. deadline ceiling。

Group member 证明这三者不总是同一个对象。`MultiTaskContext` 提供一个包可见重载，使三者可独立传入：

```java
static MultiTaskContext resolve(
        MultiTaskOptions options,
        int taskCount,
        @Nullable MultiTaskContext structuralParent,
        @Nullable CancellationToken cancellationParent,
        long deadlineCeilingNanos,
        long resolutionTimeNanos,
        @Nullable TaskGraphObservationScope observation,
        ExecutorIdentity executorIdentity,
        String parLabel);
```

语义不可合并回虚假 parent。

### 5.1 Group 在普通请求线程创建

```text
group structural parent = null
group token parent       = null
group deadline           = submit time + group timeout

member structural parent = null
member token parent       = group token
member deadline           = min(member requested deadline, group deadline)
```

### 5.2 Group 在一个正在执行的 scoped task 中创建

`TaskGroup.submit()` 按提交线程解析当时的 `TaskExecutionContext.current()` 作为结构父任务；同一个 spec 无论之后从哪个线程提交，归属都由该次提交现场决定：

```text
outerBatch = currentTask.batchContext()

group token parent        = outerBatch.cancellationToken
group deadline            = min(requested group deadline, outerBatch.deadline)

member structural parent  = outerBatch
member token parent        = group token
member deadline            = min(requested member deadline, group deadline)
```

Group 本身不是一个虚构 Batch，也不创建 Group TaskGraph node。每个 member 与 `outerBatch` 之间可以记录真实的 outer-to-member 依赖边；members 之间不得产生边。若 Group 在请求线程创建，则没有这些边。

外层 token 的取消通过 token 构造期挂接的 parent 监听传播为 group token 的
`PROPAGATED_CANCELED`，不靠轮询。收敛归因读取 `originState()`（沿 parent 链找首个非传播
终态）：外层是超时则 Group 固定 `TIMEOUT`，其余外层取消固定 `GROUP_CANCELED`（若失败/超时
已先固定则不变）。

## 6. 状态机与完成条件

`TaskGroupSpec` 是不可变纯数据，不存在配置/消费状态机；只有运行 Group 有生命周期：

```text
Group:   RUNNING -----all members terminal--> CLOSED
```

- `TaskGroupSpec.Builder` 非线程安全，调用方必须在一个配置流程中完成定义后 `build()`；build 出的
  spec 可安全共享并重复提交；
- 每次 `TaskGroup.submit()` 独立创建运行对象；spec 没有"已消费"状态；
- 返回的 Group 从一开始就持有完整、不可扩展的成员集合；
- `CLOSED` 只表示全部公开成员 future 已终态且不可变结果已经发布。

完成原因单独记录，并遵循 first-wins：

```text
null --first member failure--> 失败成员自己的 outcome（USER_FAILURE / SUBMISSION_FAILURE）
null --deadline-------------> TIMEOUT
null --group cancel/close----> GROUP_CANCELED
null --member direct cancel--> GROUP_CANCELED（级联先取消 group token）
null --parent cancel/timeout-> GROUP_CANCELED / TIMEOUT（按 originState 归因）
null --all success-----------> SUCCESS
```

规则：

- 非成功原因一旦固定，后续事件不得覆盖；
- 非成功原因会取消其他未完成成员；
- `SUCCESS` 只有在所有冻结成员均成功时才能固定；
- 单个成员被调用方直接取消时立即级联：先取消 group token，再取消其余未完成成员的 token，
  Group 原因在最终收敛时固定为 `GROUP_CANCELED`（若失败/超时已先固定则不被覆盖）；
- 全部成员终态且存在直接取消成员时，若没有更早的组级失败/超时，Group 原因固定为 `GROUP_CANCELED`；
- 组在 group token 仍 `RUNNING` 时收敛（成员 observer 先于 group bind 回调触发）且并非全部
  成功时，Group 原因固定为 `MEMBER_CANCELED`；
- `CLOSED` 只在 `terminalCount == memberCount` 时发布；
- Group 原因可以先固定，但 completion future 仍必须等所有公开成员 future 达到终态；
- 空 spec submit 后返回立即以 `SUCCESS` 完成的 Group，不启动物理 deadline timer；

建议 Group registry 在发布前构造完成，此后只读；完成原因和计数转换使用原子操作或一把私有 lock。成员 future 的完成 callback 在 lock 内只更新小型状态和决定后续动作，取消 future、触发 listener 等外部调用必须在 lock 外执行，防止重入和长时间占锁。

## 12. GlobalPar 关闭与资源所有权

- `TaskGroupSpec` 是纯配置对象，创建它不验证 GlobalPar 状态，也不长期 retain；
- `TaskGroup.submit()` 的冻结、运行对象创建和全量成员 admission 必须整体通过一次 `GlobalPar.whileOpen()`，使 submit 要么在线性化点先于 close 接纳完整组，要么完整拒绝；不得出现只接纳一部分成员；
- submit 完成 admission 后，即使 GlobalPar 随后关闭，冻结成员也必须完成取消、timeout、listener 和结果收敛；
- 每个冻结成员通过 `retainUntilComplete()` 计入活动运行；可以增加 group-aware retain helper，但不得提前关闭 timer/submitter/maintenance 服务；
- Group 不创建或关闭业务 executor；
- `GlobalPar.close()` 不阻塞，不关闭注册 executor；
- Group deadline 使用 GlobalPar 拥有的 scheduler。
