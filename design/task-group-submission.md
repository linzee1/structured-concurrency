# TaskGroup 设计契约：提交与 rejection

> 本文是 TaskGroup 设计契约系列之一（由原《独立并行任务组最终设计契约》按章节拆分）。
> 系列导航：[API 与选项](task-group-api-and-options.md) · [生命周期与状态机](task-group-lifecycle.md) · [提交与 rejection](task-group-submission.md) · [取消与归因](task-group-cancellation.md) · [监听、观测与验收](task-group-observability-and-verification.md)；路由索引见 [design/AGENTS.md](AGENTS.md)。

## 7. submit、冻结与统一提交契约

### 7.1 配置期校验

`TaskGroupSpec.Builder.task()` 应尽早拒绝以下定义错误，且不得产生任何运行状态：

- memberName 为空或重复；
- 参数为 null。

成员 executor 按注册名在 `submit()` 时经 `GlobalPar.par(executorName)` 解析，未知名称抛出
`IllegalArgumentException`。`task()` 不检查或消耗 deadline，因为 Group 的逻辑执行时间从
submit 开始。

executor rejection 只有实际提交时才能知道，因此属于 submit 后的成员运行结果，不是 spec 校验失败。被目标 executor 拒绝的 CPU-bound 成员按现有策略在提交线程 inline 执行，属于正常执行路径；非 CPU-bound 成员被拒绝时不运行用户 callable，公开 future 以 `SUBMISSION_FAILURE` 终态并触发 Group fail-fast（批次侧同一拒绝会使整批 fail-fast）。

### 7.2 submit 线性化与步骤

`TaskGroup.submit()` 必须作为一次整体 admission 与 `GlobalPar.close()` 线性化，不能按成员分别跨越关闭边界。推荐让下列准备和注册阶段整体处于一次 `GlobalPar.whileOpen()` 中；实际 executor 调用仍须在内部锁和 GlobalPar admission lock 外进行：

本文所称“统一提交”是指所有成员共享一个逻辑 submission boundary：submit 前没有任何运行状态或执行，submit 时一次性冻结完整集合并使用同一个提交基准时间。它不表示对多个不同 executor 的 `execute()` 做物理原子广播；这些调用必然有先后，但只能在全部成员完成准备和注册后开始。

必须满足：

1. 在 `GlobalPar.whileOpen()` 内解析结构父任务/observation、按注册名解析成员 executor 并冻结有序任务定义；
2. 读取统一的 `startTimeNanos`，解析 Group deadline，并创建 Group token/运行对象；
3. 为每个定义创建 member Batch、TaskExecutionContext、公开 future 和执行权竞争对象；
4. 将全部 `MemberState` 注册到 Group，发布完整 members registry；
5. 空组立即发布 `SUCCESS` 并返回，不创建 timer；非空组安排 Group deadline timer；
6. 退出所有 registry/admission lock；
7. 按定义顺序向各自目标 executor 提交同一个 prepared future；
8. 提交循环结束后返回 Group；若某个成员 inline 执行、失败或触发 fail-fast，剩余尚未调用 executor 的 prepared future 也必须被取消并达到终态。

不能在全部成员注册前调用任何 `executor.execute()`，否则 direct executor 或 rejection fallback 可能在 Group 看见完整成员集合前执行用户代码。

不能为了避免该竞态而在持有 Group lock 时调用 `executor.execute()`；executor 可能 inline 执行任意用户代码，导致 close/cancel 长时间无法取得锁。

`TaskGroup.submit()` 正常返回时必须保证完整 members registry 已发布（`future(TaskRef)`
可立即解析），并且每个仍未因 fail-fast/timeout/cancel 终结的成员都已经尝试过一次目标 executor 提交。由于 direct executor 可以 inline 执行，返回时部分甚至全部成员已经终态属于合法行为。

成功跨过全量注册后，单个 executor rejection、inline 用户异常或 fail-fast 均通过成员 future 和 `TaskGroupResult` 表达，`submit()` SHOULD 仍返回 Group，而不是因任务运行结果抛异常。只有定义校验、GlobalPar 已关闭，或无法建立完整运行对象的框架级准备错误才允许 submit 直接抛出；此时必须终结已创建的 future、释放 retain/timer 等资源，并且不得执行任何用户 callable。

### 7.3 Prepared single-task submission

两阶段单任务提交内核位于 `internal.TaskSubmissions`（`prepare` / `submitScoped`）与
`internal.ExecutionPhaseHintFuture`：`Par.map` 与 Group 共用同一内核，避免两套
取消/phase/TTL/ScopedCallable 实现：

```java
ExecutionPhaseHintFuture<Object> prepared = TaskSubmissions.prepare(taskContext, callable, listeners, phaseObserver);
// prepared 已存在（对外公开 future），但尚未交给 executor

registerAll(preparedTasks); // 所有 future 同时成为完整冻结集合

TaskSubmissions.submitScoped(prepared, batchContext, executor, cpuBound); // executor.execute outside group lock
```

必须保证：

- 公开 future 是对外返回、参与执行权竞争和 phase 观测的同一个逻辑 future；
- 区分用户直消与 Group 传播取消、fail-fast、timeout 不靠 `isCancelled()` 事后猜测；归因在
  收敛时读取 member/group token 状态（见 [取消与归因 §8.4](cancellation.md#84-deadline)）；
- `cancel()` 在线程取得执行权前成功后，之后的 prepared submission 不得进入用户 callable；
- prepared submission 被 executor 拒绝时，可以把 future 完成为 submission failure，不能遗留 pending future；
- 用户 callable 最多执行一次；
- phase 继续区分 `CANCELED_BEFORE_RUN` 和 `CANCEL_REQUESTED_RUNNING`；
- `SubmissionScope` 只包住实际 `executor.execute()`；
- 支持现有 CPU-bound rejection 后 inline 执行策略，但 inline 也必须遵守已注册和执行权竞态；
- 每个冻结 future 最终达到终态。

该内核同时供 `Par.map()` 和 Group 使用，避免两套取消/phase/TTL/ScopedCallable 实现。Batch 仍在其上保留 `SlidingWindowSubmitter` 的滑动窗口，Group 不使用滑动窗口。

## 9. 单任务运行内核的复用边界

### 9.1 必须复用

| 现有能力 | Group 中的用途 |
|---|---|
| `GlobalPar.whileOpen()` | 整体 submit 与 shutdown 的线性化 |
| `GlobalPar.timeoutScheduler()` | Group/member deadline |
| `GlobalPar.retainUntilComplete()` | 冻结成员完成前保留内部服务 |
| `Par`/`ExecutorRuntime` | executor、identity、label、blocking risk、phase observer |
| `ScopedCallable` | current task、checkpoint、计时、TaskListener、恢复 |
| `TaskExecutionContext` | 单成员任务执行身份与 timing |
| `SubmissionScope` | 一次 executor submission 的队列策略 |
| `ExecutionPhaseHintFuture` | run/cancel 执行权竞态与 purge phase |
| `CancellationToken` | outer→group→member 取消传播和中断 |
| `HeuristicPurger` | 取消排队任务后的有界队列清理 |
| `TtlCallable` | 已配置 TTL 的提交时快照与恢复 |
| `TaskGraphObservationScope` | 请求级观测归属 |

### 9.2 不得复用

- `SlidingWindowSubmitter`：Group 不使用滑动窗口、placeholder 或 completion queue 驱动提交；
- `TaskBatchResult`：Group 是异构成员和固定的具名集合；
- `Par.map(singletonList, ...)`：会引入错误抽象和不必要包装；
- 虚构的 Group `MultiTaskContext`：membership 不是 Batch；
- Group ThreadLocal/TTL：Group 由显式对象持有，不是线程隐式状态。

### 9.3 代码组织

```text
scope/
  TaskGroup.java
  TaskGroupSpec.java
  TaskRef.java
  TaskGroupResult.java
  TaskGroupMemberResult.java
  TaskOutcome.java

spi/
  TaskGroupListener.java

internal/
  TaskSubmissions.java            // prepare / submitScoped 两阶段内核
```

`ExecutorRuntime` 是 `scope` 包私有类型，`internal.TaskSubmissions` MUST NOT 直接依赖或公开它。
`Par` 提供包可见的单任务准备入口 `prepareGroupTask(...)`，在 `scope` 包内完成 owner、policy、
runtime identity、executor 和 phase observer 的解析，再把普通参数传给 internal kernel。
`TaskGroup` 与 `Par` 同包，可调用该入口；公共 API 不暴露 runtime。

共享内核至少分离以下阶段：

```text
Par/package-private entry
  ├─ validate owner and resolve member MultiTaskContext
  ├─ create TaskExecutionContext + ScopedCallable + TTL wrapper
  └─ internal kernel prepare execution future

Group
  ├─ register prepared future (linearization)
  └─ invoke prepared submission outside Group lock
```

Batch 路径由 `SlidingWindowSubmitter` 组织滑动窗口，但其 future 创建、phase claim、
SubmissionScope、rejection/inline 和 scoped callable 包装与 Group 共享同一个
`TaskSubmissions` 内核。不要为 Group 单独复制 `ScopedCallable`、execution phase future 或
cancellation token。
