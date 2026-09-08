# v0.2 迁移指南

`0.2.0` 用不可变执行拓扑替代可变配置和运行期 resolver，是一次源码级破坏性迁移。

| `0.1.x` | `0.2.0` |
|---|---|
| `ParConfig.builder().executor(name, executor)` | `GlobalPar.builder().register(name, executor)` |
| `new Par(config)` | `global.par(name)` |
| `ParOptions` | `MultiTaskOptions` |
| `par.map(name, items, fn, options)` | `par.map(items, fn, options)` |
| `ParConfig` 的 timeout/listener 默认值 | `GlobalExecutionPolicy` |
| `ParConfig` 的 livelock 设置 | `GlobalParDeadlockPolicy` |
| `ParConfig` 的 purge 设置 | `GlobalParPurgePolicy` |
| 调用时按名称解析执行器 | `GlobalPar` 构建期绑定执行器 |
| `TaskGraph.destroyAfterRequest(config)` | `global.openTaskGraphObservation()` 作用域 |

新的类型边界是刻意设计：`MultiTaskOptions` 是调用方输入，`MultiTaskContext` 是单批运行时状态。取消、deadline 和执行器 identity 通过父子批次上下文传播，也支持跨具名 `Par` 的嵌套调用。

早期 `0.2.x` 快照曾将该类型命名为 `ExecutionOptions`，随后改为 `BatchExecutionOptions`。请将 import、变量声明和 `Par.map` 参数统一改为 `MultiTaskOptions`；在 `0.x` 阶段不保留兼容别名。

单次调用的运行时上下文也因同样理由改名：原 `BatchExecutionContext` 现为 `MultiTaskContext`，因为它同时支撑 `Par.map` 批次与任务组成员。请更新 import 与 `resolve(...)` 调用点；`batchId()`、`batchContext()` 等访问器名称不变。

批次与任务组的选项类型已统一为单一的 `MultiTaskOptions`；原先的 `BatchExecutionOptions`
和 `TaskGroupOptions` 已删除。`taskName()`/`groupName()` 访问器及对应的 builder 方法统一为
`name()`；其余 builder 方法名称不变。批次读取 name/parallelism/timeout/taskType/rejectEnqueue；
任务组读取 name/timeout/listeners，成员的执行策略按 `TaskGroupSpec.Builder.task` 逐个传入。

`MultiTaskOptions.timeout` 现在必须在两个互斥的 builder 声明中显式二选一：
`timeout(Duration)` 设置正数显式超时，`inheritTimeout()` 声明继承外层作用域的 deadline。
两者都未声明或同时声明时 `build()` 抛出 `IllegalArgumentException`。访问器由
`Duration timeout()` 改为 `Optional<Duration> timeout()`；空值表示继承。
`GlobalExecutionPolicy.defaultTimeoutMillis` 已删除，不再存在隐式的全局默认超时。
`MultiTaskContext.resolve` 相应不再接收 policy 参数，调用时删除该实参。

deadline 解析遵循统一规则：显式 timeout 取自身上限与外层硬 deadline 的较早者；继承时解析为
外层 deadline——`Par.map` 批次或任务组继承所在 scoped task 的 deadline，组成员继承组的
deadline。没有外层 deadline 可继承时，入口点直接拒绝：顶层 `Par.map` 与顶层
`TaskGroup.submit` 都抛出 `IllegalArgumentException`，提示改用 `timeout(Duration)`。

任务组 API 现在以不可变、可复用的 spec 为中心。请把早期的 builder 流程——
`GlobalPar.taskGroupBuilder(options)`、`ParallelTaskGroup.Builder.addTask(name, par, callable,
options)`、一次性的 `buildAndSubmitAll()` 和 `ParallelTaskGroup.TaskHandle<T>`——替换为
`TaskGroupSpec.builder(groupOptions)`、`TaskGroupSpec.Builder.task(ref, executorName,
callable, options)`、一次性的 `TaskGroup.submit(global, spec)` 和 `TaskRef<T>`。
成员按注册名而不是 `Par` 对象引用执行器。`TaskRef<T>` 由调用方以匿名子类形式创建——
`new TaskRef<List<Order>>("orders") {}`——因此令牌同时携带成员名并在运行时捕获结果类型；
将它传给 `task()`，提交后通过 `group.future(ref)` 取回成员 future，若令牌的 raw 结果类型
不能覆盖注册类型则会被拒绝。spec 不捕获线程上下文，结构父任务与观测
作用域在每次 `submit` 时按提交线程解析，因此同一个 spec 可以重复提交。组入口类本身也由
`ParallelTaskGroup` 改名为 `TaskGroup`，归入 `TaskGroupSpec`/`TaskGroupResult`/`TaskGroupListener`
家族。早期 builder API 从未作为稳定契约发布，因此不提供兼容 shim。

早期快照还曾将这套检测命名为 `GlobalParLivelockPolicy` 和 `LivelockListener`。请分别改为 `GlobalParDeadlockPolicy` 和 `DeadlockDetectionListener`；当前检测针对依赖图中的潜在死锁结构，不证明运行时已经死锁，也不检测活锁。

`TaskListener.TaskEvent` 现在通过 `getTaskContext()` 暴露已完成任务，并通过
`isSuccessful()`、`getResult()` 和 `getException()` 表达执行结果。成功任务也可能返回
null，因此不要用 result 是否为 null 判断成败。监听器回调不属于已完成任务的动态执行作用域，
应读取 event，而不是依赖 `TaskExecutionContext.current()`。

任务终态分类已统一为单个枚举 `TaskOutcome`，取代原先的
`io.github.huatalk.parallelinscope.internal.FutureState` 与
`io.github.huatalk.parallelinscope.scope.TaskGroupMemberReason`。`TaskOutcome` 在原成员原因值
之上补充了 `RUNNING`，因此可同时服务批量报告与组成员结果。映射关系：`FutureState.FAILED` →
`TaskOutcome.USER_FAILURE`，`FutureState.CANCELLED` → `TaskOutcome.MEMBER_CANCELED`，
`TaskGroupMemberReason.X` → `TaskOutcome.X`（同名）。相应地，
`AsyncBatchResult.BatchReport.stateCounts()` 现在以 `TaskOutcome` 为键，
`TaskGroupMemberResult` 的成员终态由 `outcome()` 暴露并返回 `TaskOutcome`（由早期的
`completionReason()` 改名而来）。

## 终态词汇统一

`TaskGroupCompletionReason` 已删除，组级结果复用 `TaskOutcome`：
`TaskGroupResult.completionReason()` 改名为 `outcome()`，返回 `TaskOutcome`。映射关系：
`SUCCESS` → `TaskOutcome.SUCCESS`；`TIMEOUT` → `TaskOutcome.TIMEOUT`；`FAILED` → 失败组员
自己的 outcome（`USER_FAILURE` 或 `SUBMISSION_FAILURE`，见 `failedMemberName()`）；
`CANCELED` → 组被整体取消或取消自上传播时为 `GROUP_CANCELED`，取消源自组员时为
`MEMBER_CANCELED`。

`CancellationToken.State` 值名对齐同一词汇：`FAIL_FAST_CANCELED` → `FAIL_FAST`，
`TIMEOUT_CANCELED` → `TIMEOUT`，`MUTUAL_CANCELED` → `CANCELED`，`PROPAGATING_CANCELED` →
`PROPAGATED_CANCELED`。`RUNNING`、`SUCCESS` 不变，`code()` 值与
`shouldInterruptCurrentThread()` 语义不变。

`ExecutionPhase.CANCELLED_BEFORE_RUN` 拼写修正为 `CANCELED_BEFORE_RUN`，与库内统一的
单 L `CANCELED` 拼写一致。

旧的 `ParConfig`、`ParOptions`、`ExecutorResolver`、`GlobalParConfig` 及旧版 `Par` 入口都不是兼容别名。迁移时请同时更新 import、构建方式和调用方式。注册的执行器仍由应用拥有并负责关闭。
