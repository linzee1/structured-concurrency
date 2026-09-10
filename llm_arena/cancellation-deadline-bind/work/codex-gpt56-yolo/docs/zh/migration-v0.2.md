# v0.2 迁移指南

`0.2.0` 用不可变执行拓扑替代可变配置和运行期 resolver，是一次源码级破坏性迁移。

| `0.1.x` | `0.2.0` |
|---|---|
| `ParConfig.builder().executor(name, executor)` | `GlobalPar.builder().register(name, executor)` |
| `new Par(config)` | `global.par(name)` |
| `ParOptions` | `BatchExecutionOptions` |
| `par.map(name, items, fn, options)` | `par.map(items, fn, options)` |
| `ParConfig` 的 timeout/listener 默认值 | `GlobalExecutionPolicy` |
| `ParConfig` 的 livelock 设置 | `GlobalParDeadlockPolicy` |
| `ParConfig` 的 purge 设置 | `GlobalParPurgePolicy` |
| 调用时按名称解析执行器 | `GlobalPar` 构建期绑定执行器 |
| `TaskGraph.destroyAfterRequest(config)` | `global.openTaskGraphObservation()` 作用域 |

新的类型边界是刻意设计：`BatchExecutionOptions` 是调用方输入，`BatchExecutionContext` 是单批运行时状态。取消、deadline 和执行器 identity 通过父子批次上下文传播，也支持跨具名 `Par` 的嵌套调用。

早期 `0.2.x` 快照曾将该类型命名为 `ExecutionOptions`。请将 import、变量声明和 `Par.map` 参数统一改为 `BatchExecutionOptions`；在 `0.x` 阶段不保留兼容别名。

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
`TaskGroupMemberResult.completionReason()` 返回 `TaskOutcome`。

旧的 `ParConfig`、`ParOptions`、`ExecutorResolver`、`GlobalParConfig` 及旧版 `Par` 入口都不是兼容别名。迁移时请同时更新 import、构建方式和调用方式。注册的执行器仍由应用拥有并负责关闭。


### CancellationToken 持有 deadline，Group 取消语义与 Batch 对齐

`CancellationToken` 现在持有自己的 deadline。原来接收 `Duration` 的 `bind` 重载被移除：

| 早期快照 | 现状 |
|---|---|
| `new CancellationToken()` | 不变（根 token，无 deadline） |
| `new CancellationToken(parent)` | 不变（子 token 继承 parent deadline 上限） |
| `new CancellationToken(parent, deadlineNanos)` | 新增；生效 deadline 为 `min(deadlineNanos, parent deadline)` |
| `token.bind(futures, Duration, submitCanceller, timer)` | `token.bind(futures, submitCanceller, timer)` —— 剩余时间由 token 的 deadline 推导 |
| `State.NO_OP` | 删除 |
| — | 新增 `deadlineNanos()`、`remaining()`、`timeoutCancel()`、`addStateListener(...)` |

`bind` 是唯一的 deadline + fail-fast 执行体：bind 时 deadline 已到会同步走超时路径（取消未完成
future 与 submitter，已完成 future 保留结果），未到则用传入 timer 调度剩余时间。每个终态都先
CAS 记录、再同步通知 state listener、最后执行取消动作，因此取消归因改为读取 token state，而
不是在发起取消处手写 reason。

早期 `0.2.x` 快照把外部取消的绑定 future 归类为 batch fail-fast（`FAIL_FAST_CANCELED`），但
fail-fast 聚合一旦已经完成就不会再取消其余 siblings。现在 `bind` 直接取消绑定 futures，因此
元素被取消或失败一定会停止未完成 siblings 和提交循环。`ParallelTaskGroup` 使用同一套 token
语义：直接取消某个成员 future 现在会级联取消整个 group（源成员 `MEMBER_CANCELED`，siblings
`FAIL_FAST`，组 reason `CANCELED`），而不是让 siblings 继续运行；先到期的成员 deadline 会把
group 升级为 `TIMEOUT`。
