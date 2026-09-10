# v0.2 迁移指南

`0.2.0` 用不可变执行拓扑替代可变配置和运行期 resolver，是一次源码级破坏性迁移。

发布身份同时变更：GitHub 账号由 `huatalk` 改名为 `monadrome`，`io.github.huatalk:parallel-in-scope` 变为 `io.github.monadrome:parallel-in-scope`，根 Java 包 `io.github.huatalk.parallelinscope` 变为 `io.github.monadrome.parallelinscope`。同步更新依赖坐标、import、`package` 声明和 SPI 服务加载名。`0.1.0` 仍以旧坐标发布在 Maven Central 上。

`0.2.0` 最终包结构同时将所有公开 API 和回调类型收敛到
`io.github.monadrome.parallelinscope`。将早期 `.scope` / `.cancel` / `.context` / `.spi`
包的 import 改为根包；`SmartBlockingQueue` 也从 `.queue` 移到根包。只有
`DrainingBlockingQueue` 和 `VariableLinkedBlockingQueue` 保留在
`io.github.monadrome.parallelinscope.queue`。旧 `.internal` / `.control` 包已移除，
上下文、图、提交、purge 和执行阶段等实现类型改为包私有；`0.x` 阶段不保留兼容 shim。

| `0.1.x` | `0.2.0` |
|---|---|
| `ParConfig.builder().executor(name, executor)` | `GlobalPar.builder().register(ParName.of(name), executor)` |
| `new Par(config)` | `global.par(ParName.of(name))` |
| `ParOptions` | `BatchOptions`（批次）/ `TaskGroupOptions`（组）/ `TaskOptions`（成员与 combine） |
| `par.map(name, items, fn, options)` | `par.map(items, fn, options)` |
| `ParConfig` 的 timeout/listener 默认值 | `GlobalPar.Builder.taskListener(...)`（timeout 仍按调用声明） |
| `ParConfig` 的 livelock 设置 | `GlobalParDeadlockPolicy` |
| `ParConfig` 的 purge 设置 | `GlobalParPurgePolicy` |
| 调用时按名称解析执行器 | `GlobalPar` 构建期绑定执行器 |
| `TaskGraph.destroyAfterRequest(config)` | `global.openTaskGraphObservation()` 作用域 |

公开面与运行时的边界是刻意设计：选项类型是调用方输入，包私有的
`MultiTaskContext` 是单次调用的运行时状态。取消、deadline 和执行器 identity 仍通过父子上下文
传播，但应用只应通过选项类型配置语义，不应构造或缓存运行时 context。

执行器查找键由裸 `String` 改为值类型 `ParName`。所有表示 `Par` 名称的位置都接收 `ParName`：`GlobalPar.Builder.register`/`defaultPar`/`parTaskListener`、`GlobalPar.par`/`find`/`taskListenersFor`、`GlobalPar.pars()`，以及 `TaskGroupDefinition.Builder.task`/`combine`。构造时一次性校验（非 null、非空白），值按原样使用——不做 trim 或小写规范化——因此既有键的语义不变。`ParName` 是逻辑查找键，不是资源身份：它不能替代 `ExecutorIdentity`，后者仍是 deadlock 检测和 purge 所依据的引用相等键。格式合法的 `ParName` 也不代表名称已注册；未注册名称仍分别在 `GlobalPar.Builder.build()` 和 `TaskGroup.submit` 被拒绝。

早期 `0.2.x` 快照曾把批次选项命名为 `ExecutionOptions`，随后改为 `BatchExecutionOptions`，再短暂统一为超集类型 `MultiTaskOptions`。最终按作用域拆分为三个类型：`BatchOptions`（`Par.map`）、`TaskGroupOptions`（`TaskGroupDefinition.builder`）、`TaskOptions`（`TaskGroupDefinition.Builder.task`/`combine`）。请将 import、变量声明和实参改为对应作用域的类型；在 `0.x` 阶段不保留兼容别名。

早期 `0.2.x` 快照曾将内部 `BatchExecutionContext` 改名为 `MultiTaskContext`，因为它同时支撑
`Par.map` 批次与任务组成员。最终 API 完全隐藏该运行时 context 及其 `resolve(...)` 方法。
删除对 `MultiTaskContext`、`SubmissionScope` 和 `TaskExecutionContext` 的 import 或直接构造；
调用配置使用批次或成员自己的选项类型，已完成任务通过 `TaskCompletion` 观测。原先的只读
`TaskContext` 视图也已移除，其对用户有用的身份和计时字段已打平到 `TaskCompletion`。

选项类型与作用域一一对应，每种类型只声明其消费者读取的字段：`BatchOptions` 声明
name/parallelism/timeout/taskType/rejectEnqueue；`TaskGroupOptions` 只声明 name/timeout/listeners；
`TaskOptions` 只声明 timeout/taskType/rejectEnqueue。身份不属于选项——成员名来自 `TaskKey`；
并发度属于扇出——单任务没有可限流的对象。`taskName()`/`groupName()` 访问器及对应方法统一为
`name()`；`TaskGroupDefinition.Builder.task`/`combine` 的实参由超集类型收窄为 `TaskOptions`。

timeout 现在必须在两个互斥的静态工厂里显式二选一：`timeout(name, Duration)` 设置正数显式
超时，`inheritTimeout(name)` 声明继承外层作用域的 deadline。没有 Builder，也没有第三种状态：
遗漏声明不是运行时异常而是编译错误，两个声明也不可能同时出现。`TaskOptions` 的两个工厂不带
name。访问器仍是 `Optional<Duration> timeout()`；空值表示继承。
原先的全局默认超时已删除，不再存在隐式的全局默认超时；deadline 解析现由执行内核完全负责。

deadline 解析遵循统一规则：显式 timeout 取自身上限与外层硬 deadline 的较早者；继承时解析为
外层 deadline——`Par.map` 批次或任务组继承所在 scoped task 的 deadline，组成员继承组的
deadline。没有外层 deadline 可继承时，入口点直接拒绝：顶层 `Par.map` 与顶层
`TaskGroup.submit` 都抛出 `IllegalArgumentException`，提示改用 `timeout(Duration)`。

任务组 API 现在以不可变、可复用的 definition 为中心。请把早期的 builder 流程——
`GlobalPar.taskGroupBuilder(options)`、`ParallelTaskGroup.Builder.addTask(name, par, callable,
options)`、一次性的 `buildAndSubmitAll()` 和 `ParallelTaskGroup.TaskHandle<T>`——替换为
`TaskGroupDefinition.builder(groupOptions)`、
`TaskGroupDefinition.Builder.task(key, executorName, callable, options)`、一次性的
`TaskGroup.submit(global, definition)` 和 `TaskKey<T>`。
成员按注册名而不是 `Par` 对象引用执行器。`TaskKey<T>` 由调用方以匿名子类形式创建——
`new TaskKey<List<Order>>("orders") {}`——因此键同时携带成员名并在运行时捕获结果类型；
将它传给 `task()`，提交后通过 `group.future(key)` 取回成员 future，若键的 raw 结果类型
不能覆盖注册类型则会被拒绝。definition 不捕获线程上下文，结构父任务与观测
作用域在每次 `submit` 时按提交线程解析，因此同一个 definition 可以重复提交。组入口类本身也由
`ParallelTaskGroup` 改名为 `TaskGroup`，归入 `TaskGroupDefinition`/`TaskGroupResult`/`TaskGroupListener`
家族。早期 builder API 从未作为稳定契约发布，因此不提供兼容 shim。

早期快照还曾将这套检测命名为 `GlobalParLivelockPolicy` 和 `LivelockListener`。请分别改为 `GlobalParDeadlockPolicy` 和 `DeadlockDetectionListener`；当前检测针对依赖图中的潜在死锁结构，不证明运行时已经死锁，也不检测活锁。

已完成任务的记录统一为单个类 `io.github.monadrome.parallelinscope.TaskCompletion`：
`TaskListener` 在任务完成时收到它，`TaskGroupResult.members()` 也以它为每个成员的终端快照。
它同时取代旧的 `TaskListener.TaskEvent` 与 `TaskGroupMemberResult`，以打平字段暴露任务身份与
计时——`taskName()`、`unitId()`、`taskIndex()`、`submitTimeNanos()`、`startTimeNanos()`、
`endTimeNanos()`——外加 `outcome()`、`successful()`、`result()`、`failure()` 和
`enqueued()`（现由队列等待时长派生）。原先的只读 `TaskContext` 视图已移除，此前可经
`TaskContext.multiTaskContext()` 触及的引擎管道（取消令牌、deadline、结构父级）不再出现在
监听器/结果 API 面上。`result()` 仅在监听器投递成功任务时非 null——组成员的结果留在其
future 中——`taskIndex()` 对组成员恒为 0。成功任务也可能返回
null，因此不要用 result 是否为 null 判断成败。监听器回调不属于已完成任务的动态执行作用域，
应把 event 作为观测边界。

任务终态分类已统一为单个枚举 `TaskOutcome`，取代原先的
早期内部的 `FutureState` 与 `TaskGroupMemberReason`。`TaskOutcome` 在原成员原因值
之上补充了 `RUNNING`，因此可同时服务批量报告与组成员结果。映射关系：`FutureState.FAILED` →
`TaskOutcome.USER_FAILURE`，`FutureState.CANCELLED` → `TaskOutcome.MEMBER_CANCELED`，
`TaskGroupMemberReason.X` → `TaskOutcome.X`（同名）。相应地，
`TaskBatchResult.BatchReport.stateCounts()` 现在以 `TaskOutcome` 为键，
组成员的终端快照（`TaskGroupResult.members()` 的值，统一为 `TaskCompletion` 类型）的成员终态
由 `outcome()` 暴露并返回 `TaskOutcome`（由早期的
`completionReason()` 改名而来）。

## 终态词汇统一

`TaskGroupCompletionReason` 已删除，组级结果复用 `TaskOutcome`：
`TaskGroupResult.completionReason()` 改名为 `outcome()`，返回 `TaskOutcome`。映射关系：
`SUCCESS` → `TaskOutcome.SUCCESS`；`TIMEOUT` → `TaskOutcome.TIMEOUT`；`FAILED` → 失败任务
自己的 outcome（`USER_FAILURE` 或 `SUBMISSION_FAILURE`，见 `failedTaskName()`）；
`CANCELED` → 组被整体取消或取消自上传播时为 `GROUP_CANCELED`，取消源自组员时为
`MEMBER_CANCELED`。

任务组成员或终端 combine 的执行期诊断名现在取自其 `TaskKey`，不再取自
选项的 name。checkpoint、任务监听器事件和任务图 label 因此与取 future 和结果快照时使用的名称
一致；成员与 combine 的 `TaskOptions` 不含 name 字段，不存在被忽略的配置。由于失败源也可能是终端 combine，
`TaskGroupResult.failedMemberName()` 同步改名为 `failedTaskName()`。

`CancellationToken.State` 值名对齐同一词汇：`FAIL_FAST_CANCELED` → `FAIL_FAST`，
`TIMEOUT_CANCELED` → `TIMEOUT`，`MUTUAL_CANCELED` → `CANCELED`，`PROPAGATING_CANCELED` →
`PROPAGATED_CANCELED`。`RUNNING`、`SUCCESS` 不变；`code()` 已删除（整数编码是没有消费方的
实现细节），`shouldInterruptCurrentThread()` 语义不变，改为直接的枚举比较。

批次报告现在基于批次 token 的已提交状态精确归因被取消的元素（`Par.map` 返回的结果始终
携带 token）：deadline 到期记 `TIMEOUT`，兄弟失败级联记 `FAIL_FAST`，整批取消或取消自上
传播记 `GROUP_CANCELED`，无框架路径提交（用户直消）记 `MEMBER_CANCELED`。批次内所有元素
共享同一 token，因此直接取消并触发级联的那个元素同样记 `FAIL_FAST`；需要逐个元素区分
发起者时请使用任务组。`TaskBatchResult` 实例由执行 API 构造；原先公开的
`of(...)` 工厂现已改为包私有。

`ExecutionPhase.CANCELLED_BEFORE_RUN` 拼写修正为 `CANCELED_BEFORE_RUN`，与库内统一的
单 L `CANCELED` 拼写一致。

`GlobalExecutionPolicy` 已删除：它的唯一内容是 `TaskListener` 列表，监听器现在直接注册在
`GlobalPar.Builder` 上。原先 `GlobalExecutionPolicy.builder().taskListener(l).build()` 传给
`executionPolicy(policy)` 的写法改为 builder 上的 `taskListener(l)`；按 Par 覆盖的
`parPolicyOverride(name, policy)` 改为每个监听器一次 `parTaskListener(ParName.of(name), l)`——同一 name
重复调用是追加而不是报错，覆盖列表对该 Par 仍然整体替换默认列表。`GlobalPar` 的
`executionPolicy()`/`executionPolicyFor(name)` 访问器相应改为
`taskListeners()`/`taskListenersFor(ParName)`。

`AsyncBatchResult` 改名为 `TaskBatchResult`（嵌套类型 `BatchReport` 名称不变）：它是"一批任务
的结果"，并非 async 专属概念。内部的 `ConcurrentLimitExecutor` 改名为
`SlidingWindowSubmitter`，与实际职责一致——按滑动窗口提交已准备的 tasks。
`TaskGraphObservationContext` 改名为 `TaskGraphObservationScope`：它是可关闭的观察作用域，
命名规则统一为 `Scope` 表示有生命周期的可关闭作用域、`Context` 表示数据载体。入口方法
`GlobalPar.openTaskGraphObservation()` 名称不变；`MultiTaskContext` 的访问器相应由
`taskGraphObservationContext()` 改名为 `taskGraphObservationScope()`。

旧的 `ParConfig`、`ParOptions`、`ExecutorResolver`、`GlobalParConfig` 及旧版 `Par` 入口都不是兼容别名。迁移时请同时更新 import、构建方式和调用方式。注册的执行器仍由应用拥有并负责关闭。
