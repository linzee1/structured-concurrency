# 使用指南

> 本文档面向当前 `0.2.0` API。使用 `ParConfig` 或 `ParOptions` 的 `0.1.x` 示例不能直接用于本版本，请先阅读 [v0.2 迁移指南](migration-v0.2.md)。

`parallel-in-scope` 将一个有限列表作为可取消的批次执行。应用装配层负责长期资源，`Par` 负责一个已绑定的执行器，`MultiTaskContext` 负责单次调用的运行时状态。

## 构建执行拓扑

在 composition root 创建 `GlobalPar`。每个逻辑入口在注册时绑定应使用的执行器，并将取得的 `Par` 注入需要它的组件。

```java
ParName DATABASE = ParName.of("database");
ParName HTTP = ParName.of("http");

GlobalPar global = GlobalPar.builder()
        .taskListener(metricsListener)
        .register(DATABASE, databaseExecutor)
        .register(HTTP, httpExecutor)
        .defaultPar(HTTP)
        .build();

Par httpPar = global.par(HTTP);
```

`ParName` 是值对象：构造时一次性校验（非 null、非空白），按值相等，因此可以把名称声明为常量复用。它是逻辑查找键，不是资源身份——物理线程池由 `ExecutorIdentity` 按对象引用判定，两个名称可以有意共享同一个执行器。

名称会在构建期校验；`build()` 后 `GlobalPar` 不可变，未知名称的 `par(name)` 会失败。注册的执行器属于调用方：关闭 `GlobalPar` 只会关闭内部 timer 和 submitter 服务，绝不会关闭它们。

需要进程级便捷入口时，在启动阶段安装一个已构建的拓扑即可：

```java
GlobalPar.installGlobal(global);
Par defaultPar = GlobalPar.global().defaultPar();
```

测试和库代码应优先显式注入。`installGlobal` 只能成功一次，不能替换已有实例。

## 执行批次

`BatchOptions` 是一次批次调用的不可变输入。选项类型与作用域一一对应：批次用 `BatchOptions`，任务组用 `TaskGroupOptions`，单个成员或 combine 用 `TaskOptions`。库把作用域的 name、并发度与执行策略，连同任务数量、父批次和绑定的执行器 identity，一起解析为内部 `MultiTaskContext`。

```java
BatchOptions options = BatchOptions.timeout("fetch-account", Duration.ofSeconds(5))
        .parallelism(16)
        .taskType(TaskType.IO_BOUND)
        .rejectEnqueue(false);

TaskBatchResult<Account> result = httpPar.map(
        accountIds,
        client::fetchAccount,
        options);

List<TaskFuture<Account>> futures = result.results();
```

`parallelism` 限制该批次的活跃提交窗口。负数表示让策略解析有效限制。timeout 必须在两个互斥的静态工厂里显式二选一：`BatchOptions.timeout(name, Duration)` 设置正数超时，`BatchOptions.inheritTimeout(name)` 继承外层作用域的 deadline——没有第三个状态，遗漏声明根本无法构造选项对象。显式 timeout 会被外层 deadline 截断；在没有外层 scoped task 时声明继承会在入口点被拒绝。`TaskType.CPU_BOUND` 与 `TaskType.IO_BOUND` 描述调度意图。`rejectEnqueue` 控制绑定执行器支持时是否拒绝排队。

结果 future 按输入顺序排列。失败、超时、取消、submitter 中断或拒绝导致窗口停止时，未提交 placeholder 也会完成或取消，因此聚合 future 不会永久停留在 live 状态。

## 执行异构任务组

当一个请求需要一小组固定、相互独立、返回类型或所用 `Par` 各不相同的操作时，使用任务组。任务组由 `TaskGroupDefinition` 描述：一个不可变、可复用的纯数据描述。`TaskGroupDefinition.Builder.task` 只记录定义；它不创建执行上下文、不捕获 TTL 值、不启动 timer、不提交任务。`TaskGroup.submit(global, definition)` 在提交时解析调用线程的上下文，冻结完整成员集合，准备全部成员后再统一提交。

组作用域声明 `TaskGroupOptions`：只含组名、组 deadline 和组收敛监听器。组不是一次任务执行，所以它没有并发度、task type 或 enqueue 策略。每个成员和 combine 需要收紧时才声明 `TaskOptions`：该类型只含这一次执行的 timeout、task type 和 enqueue 策略，省略即继承组 deadline。成员是单任务，没有扇出，所以选项里不存在并发度字段——内部嵌套提交读取的是该嵌套提交自己的选项。身份也不是选项：成员的诊断名始终是它的 `TaskKey` 名。成员默认就运行在组 deadline 之下：省略第四个实参等价于传入 `TaskOptions.inheritTimeout()`，成员永远不会因此超出组 deadline。需要更紧的预算、不同的 task type 或入队策略时才显式传入 `TaskOptions`；成员的显式 timeout 会被组 deadline 截断。声明 `inheritTimeout` 的组必须在一个 scoped task 内提交，否则 `submit` 被拒绝。

```java
TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(
        TaskGroupOptions.timeout("account-page", Duration.ofSeconds(3)));

TaskKey<User> user = definition.task(
        new TaskKey<User>("user") {},
        DATABASE, userRepository::load);
TaskKey<List<Order>> orders = definition.task(
        new TaskKey<List<Order>>("orders") {},
        HTTP, orderClient::load,
        TaskOptions.inheritTimeout().taskType(TaskType.IO_BOUND));

try (TaskGroup group = TaskGroup.submit(global, definition.build())) {
    User userValue = group.future(user).get();
    List<Order> orderValues = group.future(orders).get();
    TaskGroupResult result = group.completionFuture().get();
}
```

`TaskKey` 是以匿名子类创建的类型安全的键，在运行时捕获成员的结果类型；它在配置定义时注册，提交后 `group.future(key)` 解析成员的 future，并拒绝 raw 结果类型不能覆盖注册类型的 key。键仅按 name 值相等，因此声明父类型的 key 与注册 key 是同一个键。组完成始终返回 `TaskGroupResult`；组 outcome（`result.outcome()`，`TaskOutcome`）是结果数据，而不是 completion future 的失败。单个成员 future 保持普通 Guava 的成功、失败和取消语义。

组取消是完全结构化的，与批次语义一致：任一成员首次失败、任一成员 future 或成员 token 被直接取消、组 deadline 或任一成员自身 deadline 到期，都会取消所有未完成成员。`group.cancel()` 和 `close()` 取消未完成成员且不阻塞。成员 outcome 从取消 token 归因，因此被取消的成员报告 `MEMBER_CANCELED`、`FAIL_FAST`、`TIMEOUT` 或 `GROUP_CANCELED` 而不是笼统的取消；超出自身 deadline 的成员会把组升级为 `TIMEOUT`。组和成员的 deadline 从提交边界起算，成员 deadline 受组 deadline 截断。在 scoped task 内提交的组继承外层取消和 deadline 上限；自祖先传播的取消保留其初始原因（`CancellationToken.originState()`），因此祖先 deadline 到期仍使组收敛为 `TIMEOUT` 而不是笼统的 `GROUP_CANCELED`。每个成员仍是真实的子任务，而 membership 本身不会在兄弟之间产生依赖边。

### 终端汇合

当请求最终要把各成员的值组装成一个结果时，可以在定义中用终止方法 `buildWithCombiner` 声明一个终端 combine（它同时完成构建），而不必自己编排 `Futures` 回调。combine 是真实的 scoped 任务——submit 时与成员一样完成准备，但只在所有成员成功后才提交到它自己的 `Par`——因此它继承组的结构化取消、deadline 和可观测性：

```java
TaskKey<AccountPage> page = new TaskKey<AccountPage>("assemble-page") {};

// combine 依赖全部成员，因此由终止方法声明：注册 combine 与 build 是同一次调用
TaskGroupDefinition built = definition.buildWithCombiner(
        page,
        ParName.of("cpu"),
        values -> new AccountPage(values.value(user), values.value(orders)));

try (TaskGroup group = TaskGroup.submit(global, built)) {
    AccountPage accountPage = group.future(page).get();
}
```

`CompletedTaskValues` 视图不阻塞，只通过注册的 `TaskKey` 键暴露已成功的成员值——不暴露 future，也不提供按名称取值的 map。combine 函数恰好执行一次，运行在指定 `Par` 的 worker 线程上（绝不在成员的完成回调线程上运行；被拒绝的 combine 记 `SUBMISSION_FAILURE`，不会 inline 执行），并且它必须是 member 值与配置期捕获环境的纯函数：它在最后一个成员成功的瞬间被调度，submit 返回后提交线程上创建的状态对它不可见——那种场景请直接读成员 future 自行组装。一个组至多声明一个 combine，使用自己的 `TaskKey`；`group.future(combineKey)` 以普通 Guava 语义解析类型化的终端 future。任一成员失败时 combine 不会执行，终端 future 以组的归因 outcome 取消。combine 的快照呈现在 `TaskGroupResult.terminal()`（`members()` 保持只含成员）；combine 自身失败或被拒绝时，`failedTaskName()` 携带 combine 的注册名。组 deadline 涵盖 fan-out 与 combine，因此成员用掉大部分预算后 combine 可能尚未开始即超时——这是有意的端到端语义。

## 从 future 读取任务归因 {#task-attribution}

库为每一次任务执行交付的 future 都是 `TaskFuture<T>`：它既是 `ListenableFuture<T>`，也能回答这个任务是谁、最后如何结束。批次的每个元素、任务组的成员、终端 combine，以及组完成 future 都适用。

| 方法 | 回答 |
|---|---|
| `taskName()` | 批次名、成员/combine 的 key 名，或组名 |
| `outcome()` | 未终态为 `RUNNING`，终态后是一个 `TaskOutcome` |
| `deadlineNanos()` | 该任务在 `System.nanoTime()` 基准上的绝对 deadline |
| `remaining()` | 距该 deadline 的剩余预算，永不为负 |
| `failure()` | `USER_FAILURE` / `SUBMISSION_FAILURE` 背后的 cause，其余情况为 `null` |

这是纯增量视图。`TaskFuture` 继承 `ListenableFuture`，`Futures.allAsList`、`addCallback` 等全部 Guava 组合 API 照常工作，不检查该接口的代码行为完全不变。用 `instanceof` 检查；实现类不公开，不要书写类名。

```java
Account account = future.get();
if (future instanceof TaskFuture) {
    TaskFuture<?> task = (TaskFuture<?>) future;
    if (task.outcome() == TaskOutcome.TIMEOUT) {
        log.warn("{} timed out with {} of its budget left", task.taskName(), task.remaining());
    }
}
```

`outcome()` 是这个接口存在的理由：它消除了"future 被取消了，猜猜为什么"这一步。被取消的任务按其取消 token 归因——自身 deadline 到期记 `TIMEOUT`，兄弟任务失败后的级联记 `FAIL_FAST`，所在组或外层作用域取消记 `GROUP_CANCELED`，而没有任何框架路径取消过它（调用方直接取消了该 future）记 `MEMBER_CANCELED`。仅仅表达"已观测到取消"的失败——例如抢在级联之前的 `Checkpoints.checkpoint` 中断——同样按取消归因，不会读成用户失败。失败区分 `SUBMISSION_FAILURE`（被拒绝，或用户代码执行前就失败）与 `USER_FAILURE`，`failure()` 直接给出 cause，无需拆 `ExecutionException`。

归因按 future 逐个读取其自身 token 链，因此在所在组收敛之前就可读。组的终态归类——`TaskGroupResult.outcome()` 与每个成员的 `TaskCompletion`——在收敛后推导，仍是组级原因归属的权威：快照能区分"被直接取消的成员"与"作为连带被害者被取消的成员"，而单个 future 只能报告其 token 链最终归到组的取消。

有一个句柄刻意保持裸 future：`TaskBatchResult.submitCanceller()` 用于停止提交，不代表一次任务执行，因此不是 `TaskFuture`。

需要链式编排时用 `FluentFuture.from(task)` 获得完整的 `FluentFuture` API。链上派生的 future 是普通 `FluentFuture`：它们不是库执行的任务，没有 token 归因它们。

## 取消与嵌套批次

任一任务失败都会触发该批次的快速失败取消。超时、显式 `CancellationToken` 取消或父批次取消共享同一协作式边界：排队任务被取消；可中断的阻塞任务会被中断；CPU 密集型代码在 checkpoint 处停止。

```java
httpPar.map(accountIds, id -> {
    for (int page = 0; page < pageCount(id); page++) {
        Checkpoints.checkpoint();
        fetchPage(id, page);
    }
    return id;
}, options);
```

任务内部再次调用 `map` 时，子调用继承当前 `MultiTaskContext`。子批次继承父取消令牌和 deadline，记录父子边，并可使用不同的 `Par`：

```java
databasePar.map(ids, id -> {
    TaskBatchResult<Response> children = httpPar.map(
            endpoints(id), client::call, httpOptions);
    return collect(children);
}, databaseOptions);
```

需要跨多个 `Par` 诊断任务图时，请使用[观测作用域](#nested-observation)。

## 观测嵌套工作 {#nested-observation}

任务图观测显式绑定到一个 `GlobalPar`。作用域负责清理任务图，并在请求结束时（已启用时）调用潜在死锁检测 listener。检测到循环只表示结构风险，不证明线程当前已经死锁。

```java
try (TaskGraphObservationScope observation = global.openTaskGraphObservation()) {
    // 这里及其嵌套调用使用 global 中的多个 Par 时，写入同一张任务图。
    service.handleRequest();
}
```

在构建拓扑时配置策略：

```java
GlobalParDeadlockPolicy deadlock = GlobalParDeadlockPolicy.builder()
        .enabled(true)
        .listener(event -> log.warn("Potential deadlock: {}", event))
        .build();
```

不同 `GlobalPar` 的观测作用域不会合并任务图。

## 清理已取消的排队任务

purge 是可选能力，仅在 supplied executor 是 `ThreadPoolExecutor` 时生效。执行前取消会发出 execution phase 信号；`GlobalPar` 按物理执行器 identity 合并维护任务，因此同一线程池的别名或多个 `Par` 不会创建重复协调器。

```java
GlobalParPurgePolicy purge = GlobalParPurgePolicy.builder()
        .enabled(true)
        .queuePressureThreshold(0.80)
        .canceledTaskRatioThreshold(0.05)
        .build();

GlobalPar global = GlobalPar.builder()
        .purgePolicy(purge)
        .register(ParName.of("io"), ioThreadPool)
        .build();
```

两个阈值都达到后才会请求 `ThreadPoolExecutor.purge()`。purge 只能删除仍留在队列里的已取消任务，无法停止忽略中断的任务体。

## 生命周期队列

`DrainingBlockingQueue` 是一个有界 `BlockingQueue` 实现，提供单向排干式关闭：`close()` 永久拒绝新生产（写操作抛 `IllegalStateException` 或返回 `false`），消费端继续取走存量，直到排空进入终态后暴露终结信号（配置的 poison 对象，或 `NoSuchElementException` / `null`）。

```java
DrainingBlockingQueue<Job> queue = new DrainingBlockingQueue<>(100, poison);
queue.put(job);
queue.close();          // 生产端关闭，不丢任何已入队元素
queue.awaitDrained();   // 可选：等待排空

Job job = queue.take(); // 排空前返回真实元素；排空后返回 poison
```

关闭后消费端仍能取到关闭前已入队的元素，无需恢复通道；`drainTo` 在任何状态下都可用，用于主动放弃剩余存量。用 `isShutdown()` 判断"生产端已关"，用 `isDrained()` 判断"已排空"。完整契约见 [排干式关闭契约](https://github.com/monadrome/parallel-in-scope/blob/main/design/draining-queue-contract.md)。

## 运行规则

- `GlobalPar` 应覆盖应用生命周期，并在应用关闭时关闭它。
- 注册执行器的所有权在库外；由拥有它的组件负责关闭。
- 为每批任务提供稳定 task name，并在长 CPU 任务中设置 checkpoint。
- 需要隔离的资源应使用不同 `Par`，即使它们同为 IO。
- `MultiTaskContext`、`ExecutorRuntime` 和 `ExecutorIdentity` 是运行时/内部概念，不应由应用构造或缓存。
