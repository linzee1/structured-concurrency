# 使用指南

> 本文档面向当前 `0.2.0-SNAPSHOT` API。使用 `ParConfig` 或 `ParOptions` 的 `0.1.x` 示例不能直接用于本版本，请先阅读 [v0.2 迁移指南](migration-v0.2.md)。

`parallel-in-scope` 将一个有限列表作为可取消的批次执行。应用装配层负责长期资源，`Par` 负责一个已绑定的执行器，`BatchExecutionContext` 负责单次调用的运行时状态。

## 构建执行拓扑

在 composition root 创建 `GlobalPar`。每个逻辑入口在注册时绑定应使用的执行器，并将取得的 `Par` 注入需要它的组件。

```java
GlobalExecutionPolicy defaults = GlobalExecutionPolicy.builder()
        .defaultTimeoutMillis(10_000)
        .taskListener(metricsListener)
        .build();

GlobalPar global = GlobalPar.builder()
        .executionPolicy(defaults)
        .register("database", databaseExecutor)
        .register("http", httpExecutor)
        .defaultPar("http")
        .build();

Par httpPar = global.par("http");
```

名称会在构建期校验；`build()` 后 `GlobalPar` 不可变，未知名称的 `par(name)` 会失败。注册的执行器属于调用方：关闭 `GlobalPar` 只会关闭内部 timer 和 submitter 服务，绝不会关闭它们。

需要进程级便捷入口时，在启动阶段安装一个已构建的拓扑即可：

```java
GlobalPar.installGlobal(global);
Par defaultPar = GlobalPar.global().defaultPar();
```

测试和库代码应优先显式注入。`installGlobal` 只能成功一次，不能替换已有实例。

## 执行批次

`BatchExecutionOptions` 是单次调用的不可变输入。库将它与 `GlobalExecutionPolicy`、任务数量、父批次和绑定的执行器 identity 解析为内部 `BatchExecutionContext`。

```java
BatchExecutionOptions options = BatchExecutionOptions.of("fetch-account")
        .taskType(TaskType.IO_BOUND)
        .parallelism(16)
        .timeout(Duration.ofSeconds(5))
        .rejectEnqueue(false)
        .build();

AsyncBatchResult<Account> result = httpPar.map(
        accountIds,
        client::fetchAccount,
        options);

List<ListenableFuture<Account>> futures = result.getResults();
```

`parallelism` 限制该批次的活跃提交窗口。负数表示让策略解析有效限制；显式 timeout 必须为正，未设置时使用全局默认值。`TaskType.CPU_BOUND` 与 `TaskType.IO_BOUND` 描述调度意图。`rejectEnqueue` 控制绑定执行器支持时是否拒绝排队。

结果 future 按输入顺序排列。失败、超时、取消、submitter 中断或拒绝导致窗口停止时，未提交 placeholder 也会完成或取消，因此聚合 future 不会永久停留在 live 状态。

## 取消与嵌套批次

任一任务失败都会触发该批次的快速失败取消。超时、显式 `CancellationToken` 取消或父批次取消共享同一协作式边界：排队任务被取消；可中断的阻塞任务会被中断；CPU 密集型代码在 checkpoint 处停止。

```java
httpPar.map(accountIds, id -> {
    for (int page = 0; page < pageCount(id); page++) {
        Checkpoints.checkpoint("fetch-account", true);
        fetchPage(id, page);
    }
    return id;
}, options);
```

任务内部再次调用 `map` 时，子调用继承当前 `BatchExecutionContext`。子批次继承父取消令牌和 deadline，记录父子边，并可使用不同的 `Par`：

```java
databasePar.map(ids, id -> {
    AsyncBatchResult<Response> children = httpPar.map(
            endpoints(id), client::call, httpOptions);
    return collect(children);
}, databaseOptions);
```

需要跨多个 `Par` 诊断任务图时，请使用[观测作用域](#观测嵌套工作)。

## 观测嵌套工作

任务图观测显式绑定到一个 `GlobalPar`。作用域负责清理任务图，并在请求结束时（已启用时）调用潜在死锁检测 listener。检测到循环只表示结构风险，不证明线程当前已经死锁。

```java
try (TaskGraphObservationContext observation = global.openTaskGraphObservation()) {
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
        .register("io", ioThreadPool)
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

关闭后消费端仍能取到关闭前已入队的元素，无需恢复通道；`drainTo` 在任何状态下都可用，用于主动放弃剩余存量。用 `isShutdown()` 判断"生产端已关"，用 `isDrained()` 判断"已排空"。完整契约见 [排干式关闭契约](design/draining-blocking-queue-contract.md)。

## 运行规则

- `GlobalPar` 应覆盖应用生命周期，并在应用关闭时关闭它。
- 注册执行器的所有权在库外；由拥有它的组件负责关闭。
- 为每批任务提供稳定 task name，并在长 CPU 任务中设置 checkpoint。
- 需要隔离的资源应使用不同 `Par`，即使它们同为 IO。
- `BatchExecutionContext`、`ExecutorRuntime` 和 `ExecutorIdentity` 是运行时/内部概念，不应由应用构造或缓存。
