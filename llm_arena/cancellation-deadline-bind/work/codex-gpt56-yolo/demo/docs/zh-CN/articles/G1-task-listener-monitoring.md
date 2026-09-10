# G1. 任务执行看不见——接入监控

## 问题

并行任务执行是个黑盒——不知道每个任务花了多久、哪个失败了、总耗时多少。标准 Java 的 `ExecutorService.submit()` 返回的 `Future` 只能通过 `get()` 阻塞获取结果，没有任何回调机制可以观察任务的生命周期。想接入监控系统（Prometheus、Micrometer）计算 P99 延迟、记录失败率，但根本没有切入点。

唯一的方式是在每个任务的 lambda 里手动埋点：记录开始时间、结束时间、异常信息，然后推送到监控系统。但这意味着监控逻辑和业务逻辑耦合在一起，每个任务都要写一遍样板代码，而且容易遗漏。更麻烦的是，lambda 里拿不到"等待时间"（从提交到真正开始执行的间隔），而这恰恰是线程池压力的关键指标。

## 问题复现

```java
ExecutorService pool = Executors.newFixedThreadPool(4);
// 想监控每个任务的耗时？只能在 lambda 里手动埋点
Future<String> future = pool.submit(() -> {
    long start = System.nanoTime();
    try {
        String result = callRemoteService();
        // 手动记录成功耗时
        metrics.record("task", System.nanoTime() - start);
        return result;
    } catch (Exception e) {
        // 手动记录失败
        metrics.recordFailure("task", e);
        throw e;
    }
});
// 问题：监控代码散布在每个任务里，拿不到等待时间，遗漏风险高
```

## 解决方法

`parallel-in-scope` 提供了 `TaskListener` SPI 扩展点。通过 `GlobalPar.builder().executionPolicy(GlobalExecutionPolicy.builder().taskListener(listener).build())` 注册监听器，框架会在每个任务完成时自动回调 `onTaskComplete(TaskEvent)`，无需侵入业务代码。

`TaskEvent` 包含完整的任务生命周期信息：
- `getTaskContext()` — 当前 task 的只读上下文，包含 batch、taskIndex 和计时
- `getTaskName()` — 任务名称（来自 `BatchExecutionOptions.of("taskName")`）
- `isSuccessful()` / `getResult()` — 成功状态和任务返回值
- `executionTime()` — 实际执行耗时，返回 `Duration`
- `waitTime()` — 等待耗时（从提交到开始执行的间隔），返回 `Duration`
- `totalTime()` — 总耗时（等待 + 执行），返回 `Duration`
- `getException()` — 任务异常（成功时为 null）

这些数据足以对接任何监控系统：用 `executionTime().toMillis()` 计算延迟直方图，用 `isSuccessful()` 统计成功率，用 `waitTime().toMillis()` 监控线程池水位，并可通过 `getTaskContext().taskIndex()` 定位批次中的具体输入位置。

## 代码

```java

// 注册监控监听器
ConcurrentHashMap<String, Long> taskTimings = new ConcurrentHashMap<>();
GlobalPar config = GlobalPar.builder()
        .register("my-pool", pool)
        .taskListener(event -> {
            // 每个任务完成时自动回调，零侵入
            taskTimings.put(event.getTaskName(), event.executionTime().toMillis());
            if (event.getException() != null) {
                log.error("Task {} failed: {}", event.getTaskName(),
                        event.getException().getMessage());
            }
            // 推送到 Prometheus / Micrometer
            Timer.builder("task.duration")
                .tag("name", event.getTaskName())
                .register(meterRegistry)
                .record(event.executionTime().toNanos(), TimeUnit.NANOSECONDS);
        })
        .build();
Par par = config.defaultPar();

// 业务代码无需任何监控逻辑
BatchExecutionOptions opts = BatchExecutionOptions.of("order-query").parallelism(5).timeout(java.time.Duration.ofMillis(3000)).build();
AsyncBatchResult<Order> result = par.map( orderIds, id -> {
    return orderService.query(id);  // 纯业务逻辑，不碰监控
}, opts);
```

---

> 📁 完整测试代码：[G1_TaskListenerMonitoringTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/G1_TaskListenerMonitoringTest.java)
