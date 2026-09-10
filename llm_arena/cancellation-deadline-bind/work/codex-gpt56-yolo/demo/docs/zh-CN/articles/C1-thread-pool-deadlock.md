# C1. 线程池死锁

## 问题

在使用固定大小线程池（`Executors.newFixedThreadPool`）进行嵌套并行调用时，极易产生一种"隐形死锁"。假设外层有 N 个任务提交到线程池，每个外层任务在执行过程中又向**同一个线程池**提交 M 个内层子任务。当 N 等于线程池大小时，所有线程都被外层任务占满，内层子任务只能排队等待；而外层任务又在阻塞等待内层子任务完成——经典的循环等待死锁就发生了。

这种死锁之所以难以诊断，是因为线程转储（thread dump）中看不到任何 `BLOCKED` 或 `WAITING` 状态的线程。所有线程都表现为 `RUNNABLE`（在 `ThreadPoolExecutor.getTask()` 中等待队列取任务），没有传统的 monitor 锁竞争痕迹。运维人员看到线程全忙、CPU 空闲、请求超时，却找不到任何死锁证据，排查极为困难。在微服务架构中，这种问题尤为隐蔽——上游服务超时，下游服务线程池满载，但所有指标都显示"正常运行中"。

## 问题复现

```java
ExecutorService pool = Executors.newFixedThreadPool(2);

// 外层：2 个任务，正好占满线程池
for (int i = 0; i < 2; i++) {
    pool.submit(() -> {
        // 内层：向同一个池提交子任务并等待结果
        Future<String> inner = pool.submit(() -> "done");
        inner.get(3, TimeUnit.SECONDS);  // 永远等不到——线程已被占满
    });
}
// 结果：TimeoutException，死锁！
```

## 解决方法

解决线程池嵌套死锁的核心原则是**打破循环等待**。最直接的方案是为不同层级的任务使用独立的线程池——外层用 `FixedThreadPool` 控制并发度，内层用 `CachedThreadPool` 保证线程供给充足。

`parallel-in-scope` 在此基础上提供了自动化的死锁检测能力。`TaskGraph` 会自动记录父子任务依赖关系，构建 DAG。在请求结束时，通过 Guava `Graphs.hasCycle()` 检测是否存在环路，并结合 `canDeadlock()` 检查涉及的线程池是否为有界池（`FixedThreadPool`）——只有有界池才会触发死锁告警，无界池（`CachedThreadPool`）天然安全。

此外，`parallel-in-scope` 通过滑动窗口调度（`ConcurrentLimitExecutor`）和超时控制双管齐下：
- 滑动窗口确保不会一次性向池中灌入过多任务，降低死锁概率
- `BatchExecutionOptions.timeout()` 提供任务级超时，即使发生死锁也能快速失败，避免线程被永久占用

## 代码

```java

// 方案：内层使用独立的 CachedThreadPool，避免嵌套死锁
ExecutorService outerPool = Executors.newFixedThreadPool(2);
ExecutorService innerPool = Executors.newCachedThreadPool();

GlobalPar config = GlobalPar.builder()
        .register("outer-pool", outerPool)
        .register("inner-pool", innerPool)
        .build();
Par par = config.defaultPar();

// 外层任务
BatchExecutionOptions outerOpts = BatchExecutionOptions.of("outer-task")
        .parallelism(2)
        .timeout(java.time.Duration.ofMillis(10_000))
        .taskType(TaskType.IO_BOUND)
        .build();

List<Integer> items = Arrays.asList(1, 2, 3, 4);
AsyncBatchResult<String> result = par.map( items, item -> {
    // 内层任务使用不同线程池，不会死锁
    BatchExecutionOptions innerOpts = BatchExecutionOptions.of("inner-task")
            .parallelism(2)
            .timeout(java.time.Duration.ofMillis(5_000))
            .build();

    List<String> subItems = Arrays.asList("a", "b");
    AsyncBatchResult<String> innerResult = par.map( subItems, sub -> {
        return item + "-" + sub;
    }, innerOpts);

    return "outer-" + item;
}, outerOpts);

// 所有任务正常完成，无死锁
```

---

> 📁 完整测试代码：[C1_ThreadPoolDeadlockTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/C1_ThreadPoolDeadlockTest.java)
