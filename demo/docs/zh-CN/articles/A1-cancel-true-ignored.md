# A1. cancel(true) 被忽略了


## 问题

在 Java 并发编程中，`Future.cancel(true)` 是取消正在执行任务的标准方式。它通过调用 `Thread.interrupt()` 设置线程的中断标志来尝试停止任务。然而，如果任务代码不检查中断标志——比如执行 JDBC 查询、HTTP 请求、或者是一个没有检查 `Thread.interrupted()` 的紧密循环——中断就会被静默忽略，任务继续运行直到自然结束。

这意味着你调用了 `cancel(true)`，任务却依然在消耗线程池资源。在批量处理场景中，这种"取消失败"会导致线程池被耗尽，后续任务无法及时调度，整体吞吐量急剧下降。更糟糕的是，标准 Java 没有任何其他机制来可靠地取消这类非协作式任务。

## 问题复现

```java
ExecutorService pool = Executors.newFixedThreadPool(4);
List<Future<?>> futures = new ArrayList<>();
for (int i = 0; i < 3; i++) {
    futures.add(pool.submit(() -> {
        // 紧密循环，忽略中断标志
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < end) { /* 忙等待 */ }
    }));
}
// 尝试取消所有任务
futures.forEach(f -> f.cancel(true));
// 结果：所有 3 个任务仍然完成，cancel(true) 完全无效
```

## 解决方法

`parallel-in-scope` 的 `Par.map()` 提供批次级超时与协作式取消。`MultiTaskOptions.timeout()` 限制本次 `map` 调用的整体执行时间；超时后框架会取消该批次中所有未完成任务并尝试中断执行线程。任务仍需响应中断，或通过 `Checkpoints` 主动检查取消状态，才能及时退出。

配合 `MultiTaskOptions` 的 `parallelism()` 和 `taskType()` 配置，可以精确控制并发行为：
- `parallelism(3)` 限制最大并行数为 3，采用滑动窗口调度避免线程池过载
- `taskType(TaskType.IO_BOUND)` 标记为 IO 密集型任务，影响调度策略
- `timeout(500)` 设置本次批量调用的 500ms 超时，超时后取消未完成任务

## 代码

```java

// 配置线程池和 Par 实例
ExecutorService pool = Executors.newFixedThreadPool(4);
GlobalPar config = GlobalPar.builder()
        .register("my-pool", pool)
        .build();
Par par = config.defaultPar();

// 设置选项：3 并发，500ms 超时
MultiTaskOptions opts = MultiTaskOptions.of("http-call")
        .parallelism(3)
        .timeout(java.time.Duration.ofMillis(500))
        .taskType(TaskType.IO_BOUND)
        .build();

// 并行执行，超时自动取消
List<String> urls = Arrays.asList("url1", "url2", "url3");
TaskBatchResult<String> result = par.map( urls, url -> {
    // 模拟 IO 操作（Checkpoints.sleep 响应框架取消信号）
    Checkpoints.sleep(5000);
    return fetchContent(url);
}, opts);

// 结果：500ms 后自动取消，总耗时远小于 3 × 5s
```

---

> 📁 完整测试代码：[A1_CancelTrueInvalidTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/A1_CancelTrueInvalidTest.java)
