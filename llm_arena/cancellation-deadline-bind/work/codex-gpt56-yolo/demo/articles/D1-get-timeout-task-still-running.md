# D1. get(timeout) 后任务还在跑

## 问题

在 Java 并发编程中，`Future.get(timeout, TimeUnit)` 是等待任务结果的标准方式。当任务超时时，它会抛出 `TimeoutException`，调用者可以据此决定后续逻辑。然而，这个超时只是"调用者不再等待"，并不是"任务被取消"。任务本身依然在线程池中继续运行，占用着线程资源。

假设你有一个固定大小为 10 的线程池，提交了 10 个任务，每个任务都需要 10 秒才能完成。如果你设置 `get(1, SECONDS)`，1 秒后调用者确实会收到 `TimeoutException` 并继续执行。但此时线程池中的 10 个线程仍然被这 10 个任务占满，新提交的任务只能排队等待。在高并发场景下，这种"任务泄漏"会导致线程池逐渐被耗尽，最终系统吞吐量崩溃。

标准 Java 的 `Future.cancel(true)` 可以尝试中断任务，但它有两个问题：第一，`cancel` 需要在 `get` 之后手动调用，容易遗漏；第二，对于不响应中断的任务，`cancel(true)` 也无能为力。更重要的是，在批量任务场景中，你需要逐个检查每个 Future 是否超时，然后逐个调用 cancel，代码变得异常繁琐。

## 问题复现

```java
ExecutorService pool = Executors.newFixedThreadPool(2);
List<Future<?>> futures = new ArrayList<>();
for (int i = 0; i < 2; i++) {
    futures.add(pool.submit(() -> {
        // 模拟长时间运行的任务
        Thread.sleep(5000);
        return "done";
    }));
}
// 调用者等待 500ms 后超时
for (Future<?> f : futures) {
    try {
        f.get(500, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
        // 调用者收到超时异常，继续执行
    }
}
// 此时 2 个任务仍在运行，线程池已被占满
pool.submit(() -> "new task"); // 这个任务必须等待前面的任务完成
```

## 解决方法

`parallel-in-scope` 的 `Par.map()` 将超时控制与取消机制集成在一起。通过 `BatchExecutionOptions.timeout()` 设置超时时间后，框架会在超时触发时自动执行以下操作：

1. **中断线程**：通过 `CancellationToken` 的 `lateBind` 机制，超时后立即中断所有正在执行的任务线程，释放线程池资源。

2. **协作式取消**：对于 CPU 密集型任务，中断可能无法立即生效。配合 `Checkpoints.check()` 检查点，任务可以在循环中主动检测取消状态并提前退出。

3. **批量管理**：无需逐个处理每个 Future，`Par.map()` 统一管理所有任务的生命周期，超时后自动取消剩余任务。

## 代码

```java
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.BatchExecutionOptions;
import io.github.huatalk.parallelinscope.scope.AsyncBatchResult;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.TaskType;

// 配置线程池和 Par 实例
ExecutorService pool = Executors.newFixedThreadPool(2);
GlobalPar config = GlobalPar.builder()
        .register("my-pool", pool)
        .build();
Par par = config.defaultPar();

// 设置选项：2 并发，500ms 超时
BatchExecutionOptions opts = BatchExecutionOptions.of("api-call")
        .parallelism(2)
        .timeout(java.time.Duration.ofMillis(500))
        .timeUnit(TimeUnit.MILLISECONDS)
        .taskType(TaskType.IO_BOUND)
        .build();

// 并行执行，超时自动取消
List<String> urls = Arrays.asList("url1", "url2");
AsyncBatchResult<String> result = par.map( urls, url -> {
    // 模拟长时间 IO 操作（响应中断）
    Thread.sleep(5000);
    return fetchContent(url);
}, opts);

// 结果：500ms 后自动取消，线程池资源及时释放
// 与标准 Future.get(timeout) 不同，任务不会继续占用线程
```

---

> 📁 完整测试代码：[D1_GetTimeoutStillRunningTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/D1_GetTimeoutStillRunningTest.java)
