# G3. 任务取消后还在算——显式检查点


## 问题

Java 的 `Thread.interrupt()` 只对阻塞操作（`sleep`、`wait`、`IO`）有效。对于 CPU 密集型任务——大循环、批量计算、哈希迭代——中断信号完全被忽略。你调了 `Future.cancel(true)`，任务照样跑完，线程池资源白白浪费。

这是 Java 并发的一个根本性盲区：**非阻塞代码无法被外部中断**。在批量处理场景中，一旦某个任务超时，你希望立即停止它、释放线程给后续任务。但如果任务是一个紧密的 `for` 循环或者 `while` 计算，`cancel(true)` 只是设置了 Future 的取消状态，线程里的代码根本感知不到，继续算到自然结束。

更隐蔽的是，很多开发者以为 `cancel(true)` 能可靠地停止任何任务。当它失效时，排查起来非常困难——任务状态显示已取消，但 CPU 利用率没有下降，线程池依然被占满。

## 问题复现

```java
ExecutorService pool = Executors.newFixedThreadPool(4);
List<Future<?>> futures = new ArrayList<>();
for (int i = 0; i < 3; i++) {
    futures.add(pool.submit(() -> {
        // CPU 密集循环——不调用 sleep/wait/IO，中断信号打不进来
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < end) { /* 忙等待 */ }
    }));
}
// 立即取消所有任务
futures.forEach(f -> f.cancel(true));
// 结果：所有 3 个任务仍然完成，cancel(true) 完全无效
// Future.isCancelled() == true，但任务代码从未检查中断标志
```

`cancel(true)` 只做了两件事：(1) 设置 Future 的取消状态；(2) 调用 `Thread.interrupt()` 设置中断标志。但紧密循环不调用任何会检查中断标志的方法，所以中断标志被静默忽略。

## 解决方法

`parallel-in-scope` 提供了 `Checkpoints.checkpoint(taskName, lean)` 这一协作式检查点。在循环体内调用它，框架会在任务名匹配时检查当前 `CancellationToken` 是否已取消。如果已取消，将根据 `lean` 参数抛出对应的取消异常，终止当前任务。

```java
// 在 CPU 密集循环中插入检查点
for (int i = 0; i < 1_000_000; i++) {
    Checkpoints.checkpoint("cpu-task", true); // 任务名必须与 MultiTaskOptions 一致
    result += heavyCompute(i);
}
```

此外，`Checkpoints.sleep(ms)` 替代 `Thread.sleep()`，自动将中断信号转化为协作式取消异常，统一了取消语义。

配合 `MultiTaskOptions.timeout()` 设置任务超时，框架在超时后自动触发取消。对于 IO 任务（`Thread.sleep`、阻塞读写），中断机制天然有效；对于 CPU 任务，在循环体内插入 `Checkpoints.checkpoint(taskName, lean)` 即可及时响应取消。

## 代码

```java

// 配置线程池和 Par 实例
ExecutorService pool = Executors.newFixedThreadPool(4);
GlobalPar config = GlobalPar.builder()
        .register("my-pool", pool)
        .build();
Par par = config.defaultPar();

// IO 任务：Thread.sleep 响应中断，超时取消自然生效
MultiTaskOptions ioOpts = MultiTaskOptions.of("api-call").taskType(TaskType.IO_BOUND)
        .parallelism(3)
        .timeout(java.time.Duration.ofMillis(500))
        .build();

List<String> urls = Arrays.asList("url1", "url2", "url3");
TaskBatchResult<String> result = par.map( urls, url -> {
    Thread.sleep(5000);  // 响应中断，500ms 后被取消
    return fetchContent(url);
}, ioOpts);
// 结果：500ms 后自动取消，总耗时远小于 3 * 5s
```

CPU 任务与 IO 任务的取消能力对比：

| 维度 | CPU 密集任务 | IO 密集任务 |
|------|------------|------------|
| Thread.interrupt() | 无效（不检查中断标志） | 有效（sleep/wait/IO 抛 InterruptedException） |
| cancel(true) | Future 标记取消，任务继续执行 | 线程被中断，任务立即停止 |
| Checkpoints.checkpoint(taskName, lean) | 有效（显式检查 CancellationToken） | 通常不需要（中断机制已足够） |
| 推荐做法 | 循环体内插入 checkpoint | 使用 Par.map() + timeout |

---

> 📁 完整测试代码：[G3_CheckpointsCooperativeCancelTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/G3_CheckpointsCooperativeCancelTest.java)
