# A3. 取消异常太重


## 问题

在高频取消场景中，每次取消都会抛出一个 `CancellationException`。Java 中 `Exception.fillInStackTrace()` 是 JVM 最昂贵的操作之一——它需要遍历整个调用栈，为每一帧收集 `StackTraceElement` 对象。如果你有 1000 个任务被取消，就意味着 1000 次昂贵的栈追踪采集，而这些栈追踪信息通常只是被吞掉或记一行日志，完全没有被利用。

更糟糕的是，`fillInStackTrace()` 的开销与栈深度成正比。在典型的 Spring/Web 框架应用中，调用栈动辄 30-50 层深。当批量任务超时触发取消时，所有正在执行的线程同时抛出异常，瞬间产生大量 `StackTraceElement` 数组，造成内存分配压力和 GC 暂停。这恰恰发生在你最不希望 JVM 被拖慢的时刻——系统正在努力清理超时任务、恢复吞吐量。

## 问题复现

```java
// 模拟 10000 次取消异常的创建
long start = System.nanoTime();
for (int i = 0; i < 10000; i++) {
    Exception e = new CancellationException("task-" + i + " cancelled");
    // fillInStackTrace() 在构造时自动调用，开销已被付出
}
long elapsed = System.nanoTime() - start;
// 结果：通常需要数百毫秒，每次 ~50-100 微秒
System.out.println("10000 exceptions: " + elapsed / 1_000_000 + " ms");
```

`fillInStackTrace()` 的每次调用都是一个 JNI 操作，需要 JVM 暂停当前线程去遍历栈帧。在批量取消场景下，这个开销会叠加放大。

## 解决方法

`parallel-in-scope` 对高频取消使用轻量异常，并直接复用 JDK 标准异常承载诊断堆栈：

- **`LeanCancellationException`**：重写 `fillInStackTrace()` 为空操作（返回 `this`），同时通过 `setStackTrace(new StackTraceElement[0])` 清空栈追踪。构造开销接近于零，专为高频取消路径设计。
- **`CancellationException`**：保留完整的栈追踪，用于调试阶段追踪取消来源。

`Par.map()` 配合 `BatchExecutionOptions.timeout()` 使用时，框架内部自动使用轻量级异常处理取消流程。开发者无需关心异常类型的选择——超时取消的性能开销已经被框架内部优化到最低。

## 代码

```java

// 配置线程池和 Par 实例
ExecutorService pool = Executors.newFixedThreadPool(4);
GlobalPar config = GlobalPar.builder()
        .register("my-pool", pool)
        .build();
Par par = config.defaultPar();

// 设置选项：8 并发，200ms 超时
BatchExecutionOptions opts = BatchExecutionOptions.of("fast-task")
        .parallelism(8)
        .timeout(java.time.Duration.ofMillis(200))
        .build();

// 提交 100 个任务，大部分会超时被取消
List<Integer> items = IntStream.rangeClosed(1, 100).boxed().collect(Collectors.toList());
AsyncBatchResult<String> result = par.map( items, id -> {
    // 模拟慢操作，超过 200ms 超时
    Thread.sleep(5000);
    return "done-" + id;
}, opts);

// 框架内部使用 LeanCancellationException 取消超时任务
// 100 次取消的异常创建开销几乎为零
System.out.println(result.reportString());
```

对比两种异常的开销：

| 维度 | 普通 CancellationException | LeanCancellationException |
|------|--------------------------|--------------------------|
| fillInStackTrace() | 完整遍历栈帧（JNI） | 空操作，直接返回 this |
| 栈追踪数组 | 每个 30-50 个 StackTraceElement | 空数组，零元素 |
| 构造 10000 个耗时 | ~500ms | ~1ms |
| 适用场景 | 调试、日志排查 | 生产环境高频取消 |

---

> 📁 完整测试代码：[A3_LeanVsFatExceptionTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/A3_LeanVsFatExceptionTest.java)
