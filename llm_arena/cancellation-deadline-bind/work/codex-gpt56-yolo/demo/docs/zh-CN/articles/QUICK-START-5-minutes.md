# 5 分钟快速上手 parallel-in-scope


`parallel-in-scope` 是一个面向 Java 8+ 的结构化并发工具库，基于 Guava `ListenableFuture` 构建。它提供滑动窗口并发控制、协作式取消、跨线程上下文传播等能力，让你用最少的代码完成批量并行任务。

## 1. 添加依赖

在 `pom.xml` 中加入：

```xml
<dependency>
    <groupId>io.github.huatalk</groupId>
    <artifactId>parallel-in-scope</artifactId>
    <version>0.2.0-SNAPSHOT</version>
</dependency>
```

## 2. 最小示例

只需三步：创建线程池 -> 配置 -> 调用 `par.map()`。

```java
// 1. 准备线程池和配置（应用启动时做一次即可）
ExecutorService pool = Executors.newFixedThreadPool(4);
GlobalPar config = GlobalPar.builder()
        .register("my-pool", pool)
        .build();
Par par = config.defaultPar();

// 2. 定义任务选项
BatchExecutionOptions options = BatchExecutionOptions.of("square").build();

// 3. 并行执行
List<Integer> numbers = Arrays.asList(1, 2, 3);
AsyncBatchResult<Integer> result = par.map( numbers, n -> n * n, options);

// 4. 获取结果
for (Future<Integer> future : result.getResults()) {
    System.out.println(future.get()); // 1, 4, 9
}
```

`par.map()` 会为列表中每个元素并行执行函数，返回 `AsyncBatchResult`，其中包含与输入顺序一一对应的 `ListenableFuture` 列表。

## 3. 设置超时

生产环境必须设置超时，防止任务无限挂起。在 `BatchExecutionOptions` 中通过 `.timeout()` 指定毫秒数：

```java
BatchExecutionOptions options = BatchExecutionOptions.of("square")
        .timeout(java.time.Duration.ofMillis(500))   // 500ms 超时
        .build();

AsyncBatchResult<Integer> result = par.map( numbers, n -> {
    // 模拟耗时操作
    Thread.sleep(100);
    return n * n;
}, options);
```

超时后，未完成的任务会被自动取消（cooperative cancellation）。你也可以在任务内部使用 `Checkpoints.sleep()` 替代 `Thread.sleep()`，实现协作式中断响应。

## 4. 控制并发度

当任务数量很大或下游服务有速率限制时，通过 `.parallelism()` 控制同时执行的任务数：

```java
BatchExecutionOptions options = BatchExecutionOptions.of("process")
        .parallelism(2)   // 最多同时执行 2 个任务
        .timeout(java.time.Duration.ofMillis(5000))
        .build();

List<Integer> bigList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
AsyncBatchResult<Integer> result = par.map( bigList, n -> n * 2, options);
```

框架采用滑动窗口策略：每完成一个任务才提交下一个，始终保持最多 `parallelism` 个任务在执行，避免线程池被打满。

## 5. 查看结果

`AsyncBatchResult` 提供两种结果查看方式：

```java
AsyncBatchResult<Integer> result = par.map( numbers, n -> n * n, options);

// 方式一：快速概览——一行代码看全貌
String report = result.reportString();
// 输出示例："SUCCESS:3" 或 "SUCCESS:2,FAILED:1 | firstException=xxx"

// 方式二：逐个获取结果值
for (Future<Integer> future : result.getResults()) {
    Integer value = future.get(); // 阻塞等待并获取返回值
    System.out.println(value);
}

// 方式三：结构化报告
AsyncBatchResult.BatchReport batchReport = result.report();
Map<FutureState, Integer> counts = batchReport.getStateCounts(); // {SUCCESS=3}
Throwable firstError = batchReport.getFirstException();          // null if all success
```

## 下一步

- **TaskType**：通过 `BatchExecutionOptions.ioTask()` 或 `BatchExecutionOptions.cpuTask()` 区分 IO/CPU 任务，框架会自动选择最优调度策略
- **TaskListener**：注册 SPI 监听器，获取每个任务的执行时间、排队时间等指标
- **Checkpoints**：在长任务中插入 `Checkpoints.checkpoint()`，实现细粒度的协作式取消
- **嵌套并行**：`par.map()` 支持嵌套调用，`CancellationToken` 会自动从外层传播到内层

更多用法请参考项目 README 和 `demo/` 目录下的示例代码。

---
> :file_folder: 完整测试代码：[QuickStartTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/QuickStartTest.java)
