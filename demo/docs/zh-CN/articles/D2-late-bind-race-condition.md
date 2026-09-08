# D2. 为什么使用统一的超时时间

## 问题

批量任务采用滑动窗口调度时，任务不会同时提交：初始窗口先进入执行器，后续任务要等已有任务完成后才补入。如果每个任务在实际提交时各自启动超时计时器，那么它们会拥有不同的截止时间。

这会带来两个问题：

- 批次没有明确的完成期限。后提交任务的截止时间不断后移，整个调用可能远超调用方配置的超时。
- 取消语义被拆散。某个任务超时只影响自身，提交循环和其他未完成任务仍可能继续运行。

调用方配置的 `timeout` 表达的是“这个批次最多执行多久”，而不是“每个任务从提交后还能运行多久”。因此，超时必须绑定到整个批次，并共享一个截止时间。

## 问题复现

```java
List<ListenableFuture<String>> futures = new ArrayList<>();

for (int i = 0; i < taskCount; i++) {
    ListenableFuture<String> submitted = submitWhenWindowAvailable(i);

    // 每个任务在实际提交时才开始计时，越晚提交，截止时间越晚。
    futures.add(FluentFuture.from(submitted)
            .withTimeout(timeout, TimeUnit.MILLISECONDS, timer));
}
```

假设并行度为 2，每轮任务执行 1 秒。第 1 批任务的截止时间从 T=0 起算，第 2 批从约 T=1s 起算，第 3 批从约 T=2s 起算。即使每个任务都设置 3 秒超时，整个批次仍可能持续远超 3 秒。

## 解决方法

`parallel-in-scope` 为批次建立一个统一超时：

1. `SlidingWindowSubmitter.submitAll()` 提交初始窗口，并为其余逻辑任务创建 Future 槽位；异步提交循环随后按完成事件补充任务。
2. `CancellationToken.lateBind()` 将全部逻辑 Future 和提交循环绑定到同一个聚合 Future。
3. `FluentFuture.withTimeout()` 只为这个聚合 Future 设置一次超时，形成整个批次共享的截止时间。
4. 截止时间到达或任一任务失败时，提交循环和所有未完成任务一起取消。

这种设计让 `MultiTaskOptions.timeout()` 成为清晰的批次级契约：计时从批次调度建立完成后开始，后续任务不会因为提交较晚而延长整个批次的期限。

## 代码

```java
ExecutorService pool = Executors.newFixedThreadPool(4);
GlobalPar config = GlobalPar.builder()
        .register("my-pool", pool)
        .build();

MultiTaskOptions options = MultiTaskOptions.of("data-task")
        .parallelism(10)
        .timeout(java.time.Duration.ofMillis(5000))
        .taskType(TaskType.IO_BOUND)
        .build();

TaskBatchResult<Result> result = config.par("my-pool").map(
        loadLargeDataset(),
        this::process,
        options);
```

上例中的 5 秒是整个 `map()` 批次的统一超时。任务失败或超时后，批次不会继续补充新任务，尚未完成的任务会收到取消信号。

| 维度 | 每个任务独立计时 | 批次统一计时 |
|---|---|---|
| 截止时间 | 随提交时刻漂移 | 全批次共享 |
| 批次总时长 | 可能持续后移 | 受配置超时约束 |
| 失败处理 | 通常只影响单个任务 | fail-fast 取消其余任务 |
| 提交循环 | 可能继续提交 | 随批次一起取消 |

---

> 完整测试代码：[D2_LateBindRaceConditionTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/D2_LateBindRaceConditionTest.java)
