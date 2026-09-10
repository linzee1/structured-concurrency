# D2. 先提交后绑定的竞态

## 问题

（接上篇餐厅比喻：大堂经理安排了 100 桌客人，4 桌一轮。问题来了——限时什么时候宣布？）

在并行处理大批量任务时，超时控制是防止任务无限挂起的关键手段。一个直觉上的做法是：每个任务提交时就绑定超时——比如用 Guava 的 `FluentFuture.withTimeout()` 在 `submit()` 的那一刻启动计时器。这在任务数量少、执行时间均匀的场景下没有问题，但一旦引入**滑动窗口调度**，这个做法就会产生严重的不公平问题。

考虑一个具体场景：100 个任务，并行度 10，每个任务的超时设为 5 秒。由于滑动窗口的特性，第 1 个任务在 T=0 时刻提交，超时计时器立即开始倒计时；但第 100 个任务要等到前 90 个任务依次完成后才能被提交，假设每个任务耗时约 300ms，那么第 100 个任务大约在 T=27s 才被提交。此时它的 5 秒超时还没问题，但它的"可用执行时间"和第 1 个任务完全相同——都是 5 秒。真正的问题在于：如果任务执行时间波动较大，前面的任务卡住了，后面的大量任务可能在还没开始执行时就已经超时了。更极端的情况是，当所有任务的超时总和小于任务排队等待的总时间时，后面的批量任务会**全部超时**，即使它们本身只需要很短的执行时间。

本质原因在于：**超时的起算点是任务提交时刻，而非任务批量的统一时刻**。先提交的任务"偷跑"了计时器，后提交的任务被迫承受不公平的超时压力。

## 问题复现

```java
// 模拟：10 个任务，并行度 2，每个任务超时 2 秒
// 但每个任务实际需要 3 秒才能完成
ExecutorService pool = Executors.newFixedThreadPool(4);
ListeningExecutorService listening = MoreExecutors.listeningDecorator(pool);

int taskCount = 10;
long perTaskTimeoutMs = 2000;
List<ListenableFuture<String>> futures = new ArrayList<>();

for (int i = 0; i < taskCount; i++) {
    final int taskId = i;
    // 逐个提交，每个 Future 独立设置超时
    ListenableFuture<String> future = listening.submit(() -> {
        Thread.sleep(3000); // 模拟耗时操作
        return "task-" + taskId;
    });
    // 超时从提交瞬间开始计时
    futures.add(FluentFuture.from(future)
            .withTimeout(perTaskTimeoutMs, TimeUnit.MILLISECONDS, timer));
}

// 结果：第 1~2 个任务可能超时，第 3~10 个任务在提交时
// 前面的任务还没完成，它们的超时被前面的排队时间"消耗"掉了
// 后续任务还没开始就已经超时
```

## 解决方法

就像餐厅的做法：100 桌客人全部到齐登记后，经理才统一宣布"就餐限时 1 小时"。而不是第 1 桌坐下就开始计时——否则第 1 桌都吃完了，第 100 桌还没坐下，限时形同虚设。

`parallel-in-scope` 采用**先提交后绑定**（Late Binding）策略，核心思路是将"提交任务"和"绑定超时"拆分为两个独立的阶段：

1. **提交阶段**：`ConcurrentLimitExecutor.submitAll()` 通过滑动窗口逐个提交任务，返回 `AsyncBatchResult`（包含所有 `ListenableFuture`）
2. **绑定阶段**：所有任务提交完成后，`CancellationToken.lateBind()` 统一为整个任务批量设置超时

这样，超时计时器的起算点是**所有任务提交完毕的时刻**，而非第一个任务提交的时刻。无论任务数量多少、滑动窗口如何调度，每个任务都能获得公平的超时窗口。

`lateBind()` 内部使用 Guava 的 `Futures.allAsList()` 实现 fail-fast（任一失败则全部取消），再用 `FluentFuture.withTimeout()` 设置统一超时。超时触发后，所有未完成的任务会被中断并转入 `TIMEOUT_CANCELED` 状态。

## 代码

```java
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.BatchExecutionOptions;
import io.github.huatalk.parallelinscope.scope.AsyncBatchResult;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.TaskType;

// 配置线程池和 Par 实例
ExecutorService pool = Executors.newFixedThreadPool(4);
GlobalPar config = GlobalPar.builder()
        .register("my-pool", pool)
        .build();
Par par = config.defaultPar();

// 100 个任务，并行度 10，统一超时 5 秒
BatchExecutionOptions options = BatchExecutionOptions.of("data-task")
        .parallelism(10)
        .timeout(java.time.Duration.ofMillis(5000))
        .taskType(TaskType.IO_BOUND)
        .build();

List<Data> items = loadLargeDataset(); // 100+ items

// Par.map() 内部先通过滑动窗口提交所有任务，
// 然后调用 lateBind() 统一绑定超时
AsyncBatchResult<Result> result = par.map( items, item -> {
    return process(item); // 每个任务公平享有 5 秒超时
}, options);

// 所有任务的超时起算点相同——全部提交完毕的时刻
// 不会出现"先提交的多跑、后提交的少跑"的不公平现象
```

---

> 📁 完整测试代码：[D2_LateBindRaceConditionTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/D2_LateBindRaceConditionTest.java)
