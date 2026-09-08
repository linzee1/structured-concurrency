# G2. 批量结果统计——一行代码拿到全貌

## 问题

并行执行完 N 个任务后，最常见也最烦人的事情就是统计结果：多少成功了？多少失败了？多少被取消了？用原生 Java `Future` 做这件事，代码又臭又长：

```java
int success = 0, failed = 0, cancelled = 0;
Throwable firstError = null;
for (Future<String> f : futures) {
    if (f.isCancelled()) {
        cancelled++;
    } else if (f.isDone()) {
        try {
            f.get();
            success++;
        } catch (ExecutionException e) {
            failed++;
            if (firstError == null) firstError = e.getCause();
        }
    }
}
```

这段代码不仅冗长，还容易出错——忘了处理 `ExecutionException`、忘了 `getCause()` 拿根因、忘了区分取消和异常。在批量处理几十上百个任务的场景中，几乎每个项目都会重复写一遍类似的统计逻辑。

## 问题复现

```java
ExecutorService pool = Executors.newFixedThreadPool(4);
List<Future<Integer>> futures = new ArrayList<>();
for (int i = 0; i < 10; i++) {
    final int val = i;
    futures.add(pool.submit(() -> {
        if (val % 3 == 0) throw new RuntimeException("task-" + val + " failed");
        return val * 2;
    }));
}
// 手动统计：逐个检查 isDone / isCancelled / get + catch
// 超过 20 行才能拿到 "6 成功, 4 失败, 0 取消" 的概览
```

每多加一个统计维度（比如"第一个异常是什么"），代码量就再翻一倍。

## 解决方法

`TaskBatchResult` 封装了所有任务的 `ListenableFuture`，一行调用 `reportString()` 即可拿到人类可读的状态概览。报告反映的是调用时每个 Future 的真实终态，包括 `SUCCESS`、`USER_FAILURE` 和 `MEMBER_CANCELED`。

```
SUCCESS:6
```

如果需要结构化数据，调用 `report()` 返回 `BatchReport` 对象：
- `stateCounts()` — 返回 `Map<TaskOutcome, Integer>`，按状态分组计数
- `firstException()` — 返回第一个失败任务的异常（无失败时为 null）

还可以自定义格式拼接，比如用于日志或监控上报。

## 代码

```java
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;

ExecutorService pool = Executors.newFixedThreadPool(4);
GlobalPar config = GlobalPar.builder()
        .register("my-pool", pool)
        .build();
Par par = config.defaultPar();

MultiTaskOptions opts = MultiTaskOptions.of("batch-task")
        .parallelism(4)
        .timeout(java.time.Duration.ofMillis(5000))
        .build();

List<Integer> items = Arrays.asList(1, 2, 3, 4, 5, 6);
TaskBatchResult<Integer> result = par.map( items, x -> {
    return x * 2;
}, opts);

// 一行拿到全貌
System.out.println(result.reportString());
// 输出示例: SUCCESS:6

// 结构化访问
TaskBatchResult.BatchReport report = result.report();
System.out.println("状态统计: " + report.stateCounts());
System.out.println("首个异常: " + report.firstException());
```

### Fail-fast 场景

如果任务函数抛出异常，`Par.map()` 会触发 fail-fast，取消尚未完成的 sibling。因此输入中有 4 个“理论上会失败”的任务，并不保证最终报告有 4 个 `USER_FAILURE`：其中一些任务可能在执行到抛异常之前就已经被取消。

这类场景应验证稳定不变量，而不是固定状态数量：

```java
TaskBatchResult.BatchReport report = result.report();
int total = report.stateCounts().values().stream()
        .mapToInt(Integer::intValue)
        .sum();

assert total == items.size();
assert report.firstException() != null;
```

报告示例可能是 `USER_FAILURE:1,MEMBER_CANCELED:5`，也可能包含已经完成的 `SUCCESS`；具体数量取决于任务完成与 fail-fast 取消之间的竞态。

对比两种方式：

| 维度 | 原生 Future 手动统计 | TaskBatchResult.reportString() |
|------|---------------------|--------------------------------|
| 代码量 | 20+ 行 | 1 行 |
| 异常提取 | 手动 getCause() | 自动取第一个失败异常 |
| 可读性 | 散落在业务逻辑中 | 统一格式，一目了然 |
| 扩展性 | 每加一个维度多写一段 | report() 返回结构化对象 |
| fail-fast 语义 | 通常没有统一取消协议 | 报告同时反映失败与取消 |

---

> 📁 完整测试代码：[G2_BatchResultReportTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/G2_BatchResultReportTest.java)
