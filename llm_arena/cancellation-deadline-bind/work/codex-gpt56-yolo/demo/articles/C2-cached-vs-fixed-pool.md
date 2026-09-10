# C2. CachedThreadPool 不会死锁

## 问题

在 C1 中我们看到，嵌套并行调用同一个 `FixedThreadPool` 会导致死锁。但这并不意味着所有嵌套并行都是危险的。关键区别在于线程池的类型：**有界池**（bounded pool）会死锁，**无界池**（unbounded pool）不会。

`Executors.newFixedThreadPool(n)` 底层使用 `LinkedBlockingQueue`，线程数固定为 n。当所有线程被外层任务占满，内层子任务只能排队等待——而外层任务又在等内层完成，循环等待形成死锁。而 `Executors.newCachedThreadPool()` 底层使用 `SynchronousQueue`，没有任务队列，每当有新任务提交时，如果没有空闲线程就会立即创建新线程。这意味着内层子任务总能获得线程执行，不会被阻塞。

很多开发者在遇到嵌套死锁后，第一反应是"嵌套并行就是不行"，然后改用串行化或者复杂的线程池拆分方案。实际上，如果内外层都使用 `CachedThreadPool`，同样的嵌套模式完全可以正常工作。只有当线程池存在**有界队列 + 固定线程数**的组合时，才需要担心死锁问题。

## 问题复现

```java
// 场景：同一个嵌套模式，分别用 FixedThreadPool 和 CachedThreadPool

// === FixedThreadPool：死锁 ===
ExecutorService fixedPool = Executors.newFixedThreadPool(2);
for (int i = 0; i < 2; i++) {
    fixedPool.submit(() -> {
        Future<String> inner = fixedPool.submit(() -> "done");
        inner.get(3, TimeUnit.SECONDS);  // 超时！2 个线程全被占满
    });
}
// 结果：TimeoutException，死锁

// === CachedThreadPool：正常完成 ===
ExecutorService cachedPool = Executors.newCachedThreadPool();
for (int i = 0; i < 2; i++) {
    cachedPool.submit(() -> {
        Future<String> inner = cachedPool.submit(() -> "done");
        inner.get(3, TimeUnit.SECONDS);  // 正常返回，CachedThreadPool 自动创建新线程
    });
}
// 结果：所有任务正常完成
```

## 解决方法

`parallel-in-scope` 的死锁检测模块内置了 `canDeadlock()` 判断逻辑。它会检查线程池的队列类型：

| 线程池类型 | 底层队列 | 是否有界 | 能否死锁 |
|-----------|---------|---------|---------|
| `FixedThreadPool` | `LinkedBlockingQueue` | 有界（固定线程数） | 是 |
| `CachedThreadPool` | `SynchronousQueue` | 无界（按需创建线程） | 否 |
| 自定义（`maxPoolSize = MAX_VALUE`） | 任意 | 无界 | 否 |

只有当线程池是有界池时，`TaskGraph` 才会将其纳入死锁图进行环路检测。`CachedThreadPool` 因为使用 `SynchronousQueue`（没有排队能力，每提交一个任务就必须有线程来接），天然不会产生循环等待。

在实际开发中，如果你的嵌套并行场景确实需要共享线程池，使用 `CachedThreadPool` 是最简单的安全方案。如果对线程数有严格控制，则应为不同层级的任务分配独立的 `FixedThreadPool`。

## 代码

```java
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.BatchExecutionOptions;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.AsyncBatchResult;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// CachedThreadPool：嵌套并行不会死锁
ExecutorService cachedPool = Executors.newCachedThreadPool();

GlobalPar config = GlobalPar.builder()
        .register("cached-pool", cachedPool)
        .build();
Par par = config.defaultPar();

// 外层任务
BatchExecutionOptions outerOpts = BatchExecutionOptions.of("outer-task")
        .parallelism(4)
        .timeout(java.time.Duration.ofMillis(10_000))
        .build();

List<Integer> items = Arrays.asList(1, 2, 3, 4);
AsyncBatchResult<String> result = par.map( items, item -> {
    // 内层任务使用同一个 CachedThreadPool，不会死锁
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
// CachedThreadPool 按需创建线程，内层子任务总能获得执行机会
```

---

> 📁 完整测试代码：[C2_CachedVsFixedPoolTest.java](https://github.com/huatalk/parallel-in-scope/blob/main/demo/src/test/java/demo/article/C2_CachedVsFixedPoolTest.java)
