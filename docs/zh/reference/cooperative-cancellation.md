# 协作式取消（Cooperative Cancellation）


## 为什么需要"协作式"取消？

Java 的 `Thread.interrupt()` 只能中断**阻塞操作**（如 `Thread.sleep()`、`Object.wait()`、`BlockingQueue.take()` 等）。对于正在执行纯计算的 CPU 密集型代码，interrupt 信号不会产生任何效果——线程会一直运行直到计算结束。

parallel-in-scope 采用**协作式取消**模型：框架负责发出取消信号（通过 `CancellationToken` 的状态变更），但**任务代码需要主动检查这个信号**。这个检查动作称为 **checkpoint**。

> 类比：协作式取消就像公路上的收费站——车辆（任务）在经过收费站（checkpoint）时才会被拦截。如果一段公路上没有收费站，车辆就会一路畅行不受控制。

## 框架自动做了什么

parallel-in-scope 在以下位置**自动插入**了 checkpoint 和取消响应：

| 位置 | 机制 | 你需要做什么 |
|---|---|---|
| 任务执行前 | `ScopedCallable` 在调用你的代码之前自动执行一次 `Checkpoints.checkpoint()` | 无需任何操作 |
| I/O 阻塞期间 | `futureToken.cancel(true)` 发送 `Thread.interrupt()`，阻塞操作抛出 `InterruptedException` | 无需任何操作 |
| 滑动窗口提交循环 | `SlidingWindowSubmitter` 在每次提交前检查取消状态，发现取消后停止提交剩余任务 | 无需任何操作 |

## 滑动窗口占位 Future 的终态

滑动窗口会先为尚未提交的任务创建 placeholder，保证结果列表保持输入顺序。停止 admission
后，框架不会让这些 placeholder 永久保持 `LIVE`：

- 直接取消 placeholder，或首个已提交任务取消：剩余 placeholder 进入 `CANCELLED`；
- 取消 `TaskBatchResult.submitCanceller()`：submitter 收到 interrupt，剩余 placeholder
  以 `InterruptedException` 失败；
- 后续提交被执行器拒绝：剩余 placeholder 以拒绝异常失败。

因此 `Futures.allAsList(result.results())` 最终一定会完成。`submitCanceller()` 表示
“停止后续提交”，不保证已提交任务立即停止；任务本身仍遵循协作式取消规则。

这意味着：
- **尚未开始的任务**会被自动跳过（预执行 checkpoint 拦截）。
- **正在阻塞等待 I/O 的任务**会被 interrupt 唤醒。
- **正在排队等待提交的任务**不会被提交。

## 你需要做什么：在 CPU 密集型代码中手动插入 Checkpoint

如果你的任务包含**纯计算循环**（没有 I/O 阻塞），框架无法自动中断它。你必须在合适的位置手动调用 checkpoint。

### 基本用法

```java
MultiTaskOptions options = MultiTaskOptions.of("my-task")
        .parallelism(4)
        .timeout(Duration.ofSeconds(5))
        .build();

global.par("myExecutor").map(dataList, item -> {
    for (int i = 0; i < 1_000_000; i++) {
        // 每 1000 次迭代检查一次取消状态
        if (i % 1000 == 0) {
            Checkpoints.checkpoint("my-task", true);
        }
        heavyComputation(item, i);
    }
    return result;
}, options);
```

关键规则：
- **第一个参数必须与 `MultiTaskOptions.of(taskName)` 中的 taskName 一致**。这是一个安全守卫——checkpoint 只在 taskName 匹配时才生效，防止被不相关的代码误触发。
- **第二个参数 `lean`** 控制抛出的异常类型：
  - `true` → `LeanCancellationException`：无堆栈跟踪，零额外开销，适合生产环境。
  - `false` → 标准 `CancellationException`：完整堆栈跟踪，适合调试定位取消发生位置。

### Checkpoints API 一览

| 方法 | 用途 | 典型场景 |
|---|---|---|
| `Checkpoints.checkpoint(taskName, lean)` | 检查 `CancellationToken` 状态，已取消则抛异常 | CPU 密集型循环中的周期性检查 |
| `Checkpoints.sleep(millis)` | 取消感知的 sleep，将 `InterruptedException` 统一转换为 `LeanCancellationException` | 替代 `Thread.sleep()` |
| `Checkpoints.rawCheckpoint()` | 仅检查线程 interrupt 标志 | 不在 `Par` scope 内但仍需响应中断的场景 |
| `Checkpoints.propagateCancellation(ex)` | 在 catch 块中重新抛出取消异常 | 需要区分处理"取消"和"其他异常"时 |

## Checkpoint 插入策略

checkpoint 并非越多越好——每次调用都有微小开销（读取 `ThreadLocal` + 原子变量）。以下是推荐的插入策略：

**适合插入 checkpoint 的位置：**

```java
// 1. 长循环的每 N 次迭代
for (int i = 0; i < items.size(); i++) {
    if (i % 100 == 0) {
        Checkpoints.checkpoint("batch-process", true);
    }
    process(items.get(i));
}

// 2. 多阶段计算的阶段之间
ResultA a = phaseOne(input);
Checkpoints.checkpoint("multi-phase", true);
ResultB b = phaseTwo(a);
Checkpoints.checkpoint("multi-phase", true);
ResultC c = phaseThree(b);

// 3. 递归调用的入口处
void traverse(TreeNode node) {
    Checkpoints.checkpoint("tree-walk", true);
    if (node == null) return;
    process(node);
    traverse(node.left);
    traverse(node.right);
}
```

**不需要插入 checkpoint 的位置：**
- I/O 操作附近（HTTP 调用、数据库查询等）——interrupt 已经能中断这些操作。
- 执行时间极短的函数——任务本身很快结束，取消没有意义。
- 框架自动 checkpoint 已覆盖的地方（任务执行前）。

## 在 catch 块中正确处理取消异常

当你的任务代码中有 try-catch 时，需要注意不要意外吞掉取消异常：

```java
global.par("myExecutor").map(items, item -> {
    try {
        riskyOperation(item);
    } catch (Exception e) {
        // 错误做法：吞掉了所有异常，包括取消异常
        // log.error("failed", e);
        // return defaultValue;

        // 正确做法：先让取消异常透传，再处理其他异常
        Checkpoints.propagateCancellation(e);
        log.error("failed", e);
        return defaultValue;
    }
}, options);
```

`Checkpoints.propagateCancellation(e)` 会检查异常是否为 `CancellationException`，如果是则重新抛出；如果不是则什么都不做，让后续的异常处理逻辑继续执行。

## 用 `Checkpoints.sleep()` 替代 `Thread.sleep()`

在 `Par` 任务内部，永远使用 `Checkpoints.sleep()` 替代 `Thread.sleep()`：

```java
// 不推荐
Thread.sleep(1000);  // InterruptedException 需要你自己处理

// 推荐
Checkpoints.sleep(1000);  // 自动将 InterruptedException 转换为 LeanCancellationException
```

`Checkpoints.sleep()` 将 `InterruptedException` 统一转换为 `LeanCancellationException`，使得中断驱动的取消和协作式取消在异常类型上保持一致，同时避免为正常取消采集无用堆栈。

## 取消的四种触发源

了解取消可能从何而来，有助于理解为什么需要 checkpoint：

| 触发源 | Token 状态 | 说明 |
|---|---|---|
| 兄弟任务失败 | `FAIL_FAST` | 同一批次中某个任务抛异常，其余任务被取消 |
| 超时 | `TIMEOUT` | 超过 `MultiTaskOptions` 指定的超时时间 |
| 手动取消 | `CANCELED` | 代码调用了 `CancellationToken.cancel()` |
| 父作用域取消 | `PROPAGATED_CANCELED` | 嵌套场景下，外层作用域取消，自动传播到内层 |

所有触发源最终都通过同一个 `CancellationToken.getState().shouldInterruptCurrentThread()` 判断——checkpoint 不需要关心取消的原因，只需要知道"是否应该停止"。

## 嵌套作用域的取消传播

当 `Par.map()` 内部再次调用 `Par.map()` 形成嵌套时，框架通过 `CancellationToken` 的父子链自动传播取消：

```
外层 Par.map(["A", "B", "C"])
  ├── A → 内层 Par.map([1, 2, 3])    ← 拥有子 CancellationToken
  ├── B → 抛出异常                     ← 触发外层 fail-fast
  └── C → 内层 Par.map([4, 5, 6])    ← 拥有子 CancellationToken
```

当 B 失败时：
1. 外层 token 转为 `FAIL_FAST`。
2. A 和 C 的子 token 通过父子链自动转为 `PROPAGATED_CANCELED`。
3. A 和 C 的内层任务在下一次 checkpoint 或 I/O 阻塞时响应取消。

你不需要手动编排这个传播——前提是内层任务中有足够的 checkpoint。

## 核心要点总结

1. **协作式取消依赖你的配合**——框架发出信号，但 CPU 密集型任务需要你手动添加 `Checkpoints.checkpoint()` 才能响应。
2. **taskName 必须匹配**——`checkpoint("x", lean)` 只在当前 scope 的 taskName 等于 `"x"` 时生效。
3. **在合理的粒度插入 checkpoint**——长循环每 N 次迭代一次，多阶段计算在阶段之间，递归在入口处。
4. **不要在 catch 中吞掉取消异常**——使用 `Checkpoints.propagateCancellation(e)` 确保取消异常能透传。
5. **用 `Checkpoints.sleep()` 替代 `Thread.sleep()`**——统一取消异常类型。
6. **I/O 任务无需额外操作**——interrupt 机制已经覆盖。
