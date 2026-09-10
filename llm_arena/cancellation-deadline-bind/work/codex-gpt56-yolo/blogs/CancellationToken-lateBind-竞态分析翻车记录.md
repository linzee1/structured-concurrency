# 一次失败的并发竞态分析：SettableFuture.setFuture 并不会抛 IllegalStateException

大家好，我是桦说编程。

> 我在 code review 自己的并发取消框架时，曾断言 `lateBind` 与 `cancel` 存在会导致 `IllegalStateException` 的时序竞态。写下 1000 次并发竞态测试后，测试告诉我：**结论错了**。本文记录这次翻车过程，以及翻车后才发现的真正的坑。

## 问题背景

项目里有一个协作式取消令牌 `CancellationToken`（`src/main/java/io/github/huatalk/parallelinscope/cancel/CancellationToken.java`），核心是两个成员：

```java
private final SettableFuture<Object> futureToken = SettableFuture.create();
private final AtomicReference<CancellationToken.State> state = new AtomicReference<>(RUNNING);
```

对外暴露两个操作：

```java
// 任务提交后绑定：把令牌挂到实际任务的 future 链上
public <T> void lateBind(List<ListenableFuture<T>> futures, Duration timeout,
                         ListenableFuture<?> submitCanceller, ScheduledExecutorService timer) {
    // ... 组装 failFastFuture / 注册回调 ...
    futureToken.setFuture(failFastFuture);   // line 120
}

// 主动取消
public void cancel(boolean useInterrupt) {
    state.compareAndSet(RUNNING, MUTUAL_CANCELED);
    futureToken.cancel(useInterrupt);
}
```

两个方法操作同一个 `futureToken`，且没有任何同步协调。我的原始分析是：

> `lateBind` 直接 `futureToken.setFuture(failFastFuture)`，未判断 `futureToken` 是否已被 `cancel()` 置为取消。业务若在绑定前调用 `cancel(true)`，`setFuture` 会抛 `IllegalStateException`。

听起来逻辑自洽：future 已经完成/取消，再 `set` 当然要抛异常——`CompletableFuture.complete` 之外的很多 future 实现（包括 `SettableFuture.set()`）确实是这个语义。

**但这是拍脑袋推出来的，没跑过一行验证代码。**

## 验证：测试推翻结论

写了三个测试（`src/test/java/io/github/huatalk/parallelinscope/cancel/CancellationTokenLateBindRaceTest.java`）。

### 场景一：cancel 先于 lateBind

```java
@Test
void cancelBeforeLateBind_stillCancelsTasks() {
    CancellationToken token = CancellationToken.create();
    token.cancel(false);

    SettableFuture<String> task = SettableFuture.create();

    // 按原结论，这里应该抛 IllegalStateException
    token.lateBind(Collections.singletonList(task), Duration.ofHours(1),
                   Futures.immediateVoidFuture(), timer);

    assertThat(task).isCancelled();                                  // 任务被取消
    assertThat(token.getState()).isEqualTo(MUTUAL_CANCELED);         // 状态正确
}
```

结果：**不抛异常，任务照样被取消。**

翻 Guava 源码（33.6.0-jre，`AbstractFuture.setFuture`），真相是：

```java
public boolean setFuture(ListenableFuture<? extends V> future) {
    // ...
    if (value == null) {
        // 目标 future 尚未完成，正常委托
    }
    // 目标已完成/已取消：返回 false
    // 且若目标是 cancelled，会顺带 cancel 传入的 future
    if (wasCancelled) {
        future.cancel(wasInterrupted);
    }
    return false;
}
```

两个关键事实，都是我原分析里搞错的：

- `setFuture` **返回 `boolean`**，目标已结束时返回 `false`，**不抛异常**。抛 `IllegalStateException` 的是 `set()`/`setException()`，不是 `setFuture()`。
- 目标已取消时，`setFuture` 会**反向取消传入的 delegate future**。于是 `failFastFuture` 被取消，级联触发回调里的 `allFutures.cancel(true)`，取消最终传播到业务任务。

也就是说，`cancel` 先于 `lateBind` 时，取消意图不仅没丢，反而被 Guava 自动接力传递下去了。

### 场景二：1000 次并发竞态

```java
@Test
void concurrentCancelAndLateBind_cancelsTasksEitherWay() throws InterruptedException {
    int attempts = 1000;
    int notCancelled = 0;

    for (int i = 0; i < attempts; i++) {
        CancellationToken token = CancellationToken.create();
        SettableFuture<String> task = SettableFuture.create();
        CountDownLatch start = new CountDownLatch(1);

        Thread canceler = new Thread(() -> { await(start); token.cancel(true); });
        Thread binder   = new Thread(() -> { await(start); token.lateBind(futures, ...); });

        canceler.start();
        binder.start();
        start.countDown();
        canceler.join();
        binder.join();

        if (!task.isCancelled()) notCancelled++;
    }
    assertThat(notCancelled).isZero();
}
```

1000 次竞态，两种交错顺序都跑遍了：

- `cancel` 先赢：`futureToken` 已取消 → `setFuture` 返回 false 并取消 `failFastFuture` → 任务被取消。
- `lateBind` 先赢：`setFuture` 成功 → `cancel` 取消 `futureToken` → 委托传播取消 `failFastFuture` → 任务被取消。

**1000/1000 全部取消，无一遗漏。** `AbstractFuture` 内部的 CAS + happens-before 保证了这两条路径闭环，所谓"时序竞态"在这个层面上根本不存在。

## 真正的坑：lateBind 不是幂等的

测试推翻原结论的同时，暴露了另一个此前完全没注意到的问题：

```java
@Test
void lateBindCalledTwice_secondBindingIsIgnored() throws InterruptedException {
    CancellationToken token = CancellationToken.create();
    SettableFuture<String> firstTask = SettableFuture.create();
    SettableFuture<String> secondTask = SettableFuture.create();

    token.lateBind(Collections.singletonList(firstTask), ...);
    firstTask.set("ok");
    // 等 failFastFuture 完成传播
    Thread.sleep(50);
    assertThat(token.getState()).isEqualTo(SUCCESS);

    // 第二次绑定：futureToken 已完成，setFuture 返回 false
    token.lateBind(Collections.singletonList(secondTask), ...);

    assertThat(secondTask).isNotCancelled();          // 第二组任务无人看管
    assertThat(token.getState()).isEqualTo(SUCCESS);  // 令牌状态仍属于第一组
}
```

第二次 `lateBind` 被**静默吞掉**：`futureToken` 已完成，`setFuture` 返回 false，第二次绑定的超时检测、fail-fast 回调照常创建，但永远不会被令牌跟踪。如果调用方以为绑定生效了，第二组任务的超时和 fail-fast 保护形同虚设——这才是值得修的点，比如重复绑定时抛异常或记入日志，而不是"修复"一个根本不存在的竞态。

## 总结

- **不要凭语义直觉推断并发 bug。** `set()` 会抛 `IllegalStateException`，不代表 `setFuture()` 也会；两个方法名差一个单词，失败语义完全不同。看一眼源码签名（返回值 `boolean`）就能避免整段错误分析。
- **竞态假设必须配竞态测试。** CountDownLatch 对齐起跑、循环 1000 次，成本不到半小时，比"从代码结构推演时序"可靠得多。这次测试不仅证伪了原结论，还顺带证出了真正的非幂等缺陷。
- **Guava 的 `setFuture` 设计得很周到**：目标已取消时反向取消 delegate，恰好让 cancel-before-bind 场景自动闭环。理解依赖库的失败语义，是分析自己框架正确性的前提。

---

如果这篇文章对你有帮助，欢迎关注我，持续分享高质量技术干货，助你更快提升编程能力。
