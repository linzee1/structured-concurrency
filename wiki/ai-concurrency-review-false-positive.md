# AI 对并发代码审查的误报复盘

## 背景

在 `parallel-in-scope` 的一次重命名重构审查中，AI code review 对
`CancellationToken.lateBind()` 给出了一条严重级别结论：快速失败取消机制已经损坏。

相关代码如下：

```java
FluentFuture<?> failFastFuture = FluentFuture.from(Futures.allAsList(futures))
        .catchingAsync(Throwable.class, ex -> Futures.immediateCancelledFuture(), directExecutor())
        .withTimeout(timeout, ParConfig.getTimer());
```

AI 的判断是：

1. `Futures.allAsList(futures)` 在任意子任务失败时失败。
2. `catchingAsync` 捕获异常，并返回 `Futures.immediateCancelledFuture()`。
3. `failFastFuture` 因此成功完成。
4. `addCallback` 进入 `onSuccess`，状态被设置为 `SUCCESS`。
5. 兄弟任务不会被取消，快速失败失效。

这个推理看起来完整，但关键一步是错的。

## 误报在哪里

`Futures.immediateCancelledFuture()` 返回的是一个已经取消的 future。取消并不是成功完成。

因此，这条链路的真实行为是：

1. 某个子任务失败。
2. `Futures.allAsList(futures)` 失败。
3. `catchingAsync` 捕获失败，并返回一个已取消 future。
4. `failFastFuture` 进入失败/取消路径，而不是成功路径。
5. `addCallback` 调用 `onFailure`。
6. `onFailure` 执行 `allFutures.cancel(true)`，并把状态设置为 `FAIL_FAST_CANCELED`。

也就是说，AI 把 “捕获异常后返回 cancelled future” 误解成了 “捕获异常后恢复为成功 future”。
这类错误在并发代码中很常见：控制流不是只由异常捕获决定，还受到 future 状态机语义影响。

## 开发者如何纠正

开发人员没有只靠口头解释，而是补了一个最小、聚焦的测试，把争议点直接编码成可执行断言：

```java
@Test
public void testLateBind_failFast_cancelsSiblingAndSubmitCanceller() {
    CancellationToken token = CancellationToken.create();

    SettableFuture<String> failed = SettableFuture.create();
    SettableFuture<String> sibling = SettableFuture.create();
    SettableFuture<Void> submitCanceller = SettableFuture.create();

    token.lateBind(Arrays.asList(failed, sibling), Duration.ofSeconds(5), submitCanceller);

    failed.setException(new RuntimeException("boom"));

    await().untilAsserted(() -> {
        assertEquals(CancellationToken.State.FAIL_FAST_CANCELED, token.getState());
        assertTrue(sibling.isCancelled(), "sibling future should be cancelled by fail-fast");
        assertTrue(submitCanceller.isCancelled(), "submit canceller should be cancelled by fail-fast");
    });
}
```

这个测试验证了三个关键事实：

| 断言 | 说明 |
|---|---|
| 状态是 `FAIL_FAST_CANCELED` | 没有误入 `SUCCESS` |
| 兄弟 future 被取消 | 快速失败确实传播到了同批任务 |
| `submitCanceller` 被取消 | 剩余提交流程也被停止 |

测试结果：

```text
mvn -B -ntp -Dtest=CancellationTokenTest test
Tests run: 10, Failures: 0, Errors: 0

mvn -B -ntp test
Tests run: 101, Failures: 0, Errors: 0
```

结论：这条严重审查意见是误报。生产代码不需要按该意见修改；新增测试可以作为回归保护，防止未来真的把取消语义改坏。

## 为什么 AI 容易在并发代码上产生幻觉

并发代码比普通业务代码更容易触发 AI 的幻觉，原因主要有四个。

第一，状态语义通常藏在库 API 里。`catchingAsync`、`withTimeout`、`allAsList`、`successfulAsList`、`immediateCancelledFuture` 这些 API 的组合行为，不能只靠名字推断。AI 很容易把 “异常被捕获” 简化成 “链路恢复成功”，但 Guava future 的 cancelled 状态有独立语义。

第二，控制流是异步的。同步代码里，`try-catch-return` 的路径比较直观；异步 future 链里，结果可能是 success、failure、cancelled、timeout，回调触发时机也取决于 executor 和 future 状态。AI 如果没有逐步模拟状态机，很容易给出看似合理但实际错误的路径。

第三，并发取消有多层传播。这里同时涉及子任务 future、聚合 future、timeout future、submit canceller、token state。只看其中一层，会误判整个系统行为。

第四，AI 倾向于生成“完整故事”。一旦它在早期步骤做了错误假设，后续推理会继续补全成一条顺畅的因果链。这种回答读起来很像专业审查，但并不等于事实正确。

## 对 AI Code Review 的使用建议

AI code review 可以提高覆盖面，但对并发、异步、内存模型、取消传播、锁、线程池、future 状态机这类代码，不能把它的结论直接当作事实。

更可靠的流程是：

1. 把 AI 的问题描述转成一个可执行假设。
2. 写最小测试，只验证这条假设，不顺手重构。
3. 让测试覆盖状态、传播对象和时序边界，而不是只检查一个表面结果。
4. 测试失败再修生产代码；测试通过则把审查意见降级为误报或文档澄清。
5. 对并发库 API，优先查官方语义或用小实验确认，不靠自然语言直觉。

## 这次复盘的要点

AI 的价值在于提出值得检查的风险点，而不是替代开发者理解并发语义。

这次误报说明：AI 对并发理解还嫩，尤其容易在 future 取消、异常恢复、回调分派这类细节上产生幻觉。真正可靠的纠正方式不是争论谁的解释更像对的，而是把争议压缩成一段代码，让测试结果说话。

