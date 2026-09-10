# 迁移到 v0.3

> `0.3` 尚未发布。本文记录分支上已经落地的破坏性变更，从 `0.2` 升级时先看这里。

## 任务执行 future 统一交付为 `TaskFuture`

库交付的每一个任务执行 future 现在都实现 `TaskFuture<T>`：在普通 `ListenableFuture` 之上增加
`taskName()`、`outcome()`、`deadlineNanos()`、`remaining()` 与 `failure()`。声明返回类型相应收窄：

| 交付点 | `0.2` | `0.3` |
|---|---|---|
| `TaskBatchResult.results()` 元素 | `ListenableFuture<T>` | `TaskFuture<T>` |
| `TaskGroup.members()` 的值 | `ListenableFuture<?>` | `TaskFuture<?>` |
| `TaskGroup.findMember(String)` | `Optional<ListenableFuture<?>>` | `Optional<TaskFuture<?>>` |
| `TaskGroup.future(TaskKey<T>)`（成员或 combine） | `ListenableFuture<T>` | `TaskFuture<T>` |
| `TaskGroup.completionFuture()` | `ListenableFuture<TaskGroupResult>` | `TaskFuture<TaskGroupResult>` |
| `TaskBatchResult.submitCanceller()` | `ListenableFuture<?>` | 不变 |

这是源码兼容的变更：`TaskFuture` 继承 `ListenableFuture`，赋值、`Futures.allAsList`、`addCallback`、
`FluentFuture.from` 等全部 Guava 组合 API 照常编译、行为不变；二进制不兼容，需针对 `0.3` 重新编译。
调用方无需改动，不检查该接口的代码完全不受影响。

新增两个类型，其中一个是实现细节：

- `TaskFuture<T>` 是契约。用 `instanceof` 检查，并只面向接口编程。
- `Task<T>` 是库实际交付的实现。其构造器与工厂均为包私有；请把它当作私有类型，不要强转。

```java
for (TaskFuture<Account> future : result.results()) {
    if (future.outcome() == TaskOutcome.TIMEOUT) {
        log.warn("{} timed out with {} left", future.taskName(), future.remaining());
    }
}
```

归因规则见[使用指南](user-guide.md#从-future-读取任务归因)。

## 被放弃的批次元素改为以 `SubmissionException` 失败

从未真正进入执行器的批次元素过去直接以原始 cause 失败：初始提交被拒绝时是
`RejectedExecutionException`，submitter 被中断时是 `InterruptedException`。现在这些元素以
`SubmissionException` 包装原始 cause 失败，因此 `outcome()` 报告 `SUBMISSION_FAILURE` 而不是
`USER_FAILURE`，读者可以区分"从未执行"与"执行后抛异常"。

通过 `getCause()` 取回原始 cause（或按通用 `Throwable` 链处理）：

```java
try {
    result.results().get(0).get();
} catch (ExecutionException failure) {
    Throwable cause = failure.getCause();            // SubmissionException
    Throwable rejected = cause.getCause();           // RejectedExecutionException
}
```
