# Migrating to v0.3

> `0.3` is not released yet. This note records the breaking changes already on the branch, so an
> upgrade from `0.2` starts here.

## Task execution futures are delivered as `TaskFuture`

Every future the library delivers for a task execution now implements `TaskFuture<T>`, which adds
`taskName()`, `outcome()`, `deadlineNanos()`, `remaining()`, and `failure()` to the plain
`ListenableFuture` contract. The declared return types narrowed accordingly:

| Delivery point | `0.2` | `0.3` |
|---|---|---|
| `TaskBatchResult.results()` element | `ListenableFuture<T>` | `TaskFuture<T>` |
| `TaskGroup.members()` value | `ListenableFuture<?>` | `TaskFuture<?>` |
| `TaskGroup.findMember(String)` | `Optional<ListenableFuture<?>>` | `Optional<TaskFuture<?>>` |
| `TaskGroup.future(TaskKey<T>)` (member or combine) | `ListenableFuture<T>` | `TaskFuture<T>` |
| `TaskGroup.completionFuture()` | `ListenableFuture<TaskGroupResult>` | `TaskFuture<TaskGroupResult>` |
| `TaskBatchResult.submitCanceller()` | `ListenableFuture<?>` | unchanged |

The change is source compatible: `TaskFuture` extends `ListenableFuture`, so assignments,
`Futures.allAsList`, `addCallback`, `FluentFuture.from`, and every other Guava combinator keep
compiling and behaving exactly as before. It is binary incompatible — recompile against `0.3`. No
call site has to change, and code that never checks for the interface needs no change at all.

Two types are new, and one of them is not meant to be named:

- `TaskFuture<T>` is the contract. Check it with `instanceof` and program against the interface.
- `Task<T>` is the implementation the library delivers. Its constructor and factories are
  package-private; treat the class as private and never cast to it.

```java
for (TaskFuture<Account> future : result.results()) {
    if (future.outcome() == TaskOutcome.TIMEOUT) {
        log.warn("{} timed out with {} left", future.taskName(), future.remaining());
    }
}
```

See the [user guide](user-guide.md#read-task-attribution-from-a-future) for the attribution rules.

## Abandoned batch elements fail with `SubmissionException`

A batch element that never reached the executor used to fail with the raw cause: the
`RejectedExecutionException` of a rejected initial submission, or the `InterruptedException` of an
interrupted submitter. Those elements now fail with a `SubmissionException` wrapping that cause, so
`outcome()` reports `SUBMISSION_FAILURE` instead of `USER_FAILURE` and a reader can tell "never ran"
from "ran and threw".

Read the original cause through `getCause()`, or the `Throwable` chain generically:

```java
try {
    result.results().get(0).get();
} catch (ExecutionException failure) {
    Throwable cause = failure.getCause();            // SubmissionException
    Throwable rejected = cause.getCause();           // RejectedExecutionException
}
```
