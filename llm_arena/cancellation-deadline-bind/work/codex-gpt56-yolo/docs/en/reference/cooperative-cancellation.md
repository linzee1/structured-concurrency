# Cooperative Cancellation

## Why cooperative cancellation?

Java `Thread.interrupt()` only interrupts blocking operations such as `sleep`, `wait`, and queue operations. A CPU-bound loop can ignore the interrupt flag and continue running. `parallel-in-scope` therefore uses cooperative cancellation: the framework publishes a cancellation signal and task code checks it at explicit checkpoints.

## What the framework does automatically

| Location | Mechanism | Application work |
|---|---|---|
| Before task execution | `ScopedCallable` runs `Checkpoints.checkpoint()` | None |
| During blocking I/O | `futureToken.cancel(true)` interrupts the blocking operation | None |
| Sliding-window submission | `ConcurrentLimitExecutor` stops submitting after cancellation | None |

### Sliding-window placeholders

Unsubmitted tasks are represented by input-ordered placeholder Futures. When admission stops,
placeholders never remain live indefinitely: direct placeholder cancellation produces
`CANCELLED`; cancelling the public submitter Future interrupts the submitter and records
`InterruptedException`; a later executor rejection records its rejection cause. This guarantees
that `Futures.allAsList` over the batch results can reach a terminal state. The submitter Future
stops future admission, but does not by itself guarantee that already-submitted task bodies stop.

Tasks that have not started are skipped, blocked I/O tasks are interrupted, and queued tasks are not submitted.

## Add checkpoints to CPU-bound work

```java
BatchExecutionOptions options = BatchExecutionOptions.of("my-task")
        .parallelism(4)
        .timeout(Duration.ofSeconds(5))
        .build();

global.par("myExecutor").map(dataList, item -> {
    for (int i = 0; i < 1_000_000; i++) {
        if (i % 1000 == 0) {
            Checkpoints.checkpoint("my-task", true);
        }
        heavyComputation(item, i);
    }
    return result;
}, options);
```

The checkpoint task name must match `BatchExecutionOptions.of(taskName)`. The `lean` flag selects `LeanCancellationException` without a stack trace for production paths or the standard `CancellationException` with a stack trace for diagnostics.

## Checkpoints API

| Method | Purpose |
|---|---|
| `Checkpoints.checkpoint(taskName, lean)` | Check the current cancellation token and throw when cancelled |
| `Checkpoints.sleep(millis)` | Sleep while converting interruption into a cancellation exception |
| `Checkpoints.rawCheckpoint()` | Check only the thread interrupt flag |
| `Checkpoints.propagateCancellation(ex)` | Re-throw cancellation exceptions from a catch block |

Add checkpoints at a reasonable granularity: every N iterations of a long loop, between expensive phases, or at the entry to a recursive walk. I/O calls and very short functions normally need no manual checkpoint.

## Do not swallow cancellation

```java
global.par("myExecutor").map(items, item -> {
    try {
        riskyOperation(item);
    } catch (Exception ex) {
        Checkpoints.propagateCancellation(ex);
        log.error("failed", ex);
        return defaultValue;
    }
}, options);
```

`propagateCancellation` re-throws every `CancellationException` and leaves ordinary exceptions unchanged.

## Cancellation sources

| Source | Token state | Meaning |
|---|---|---|
| Sibling failure | `FAIL_FAST_CANCELED` | One task failed and the rest of the batch was cancelled |
| Timeout | `TIMEOUT_CANCELED` | The configured timeout elapsed |
| Manual cancellation | `MUTUAL_CANCELED` | Application code called `CancellationToken.cancel()` |
| Parent cancellation | `PROPAGATING_CANCELED` | An outer scope cancelled a nested scope |

All sources are observed through the same token state check. Nested `Par.map` calls inherit a parent token, so cancellation propagates to child tasks at their next checkpoint or blocking operation.

## Summary

1. CPU-bound tasks need explicit checkpoints.
2. The checkpoint task name must match the current scope.
3. Use `propagateCancellation` before handling other exceptions.
4. Prefer `Checkpoints.sleep()` inside a `Par` task.
5. I/O tasks already benefit from interrupt-based cancellation.
