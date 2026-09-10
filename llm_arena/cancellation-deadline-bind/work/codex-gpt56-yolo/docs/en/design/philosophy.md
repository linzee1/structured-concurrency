# Subtractive Design: `parallel-in-scope` Design Review

## Introduction

The library targets three recurring Java 8 problems: cancellation that cannot reach CPU-bound work, context lost across thread pools, and deadlocks caused by nested work submitted to a bounded pool. Its design is intentionally narrow: batch execution with explicit cancellation and diagnostics, rather than a general asynchronous workflow framework.

## The central API: `Par.map`

`Par.map` gives a batch a clear scope, executor, task type, parallelism, timeout, and result report. The scope makes failure and cancellation ownership visible. It also gives the implementation one place to enforce fail-fast behavior, sliding-window submission, and parent-child cancellation.

## Late binding

Cancellation and timeout wiring is completed after all futures have been created. This late-binding step avoids races where an early task fails before the rest of the batch has been connected to the same cancellation token. The trade-off is a slightly more explicit internal lifecycle in exchange for deterministic batch semantics.

## Two cancellation exceptions

Normal cancellation is a control-flow event, so `LeanCancellationException` avoids collecting a stack trace. Callers that request diagnostic stack traces receive the standard `CancellationException`; no redundant framework subtype is needed.

## Sliding-window scheduling

Submitting every item at once can flood a queue and make nested calls deadlock. `ConcurrentLimitExecutor` keeps only a bounded window of work in flight. CPU and I/O task types can use different scheduling policies, but both remain inside the same structured scope.

## Explicit execution context and deadlock visibility

`TaskExecutionContext` is installed only while a scoped task executes; its batch context defines
the current task's cancellation and nesting ownership. A separate internal submission scope exists
only while work is handed to an executor, so `SmartBlockingQueue` can apply batch enqueue policy
before a task starts. Neither context is relayed through arbitrary user executor submissions.
`TaskGraphObservationContext` records task and executor relationships in a request-scoped graph so
the library can detect cycles that a thread dump would otherwise reveal only after production
impact. This is executor deadlock detection, not a replacement for lock analysis.

## Deliberate boundaries

The project does not add configurable failure policies, a built-in retry engine, a Spring Boot starter, or a second fluent-composition API. Those features increase surface area and overlap with mature libraries. Callers can compose the batch API with Guava or `CompletableFuture` at their application boundary.

## Closing principle

The useful abstraction is the smallest one that makes failure ownership, cancellation, scheduling, and diagnostics explicit. More APIs are not automatically more capability if they make those guarantees harder to reason about.
