# Changelog

## [Unreleased]

### Breaking changes

- Rename `GlobalParLivelockPolicy` to `GlobalParDeadlockPolicy` and `LivelockListener` to `DeadlockDetectionListener`; the graph reports potential deadlock structures, not runtime livelock.
- Rename `ExecutionOptions` to `BatchExecutionOptions` to make its per-`Par.map` scope explicit.
- Replace the abrupt-close `ClosableBlockingQueue` (recovery lists, `remainingList()`) with `DrainingBlockingQueue`: `close()` rejects producers while consumers keep draining queued elements until the `DRAINED` terminal state. No custom shutdown exception types are introduced: write rejections throw `IllegalStateException`, drained reads throw `NoSuchElementException`.
- Merge `TaskGraph` into `TaskGraphObservationScope`: the observation scope is now a request-level `TransmittableThreadLocal` global (identity-propagated to worker threads), owns the graph lifecycle (`install`/`restore`/`data`/`logTaskPair`/`hasXxx` statics), and runs deadlock detection in `close()`. The former `TaskGraph.Data` is now the top-level `TaskGraphData`; `previousData()` and `complete()` are removed.
- Make `TaskRef` an abstract class whose anonymous subclass captures the member result type at runtime (Guava `TypeToken` style: `new TaskRef<List<Order>>("orders") {}`). `TaskGroupSpec.Builder.task` now takes the ref as its first argument and no longer takes `memberName`; `TaskGroup.future(ref)` rejects a ref whose raw result type does not cover the type the member was registered with.
- Remove `TaskGroupCompletionReason`; `TaskGroupResult.completionReason()` becomes `outcome()` returning `TaskOutcome`. A fail-fast group now reports the failed member's own outcome (`USER_FAILURE`/`SUBMISSION_FAILURE`) instead of `FAILED`, and group-wide cancellation reports `GROUP_CANCELED`.
- Rename `CancellationToken.State` values onto the `TaskOutcome` vocabulary: `FAIL_FAST_CANCELED` → `FAIL_FAST`, `TIMEOUT_CANCELED` → `TIMEOUT`, `MUTUAL_CANCELED` → `CANCELED`, `PROPAGATING_CANCELED` → `PROPAGATED_CANCELED`. `code()` values and `shouldInterruptCurrentThread()` semantics are unchanged.
- Fix the `ExecutionPhase.CANCELLED_BEFORE_RUN` spelling to `CANCELED_BEFORE_RUN`.
- Remove `GlobalExecutionPolicy` (it only carried the task-listener list): register listeners directly on `GlobalPar.Builder` via `taskListener(...)` and per-Par `parTaskListener(name, ...)` (repeated calls append; the override still replaces the default list for that Par), and read them via `GlobalPar.taskListeners()` / `taskListenersFor(name)`.
- Rename `AsyncBatchResult` to `TaskBatchResult` (the nested `BatchReport` keeps its name).
- Rename the internal `ConcurrentLimitExecutor` to `SlidingWindowSubmitter`.
- Rename `TaskGraphObservationContext` to `TaskGraphObservationScope`: it is a closeable observation scope, and the naming rule is now that `Scope` marks lifecycle scopes (`SubmissionScope`, `TaskGraphObservationScope`) while `Context` marks data carriers. `MultiTaskContext.taskGraphObservationContext()` is renamed to `taskGraphObservationScope()` accordingly; `GlobalPar.openTaskGraphObservation()` keeps its name.

## [0.2.0] - 2026-07-22

### Breaking changes

- Replace `ParConfig`, `ParOptions`, `ExecutorResolver`, and legacy `Par` entry points with immutable `GlobalPar`, executor-bound `Par`, and per-batch `BatchExecutionOptions`.
- Make `BatchExecutionContext` the source of task scope state, including cancellation, deadlines, nested batches, and executor identity.

### Features

- Bind existing futures into a task scope with cancellation, timeout, and fail-fast behavior.
- Support custom schedulers and isolate timer callback dispatch from timer threads.
- Add the public `ActionGate` API for count- and duration-based action gating.
- Add immutable multi-`Par` `GlobalPar` topology, `GlobalExecutionPolicy`, deadlock/purge policies, and explicit observation scopes.
- Add `ClosableBlockingQueue` lifecycle shutdown, recovery lists, poison signaling, and post-close FIFO `drainTo` recovery transfer.

### Fixes

- Make completion-service cancellation visible to `ThreadPoolExecutor.purge()` by queuing and returning the same Future task.
- Add opt-in `SmartBlockingQueue` purge maintenance gated by queue pressure and estimated cancelled-task ratio.
- Coalesce concurrent cancellation signals without sliding-delay starvation or lost follow-up purge demand.

### Build policy

- Maven compiler `failOnWarnings` is currently `false`; revisit this before publishing a stable release.

### Documentation and tests

- Record task/Future lifecycle and event-coalesced purge decisions as architecture decision records.
- Add layered Cartesian and latch-controlled concurrency coverage for cancellation, queue mutation, and purge races.

## [0.1.0] - 2026-07-18

Initial public release.

- Structured-concurrency toolkit for Java 8+.
- Cooperative cancellation, fail-fast execution, timeout handling, and parent-to-child cancellation propagation.
- Bounded sliding-window scheduling for batch work.
- Cross-thread `ThreadLocal` context propagation.
- CPU/IO-aware scheduling and task/executor graph cycle detection.
- Monitoring SPI for task execution, queueing, and failures.
- Runnable Java 8 demo project and bilingual documentation site.

Artifacts:

- Maven Central: `io.github.huatalk:parallel-in-scope:0.1.0`
- GitHub release: [v0.1.0](https://github.com/HuaTalk/parallel-in-scope/releases/tag/v0.1.0)
