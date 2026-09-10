# Changelog

## [Unreleased]

### Breaking changes

- Rename `GlobalParLivelockPolicy` to `GlobalParDeadlockPolicy` and `LivelockListener` to `DeadlockDetectionListener`; the graph reports potential deadlock structures, not runtime livelock.
- Rename `ExecutionOptions` to `BatchExecutionOptions` to make its per-`Par.map` scope explicit.
- Replace the abrupt-close `ClosableBlockingQueue` (recovery lists, `remainingList()`) with `DrainingBlockingQueue`: `close()` rejects producers while consumers keep draining queued elements until the `DRAINED` terminal state. No custom shutdown exception types are introduced: write rejections throw `IllegalStateException`, drained reads throw `NoSuchElementException`.
- Give `CancellationToken` ownership of its deadline: `bind(List, ListenableFuture, ScheduledExecutorService)` no longer takes a `Duration`, deadline is fixed by the new `CancellationToken(parent, deadlineNanos)` constructor (children are capped by the parent deadline), and `deadlineNanos()`/`remaining()`/`timeoutCancel()`/`addStateListener(...)` are added. Binding an already-expired deadline synchronously cancels unfinished work while completed work keeps its result, and every terminal state is CAS-recorded before any cancel action runs so attribution is read from token state.
- Align task-group cancellation with batch semantics: `bind` cancels its bound futures directly on fail-fast/timeout, cancelling one group member future directly now cascades to the whole group (source member `MEMBER_CANCELED`, siblings `FAIL_FAST`, group reason `CANCELED`), and a member deadline that expires first escalates the group to `TIMEOUT`. The previous member-cancel-without-cascade contract and the group's hand-written completion reasons are removed; group completion reasons and member outcomes are derived from token state.
- Merge `TaskGraph` into `TaskGraphObservationContext`: the observation scope is now a request-level `TransmittableThreadLocal` global (identity-propagated to worker threads), owns the graph lifecycle (`install`/`restore`/`data`/`logTaskPair`/`hasXxx` statics), and runs deadlock detection in `close()`. The former `TaskGraph.Data` is now the top-level `TaskGraphData`; `previousData()` and `complete()` are removed.

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
