# Migrating to v0.2

Version `0.2.0` replaces the mutable configuration-and-resolver API with an immutable execution topology. This is a source-breaking migration.

| `0.1.x` | `0.2.0` |
|---|---|
| `ParConfig.builder().executor(name, executor)` | `GlobalPar.builder().register(name, executor)` |
| `new Par(config)` | `global.par(name)` |
| `ParOptions` | `BatchExecutionOptions` |
| `par.map(name, items, fn, options)` | `par.map(items, fn, options)` |
| `ParConfig` timeout/listener defaults | `GlobalExecutionPolicy` |
| `ParConfig` livelock settings | `GlobalParDeadlockPolicy` |
| `ParConfig` purge settings | `GlobalParPurgePolicy` |
| executor-name resolution at call time | executor binding at `GlobalPar` build time |
| `TaskGraph.destroyAfterRequest(config)` | `global.openTaskGraphObservation()` scope |

The new split is intentional: `BatchExecutionOptions` is caller input, while `BatchExecutionContext` is per-batch runtime state. Cancellation, deadline, and executor identity flow through parent-child batch contexts, including nested calls across named `Par` entries.

Earlier `0.2.x` snapshots named this type `ExecutionOptions`. Rename imports, variable declarations, and `Par.map` arguments to `BatchExecutionOptions`; no compatibility alias is retained during the `0.x` phase.

Earlier snapshots also exposed this detector as `GlobalParLivelockPolicy` and `LivelockListener`. Rename them to `GlobalParDeadlockPolicy` and `DeadlockDetectionListener`; the detector reports potential dependency-graph deadlocks and does not prove a runtime deadlock or detect livelock.

The first task-group API in `0.2.0-SNAPSHOT` uses a fixed builder contract. If code was written
against an earlier task-group draft, replace `openTaskGroup()`, dynamic `submit()`, and `seal()` with
`taskGroupBuilder()`, `addTask()`, and the one-shot `buildAndSubmitAll()`. `addTask()` returns a
typed `ParallelTaskGroup.TaskHandle<T>`; call `future()` only after build. There is no compatibility
shim because the dynamic-admission API was not released as a stable contract.

`TaskListener.TaskEvent` now exposes the completed task through `taskContext()` and its outcome
through `successful()`, `result()`, and `exception()`. A successful task may return null, so
use `successful()` rather than testing the result for null. Listener callbacks run outside the
completed task's dynamic execution scope; use the event instead of `TaskExecutionContext.current()`.

Task outcome classification is unified into a single enum, `TaskOutcome`, replacing both
`io.github.huatalk.parallelinscope.internal.FutureState` and
`io.github.huatalk.parallelinscope.scope.TaskGroupMemberReason`. `TaskOutcome` adds `RUNNING` to the
former member-reason values so it serves both batch reports and group member results. Mapping from
the removed enums: `FutureState.FAILED` → `TaskOutcome.USER_FAILURE`, `FutureState.CANCELLED` →
`TaskOutcome.MEMBER_CANCELED`, and `TaskGroupMemberReason.X` → `TaskOutcome.X` (same names).
Consequently `AsyncBatchResult.BatchReport.stateCounts()` is now keyed by `TaskOutcome`, and
`TaskGroupMemberResult.completionReason()` returns `TaskOutcome`.

Accessors converge on the bare `x()` style; no `getX()`/`isX()` forms remain in the public API or
internals. Earlier `0.2.0-SNAPSHOT` builds used bean-style names; rename call sites mechanically:

| Earlier snapshot | `0.2.0` |
|---|---|
| `Par.getGlobalPar()` | `Par.globalPar()` |
| `Par.getDisplayName()` | `Par.displayName()` |
| `AsyncBatchResult.getSubmitCanceller()` | `AsyncBatchResult.submitCanceller()` |
| `AsyncBatchResult.getResults()` | `AsyncBatchResult.results()` |
| `AsyncBatchResult.BatchReport.getStateCounts()` | `BatchReport.stateCounts()` |
| `AsyncBatchResult.BatchReport.getFirstException()` | `BatchReport.firstException()` |
| `GlobalPar.isClosed()/isShutdown()/isTerminated()` | `GlobalPar.closed()/shutdown()/terminated()` |
| `CancellationToken.getState()` / `State.getCode()` | `state()` / `code()` |
| `TaskEvent.getTaskContext()/getTaskName()` | `taskContext()` / `taskName()` |
| `TaskEvent.getSubmitTimeNanos()/getStartTimeNanos()/getEndTimeNanos()` | `submitTimeNanos()` / `startTimeNanos()` / `endTimeNanos()` |
| `TaskEvent.isSuccessful()/getResult()/isEnqueued()/getException()` | `successful()` / `result()` / `enqueued()` / `exception()` |
| `ScopedCallable.getTaskExecutionContext()/getCancellationToken()/getExecutorName()` | `taskExecutionContext()` / `cancellationToken()` / `executorName()` |
| `TaskGraphData.getGraph()/getExecutorGraph()` | `graph()` / `executorGraph()` |
| `TaskGraphData.isTaskCycle()/isSelfLoop()/isExecutorCycle()/isExecutorSelfLoop()` | `taskCycle()` / `selfLoop()` / `executorCycle()` / `executorSelfLoop()` |
| `TaskEdge.getParallelism()/getTaskType()/getTaskCount()/getTimeoutMillis()` | `parallelism()` / `taskType()` / `taskCount()` / `timeoutMillis()` |
| `TaskEdge.getExecutorName()/getSourceExecutorName()` | `executorName()` / `sourceExecutorName()` |
| `TaskEdge.getExecutorIdentity()/getSourceExecutorIdentity()` | `executorIdentity()` / `sourceExecutorIdentity()` |
| `TaskEdge.isExecutorDeadlockProne()` | `executorDeadlockProne()` |
| `DeadlockDetectionListener.getTaskEdges()/getExecutorEdges()` | `taskEdges()` / `executorEdges()` |
| `TaskGraphObservationContext.isClosed()` | `closed()` |
| `DrainingBlockingQueue.isShutdown()/isDraining()/isDrained()` | `shutdown()` / `draining()` / `drained()` |
| `SmartBlockingQueue.getCapacity()` / `VariableLinkedBlockingQueue.getCapacity()` | `capacity()` |
| `ActionGate.isDue()` | `due()` |

Methods implementing JDK or third-party contracts keep their mandated names
(`Monitor.Guard.isSatisfied()`, `ExecutorService.isShutdown()/isTerminated()`,
`Thread.getState()`, `Map.Entry.getKey()/getValue()`).

The old `ParConfig`, `ParOptions`, `ExecutorResolver`, `GlobalParConfig`, and legacy `Par` entry points are not compatibility aliases. Update imports, construction, and method calls together. Registered executors remain borrowed and are still owned and shut down by the application.

## Cancellation token changes

`CancellationToken.lateBind(futures, timeout, submitCanceller, timer)` is now
`CancellationToken.bind(futures, submitCanceller, timer)`. The timeout argument moved into the
token itself: construct it with `new CancellationToken(parent, deadlineNanos)` (the effective
deadline is the minimum of the requested one and the parent's) and use `deadlineNanos()` /
`remaining()` to read it. Batches and task groups compute and pass the deadline at construction;
self-service callers of `bind` should do the same. A deadline that has already expired when
`bind` runs simply schedules the timeout for immediate execution. `State.NO_OP` was deleted, and
`addCompletionListener` was replaced by `addStateListener(Consumer<State>)`, which fires
synchronously after a state transition commits and before the associated cancellation actions run.

Batch-level element cancellation no longer surfaces as a bare cancellation: the token still
classifies a directly cancelled element through the same fail-fast trigger that a failed element
uses, so batch reports keep distinguishing the cancelled element via `TaskOutcome`.

Task groups changed semantics accordingly: cancelling one member (its future or its token) now
cascades to the whole group, matching batch fail-fast behavior. The directly cancelled member
reports `MEMBER_CANCELED`, unfinished siblings report `GROUP_CANCELED`, and the group converges
on `CANCELED`.
