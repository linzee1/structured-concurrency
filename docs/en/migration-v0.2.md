# Migrating to v0.2

Version `0.2.0` replaces the mutable configuration-and-resolver API with an immutable execution topology. This is a source-breaking migration.

| `0.1.x` | `0.2.0` |
|---|---|
| `ParConfig.builder().executor(name, executor)` | `GlobalPar.builder().register(name, executor)` |
| `new Par(config)` | `global.par(name)` |
| `ParOptions` | `MultiTaskOptions` |
| `par.map(name, items, fn, options)` | `par.map(items, fn, options)` |
| `ParConfig` timeout/listener defaults | `GlobalExecutionPolicy` |
| `ParConfig` livelock settings | `GlobalParDeadlockPolicy` |
| `ParConfig` purge settings | `GlobalParPurgePolicy` |
| executor-name resolution at call time | executor binding at `GlobalPar` build time |
| `TaskGraph.destroyAfterRequest(config)` | `global.openTaskGraphObservation()` scope |

The new split is intentional: `MultiTaskOptions` is caller input, while `MultiTaskContext` is per-batch runtime state. Cancellation, deadline, and executor identity flow through parent-child batch contexts, including nested calls across named `Par` entries.

Earlier `0.2.x` snapshots named this type `ExecutionOptions` and then `BatchExecutionOptions`. Rename imports, variable declarations, and `Par.map` arguments to `MultiTaskOptions`; no compatibility alias is retained during the `0.x` phase.

The per-invocation runtime context was renamed for the same reason: the former `BatchExecutionContext` is now `MultiTaskContext`, because it backs both `Par.map` batches and task-group members. Update imports and `resolve(...)` call sites; accessors such as `batchId()` and `batchContext()` keep their names.

Batch and task-group option types are unified into the single `MultiTaskOptions`; the earlier
`BatchExecutionOptions` and `TaskGroupOptions` types are removed. The `taskName()`/`groupName()`
accessors and the matching builder methods converge on `name()`; every other builder method keeps
its name. A batch reads name/parallelism/timeout/taskType/rejectEnqueue; a group reads
name/timeout/listeners, with member execution strategy supplied per `TaskGroupSpec.Builder.task`
call.

`MultiTaskOptions.timeout` is a forced explicit choice between two mutually exclusive builder
declarations: `timeout(Duration)` sets an explicit positive timeout, and `inheritTimeout()`
declares that the enclosing scope's deadline is inherited. `build()` rejects a builder that
declares neither (`IllegalArgumentException`: call `timeout(Duration)` or `inheritTimeout()`) or
both. The accessor changed from `Duration timeout()` to `Optional<Duration> timeout()`; an empty
value means inherit. `GlobalExecutionPolicy.defaultTimeoutMillis` is removed so no silent global
default remains. `MultiTaskContext.resolve` consequently no longer takes the policy; drop
that argument.

Deadline resolution follows one uniform rule. An explicit timeout resolves to the earlier of its
own bound and the enclosing hard deadline. An inherited timeout resolves to the enclosing deadline:
for a `Par.map` batch or a task group that is the deadline of the enclosing scoped task, and for a
group member it is the group deadline. Inheriting with no enclosing deadline is rejected at the
entry point: a top-level `Par.map` and a top-level `TaskGroup.submit` both throw
`IllegalArgumentException` telling you to call `timeout(Duration)`.

Earlier snapshots also exposed this detector as `GlobalParLivelockPolicy` and `LivelockListener`. Rename them to `GlobalParDeadlockPolicy` and `DeadlockDetectionListener`; the detector reports potential dependency-graph deadlocks and does not prove a runtime deadlock or detect livelock.

The task-group API now centers on an immutable, reusable spec. Replace the earlier builder
ceremony — `GlobalPar.taskGroupBuilder(options)`, `ParallelTaskGroup.Builder.addTask(name, par,
callable, options)`, the one-shot `buildAndSubmitAll()`, and `ParallelTaskGroup.TaskHandle<T>` —
with `TaskGroupSpec.builder(groupOptions)`, `TaskGroupSpec.Builder.task(ref, executorName,
callable, options)`, the one-shot `TaskGroup.submit(global, spec)`, and `TaskRef<T>`.
Members reference their executor by registered name instead of a `Par` object. A `TaskRef<T>` is
created by the caller as an anonymous subclass — `new TaskRef<List<Order>>("orders") {}` — so the
token carries the member name and captures the result type at runtime; pass it to `task()`, and
after submission resolve the member's future with `group.future(ref)`, which rejects a token whose
raw result type does not cover the registered one. A spec captures no thread context, so the
structural parent and
observation scope are resolved from the submitting thread at each `submit` call, and one spec may
be submitted repeatedly. The group entry class itself was renamed from `ParallelTaskGroup` to
`TaskGroup`, joining its `TaskGroupSpec`/`TaskGroupResult`/`TaskGroupListener` family. There is no
compatibility shim because the earlier builder API was not released as a stable contract.

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
`TaskGroupMemberResult` exposes its member outcome as `outcome()` returning `TaskOutcome` (renamed
from the earlier `completionReason()`).

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
| `TaskGroupMemberResult.completionReason()` | `TaskGroupMemberResult.outcome()` |
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
on `GROUP_CANCELED`.

## Terminal vocabulary unification

`TaskGroupCompletionReason` is removed; the group-level result reuses `TaskOutcome`.
`TaskGroupResult.completionReason()` is renamed to `outcome()` and now returns `TaskOutcome`.
Mapping from the removed enum: `SUCCESS` → `TaskOutcome.SUCCESS`; `TIMEOUT` → `TaskOutcome.TIMEOUT`;
`FAILED` → the failed member's own outcome (`USER_FAILURE` or `SUBMISSION_FAILURE`, see
`failedMemberName()`); `CANCELED` → `GROUP_CANCELED` when the group was canceled as a whole or the
cancellation propagated from an enclosing scope, and `MEMBER_CANCELED` when the cancellation
originated from a member.

`CancellationToken.State` values are renamed onto the same vocabulary: `FAIL_FAST_CANCELED` →
`FAIL_FAST`, `TIMEOUT_CANCELED` → `TIMEOUT`, `MUTUAL_CANCELED` → `CANCELED`, and
`PROPAGATING_CANCELED` → `PROPAGATED_CANCELED`. `RUNNING` and `SUCCESS` are unchanged, and the
`code()` values and `shouldInterruptCurrentThread()` semantics are unchanged.

`ExecutionPhase.CANCELLED_BEFORE_RUN` is respelled `CANCELED_BEFORE_RUN` to match the single-L
`CANCELED` spelling used across the library.
