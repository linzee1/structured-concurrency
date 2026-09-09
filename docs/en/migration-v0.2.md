# Migrating to v0.2

Version `0.2.0` replaces the mutable configuration-and-resolver API with an immutable execution topology. This is a source-breaking migration.

The publishing identity also moves because the GitHub account was renamed `huatalk` → `monadrome`: `io.github.huatalk:parallel-in-scope` becomes `io.github.monadrome:parallel-in-scope`, and the root Java package `io.github.huatalk.parallelinscope` becomes `io.github.monadrome.parallelinscope`. Update dependency coordinates, imports, `package` declarations, and service-loading names. `0.1.0` stays published under the old coordinates on Maven Central.

| `0.1.x` | `0.2.0` |
|---|---|
| `ParConfig.builder().executor(name, executor)` | `GlobalPar.builder().register(ParName.of(name), executor)` |
| `new Par(config)` | `global.par(ParName.of(name))` |
| `ParOptions` | `MultiTaskOptions` |
| `par.map(name, items, fn, options)` | `par.map(items, fn, options)` |
| `ParConfig` timeout/listener defaults | `GlobalPar.Builder.taskListener(...)` (timeouts stay per-call) |
| `ParConfig` livelock settings | `GlobalParDeadlockPolicy` |
| `ParConfig` purge settings | `GlobalParPurgePolicy` |
| executor-name resolution at call time | executor binding at `GlobalPar` build time |
| `TaskGraph.destroyAfterRequest(config)` | `global.openTaskGraphObservation()` scope |

The new split is intentional: `MultiTaskOptions` is caller input, while `MultiTaskContext` is per-batch runtime state. Cancellation, deadline, and executor identity flow through parent-child batch contexts, including nested calls across named `Par` entries.

Executor lookup keys are now the value type `ParName` instead of bare `String`. Every place that named a `Par` takes a `ParName`: `GlobalPar.Builder.register` / `defaultPar` / `parTaskListener`, `GlobalPar.par` / `find` / `taskListenersFor`, `GlobalPar.pars()`, and `TaskGroupDefinition.Builder.task` / `combine`. Construction validates once (never null, never blank) and the value is used verbatim — no trimming or lower-casing — so existing keys keep their exact meaning. `ParName` is a logical lookup key, not a resource identity: it must never replace `ExecutorIdentity`, which stays the reference-equality key for deadlock detection and purge. A well-formed `ParName` still says nothing about whether the name is registered; unknown names are rejected at `GlobalPar.Builder.build()` and `TaskGroup.submit` exactly as before.

Earlier `0.2.x` snapshots named this type `ExecutionOptions` and then `BatchExecutionOptions`. Rename imports, variable declarations, and `Par.map` arguments to `MultiTaskOptions`; no compatibility alias is retained during the `0.x` phase.

The per-invocation runtime context was renamed for the same reason: the former `BatchExecutionContext` is now `MultiTaskContext`, because it backs both `Par.map` batches and task-group members. Update imports and `resolve(...)` call sites. Its batch-biased accessors were then neutralized for the same dual role: `batchId()` → `unitId()`, `taskName()` → `name()`, `parLabel()` → `executorLabel()`, and `parent()` → `structuralParent()` (a group member's cancellation parent is the group token carried inside `cancellationToken()`, not this field). `SubmissionScope.currentBatch()` becomes `current()`. `TaskContext` (whose `batchContext()` was briefly renamed `multiTaskContext()` during the 0.2 cycle) was ultimately removed rather than renamed: its read-only content is flattened into the unified `TaskCompletion` record, as described below.

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
value means inherit. The former global default timeout is removed so no silent global
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

The completed-task record is unified into a single class, `io.github.monadrome.parallelinscope.scope.TaskCompletion`: a `TaskListener` receives it at task completion, and `TaskGroupResult.members()` embeds one per member as its terminal snapshot. It replaces both the old `TaskListener.TaskEvent` and `TaskGroupMemberResult`, and exposes the task's identity and timing as flat fields — `taskName()`, `unitId()`, `taskIndex()`, `submitTimeNanos()`, `startTimeNanos()`, `endTimeNanos()` — plus `outcome()`, `successful()`, `result()`, `failure()`, and `enqueued()` (now derived from the queue wait). The read-only `TaskContext` view was removed; the engine plumbing previously reachable through `TaskContext.multiTaskContext()` (cancellation token, deadline, structural parent) is no longer part of the listener/result surface. `result()` is only non-null on listener delivery of a successful task — a group member's result stays in its future — and `taskIndex()` is always zero for group members. A successful task may return null, so use `successful()` rather than testing the result for null. Listener callbacks run outside the completed task's dynamic execution scope; use the event instead of `TaskExecutionContext.current()`.

Task outcome classification is unified into a single enum, `TaskOutcome`, replacing both
`io.github.monadrome.parallelinscope.internal.FutureState` and
`io.github.monadrome.parallelinscope.scope.TaskGroupMemberReason`. `TaskOutcome` adds `RUNNING` to the
former member-reason values so it serves both batch reports and group member results. Mapping from
the removed enums: `FutureState.FAILED` → `TaskOutcome.USER_FAILURE`, `FutureState.CANCELLED` →
`TaskOutcome.MEMBER_CANCELED`, and `TaskGroupMemberReason.X` → `TaskOutcome.X` (same names).
Consequently `TaskBatchResult.BatchReport.stateCounts()` is now keyed by `TaskOutcome`, and
each group member's terminal snapshot (`TaskGroupResult.members()` values, of the unified
`TaskCompletion` type) exposes its member outcome as `outcome()` returning `TaskOutcome` (renamed
from the earlier `completionReason()`).

Accessors converge on the bare `x()` style; no `getX()`/`isX()` forms remain in the public API or
internals. Earlier `0.2.0-SNAPSHOT` builds used bean-style names; rename call sites mechanically:

| Earlier snapshot | `0.2.0` |
|---|---|
| `Par.getGlobalPar()` | `Par.globalPar()` |
| `Par.getDisplayName()` | `Par.name()` (now returns `ParName`; call `name().value()` for the string) |
| `AsyncBatchResult.getSubmitCanceller()` | `TaskBatchResult.submitCanceller()` |
| `AsyncBatchResult.getResults()` | `TaskBatchResult.results()` |
| `AsyncBatchResult.BatchReport.getStateCounts()` | `BatchReport.stateCounts()` |
| `AsyncBatchResult.BatchReport.getFirstException()` | `BatchReport.firstException()` |
| `GlobalPar.isClosed()/isShutdown()/isTerminated()` | `GlobalPar.closed()/shutdown()/terminated()` |
| `CancellationToken.getState()` / `State.getCode()` | `state()`; `code()` is removed — interruption semantics are expressed by the enum values themselves |
| `TaskGroupMemberResult.completionReason()` | `TaskCompletion.outcome()` (member snapshots are now `TaskGroupResult.members()` values of type `TaskCompletion`) |
| `TaskGroupMemberResult.taskContext()` | removed; timing flattened to `TaskCompletion.submitTimeNanos()` / `startTimeNanos()` / `endTimeNanos()` |
| `TaskEvent.getTaskContext()/getTaskName()` | `TaskListener` now delivers `TaskCompletion`; `taskContext()` removed (flattened to `taskName()` / `unitId()` / `taskIndex()` + timing nanos) |
| `TaskEvent.getSubmitTimeNanos()/getStartTimeNanos()/getEndTimeNanos()` | `TaskCompletion.submitTimeNanos()` / `startTimeNanos()` / `endTimeNanos()` |
| `TaskEvent.isSuccessful()/getResult()/isEnqueued()/getException()` | `TaskCompletion.successful()` / `result()` / `enqueued()` / `failure()` |
| `ScopedCallable.getTaskExecutionContext()/getCancellationToken()/getExecutorName()` | `taskExecutionContext()` / `cancellationToken()` / `executorName()` |
| `TaskGraphData.getGraph()/getExecutorGraph()` | `graph()` / `executorGraph()` |
| `TaskGraphData.isTaskCycle()/isSelfLoop()/isExecutorCycle()/isExecutorSelfLoop()` | `taskCycle()` / `selfLoop()` / `executorCycle()` / `executorSelfLoop()` |
| `TaskEdge.getParallelism()/getTaskType()/getTaskCount()/getTimeoutMillis()` | `parallelism()` / `taskType()` / `taskCount()` / `timeout()` (now returns `Duration`; call `toMillis()` yourself if needed) |
| `TaskEdge.getExecutorName()/getSourceExecutorName()` | `executorName()` / `sourceExecutorName()` |
| `TaskEdge.getExecutorIdentity()/getSourceExecutorIdentity()` | `executorIdentity()` / `sourceExecutorIdentity()` |
| `TaskEdge.isExecutorDeadlockProne()` | `executorDeadlockProne()` |
| `DeadlockDetectionListener.getTaskEdges()/getExecutorEdges()` | `taskEdges()` / `executorEdges()` |
| `TaskGraphObservationScope.isClosed()` | `closed()` |
| `MultiTaskContext.taskGraphObservationContext()` | `MultiTaskContext.taskGraphObservationScope()` |
| `MultiTaskContext.batchId()/taskName()/parLabel()/parent()` | `unitId()` / `name()` / `executorLabel()` / `structuralParent()` |
| `TaskContext.batchContext()` / `SubmissionScope.currentBatch()` | `TaskContext` removed (content flattened into `TaskCompletion`) / `SubmissionScope.current()` |
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
uses, and batch reports now attribute cancelled elements from the batch token's committed state
(`Par.map` results always carry it): `TIMEOUT` for deadline expiry, `FAIL_FAST` for the cascade
after a sibling failure, `GROUP_CANCELED` for batch-level or propagated cancellation, and
`MEMBER_CANCELED` when no framework path committed. Because the batch shares one token across
elements, the element whose direct cancellation triggered the cascade also reads `FAIL_FAST`;
per-element initiator attribution requires a task group. Results constructed via
`TaskBatchResult.of(...)` without a token keep the coarse view: every cancellation reads
`MEMBER_CANCELED`.

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
`PROPAGATING_CANCELED` → `PROPAGATED_CANCELED`. `RUNNING` and `SUCCESS` are unchanged. `code()` is
removed (the integer encoding was an implementation detail with no consumers); the
`shouldInterruptCurrentThread()` semantics are unchanged and now read as a direct enum comparison.

`ExecutionPhase.CANCELLED_BEFORE_RUN` is respelled `CANCELED_BEFORE_RUN` to match the single-L
`CANCELED` spelling used across the library.

`GlobalExecutionPolicy` is removed: its only content was the `TaskListener` list, so listeners are
now registered directly on `GlobalPar.Builder`. `GlobalExecutionPolicy.builder().taskListener(l).build()`
passed to `executionPolicy(policy)` becomes `taskListener(l)` on the `GlobalPar` builder, and a
per-Par override `parPolicyOverride(name, policy)` becomes one `parTaskListener(ParName.of(name), l)` call per
listener — repeated calls for the same name append instead of failing, and the override still
replaces the default list for that entry. The `GlobalPar.executionPolicy()` /
`executionPolicyFor(name)` accessors are replaced by `taskListeners()` / `taskListenersFor(ParName)`.

`AsyncBatchResult` is renamed to `TaskBatchResult` (its nested `BatchReport` keeps its name): the
result of a batch of tasks, not an async-specific construct. The internal
`ConcurrentLimitExecutor` is renamed to `SlidingWindowSubmitter`, matching what it actually does —
submitting a sliding window of prepared tasks. `TaskGraphObservationContext` is renamed to
`TaskGraphObservationScope`: it is a closeable observation scope, and the naming rule is now that
`Scope` marks lifecycle scopes while `Context` marks data carriers. The
`GlobalPar.openTaskGraphObservation()` entry point keeps its name.
