# Migrating to v0.2

Version `0.2.0` replaces the mutable configuration-and-resolver API with an immutable execution topology. This is a source-breaking migration.

The publishing identity also moves because the GitHub account was renamed `huatalk` → `monadrome`: `io.github.huatalk:parallel-in-scope` becomes `io.github.monadrome:parallel-in-scope`, and the root Java package `io.github.huatalk.parallelinscope` becomes `io.github.monadrome.parallelinscope`. Update dependency coordinates, imports, `package` declarations, and service-loading names. `0.1.0` stays published under the old coordinates on Maven Central.

The final `0.2.0` package layout also consolidates all public API and callback types into
`io.github.monadrome.parallelinscope`. Replace imports from the earlier `.scope`, `.cancel`,
`.context`, and `.spi` packages with the root package. `SmartBlockingQueue` likewise moves from
`.queue` to the root package. Only `DrainingBlockingQueue` and `VariableLinkedBlockingQueue` remain
in `io.github.monadrome.parallelinscope.queue`. The former `.internal` and `.control` packages and
all implementation-only context, graph, submission, purge, and execution-phase types are no longer
public; no compatibility shims are retained during the `0.x` phase.

| `0.1.x` | `0.2.0` |
|---|---|
| `ParConfig.builder().executor(name, executor)` | `GlobalPar.builder().register(ParName.of(name), executor)` |
| `new Par(config)` | `global.par(ParName.of(name))` |
| `ParOptions` | `BatchOptions` (batch) / `TaskGroupOptions` (group) / `TaskOptions` (member and combine) |
| `par.map(name, items, fn, options)` | `par.map(items, fn, options)` |
| `ParConfig` timeout/listener defaults | `GlobalPar.Builder.taskListener(...)` (timeouts stay per-call) |
| `ParConfig` livelock settings | `GlobalParDeadlockPolicy` |
| `ParConfig` purge settings | `GlobalParPurgePolicy` |
| executor-name resolution at call time | executor binding at `GlobalPar` build time |
| `TaskGraph.destroyAfterRequest(config)` | `global.openTaskGraphObservation()` scope |

The public/runtime split is intentional: the option types are caller input, while the package-private
`MultiTaskContext` is per-invocation runtime state. Cancellation, deadline, and executor identity flow
through parent-child contexts, including nested calls across named `Par` entries. Applications should
configure these semantics through the option types and must not construct or cache runtime contexts.

Executor lookup keys are now the value type `ParName` instead of bare `String`. Every place that named a `Par` takes a `ParName`: `GlobalPar.Builder.register` / `defaultPar` / `parTaskListener`, `GlobalPar.par` / `find` / `taskListenersFor`, `GlobalPar.pars()`, and `TaskGroupDefinition.Builder.task` / `buildWithCombiner`. Construction validates once (never null, never blank) and the value is used verbatim — no trimming or lower-casing — so existing keys keep their exact meaning. `ParName` is a logical lookup key, not a resource identity: it must never replace `ExecutorIdentity`, which stays the reference-equality key for deadlock detection and purge. A well-formed `ParName` still says nothing about whether the name is registered; unknown names are rejected at `GlobalPar.Builder.build()` and `TaskGroup.submit` exactly as before.

Earlier `0.2.x` snapshots named the batch option type `ExecutionOptions`, then `BatchExecutionOptions`, and briefly unified every role into the superset type `MultiTaskOptions`. The final API splits it by scope into three types: `BatchOptions` for `Par.map`, `TaskGroupOptions` for `TaskGroupDefinition.builder`, and `TaskOptions` for `TaskGroupDefinition.Builder.task` / `buildWithCombiner`. Rename imports, variable declarations, and arguments to the type of their scope; no compatibility alias is retained during the `0.x` phase.

Earlier `0.2.x` snapshots renamed the internal `BatchExecutionContext` to `MultiTaskContext` because it
backs both `Par.map` batches and task-group members. The final API hides that runtime context and its
`resolve(...)` methods completely. Remove any imports or direct construction of `MultiTaskContext`,
`SubmissionScope`, and `TaskExecutionContext`; configure calls with the option type of the scope and observe
completed work through `TaskCompletion`. The former read-only `TaskContext` view was removed, with its
publicly useful identity and timing fields flattened into `TaskCompletion` as described below.

Option types map one-to-one onto scopes, and each type declares only the fields its consumer reads.
`BatchOptions` declares name/parallelism/timeout/taskType/rejectEnqueue; `TaskGroupOptions` declares
only name/timeout/listeners; `TaskOptions` declares only timeout/taskType/rejectEnqueue. Identity is
not an option — a member's name comes from its `TaskKey` — and concurrency belongs to fan-out, so a
single task has nothing to limit. The `taskName()`/`groupName()` accessors and the matching builder
methods converge on `name()`; the `TaskGroupDefinition.Builder.task` / `buildWithCombiner` arguments narrow
from the superset type to `TaskOptions`.

The timeout is now a forced explicit choice between two mutually exclusive static factories:
`timeout(name, Duration)` sets an explicit positive timeout, and `inheritTimeout(name)` declares
that the enclosing scope's deadline is inherited. There is no builder and no third state: omitting
the choice is a compile error rather than a runtime one, and both declarations cannot coexist. The
`TaskOptions` factories take no name. The accessor changed from `Duration timeout()` to `Optional<Duration> timeout()`; an empty
value means inherit. The former global default timeout is removed so no silent global
default remains. Deadline resolution is now entirely owned by the execution kernel.

Deadline resolution follows one uniform rule. An explicit timeout resolves to the earlier of its
own bound and the enclosing hard deadline. An inherited timeout resolves to the enclosing deadline:
for a `Par.map` batch or a task group that is the deadline of the enclosing scoped task, and for a
group member it is the group deadline. Inheriting with no enclosing deadline is rejected at the
entry point: a top-level `Par.map` and a top-level `TaskGroup.submit` both throw
`IllegalArgumentException` telling you to call `timeout(Duration)`.

Earlier snapshots also exposed this detector as `GlobalParLivelockPolicy` and `LivelockListener`. Rename them to `GlobalParDeadlockPolicy` and `DeadlockDetectionListener`; the detector reports potential dependency-graph deadlocks and does not prove a runtime deadlock or detect livelock.

The task-group API now centers on an immutable, reusable definition. Replace the earlier builder
ceremony — `GlobalPar.taskGroupBuilder(options)`, `ParallelTaskGroup.Builder.addTask(name, par,
callable, options)`, the one-shot `buildAndSubmitAll()`, and `ParallelTaskGroup.TaskHandle<T>` —
with `TaskGroupDefinition.builder(groupOptions)`,
`TaskGroupDefinition.Builder.task(key, executorName, callable, options)`, the one-shot
`TaskGroup.submit(global, definition)`, and `TaskKey<T>`.
Members reference their executor by registered name instead of a `Par` object. A `TaskKey<T>` is
created by the caller as an anonymous subclass — `new TaskKey<List<Order>>("orders") {}` — so the
key carries the member name and captures the result type at runtime; pass it to `task()`, and
after submission resolve the member's future with `group.future(key)`, which rejects a key whose
raw result type does not cover the registered one. A spec captures no thread context, so the
structural parent and
observation scope are resolved from the submitting thread at each `submit` call, and one definition may
be submitted repeatedly. The group entry class itself was renamed from `ParallelTaskGroup` to
`TaskGroup`, joining its `TaskGroupDefinition`/`TaskGroupResult`/`TaskGroupListener` family. There is no
compatibility shim because the earlier builder API was not released as a stable contract.

The completed-task record is unified into a single class,
`io.github.monadrome.parallelinscope.TaskCompletion`: a `TaskListener` receives it at task completion,
and `TaskGroupResult.members()` embeds one per member as its terminal snapshot. It replaces both the
old `TaskListener.TaskEvent` and `TaskGroupMemberResult`, and exposes the task's identity and timing as
flat fields — `taskName()`, `unitId()`, `taskIndex()`, `submitTimeNanos()`, `startTimeNanos()`,
`endTimeNanos()` — plus `outcome()`, `successful()`, `result()`, `failure()`, and `enqueued()` (now
derived from the queue wait). The read-only `TaskContext` view was removed; engine plumbing such as
the cancellation token, deadline, and structural parent is no longer part of the listener/result
surface. `result()` is only non-null on listener delivery of a successful task — a group member's
result stays in its future — and `taskIndex()` is always zero for group members. A successful task may
return null, so use `successful()` rather than testing the result for null. Listener callbacks run
outside the completed task's dynamic execution scope; use the event as the observation boundary.

Task outcome classification is unified into a single enum, `TaskOutcome`, replacing both
the earlier internal `FutureState` and `TaskGroupMemberReason`. `TaskOutcome` adds `RUNNING` to the
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
| `DeadlockDetectionListener.getTaskEdges()/getExecutorEdges()` | `taskEdges()` / `executorEdges()` |
| `TaskGraphObservationScope.isClosed()` | `closed()` |
| `DrainingBlockingQueue.isShutdown()/isDraining()/isDrained()` | `shutdown()` / `draining()` / `drained()` |
| `SmartBlockingQueue.getCapacity()` / `VariableLinkedBlockingQueue.getCapacity()` | `capacity()` |

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
`bind` runs simply schedules the timeout for immediate execution. `State.NO_OP` was deleted.
`addCompletionListener` has no public replacement; state-transition listeners are now execution-kernel
machinery.

Batch-level element cancellation no longer surfaces as a bare cancellation: the token still
classifies a directly cancelled element through the same fail-fast trigger that a failed element
uses, and batch reports now attribute cancelled elements from the batch token's committed state
(`Par.map` results always carry it): `TIMEOUT` for deadline expiry, `FAIL_FAST` for the cascade
after a sibling failure, `GROUP_CANCELED` for batch-level or propagated cancellation, and
`MEMBER_CANCELED` when no framework path committed. Because the batch shares one token across
elements, the element whose direct cancellation triggered the cascade also reads `FAIL_FAST`;
per-element initiator attribution requires a task group. `TaskBatchResult` instances are constructed
by the execution API; its former public `of(...)` factories are now package-private.

Task groups changed semantics accordingly: cancelling one member (its future or its token) now
cascades to the whole group, matching batch fail-fast behavior. The directly cancelled member
reports `MEMBER_CANCELED`, unfinished siblings report `GROUP_CANCELED`, and the group converges
on `GROUP_CANCELED`.

## Terminal vocabulary unification

`TaskGroupCompletionReason` is removed; the group-level result reuses `TaskOutcome`.
`TaskGroupResult.completionReason()` is renamed to `outcome()` and now returns `TaskOutcome`.
Mapping from the removed enum: `SUCCESS` → `TaskOutcome.SUCCESS`; `TIMEOUT` → `TaskOutcome.TIMEOUT`;
`FAILED` → the failed task's own outcome (`USER_FAILURE` or `SUBMISSION_FAILURE`, see
`failedTaskName()`); `CANCELED` → `GROUP_CANCELED` when the group was canceled as a whole or the
cancellation propagated from an enclosing scope, and `MEMBER_CANCELED` when the cancellation
originated from a member.

A task-group member or terminal combine now takes its execution-time diagnostic name from its
`TaskKey`, rather than from an option name. This aligns checkpoints, task-listener events, and
task-graph labels with the name used to retrieve the future and result snapshot. Member and combine
options are `TaskOptions`, which has no name field to be ignored. Because the failure source may also be the terminal combine,
`TaskGroupResult.failedMemberName()` is renamed to `failedTaskName()`.

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
