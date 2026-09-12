# Changelog

## [Unreleased]

### Breaking changes

- `Checkpoints.checkpoint(String, boolean)` no longer fails open: a name that does not match the current scoped task — or a call outside any scoped task — throws `IllegalStateException` instead of silently skipping the cancellation check. The new no-argument `Checkpoints.checkpoint()` is the primary form and needs no name.
- `TaskGroup.future(TaskKey)` and `CompletedTaskValues.value(TaskKey)` now compare full generic types via `TypeToken.isSupertypeOf`: a key claiming `List<Integer>` no longer resolves a member registered as `List<String>` (previously accepted on raw types and failing later with a `ClassCastException` in user code).
- `CancellationToken` is now `final` and its `bind(...)` is package-private: the token carries the library's attribution truth, so subclassing and external binding are no longer possible.
- `Task` is package-private; the public contract is `TaskFuture` only.
- `Par.map` now takes any `Collection` of elements instead of only `List` (non-`List` inputs are snapshotted on entry).
- `TaskBatchResult.BatchReport.stateCounts()` is no longer `@Nullable` (the `null` case was unreachable), and the `BatchReport` constructor is package-private.
- `GlobalPar.installGlobal` is symmetric with the instance lifecycle: `close()` on the installed instance releases the global slot so a restarted context may install again.

### Features

- Add `Par.submit(String, Callable, TaskOptions)`: a unary scoped-task entry point with deadline, TTL propagation, listener notification, and structured cancellation, returning a `TaskFuture`.
- Add `TaskBatchResult.valuesOrThrow()` — the "run these, give me the results, fail if any failed" path in one call — and `TaskGroupResult.orThrow()`, so a group whose member threw can no longer be read as successful by a caller that forgets to inspect `outcome()`.
- Add `TaskGroupResult.outcomeCounts()` and `reportString()`, symmetric with the batch report.
- Add `GlobalPar.awaitQuiescence(Duration)` and `inFlight()` so application shutdown hooks can join a closing topology.
- Checkpoints now treat an expired deadline as cancelled on the wall clock: an expired token commits `TIMEOUT` at the next checkpoint even when the timer thread has not run yet, so deadline enforcement no longer depends on scheduling punctuality.
- Warn once at `GlobalPar.Builder.build()` when a registered executor is not a `ThreadPoolExecutor` the library can see through (e.g. a pre-wrapped `listeningDecorator`), since queue purge and blocking-risk detection are silently disabled for it; `rejectEnqueue` javadoc now states it is honoured only by `SmartBlockingQueue`-backed pools.

### Performance

- Share one completion aggregate between `CancellationToken.bind` and batch retention instead of building two, and skip task-graph bookkeeping entirely when no observation scope is active.
- Make the task-group join test O(1) via a success counter, memoize the derived executor graphs, and replace per-unit `SecureRandom` UUID generation with process-local sequence ids.

### Fixes

- Make `VariableLinkedBlockingQueue.drainTo` exception-safe: a target that throws mid-drain no longer loses already-unlinked elements or drifts the element count, which previously corrupted the queue permanently (later `poll`/`take` threw `NullPointerException`).
- Wake producers parked on an over-capacity `VariableLinkedBlockingQueue` after a capacity shrink followed by `clear()`/`drainTo`: the not-full signal now fires on the `count >= capacity` crossing instead of only at exact equality.
- Fix a sliding-window race in which a batch element already handed to the executor could be reported as `SUBMISSION_FAILURE`: the index is now claimed before the executor handoff so it is never abandoned afterwards, and a placeholder terminated by a racing cancellation now cancels the real future it fails to bind.
- Commit `TIMEOUT` synchronously in `CancellationToken.bind` when the deadline has already expired, instead of scheduling a zero-delay timer: members of such a group are canceled before the submission loop, so they no longer enter user code and the group no longer risks reporting `SUCCESS`.
- Make a task group's outcome independent of completion order: a recorded member or terminal-combine failure now takes precedence over a still-`RUNNING`/`SUCCESS` group token, so a lone or last-completing failure reports `USER_FAILURE`/`SUBMISSION_FAILURE` instead of `MEMBER_CANCELED`.

## [0.2.0] - 2026-09-10

### Breaking changes

- Replace `ParConfig`, `ParOptions`, `ExecutorResolver`, and legacy `Par` entry points with immutable `GlobalPar`, executor-bound `Par`, and per-batch options. The per-batch type is finally named `BatchOptions`; the intermediate `ExecutionOptions` and `BatchExecutionOptions` names from earlier `0.2.0-SNAPSHOT` builds do not survive.
- Make `BatchExecutionContext` the source of task scope state, including cancellation, deadlines, nested batches, and executor identity.
- Consolidate the public API and callback types into the root `io.github.monadrome.parallelinscope`
  package. Cancellation, context, graph, scheduling, and maintenance implementation types now share
  that package as package-private kernel code; only the independent `DrainingBlockingQueue` and
  `VariableLinkedBlockingQueue` remain in `.queue`. `SmartBlockingQueue` moves to the root package.
  The former `.scope`, `.cancel`, `.context`, `.internal`, `.spi`, and `.control` packages are
  removed without compatibility shims.
- Hide implementation-only API that was public solely for cross-package access, including runtime
  contexts, task graph data, execution-phase futures, submission helpers, purge machinery, and
  internal `CancellationToken` transitions. `ActionGate` is retained as package-private pending a
  separate removal decision.
- Rename `TaskGroupSpec` to `TaskGroupDefinition`, its nested `MemberSpec` to `TaskDefinition`, and `members()` to `tasks()`. These objects record reusable task definitions rather than specifications for execution.
- Move the Maven coordinates and the root Java package from the account's former name to its current one after the GitHub account rename `huatalk` → `monadrome`: `io.github.huatalk:parallel-in-scope` → `io.github.monadrome:parallel-in-scope`, and `io.github.huatalk.parallelinscope` → `io.github.monadrome.parallelinscope` for imports, `package` declarations, and service loading. `0.1.0` remains published under the old coordinates on Maven Central; the `0.1.0` section below keeps the historical coordinate.
- Rename `GlobalParLivelockPolicy` to `GlobalParDeadlockPolicy` and `LivelockListener` to `DeadlockDetectionListener`; the graph reports potential deadlock structures, not runtime livelock.
- Replace the abrupt-close `ClosableBlockingQueue` (recovery lists, `remainingList()`) with `DrainingBlockingQueue`: `close()` rejects producers while consumers keep draining queued elements until the `DRAINED` terminal state. No custom shutdown exception types are introduced: write rejections throw `IllegalStateException`, drained reads throw `NoSuchElementException`.
- Merge `TaskGraph` into `TaskGraphObservationScope`: the observation scope is now a request-level `TransmittableThreadLocal` global (identity-propagated to worker threads), owns the graph lifecycle (`install`/`restore`/`data`/`logTaskPair`/`hasXxx` statics), and runs deadlock detection in `close()`. The former `TaskGraph.Data` is now the top-level `TaskGraphData`; `previousData()` and `complete()` are removed.
- Make the typed key an abstract class whose anonymous subclass captures the member result type at runtime (Guava `TypeToken` style: `new TaskKey<List<Order>>("orders") {}`). The type is named `TaskKey` rather than `TaskRef`: a key identifies a definition slot and compares equal by its name alone, so a key claiming a supertype of the registered type is equal to the registered key. The key names a slot that may be a member or the terminal combine, so `TaskKey.memberName()`, `TaskDefinition.memberName()` and `CombineDefinition.memberName()` become `name()`. `TaskGroupDefinition.Builder.task` takes the key as its first argument and no longer takes `memberName`; `TaskDefinition.ref()` and `CombineDefinition.ref()` are renamed to `key()`; `TaskGroup.future(key)` rejects a key whose raw result type does not cover the type the member was registered with.
- Remove `TaskGroupCompletionReason`; `TaskGroupResult.completionReason()` becomes `outcome()` returning `TaskOutcome`. A fail-fast group now reports the failed member's own outcome (`USER_FAILURE`/`SUBMISSION_FAILURE`) instead of `FAILED`, and group-wide cancellation reports `GROUP_CANCELED`.
- Rename `CancellationToken.State` values onto the `TaskOutcome` vocabulary: `FAIL_FAST_CANCELED` → `FAIL_FAST`, `TIMEOUT_CANCELED` → `TIMEOUT`, `MUTUAL_CANCELED` → `CANCELED`, `PROPAGATING_CANCELED` → `PROPAGATED_CANCELED`. `code()` values and `shouldInterruptCurrentThread()` semantics are unchanged.
- Fix the `ExecutionPhase.CANCELLED_BEFORE_RUN` spelling to `CANCELED_BEFORE_RUN`.
- Remove `GlobalExecutionPolicy` (it only carried the task-listener list): register listeners directly on `GlobalPar.Builder` via `taskListener(...)` and per-Par `parTaskListener(name, ...)` (repeated calls append; the override still replaces the default list for that Par), and read them via `GlobalPar.taskListeners()` / `taskListenersFor(name)`.
- Rename `AsyncBatchResult` to `TaskBatchResult` (the nested `BatchReport` keeps its name).
- Rename the internal `ConcurrentLimitExecutor` to `SlidingWindowSubmitter`.
- Rename `TaskGraphObservationContext` to `TaskGraphObservationScope`: it is a closeable observation scope, and the naming rule is now that `Scope` marks lifecycle scopes (`SubmissionScope`, `TaskGraphObservationScope`) while `Context` marks data carriers. `MultiTaskContext.taskGraphObservationContext()` is renamed to `taskGraphObservationScope()` accordingly; `GlobalPar.openTaskGraphObservation()` keeps its name.
- Neutralize the batch-biased `MultiTaskContext` vocabulary, since one context backs both `Par.map` batches and task-group members: `batchId()` → `unitId()`, `taskName()` → `name()`, `parLabel()` → `executorLabel()`, `parent()` → `structuralParent()` (a member's cancellation parent is the group token inside `cancellationToken()`, not this field). `TaskContext.batchContext()` becomes `multiTaskContext()` and `SubmissionScope.currentBatch()` becomes `current()`. Deadline resolution for batches, group members, and groups now shares a single `MultiTaskContext.resolveDeadlineNanos` path.
- Make `TaskKey.name()` the single name of a task-group member or terminal combine. Its checkpoint name, task-listener `taskName()`, and nested task-graph label now use the key name; batch and group names are unchanged. Rename `TaskGroupResult.failedMemberName()` to `failedTaskName()` because the first failed task may be the terminal combine.
- Split the shared `MultiTaskOptions` superset into one option type per scope: `BatchOptions` for `Par.map` (name, parallelism, timeout, task type, enqueue policy), `TaskGroupOptions` for `TaskGroupDefinition.builder` (name, timeout, listeners), and `TaskOptions` for `TaskGroupDefinition.Builder.task` and `buildWithCombiner` (timeout, task type, enqueue policy). A member or combine is a single task, so its options no longer declare a name, a parallelism, or listeners: a field its only consumer never reads is now unrepresentable instead of silently ignored. The three types share no supertype, so the role is chosen statically by the parameter position and passing a group option to a member no longer compiles.
- Replace `TaskGroupDefinition.Builder.combine(key, parName, function, options)` with the terminal `buildWithCombiner(key, parName, function[, options])`, which registers the combine and returns the finished definition in one call. A combine depends on every member, so declaring it is now the last step of a definition rather than a separate registration followed by `build()`; the registration validation (null arguments, name collision, at most one combine) is unchanged.
- Make the timeout a type-level invariant instead of a `build()`-time check: `timeout(name, Duration)` and `inheritTimeout(name)` are the only factories (`TaskOptions` factories take no name), so a missing or doubled declaration cannot be constructed at all. Options are now built with static factories and immutable withers rather than a builder, and `MultiTaskContext.resolve` takes the package-private `UnitSpec` carrier instead of a public option type, so the kernel no longer depends on public options and a member no longer resolves an unused parallelism.
- Narrow every task execution future to `TaskFuture`: `TaskBatchResult.results()` elements, `TaskGroup.members()`, `TaskGroup.findMember(String)`, `TaskGroup.future(TaskKey)` (including the terminal combine), and `TaskGroup.completionFuture()` now declare `TaskFuture` instead of `ListenableFuture`. Source compatible (the interface extends `ListenableFuture`), binary incompatible: recompile against this version. `TaskBatchResult.submitCanceller()` deliberately stays a plain `ListenableFuture` — it is a control handle, not a task execution.
- Wrap an abandoned batch element's cause in `SubmissionException`. A batch rejected at initial submission, or one whose remaining elements are abandoned by an interrupted submitter, now fails those elements with a `SubmissionException` carrying the original cause, so they report `SUBMISSION_FAILURE` instead of `USER_FAILURE`. Read the original rejection or interruption through `getCause()`.

### Features

- Add `TaskGroupDefinition.Builder.task(key, parName, callable)` for members that run under the group deadline: it is exactly the four-argument form with `TaskOptions.inheritTimeout()`, so a member that needs no tighter budget, different task type, or different enqueue policy declares no options. The forced deadline choice remains at the group level, and a member that inherits the group deadline still cannot outlive it. The terminal combine gets the same treatment through `buildWithCombiner(key, parName, function)`.
- Add the public `TaskFuture<T>` contract and its package-private-constructed `Task<T>` implementation: every delivered task execution future reports its `taskName()`, `outcome()`, `deadlineNanos()`, `remaining()`, and `failure()`. The view is additive over `ListenableFuture`, so Guava combinators and code that never checks the interface are unaffected; `FluentFuture.from(task)` remains the route to fluent chaining.

### Features

- Bind existing futures into a task scope with cancellation, timeout, and fail-fast behavior.
- Support custom schedulers and isolate timer callback dispatch from timer threads.
- Add the public `ActionGate` API for count- and duration-based action gating.
- Add immutable multi-`Par` `GlobalPar` topology, deadlock and purge policies, and explicit observation scopes.
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

Artifacts:

- Maven Central: `io.github.monadrome:parallel-in-scope:0.2.0`
- GitHub release: [v0.2.0](https://github.com/monadrome/parallel-in-scope/releases/tag/v0.2.0)

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

- Maven Central: `io.github.huatalk:parallel-in-scope:0.1.0` (published under the account's former name; the coordinate and package move to `io.github.monadrome` in 0.2.0)
- GitHub release: [v0.1.0](https://github.com/monadrome/parallel-in-scope/releases/tag/v0.1.0)
