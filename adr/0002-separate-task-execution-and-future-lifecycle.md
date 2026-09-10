# ADR 0002: Separate Task Execution and Future Lifecycle Enhancement

- Status: Accepted
- Date: 2026-07-26
- Decision scope: Callable, Runnable, and Future execution architecture
- Supersedes: None

## Context

Java 8 exposes three related but distinct abstractions:

- `Callable` and `Runnable` describe work that may execute;
- `Future` describes a submitted computation from the caller's perspective;
- an executor queue stores a `Runnable`, which need not be the `Future` returned
  to the caller.

The library needs to add task context, cooperative cancellation, lifecycle
metrics, bounded admission, completion-driven submission, timeout, fail-fast,
parent-child cancellation, batch reporting, and cancelled-task cleanup. These
capabilities cannot be represented accurately by enhancing only one of the
three JDK abstractions.

In particular:

- a cancelled Future does not prove that its task body stopped;
- a Future that is not done may be queued, claimed by a worker, or running;
- cancelling the Future returned by JDK `ExecutorCompletionService` does not
  cancel its distinct queued `QueueingFuture` wrapper;
- later tasks in a sliding window may have result placeholders before their
  actual task is submitted;
- task context exists on the worker side, while timeout, fail-fast, and parent
  propagation operate on the result side.

The architecture therefore needs explicit ownership for execution-side and
result-side behavior, plus a bridge that preserves their relationship without
claiming stronger cancellation guarantees than Java executors provide.

## Decision

The library SHALL separate task-execution enhancement from Future-lifecycle
enhancement.

`ScopedCallable` owns worker-side execution context and instrumentation.
`FutureRunnable`, Guava `ListenableFuture`, and `AsyncBatchResult` own
caller-side result observation and control. `CancellationToken`,
`ListenableCompletionService`, and `ConcurrentLimitExecutor` bridge the two
sides.

The normal execution pipeline is:

```text
Function<T, R>
  -> Callable<R>
  -> ScopedCallable<R>
  -> FutureRunnable<R> backed by ListenableFutureTask<R>
  -> executor queue and worker
  -> ListenableFuture<R>
  -> AsyncBatchResult<R>
```

### Callable enhancement

`ScopedCallable<V>` SHALL be the central wrapper for task-body execution. It
provides the following capabilities:

| Capability | Contract |
|---|---|
| Logical identity | Retain task name and logical executor name |
| Task options | Expose the normalized `ParOptions` for the current task |
| Cancellation context | Install the task's `CancellationToken` before invoking user code |
| Entry checkpoint | Check cooperative cancellation before the delegate starts |
| Local context | Initialize and remove `TaskScopeTl` in the same worker thread |
| Child relay | Publish cancellation token, options, task name, and executor name through `ThreadRelay` |
| Current task access | Expose the executing wrapper through `ScopedCallable.current()` |
| Timing | Record submission, start, and end timestamps |
| Durations | Report queue wait, execution, and total duration |
| Outcome capture | Retain the thrown failure for lifecycle reporting and rethrow it unchanged |
| Monitoring | Emit `TaskListener.TaskEvent` on success and failure |
| Listener isolation | Log listener failures without changing the task outcome |
| Cleanup | Remove task-local state and the current-wrapper reference in `finally` |

`Par.map` SHALL convert each input function invocation into a `Callable`, wrap
it in `ScopedCallable`, and submit only the wrapped form. User task bodies SHALL
not need cancellation, context, timing, or listener parameters added to their
function signatures.

### Runnable enhancement

`FutureRunnable<V>` SHALL be the object passed to the executor. It SHALL also
implement `ListenableFuture<V>` by forwarding to one
`ListenableFutureTask<V>`. It supports both `Callable<V>` and `Runnable` with a
fixed result.

The same `FutureRunnable` instance SHALL be:

- the `Runnable` stored in the executor queue;
- the Future returned to the submitter;
- the object published to the completion queue.

`FutureRunnable` SHALL maintain an execution phase separate from the delegated
Future state:

| Phase | Meaning |
|---|---|
| `SUBMITTED` | No worker has claimed `run()`; the object may still be queued |
| `RUNNING` | A worker won the claim race and removed the runnable from the queue |
| `CANCELLED_BEFORE_RUN` | Cancellation won before worker claim |
| `CANCEL_REQUESTED_RUNNING` | The Future accepted cancellation after worker claim |
| `TERMINAL` | `run()` returned |

The phase transition is used only to classify queue-relevant cancellation. It
does not replace the result state maintained by `ListenableFutureTask` and does
not prove task-body entry, interrupt observation, or worker termination.

When cancellation wins from `SUBMITTED`, `FutureRunnable` SHALL invoke the
bound queued-cancellation observer once. When a worker has already claimed the
task, cancellation SHALL NOT be reported as queued cancellation.

Runnable-based helper APIs have narrower responsibilities:

- `Checkpoints.checkRunnable` performs a pre-execution cancellation check and
  translates a declared failure type to `FatCancellationException`;
- `ActionGate` uses a `Runnable` callback to separate action cadence from the
  cancellation, purge, or maintenance mechanism;
- timer and submitter executors may dispatch `Runnable` or `Callable` work, but
  those adapters do not replace `ScopedCallable` as the business-task wrapper.

### Completion service

`ListenableCompletionService<V>` SHALL implement the JDK `CompletionService`
contract using `FutureRunnable` rather than JDK
`ExecutorCompletionService.QueueingFuture`.

Before submitting a task, it SHALL register a direct listener that publishes
the same task object to the completion queue. It SHALL support:

- `submit(Callable<V>)`;
- `submit(Runnable, V result)`;
- blocking `take()`;
- immediate `poll()`;
- timed `poll(timeout, unit)`.

The completion queue is ordered by completion publication, while the batch
result list remains ordered by input position. These are separate contracts.

### Sliding-window submission

`ConcurrentLimitExecutor<V>` SHALL accept a list of enhanced `Callable`
instances and implement a completion-driven sliding window:

1. submit at most `parallelism` initial tasks;
2. wait for one completion event;
3. submit one remaining task for the freed slot;
4. stop later submission when cancellation or submitter interruption requires
   the window to stop.

For tasks outside the initial window, the result list SHALL contain
`SettableFuture` placeholders. When a task is admitted, its placeholder SHALL
delegate to the actual submitted Future using `setFuture`. This preserves input
order and makes the complete result shape available before every task is
physically submitted.

The Future running the remaining-submission loop SHALL be returned separately
as `submitCanceller`. Cancelling a batch must be able to stop both task Futures
and future admissions.

When admission stops, every placeholder that will never be submitted SHALL reach
a terminal state. Direct placeholder cancellation produces `CANCELLED`; cancelling
the submitter Future interrupts its submission loop and records `InterruptedException`
on abandoned placeholders; a later submission rejection records the rejection cause.
No abandoned placeholder may remain `LIVE` indefinitely, and `Futures.allAsList` over
the batch results must be able to complete.

When executor submission rejects a `CPU_BOUND` task, the executor may use the
existing direct-executor fallback. Other rejection behavior remains explicit
and is not hidden by Future adaptation.

### Future enhancement

The internal result type SHALL be Guava `ListenableFuture` rather than plain
JDK `Future`. The architecture relies on these capabilities:

| Capability | Implementation |
|---|---|
| Completion listener | `ListenableFuture.addListener` |
| Callable/Runnable result | `ListenableFutureTask` |
| Deferred actual Future | `SettableFuture.setFuture` |
| Fail-fast aggregation | `Futures.allAsList` |
| Completion-tolerant aggregation | `Futures.successfulAsList` |
| Timeout | `FluentFuture.withTimeout` |
| Callback | `FutureCallback` with direct execution |
| Existing JDK Future adaptation | `JdkFutureAdapters.listenInPoolThread` |
| Already-listenable input | Reuse the original `ListenableFuture` |

`Par.bind` SHALL attach already-submitted Futures to the current task scope
without resubmitting them. Existing `ListenableFuture` instances are reused;
ordinary JDK Futures are adapted. Input order is retained, null elements are
rejected, and cancellation remains best effort according to the supplied
Future implementation.

### Cancellation and late binding

Each `Par.map` or `Par.bind` batch SHALL create a `CancellationToken`, linked to
the current or inherited parent token when present.

After task and submission Futures exist, `CancellationToken.lateBind` SHALL
connect:

- parent cancellation to the child token;
- task failure to fail-fast cancellation;
- the configured timeout to task cancellation;
- task Futures and the submission-loop Future to common cleanup;
- successful aggregate completion to the token's success state.

The token states distinguish the reason observed by the framework:

| State | Meaning |
|---|---|
| `RUNNING` | No terminal group outcome has won |
| `SUCCESS` | The aggregate completed successfully |
| `MUTUAL_CANCELED` | Application code cancelled the token |
| `PROPAGATING_CANCELED` | A parent cancellation propagated to this token |
| `FAIL_FAST_CANCELED` | A task failure cancelled the group |
| `TIMEOUT_CANCELED` | The group timeout elapsed |

Fail-fast aggregation SHALL use `Futures.allAsList`. Completion and cleanup
aggregation SHALL use `Futures.successfulAsList` so that individual failures or
cancellations do not prevent the aggregate cleanup path from completing.

Cancellation SHALL remain best effort:

- Future cancellation may request interruption but cannot force user code to
  terminate;
- a task that ignores interruption or blocks in non-interruptible IO may keep a
  worker occupied after its Future becomes cancelled;
- `CompletableFuture.cancel(boolean)` does not use the flag to interrupt an
  underlying computation;
- cooperative tasks should use `Checkpoints` or `ActionGate` at
  suitable boundaries.

### Cooperative blocking operations

`Checkpoints` SHALL provide cancellation-aware adapters for task-side
operations that can block or form useful cooperative boundaries:

- named and raw checkpoints;
- sleep;
- `CountDownLatch.await`;
- `Condition.await`;
- thread `join`;
- `Future.get`;
- blocking-queue `take` and `put`;
- semaphore timed acquisition;
- lock timed acquisition;
- executor termination waits;
- Runnable and Supplier exception-to-cancellation translation;
- cancellation-exception propagation from catch blocks.

These adapters check the current token before the operation. When an
`InterruptedException` is caught, they restore the interrupt flag and throw a
`LeanCancellationException`.

### Future state inspection and batch reporting

`FutureInspector` and `FutureState` SHALL provide the Java 8-compatible state
view needed by reporting:

- `RUNNING` for any Future that is not done;
- `SUCCESS` for normal completion;
- `FAILED` for exceptional completion;
- `CANCELLED` for cancellation.

`RUNNING` is deliberately a caller-side Future state. It does not distinguish
not-yet-submitted placeholders, executor-queued tasks, worker-claimed tasks, or
entered task bodies.

`FutureInspector.exceptionNow` SHALL return the cause only for a completed,
failed Future and reject pending, cancelled, and successful Futures.

`AsyncBatchResult<T>` SHALL contain:

- the input-ordered list of individual `ListenableFuture<T>` results;
- the submission-loop Future, or an immediate completed Future when all tasks
  were submitted synchronously;
- a snapshot report counting Futures by `FutureState`;
- the first observed task failure;
- a compact report string for diagnostics.

The report is a snapshot. It SHALL NOT imply that pending tasks remain in the
same state after the report returns.

### Cancelled queue cleanup

Executor queue cleanup SHALL be driven by cancellation of the exact
`FutureRunnable` submitted to the executor. The queued-cancellation observer
may notify `HeuristicPurger`, which can coalesce signals and invoke
`ThreadPoolExecutor.purge()` when configured queue-pressure and cancellation
thresholds justify a scan.

The cancellation count is an advisory estimate, not authoritative physical
queue membership. Purge can remove cancelled tasks that have not started; it
cannot stop a task already claimed by a worker.

### Enforceable semantic boundaries

The following distinctions SHALL remain explicit in implementation,
documentation, and tests:

| Observation | Does not prove |
|---|---|
| `Future.isCancelled()` | The worker stopped or task body exited |
| `Future.isDone()` | The executor queue no longer contains a distinct wrapper |
| `FutureState.RUNNING` | The task body started |
| `FutureRunnable.RUNNING` phase | The user delegate received a CPU time slice |
| Queued-cancellation signal | The object is still physically in the queue |
| Completion queue publication | Task results are in input order |
| Cancelled submission-loop Future | Already-submitted tasks stopped |
| Placeholder cancellation | An actual executor task existed |

Tests of cross-layer behavior SHALL collect separate evidence for Future
state, wrapper phase, queue identity, task-body entry, interrupt observation,
submission-window progress, and cleanup where those distinctions matter.

## Alternatives Considered

### Enhance only Callable

Rejected because a Callable wrapper can install context and instrumentation,
but it cannot provide completion listeners, batch aggregation, Future
adaptation, cancellation propagation, or executor queue identity.

### Enhance only Future

Rejected because a Future cannot reliably establish task-local context,
perform an entry checkpoint, measure delegate execution boundaries, or clean
worker ThreadLocals.

### Use JDK ExecutorCompletionService directly

Rejected because JDK `ExecutorCompletionService` executes a distinct
`QueueingFuture` while returning the task Future. Cancelling the returned
Future does not make the queued wrapper cancelled, preventing ordinary
`ThreadPoolExecutor.purge()` from identifying it through the returned object.

### Use CompletableFuture as the primary abstraction

Rejected because the project targets Java 8 executor interoperability and
needs the exact queued object to remain a cancellable Future. In addition,
`CompletableFuture.cancel(boolean)` does not interrupt an underlying running
task based on `mayInterruptIfRunning`.

### Submit every task immediately and limit execution with a semaphore

Rejected because it fills the executor queue and creates one queued object per
input even when only a small parallel window is useful. Completion-driven
submission bounds admission as well as active execution.

### Treat interruption as guaranteed termination

Rejected because Java interruption is cooperative. Encoding that assumption
would make timeout, cancellation, capacity, and purge behavior incorrect for
tasks that ignore interruption or use non-interruptible operations.

### Build a new public task or Future hierarchy

Rejected because `Callable`, `Runnable`, JDK `Future`, and Guava
`ListenableFuture` already provide the required interoperability. The custom
types remain focused wrappers and coordinators rather than a parallel public
concurrency framework.

## Consequences

### Positive

- User functions keep ordinary Java signatures while receiving scoped
  cancellation, context, timing, and monitoring.
- Result Futures support listener-driven completion, aggregation, timeout,
  fail-fast, parent propagation, and reporting.
- One queued and returned `FutureRunnable` makes queued cancellation visible to
  executor maintenance.
- Sliding-window admission bounds queued work while preserving input-ordered
  result handles.
- Explicit token states retain the reason for group cancellation.
- Java 8 callers can inspect Future state without depending on later JDK APIs.
- Execution-side and result-side tests can assert their own observable state
  without inferring one from the other.

### Negative

- One logical task has several cooperating representations:
  `ScopedCallable`, `FutureRunnable`, `ListenableFutureTask`, and sometimes a
  `SettableFuture` placeholder.
- Late binding introduces a short period in which submitted work exists before
  timeout and fail-fast wiring is complete.
- Adapting a plain JDK Future may consume a helper thread while waiting for its
  completion.
- Direct listeners must stay small because they execute on the thread that
  completes the Future.
- Batch state reports are snapshots and may change immediately.
- Cancellation semantics remain dependent on task cooperation and the supplied
  Future implementation.

### Neutral constraints

- `Callable` remains the primary value-producing task representation.
- `Runnable` is primarily the executor-queue representation and callback form;
  the public facade does not add a separate Runnable batch API.
- `AsyncBatchResult.getResults()` is input ordered; the completion queue is
  completion ordered.
- Queue cleanup is heuristic maintenance, not part of the correctness contract
  for task termination.
- This ADR describes the current architecture and does not change public APIs.

## Implementation Mapping

| Responsibility | Main implementation |
|---|---|
| User-facing map and Future binding | `scope/Par.java` |
| Task execution wrapper | `internal/ScopedCallable.java` |
| Queued Runnable and returned Future identity | `internal/FutureRunnable.java` |
| Completion publication | `internal/ListenableCompletionService.java` |
| Sliding-window admission | `internal/ConcurrentLimitExecutor.java` |
| Batch result and reporting | `scope/AsyncBatchResult.java` |
| Group cancellation and timeout | `cancel/CancellationToken.java` |
| Cooperative task boundaries | `cancel/Checkpoints.java` |
| Amortized action checks | `control/ActionGate.java` |
| Java 8 Future state view | `internal/FutureInspector.java`, `internal/FutureState.java` |
| Current-task local context | `context/TaskScopeTl.java` |
| Parent-child context relay | `context/ThreadRelay.java` |
| Cancelled queue maintenance | `cancel/HeuristicPurger.java` |
| Timer and submitter services | `scope/ParConfig.java` |

The observation-plane and Cartesian-test rules that verify these distinctions
are recorded in
[`0001-layered-cartesian-concurrency-testing.md`](0001-layered-cartesian-concurrency-testing.md).

## Reconsider When

Revisit this decision when one of these conditions becomes true:

- the minimum supported Java version provides a replacement that preserves
  completion listeners, cancellation propagation, and exact queue identity;
- a structured-concurrency runtime replaces executor-queue submission as the
  primary execution model;
- the public API adopts a different cancellation primitive with stronger or
  explicitly different semantics;
- helper-thread Future adaptation becomes an observed scalability bottleneck;
- task admission no longer needs input-ordered placeholders or a sliding
  window;
- executor cleanup no longer depends on the queued Runnable also being the
  returned Future.

If the decision changes materially, add a new ADR and mark this record
`Superseded` rather than rewriting its historical context.
