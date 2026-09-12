# User Guide

> This guide documents the current `0.2.0` API. `0.1.x` examples using `ParConfig` or `ParOptions` do not compile against this version; see the [migration guide](migration-v0.2.md).

`parallel-in-scope` executes a finite list as a cancellable batch. Application wiring owns long-lived resources, a `Par` owns one executor binding, and a `MultiTaskContext` owns one invocation's runtime state.

It also coordinates a fixed heterogeneous set of named operations through `TaskGroup`. A
group is described as a reusable `TaskGroupDefinition` and submitted at one explicit boundary; it is not
a dynamically growing batch.

## Build the execution topology

Create `GlobalPar` at the composition root. Register every logical entry with the executor it must use and pass the resulting `Par` to components that need it.

```java
ParName DATABASE = ParName.of("database");
ParName HTTP = ParName.of("http");

GlobalPar global = GlobalPar.builder()
        .taskListener(metricsListener)
        .register(DATABASE, databaseExecutor)
        .register(HTTP, httpExecutor)
        .defaultPar(HTTP)
        .build();

Par httpPar = global.par(HTTP);
```

`ParName` is a value object: it is validated once at construction (never null, never blank) and compared by value, so the same name can be declared as a constant and reused. It is a logical lookup key, not a resource identity — the physical pool is identified by `ExecutorIdentity` through object reference, and two names may deliberately share one executor.

Names are validated at build time. `GlobalPar` is immutable after `build()`, and `par(name)` fails for an unknown name. The supplied executors are borrowed: closing `GlobalPar` shuts down its internal timer and submitter services only, never a registered executor.

For a process-wide convenience entry point, install exactly one already-built topology during bootstrap:

```java
GlobalPar.installGlobal(global);
Par defaultPar = GlobalPar.global().defaultPar();
```

Prefer explicit injection in tests and libraries. `installGlobal` is one-time and intentionally rejects replacement.

## Execute a batch

`BatchOptions` is the immutable input of one batch call. Option types map one-to-one onto scopes: a batch declares `BatchOptions`, a group declares `TaskGroupOptions`, and a single member or combine declares `TaskOptions`. The library resolves a scope's name, concurrency, and execution policy together with the item count, any parent batch, and the bound executor identity into an internal `MultiTaskContext`.

```java
BatchOptions options = BatchOptions.timeout("fetch-account", Duration.ofSeconds(5))
        .parallelism(16)
        .taskType(TaskType.IO_BOUND)
        .rejectEnqueue(false);

TaskBatchResult<Account> result = httpPar.map(
        accountIds,
        client::fetchAccount,
        options);

List<TaskFuture<Account>> futures = result.results();
```

`parallelism` limits this batch's active submission window. A negative value leaves the effective limit to policy resolution. The timeout is a forced explicit choice between two mutually exclusive factories: `BatchOptions.timeout(name, Duration)` sets an explicit positive bound, `BatchOptions.inheritTimeout(name)` adopts the enclosing scope's deadline — there is no third state, so omitting the choice does not compile. An explicit timeout is capped by any enclosing deadline; an inherited timeout with no enclosing scoped task is rejected at the entry point. `TaskType.CPU_BOUND` and `TaskType.IO_BOUND` describe scheduling intent. `rejectEnqueue` controls whether the batch rejects queueing when the bound executor supports that behavior.

The returned futures remain in input order. If failure, timeout, cancellation, submitter interruption, or rejection stops the window, the never-submitted placeholders are completed or cancelled so aggregate futures do not remain live indefinitely.

## Execute a heterogeneous task group

Use a task group when a request has a small fixed set of independent operations that may return
different types or use different `Par` entries. A group is described by a `TaskGroupDefinition`: an
immutable, reusable, pure-data description. `TaskGroupDefinition.Builder.task` only records a definition;
it does not create execution contexts, capture TTL values, start timers, or submit work.
`TaskGroup.submit(global, definition)` resolves the calling thread's context at submission time,
freezes the complete member set, prepares every member, and then submits them.

A group scope declares `TaskGroupOptions`: the group name, the group deadline, and the group's
convergence listeners. A group is not a task execution, so it has no concurrency, task type, or
enqueue policy. A member or combine declares `TaskOptions` only when it must differ: the type carries
just that execution's timeout, task type, and enqueue policy. A member is a single task with no
fan-out, so no parallelism field exists in its options — nested work submitted inside a member reads
that nested submission's own options. Identity is not an option either: a member's diagnostic name is
always its `TaskKey` name. A member runs under the group deadline by default — omitting the fourth
argument is exactly `TaskOptions.inheritTimeout()`, so a member can never outlive the group deadline
that way. Pass options explicitly only for a tighter budget, a different task type, or a different
enqueue policy; an explicit member timeout is capped by the group deadline. A group that declares `inheritTimeout` must be submitted from
inside a scoped task, otherwise `submit` is rejected.

```java
TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(
        TaskGroupOptions.timeout("account-page", Duration.ofSeconds(3)));

TaskKey<User> user = definition.task(
        new TaskKey<User>("user") {},
        DATABASE, userRepository::load);
TaskKey<List<Order>> orders = definition.task(
        new TaskKey<List<Order>>("orders") {},
        HTTP, orderClient::load,
        TaskOptions.inheritTimeout().taskType(TaskType.IO_BOUND));

try (TaskGroup group = TaskGroup.submit(global, definition.build())) {
    User userValue = group.future(user).get();
    List<Order> orderValues = group.future(orders).get();
    TaskGroupResult result = group.completionFuture().get();
}
```

A `TaskKey` is a type-safe key created as an anonymous subclass so the member's result type is
captured at runtime; it is registered while configuring the definition, and after submission
`group.future(key)` resolves the member's future, rejecting a key whose raw result type does not
cover the registered one. Keys compare equal by name alone, so a key claiming a supertype of
the registered type is equal to the registered key. Group completion always returns a
`TaskGroupResult`; the group outcome (`result.outcome()`, a `TaskOutcome`) is result data rather
than a failure of the completion future. Individual member futures retain normal Guava success,
failure, and cancellation behavior.

Group cancellation is fully structured, matching batch semantics: the first member failure, a
direct cancellation of any member future or member token, the group deadline, or any single member
deadline cancels every unfinished member. `group.cancel()` and `close()` cancel unfinished members
without blocking. Member outcomes are attributed from the cancellation tokens, so a cancelled
member reports `MEMBER_CANCELED`, `FAIL_FAST`, `TIMEOUT`, or `GROUP_CANCELED` rather than a bare
cancellation; a member exceeding its own deadline escalates the group to `TIMEOUT`. Group and
member deadlines start at the submission boundary, and member deadlines are capped by the group
deadline. A group submitted inside a scoped task inherits outer cancellation and its deadline
ceiling; cancellation propagated from an ancestor keeps its originating reason
(`CancellationToken.originState()`), so an ancestor deadline expiring still converges the group as
`TIMEOUT` rather than a plain `GROUP_CANCELED`. Each member remains a real child task, while membership
itself does not add dependency edges between siblings.

### Terminal combine

When the request ends by assembling the member values into one result, the terminal `buildWithCombiner` call declares
a single combine — registering it and building the definition at once — instead of making you wire
`Futures` callbacks yourself. The combine is a real
scoped task — prepared at submit like a member, but submitted to its own `Par` only after every
member succeeds — so it inherits the group's structured cancellation, deadline, and observability:

```java
TaskKey<AccountPage> page = new TaskKey<AccountPage>("assemble-page") {};

// The combine depends on every member, so the terminal call declares it and builds the definition
TaskGroupDefinition built = definition.buildWithCombiner(
        page,
        ParName.of("cpu"),
        values -> new AccountPage(values.value(user), values.value(orders)));

try (TaskGroup group = TaskGroup.submit(global, built)) {
    AccountPage accountPage = group.future(page).get();
}
```

The `CompletedTaskValues` view is non-blocking and exposes only successful member values through
the registered `TaskKey` keys — no futures, no name-keyed map. The combine function runs exactly
once on a worker of the named `Par` (never on a member's completion thread; a rejected combine
fails as `SUBMISSION_FAILURE` instead of running inline), and it must be a pure function of member
values and configuration-time captures: it is scheduled the moment the last member succeeds, so
state created by the submitting thread after `submit` returns is not visible to it — read the
member futures directly for that. A group accepts at most one combine, declared with its own
`TaskKey`; `group.future(combineKey)` resolves the typed terminal future with normal Guava
semantics. If any member fails, the combine never runs and the terminal future is cancelled with
the group's attributed outcome. The combine's snapshot appears as `TaskGroupResult.terminal()`
(`members()` stays member-only), and when the combine itself fails or is rejected,
`failedTaskName()` carries the combine's registered name. The group deadline spans the fan-out
and the combine, so a combine whose members consumed most of the budget may time out before it
starts — that is the intended end-to-end semantics.

## Read task attribution from a future

Every future the library delivers for a task execution is a `TaskFuture<T>`: a `ListenableFuture<T>`
that also answers what the task is and how it ended. That covers a batch's elements, a group's
members, its terminal combine, and the group completion future.

| Method | Answer |
|---|---|
| `taskName()` | The batch name, the member or combine key name, or the group name |
| `outcome()` | `RUNNING` while pending, then one terminal `TaskOutcome` |
| `deadlineNanos()` | The task's absolute deadline on the `System.nanoTime()` clock |
| `remaining()` | The budget left before that deadline, never negative |
| `failure()` | The cause behind `USER_FAILURE` / `SUBMISSION_FAILURE`, otherwise `null` |

The view is purely additive. `TaskFuture` extends `ListenableFuture`, so `Futures.allAsList`,
`addCallback`, and every other Guava combinator keep working on it unchanged, and code that never
checks the interface behaves exactly as before. Check with `instanceof`; the implementation class is
private and must never be named.

```java
Account account = future.get();
if (future instanceof TaskFuture) {
    TaskFuture<?> task = (TaskFuture<?>) future;
    if (task.outcome() == TaskOutcome.TIMEOUT) {
        log.warn("{} timed out with {} of its budget left", task.taskName(), task.remaining());
    }
}
```

`outcome()` is why the interface exists: it removes the "the future is cancelled, so guess why"
step. A cancelled task is attributed from its cancellation token — `TIMEOUT` for a deadline,
`FAIL_FAST` for the cascade after a sibling failed, `GROUP_CANCELED` for its group's or an enclosing
scope's cancellation, and `MEMBER_CANCELED` when no framework path cancelled it (the caller
cancelled that future directly). A failure that only reports observed cancellation — a
`Checkpoints.checkpoint` interruption that won the race against the cascade — is attributed the same
way instead of reading as a user failure. A failed task separates `SUBMISSION_FAILURE` (rejected, or
failed before user code ran) from `USER_FAILURE`, and `failure()` hands back the cause without
unwrapping an `ExecutionException`.

Attribution is per future, read from that task's own token chain, so it is available while the
enclosing group is still converging. The group's terminal classification — `TaskGroupResult.outcome()`
and each member's `TaskCompletion` — is derived after convergence and remains the authority on
group-level reasons: the snapshot keeps a member cancelled directly distinct from one cancelled as
fallout, while a future can only report that its token chain ends in the group's cancellation.

One handle stays a plain future on purpose: `TaskBatchResult.submitCanceller()` stops submission, it
does not represent a task execution, so it is not a `TaskFuture`.

Need fluent chaining? `FluentFuture.from(task)` gives the full `FluentFuture` API. The futures on
such a chain are ordinary `FluentFuture`s: they are not executions the library ran, and no token
owns them.

## Cancellation and nested batches

Any task failure triggers fail-fast cancellation for its batch. A timeout, explicit `CancellationToken` cancellation, or cancellation of a parent batch has the same cooperative boundary: queued work is cancelled, blocking work is interrupted where possible, and CPU-bound code stops at a checkpoint.

```java
httpPar.map(accountIds, id -> {
    for (int page = 0; page < pageCount(id); page++) {
        Checkpoints.checkpoint();
        fetchPage(id, page);
    }
    return id;
}, options);
```

Nested `map` calls inherit the current `MultiTaskContext` when they run inside a task. The child receives the parent cancellation token and deadline, records an edge to the parent, and may target a different `Par`:

```java
databasePar.map(ids, id -> {
    TaskBatchResult<Response> children = httpPar.map(
            endpoints(id), client::call, httpOptions);
    return collect(children);
}, databaseOptions);
```

Use an [observation scope](#observe-nested-work) when the request needs graph diagnostics across multiple `Par` entries.

## Observe nested work

Task-graph observation is explicitly scoped to one `GlobalPar`. The scope owns graph cleanup and, when enabled, invokes potential-deadlock listeners at the end of the request. A cycle is a structural risk signal, not proof that threads are currently deadlocked.

```java
try (TaskGraphObservationScope observation = global.openTaskGraphObservation()) {
    // Calls made below this scope, including nested calls on other Pars in global,
    // are recorded in the same graph.
    service.handleRequest();
}
```

Configure the policy while building the topology:

```java
GlobalParDeadlockPolicy deadlock = GlobalParDeadlockPolicy.builder()
        .enabled(true)
        .listener(event -> log.warn("Potential deadlock: {}", event))
        .build();
```

An observation scope does not merge graphs from separate `GlobalPar` instances.

## Purge cancelled queue entries

Purge is optional and applies only when a supplied executor is a `ThreadPoolExecutor` backed by a bounded `BlockingQueue` (for example `SmartBlockingQueue`, a bounded `LinkedBlockingQueue`, or `ArrayBlockingQueue`). Queues without a finite positive capacity — `SynchronousQueue` and unbounded queues such as `new LinkedBlockingQueue()` — receive a no-op observer. Cancellation before execution emits an execution phase signal; `GlobalPar` coalesces maintenance by physical executor identity, so aliases or multiple `Par` entries backed by the same pool do not start duplicate purge coordinators.

```java
GlobalParPurgePolicy purge = GlobalParPurgePolicy.builder()
        .enabled(true)
        .queuePressureThreshold(0.80)
        .canceledTaskRatioThreshold(0.05)
        .build();

GlobalPar global = GlobalPar.builder()
        .purgePolicy(purge)
        .register(ParName.of("io"), ioThreadPool)
        .build();
```

Both thresholds must be reached before `ThreadPoolExecutor.purge()` is requested. Purge only removes cancelled work still retained in the queue; it cannot stop a task body that ignores interruption.

## Lifecycle-aware queues

`DrainingBlockingQueue` is a bounded `BlockingQueue` implementation with a one-way draining close: `close()` permanently rejects new production (write operations throw `IllegalStateException` or return `false`), while consumers keep taking existing elements until the queue is empty and reaches its terminal state, after which they observe the configured poison object, `null`, or `NoSuchElementException`.

```java
DrainingBlockingQueue<Job> queue = new DrainingBlockingQueue<>(100, poison);
queue.put(job);
queue.close();          // producers are closed; queued elements are never dropped
queue.awaitDrained();   // optional: wait until drained

Job job = queue.take(); // real element before drained; poison after drained
```

Consumers can still take elements that were queued before `close()`; no recovery channel is needed, and `drainTo` stays available in every state for discarding remaining work. Use `shutdown()` for "production is closed" and `drained()` for "the queue is empty and terminal". Full contract: [draining-close contract](https://github.com/monadrome/parallel-in-scope/blob/main/design/draining-queue-contract.md).

## Operational rules

- Keep a `GlobalPar` for the application lifetime and close it during application shutdown.
- Keep registered executor ownership outside the library; shut executors down in the owning component.
- Give each batch a stable task name and add checkpoints to long CPU work.
- Use different `Par` entries for resources that require isolation, even when both are IO-bound.
- Treat `MultiTaskContext`, `ExecutorRuntime`, and `ExecutorIdentity` as runtime/internal concepts, not configuration objects to cache or construct.
