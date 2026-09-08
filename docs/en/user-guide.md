# User Guide

> This guide documents the current `0.2.0-SNAPSHOT` API. `0.1.x` examples using `ParConfig` or `ParOptions` do not compile against this version; see the [migration guide](migration-v0.2.md).

`parallel-in-scope` executes a finite list as a cancellable batch. Application wiring owns long-lived resources, a `Par` owns one executor binding, and a `MultiTaskContext` owns one invocation's runtime state.

It also coordinates a fixed heterogeneous set of named operations through `TaskGroup`. A
group is described as a reusable `TaskGroupSpec` and submitted at one explicit boundary; it is not
a dynamically growing batch.

## Build the execution topology

Create `GlobalPar` at the composition root. Register every logical entry with the executor it must use and pass the resulting `Par` to components that need it.

```java
GlobalExecutionPolicy defaults = GlobalExecutionPolicy.builder()
        .taskListener(metricsListener)
        .build();

GlobalPar global = GlobalPar.builder()
        .executionPolicy(defaults)
        .register("database", databaseExecutor)
        .register("http", httpExecutor)
        .defaultPar("http")
        .build();

Par httpPar = global.par("http");
```

Names are validated at build time. `GlobalPar` is immutable after `build()`, and `par(name)` fails for an unknown name. The supplied executors are borrowed: closing `GlobalPar` shuts down its internal timer and submitter services only, never a registered executor.

For a process-wide convenience entry point, install exactly one already-built topology during bootstrap:

```java
GlobalPar.installGlobal(global);
Par defaultPar = GlobalPar.global().defaultPar();
```

Prefer explicit injection in tests and libraries. `installGlobal` is one-time and intentionally rejects replacement.

## Execute a batch

`MultiTaskOptions` is immutable input for one call. The library resolves it with the item count, any parent batch, and the bound executor identity into an internal `MultiTaskContext`.

```java
MultiTaskOptions options = MultiTaskOptions.of("fetch-account")
        .taskType(TaskType.IO_BOUND)
        .parallelism(16)
        .timeout(Duration.ofSeconds(5))
        .rejectEnqueue(false)
        .build();

AsyncBatchResult<Account> result = httpPar.map(
        accountIds,
        client::fetchAccount,
        options);

List<ListenableFuture<Account>> futures = result.results();
```

`parallelism` limits this batch's active submission window. A negative value leaves the effective limit to policy resolution. The timeout is a forced explicit choice: call `timeout(Duration)` for an explicit positive bound, or `inheritTimeout()` to adopt the enclosing scope's deadline — `build()` rejects a builder that declares neither or both. An explicit timeout is capped by any enclosing deadline; an inherited timeout with no enclosing scoped task is rejected at the entry point. `TaskType.CPU_BOUND` and `TaskType.IO_BOUND` describe scheduling intent. `rejectEnqueue` controls whether the batch rejects queueing when the bound executor supports that behavior.

The returned futures remain in input order. If failure, timeout, cancellation, submitter interruption, or rejection stops the window, the never-submitted placeholders are completed or cancelled so aggregate futures do not remain live indefinitely.

## Execute a heterogeneous task group

Use a task group when a request has a small fixed set of independent operations that may return
different types or use different `Par` entries. A group is described by a `TaskGroupSpec`: an
immutable, reusable, pure-data description. `TaskGroupSpec.Builder.task` only records a definition;
it does not create execution contexts, capture TTL values, start timers, or submit work.
`TaskGroup.submit(global, spec)` resolves the calling thread's context at submission time,
freezes the complete member set, prepares every member, and then submits them.

Groups and batches share one option type, `MultiTaskOptions`: the group reads name, timeout,
and listeners, while each member reads the execution subset (parallelism, task type, enqueue
policy, timeout). Members typically declare `inheritTimeout()` so they run under the group
deadline; an explicit member timeout is capped by it. A group that declares `inheritTimeout()`
must be submitted from inside a scoped task, otherwise `submit` is rejected.

```java
TaskGroupSpec.Builder spec = TaskGroupSpec.builder(
        MultiTaskOptions.of("account-page")
                .timeout(Duration.ofSeconds(3))
                .build());

TaskRef<User> user = spec.task(
        "user", "database", userRepository::load,
        MultiTaskOptions.of("load-user").inheritTimeout().build());
TaskRef<List<Order>> orders = spec.task(
        "orders", "http", orderClient::load,
        MultiTaskOptions.of("load-orders").taskType(TaskType.IO_BOUND).inheritTimeout().build());

try (TaskGroup group = TaskGroup.submit(global, spec.build())) {
    User userValue = group.future(user).get();
    List<Order> orderValues = group.future(orders).get();
    TaskGroupResult result = group.completionFuture().get();
}
```

A `TaskRef` is a type-safe token handed out while configuring the spec; after submission,
`group.future(ref)` resolves the member's future. Group completion always returns a
`TaskGroupResult`; `FAILED`, `TIMEOUT`, and `CANCELED` are result reasons rather than failures of
the completion future. Individual member futures retain normal Guava success, failure, and
cancellation behavior.

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
`TIMEOUT` rather than a plain `CANCELED`. Each member remains a real child task, while membership
itself does not add dependency edges between siblings.

## Cancellation and nested batches

Any task failure triggers fail-fast cancellation for its batch. A timeout, explicit `CancellationToken` cancellation, or cancellation of a parent batch has the same cooperative boundary: queued work is cancelled, blocking work is interrupted where possible, and CPU-bound code stops at a checkpoint.

```java
httpPar.map(accountIds, id -> {
    for (int page = 0; page < pageCount(id); page++) {
        Checkpoints.checkpoint("fetch-account", true);
        fetchPage(id, page);
    }
    return id;
}, options);
```

Nested `map` calls inherit the current `MultiTaskContext` when they run inside a task. The child receives the parent cancellation token and deadline, records an edge to the parent, and may target a different `Par`:

```java
databasePar.map(ids, id -> {
    AsyncBatchResult<Response> children = httpPar.map(
            endpoints(id), client::call, httpOptions);
    return collect(children);
}, databaseOptions);
```

Use an [observation scope](#observe-nested-work) when the request needs graph diagnostics across multiple `Par` entries.

## Observe nested work

Task-graph observation is explicitly scoped to one `GlobalPar`. The scope owns graph cleanup and, when enabled, invokes potential-deadlock listeners at the end of the request. A cycle is a structural risk signal, not proof that threads are currently deadlocked.

```java
try (TaskGraphObservationContext observation = global.openTaskGraphObservation()) {
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
        .register("io", ioThreadPool)
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

Consumers can still take elements that were queued before `close()`; no recovery channel is needed, and `drainTo` stays available in every state for discarding remaining work. Use `shutdown()` for "production is closed" and `drained()` for "the queue is empty and terminal". Full contract: [draining-close contract](../../zh/design/draining-blocking-queue-contract.md).

## Operational rules

- Keep a `GlobalPar` for the application lifetime and close it during application shutdown.
- Keep registered executor ownership outside the library; shut executors down in the owning component.
- Give each batch a stable task name and add checkpoints to long CPU work.
- Use different `Par` entries for resources that require isolation, even when both are IO-bound.
- Treat `MultiTaskContext`, `ExecutorRuntime`, and `ExecutorIdentity` as runtime/internal concepts, not configuration objects to cache or construct.
