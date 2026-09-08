# User Guide

> This guide documents the current `0.2.0-SNAPSHOT` API. `0.1.x` examples using `ParConfig` or `ParOptions` do not compile against this version; see the [migration guide](migration-v0.2.md).

`parallel-in-scope` executes a finite list as a cancellable batch. Application wiring owns long-lived resources, a `Par` owns one executor binding, and a `BatchExecutionContext` owns one invocation's runtime state.

It also coordinates a fixed heterogeneous set of named operations through `ParallelTaskGroup`. A
group is configured first and submitted at one explicit build boundary; it is not a dynamically
growing batch.

## Build the execution topology

Create `GlobalPar` at the composition root. Register every logical entry with the executor it must use and pass the resulting `Par` to components that need it.

```java
GlobalExecutionPolicy defaults = GlobalExecutionPolicy.builder()
        .defaultTimeoutMillis(10_000)
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

`BatchExecutionOptions` is immutable input for one call. The library resolves it with `GlobalExecutionPolicy`, the item count, any parent batch, and the bound executor identity into an internal `BatchExecutionContext`.

```java
BatchExecutionOptions options = BatchExecutionOptions.of("fetch-account")
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

`parallelism` limits this batch's active submission window. A negative value leaves the effective limit to policy resolution. An explicit timeout must be positive; omitted timeout uses the global default. `TaskType.CPU_BOUND` and `TaskType.IO_BOUND` describe scheduling intent. `rejectEnqueue` controls whether the batch rejects queueing when the bound executor supports that behavior.

The returned futures remain in input order. If failure, timeout, cancellation, submitter interruption, or rejection stops the window, the never-submitted placeholders are completed or cancelled so aggregate futures do not remain live indefinitely.

## Execute a heterogeneous task group

Use a task group when a request has a small fixed set of independent operations that may return
different types or use different `Par` entries. `addTask` only records definitions. It does not
create execution contexts, capture TTL values, start timers, or submit work. `buildAndSubmitAll`
freezes the complete set, prepares every member, and then submits them.

```java
ParallelTaskGroup.Builder builder = global.taskGroupBuilder(
        TaskGroupOptions.of("account-page")
                .timeout(Duration.ofSeconds(3))
                .build());

ParallelTaskGroup.TaskHandle<User> user = builder.addTask(
        "user", databasePar, userRepository::load,
        BatchExecutionOptions.of("load-user").build());
ParallelTaskGroup.TaskHandle<List<Order>> orders = builder.addTask(
        "orders", httpPar, orderClient::load,
        BatchExecutionOptions.of("load-orders").taskType(TaskType.IO_BOUND).build());

try (ParallelTaskGroup group = builder.buildAndSubmitAll()) {
    User userValue = user.future().get();
    List<Order> orderValues = orders.future().get();
    TaskGroupResult result = group.completionFuture().get();
}
```

The builder is one-shot and not thread-safe. A `TaskHandle` is type-safe, but its `future()` is
available only after a successful build. Group completion always returns a `TaskGroupResult`;
`FAILED`, `TIMEOUT`, and `CANCELED` are result reasons rather than failures of the completion
future. Individual member futures retain normal Guava success, failure, and cancellation behavior.

The first member failure triggers fail-fast cancellation of unfinished siblings. `group.cancel()`
and `close()` cancel unfinished members without blocking. Cancelling one member future directly
does not cancel its siblings. Group and member deadlines start at the build boundary, and member
deadlines are capped by the group deadline. A group built inside a scoped task inherits outer
cancellation and its deadline ceiling; each member remains a real child task, while membership
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

Nested `map` calls inherit the current `BatchExecutionContext` when they run inside a task. The child receives the parent cancellation token and deadline, records an edge to the parent, and may target a different `Par`:

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
- Treat `BatchExecutionContext`, `ExecutorRuntime`, and `ExecutorIdentity` as runtime/internal concepts, not configuration objects to cache or construct.
