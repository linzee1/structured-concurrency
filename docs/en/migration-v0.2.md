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

`TaskListener.TaskEvent` now exposes the completed task through `getTaskContext()` and its outcome
through `isSuccessful()`, `getResult()`, and `getException()`. A successful task may return null, so
use `isSuccessful()` rather than testing the result for null. Listener callbacks run outside the
completed task's dynamic execution scope; use the event instead of `TaskExecutionContext.current()`.

The old `ParConfig`, `ParOptions`, `ExecutorResolver`, `GlobalParConfig`, and legacy `Par` entry points are not compatibility aliases. Update imports, construction, and method calls together. Registered executors remain borrowed and are still owned and shut down by the application.
