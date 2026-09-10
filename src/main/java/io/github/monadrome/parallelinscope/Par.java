package io.github.monadrome.parallelinscope;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Main facade for parallel execution.
 *
 * <p>Each {@code Par} is created by one {@link GlobalPar} and remains bound to its executor
 * runtime. Calls are rejected once its owner begins shutdown.
 *
 * <p>Provides the {@link #map} instance method that wires together the entire parallel execution
 * pipeline:
 *
 * <ul>
 *   <li>Resolution of {@link BatchOptions} into a batch context
 *   <li>Scoped task preparation via {@link io.github.monadrome.parallelinscope.TaskSubmissions}
 *   <li>Concurrency-limited submission via {@link SlidingWindowSubmitter}
 *   <li>Parent-child {@link CancellationToken} chaining
 *   <li>Late binding for timeout and fail-fast cancellation
 *   <li>Heuristic cleanup of canceled queued tasks
 * </ul>
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public final class Par {

    private final GlobalPar globalPar;
    private final ExecutorRuntime runtime;
    private final ParName name;

    private Par(GlobalPar globalPar, ParName name, ExecutorRuntime runtime) {
        this.globalPar = Objects.requireNonNull(globalPar, "globalPar cannot be null");
        this.runtime = Objects.requireNonNull(runtime, "runtime cannot be null");
        this.name = Objects.requireNonNull(name, "name cannot be null");
    }

    static Par forGlobal(GlobalPar globalPar, ParName name, ExecutorRuntime runtime) {
        return new Par(globalPar, name, runtime);
    }

    /** Returns the owning immutable GlobalPar. */
    public GlobalPar globalPar() {
        return globalPar;
    }

    /** Returns the logical name this entry is registered under. */
    public ParName name() {
        return name;
    }

    ExecutorRuntime runtime() {
        return runtime;
    }

    ExecutionPhaseHintFuture<Object> prepareGroupTask(
            Callable<Object> callable, MultiTaskContext unit, TaskExecutionContext taskContext) {
        return TaskSubmissions.prepare(
                taskContext, callable, globalPar.taskListenersFor(name), runtime.phaseObserver());
    }

    ExecutorIdentity executorIdentity() {
        return runtime.identity();
    }

    java.util.concurrent.Executor submissionExecutor() {
        return runtime.submissionExecutor();
    }

    /**
     * Executes a batch using the executor bound when the owning {@link GlobalPar} was built.
     *
     * <p>The supplied list is snapshotted only as task callables are created; callers must not
     * structurally mutate it while this method runs. A {@code null} or empty list returns an empty
     * result without submitting work. When invoked within another scoped task, the child batch
     * inherits cancellation and cannot outlive its parent's deadline. The selected executor never
     * changes per call and is not owned by this {@code Par}. Once the owning {@link GlobalPar} is
     * closed, this method throws {@link IllegalStateException} before submitting any task.
     *
     * @param list input elements, or {@code null} for an empty batch
     * @param function synchronous mapping function, run at most once for each submitted element
     * @param options immutable per-batch request; it cannot select an executor
     * @throws IllegalArgumentException if the options declare an inherited timeout and no scoped
     *     task encloses this call
     * @throws IllegalStateException if the owning GlobalPar has begun shutdown
     */
    public <T, R> TaskBatchResult<R> map(
            @Nullable List<T> list, Function<? super T, ? extends R> function, BatchOptions options) {
        Objects.requireNonNull(options, "options cannot be null");
        return globalPar.whileOpen(() -> mapWhileOpen(list, function, options));
    }

    private <T, R> TaskBatchResult<R> mapWhileOpen(
            @Nullable List<T> list, Function<? super T, ? extends R> function, BatchOptions options) {
        int taskCount = list == null ? 0 : list.size();
        TaskExecutionContext currentTask = TaskExecutionContext.current();
        if (!options.timeout().isPresent() && currentTask == null) {
            throw new IllegalArgumentException("no enclosing deadline to inherit; call timeout(Duration)");
        }
        MultiTaskContext parent = currentTask == null ? null : currentTask.multiTaskContext();
        TaskGraphObservationScope currentObservation = TaskGraphObservationScope.current();
        TaskGraphObservationScope observation = parent != null
                        && parent.taskGraphObservationScope() != null
                        && parent.taskGraphObservationScope().owner() == globalPar
                ? parent.taskGraphObservationScope()
                : parent == null && currentObservation != null && currentObservation.owner() == globalPar
                        ? currentObservation
                        : null;
        MultiTaskContext unit = MultiTaskContext.resolve(
                options.spec(), taskCount, parent, observation, runtime.identity(), name.value());
        return executeGlobal(list, item -> () -> function.apply(item), unit);
    }

    private <T, R> TaskBatchResult<R> executeGlobal(
            @Nullable List<T> list, Function<T, Callable<R>> callableMapper, MultiTaskContext unit) {
        if (list == null || list.isEmpty()) return emptyBatchResult();
        TaskEdge edge = new TaskEdge(
                unit.effectiveParallelism(),
                unit.taskType(),
                unit.executorIdentity(),
                unit.structuralParent() == null ? null : unit.structuralParent().executorIdentity(),
                unit.executorLabel(),
                unit.structuralParent() == null ? "NA" : unit.structuralParent().executorLabel(),
                list.size(),
                unit.remaining(),
                runtime.blockingRisk() == BlockingRisk.BOUNDED_PLATFORM_POOL);
        logForking(unit, edge);
        com.google.common.base.Ticker ticker = com.google.common.base.Ticker.systemTicker();
        List<ExecutionPhaseHintFuture<R>> tasks = java.util.stream.IntStream.range(0, list.size())
                .mapToObj(index -> TaskSubmissions.prepare(
                        new TaskExecutionContext(unit, index, ticker.read()),
                        callableMapper.apply(list.get(index)),
                        globalPar.taskListenersFor(name),
                        runtime.phaseObserver()))
                .collect(toImmutableList());
        TaskBatchResult<R> result = new SlidingWindowSubmitter<R>(
                        runtime.submissionExecutor(), unit, globalPar.submitterPool())
                .submitAll(tasks);
        unit.cancellationToken().bind(result.results(), result.submitCanceller(), globalPar.timeoutScheduler());
        globalPar.retainUntilComplete(result.results());
        return result;
    }

    /**
     * Records one parent-to-child unit edge. Unit IDs, rather than reusable task names, preserve
     * graph correctness when the same named operation is invoked concurrently.
     */
    private static void logForking(MultiTaskContext context, TaskEdge edge) {
        MultiTaskContext parent = context.structuralParent();
        TaskGraphObservationScope.logTaskPair(
                parent == null ? null : parent.unitId(),
                parent == null ? null : parent.name(),
                context.unitId(),
                context.name(),
                edge);
    }

    private static <T> TaskBatchResult<T> emptyBatchResult() {
        return TaskBatchResult.of(ImmutableList.of());
    }
}
