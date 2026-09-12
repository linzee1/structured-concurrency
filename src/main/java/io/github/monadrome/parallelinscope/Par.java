package io.github.monadrome.parallelinscope;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
 *   <li>Scoped task preparation via {@code TaskSubmissions}
 *   <li>Concurrency-limited submission via {@code SlidingWindowSubmitter}
 *   <li>Parent-child {@link CancellationToken} chaining
 *   <li>Late binding for timeout and fail-fast cancellation
 *   <li>Heuristic cleanup of canceled queued tasks
 * </ul>
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public final class Par {

    /** Null-object submission canceller: a single task carries no submission pipeline to stop. */
    private static final ListenableFuture<Void> NO_SUBMISSION = Futures.immediateVoidFuture();

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
     * <p>The supplied elements are snapshotted on entry unless the collection is already a {@link
     * List}, in which case callers must not structurally mutate it while this method runs. A
     * {@code null} or empty collection returns an empty result without submitting work. When
     * invoked within another scoped task, the child batch inherits cancellation and cannot outlive
     * its parent's deadline. The selected executor never changes per call and is not owned by this
     * {@code Par}. Once the owning {@link GlobalPar} is closed, this method throws {@link
     * IllegalStateException} before submitting any task.
     *
     * @param elements input elements, or {@code null} for an empty batch
     * @param function synchronous mapping function, run at most once for each submitted element
     * @param options immutable per-batch request; it cannot select an executor
     * @throws IllegalArgumentException if the options declare an inherited timeout and no scoped
     *     task encloses this call
     * @throws IllegalStateException if the owning GlobalPar has begun shutdown
     */
    public <T, R> TaskBatchResult<R> map(
            @Nullable Collection<T> elements, Function<? super T, ? extends R> function, BatchOptions options) {
        Objects.requireNonNull(options, "options cannot be null");
        return globalPar.whileOpen(() -> mapWhileOpen(elements, function, options));
    }

    /**
     * Submits one scoped task to the executor bound when the owning {@link GlobalPar} was built.
     *
     * <p>This is the unary entry point of the same pipeline {@link #map} uses: the task gets its
     * own child {@link CancellationToken} with the resolved deadline, a TTL snapshot taken on this
     * thread, task-listener notification, and structured cancellation from any enclosing scope. A
     * deadline that expires before the task starts never enters user code.
     *
     * @param taskName the task name reported on the returned future
     * @param task the task body
     * @param options immutable per-task request; it cannot select an executor
     * @return the task's future view, carrying its name and cancellation attribution
     * @throws IllegalArgumentException if the options declare an inherited timeout and no scoped
     *     task encloses this call
     * @throws IllegalStateException if the owning GlobalPar has begun shutdown
     */
    public <T> TaskFuture<T> submit(String taskName, Callable<T> task, TaskOptions options) {
        Objects.requireNonNull(task, "task cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
        return globalPar.whileOpen(() -> submitWhileOpen(taskName, task, options));
    }

    private <T> TaskFuture<T> submitWhileOpen(String taskName, Callable<T> task, TaskOptions options) {
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
                options.spec(taskName), 1, parent, observation, runtime.identity(), name.value());
        if (observation != null) {
            TaskEdge edge = new TaskEdge(
                    1,
                    unit.taskType(),
                    unit.executorIdentity(),
                    parent == null ? null : parent.executorIdentity(),
                    unit.executorLabel(),
                    parent == null ? "NA" : parent.executorLabel(),
                    1,
                    unit.remaining(),
                    runtime.blockingRisk() == BlockingRisk.BOUNDED_PLATFORM_POOL);
            logForking(unit, edge);
        }
        TaskExecutionContext taskContext = new TaskExecutionContext(
                unit, 0, com.google.common.base.Ticker.systemTicker().read());
        ExecutionPhaseHintFuture<T> future =
                TaskSubmissions.prepare(taskContext, task, globalPar.taskListenersFor(name), runtime.phaseObserver());
        Task<T> view = Task.of(unit.name(), unit.cancellationToken(), future);
        // Bind before submitting: a deadline expiring during submission cancels the prepared
        // future, whose phase claim then never lets it enter user code.
        ListenableFuture<?> completion = unit.cancellationToken()
                .bind(Collections.singletonList(future), NO_SUBMISSION, globalPar.timeoutScheduler());
        globalPar.retainUntilComplete(completion);
        TaskSubmissions.submitScoped(future, unit, runtime.submissionExecutor(), unit.taskType() == TaskType.CPU_BOUND);
        return view;
    }

    private <T, R> TaskBatchResult<R> mapWhileOpen(
            @Nullable Collection<T> elements, Function<? super T, ? extends R> function, BatchOptions options) {
        int taskCount = elements == null ? 0 : elements.size();
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
        return executeGlobal(elements, item -> () -> function.apply(item), unit);
    }

    @SuppressWarnings("unchecked")
    private <T, R> TaskBatchResult<R> executeGlobal(
            @Nullable Collection<T> elements, Function<T, Callable<R>> callableMapper, MultiTaskContext unit) {
        if (elements == null || elements.isEmpty()) return emptyBatchResult();
        List<T> list = elements instanceof List ? (List<T>) elements : new ArrayList<>(elements);
        // Graph bookkeeping only pays off when a request-level observation scope is recording;
        // skip the edge allocation and remaining() read on the common unobserved path.
        if (TaskGraphObservationScope.current() != null) {
            TaskEdge edge = new TaskEdge(
                    unit.effectiveParallelism(),
                    unit.taskType(),
                    unit.executorIdentity(),
                    unit.structuralParent() == null
                            ? null
                            : unit.structuralParent().executorIdentity(),
                    unit.executorLabel(),
                    unit.structuralParent() == null
                            ? "NA"
                            : unit.structuralParent().executorLabel(),
                    list.size(),
                    unit.remaining(),
                    runtime.blockingRisk() == BlockingRisk.BOUNDED_PLATFORM_POOL);
            logForking(unit, edge);
        }
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
        ListenableFuture<?> completion =
                unit.cancellationToken().bind(result.results(), result.submitCanceller(), globalPar.timeoutScheduler());
        globalPar.retainUntilComplete(completion);
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
