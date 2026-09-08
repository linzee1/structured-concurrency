package io.github.huatalk.parallelinscope.scope;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.collect.ImmutableList;
import io.github.huatalk.parallelinscope.cancel.CancellationToken;
import io.github.huatalk.parallelinscope.context.TaskGraphObservationContext;
import io.github.huatalk.parallelinscope.context.graph.TaskEdge;
import io.github.huatalk.parallelinscope.internal.ConcurrentLimitExecutor;
import io.github.huatalk.parallelinscope.internal.ExecutionPhaseHintFuture;
import io.github.huatalk.parallelinscope.internal.TaskExecutionContext;
import io.github.huatalk.parallelinscope.internal.TaskSubmissions;
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
 *   <li>Resolution of {@link MultiTaskOptions} into a batch context
 *   <li>Scoped task preparation via {@link io.github.huatalk.parallelinscope.internal.TaskSubmissions}
 *   <li>Concurrency-limited submission via {@link ConcurrentLimitExecutor}
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
    private final String displayName;

    private Par(GlobalPar globalPar, String displayName, ExecutorRuntime runtime) {
        this.globalPar = Objects.requireNonNull(globalPar, "globalPar cannot be null");
        this.runtime = Objects.requireNonNull(runtime, "runtime cannot be null");
        this.displayName = Objects.requireNonNull(displayName, "displayName cannot be null");
    }

    static Par forGlobal(GlobalPar globalPar, String displayName, ExecutorRuntime runtime) {
        return new Par(globalPar, displayName, runtime);
    }

    /** Returns the owning immutable GlobalPar. */
    public GlobalPar globalPar() {
        return globalPar;
    }

    /** Returns this Par's diagnostic label. */
    public String displayName() {
        return displayName;
    }

    ExecutorRuntime runtime() {
        return runtime;
    }

    ExecutionPhaseHintFuture<Object> prepareGroupTask(
            Callable<Object> callable, MultiTaskContext batchContext, TaskExecutionContext taskContext) {
        return TaskSubmissions.prepare(
                taskContext,
                callable,
                globalPar.executionPolicyFor(displayName).taskListeners(),
                runtime.phaseObserver());
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
    public <T, R> AsyncBatchResult<R> map(
            @Nullable List<T> list, Function<? super T, ? extends R> function, MultiTaskOptions options) {
        Objects.requireNonNull(options, "options cannot be null");
        return globalPar.whileOpen(() -> mapWhileOpen(list, function, options));
    }

    private <T, R> AsyncBatchResult<R> mapWhileOpen(
            @Nullable List<T> list, Function<? super T, ? extends R> function, MultiTaskOptions options) {
        int taskCount = list == null ? 0 : list.size();
        TaskExecutionContext currentTask = TaskExecutionContext.current();
        if (!options.timeout().isPresent() && currentTask == null) {
            throw new IllegalArgumentException("no enclosing deadline to inherit; call timeout(Duration)");
        }
        MultiTaskContext parent = currentTask == null ? null : currentTask.batchContext();
        TaskGraphObservationContext currentObservation = TaskGraphObservationContext.current();
        TaskGraphObservationContext observation = parent != null
                        && parent.taskGraphObservationContext() != null
                        && parent.taskGraphObservationContext().owner() == globalPar
                ? parent.taskGraphObservationContext()
                : parent == null && currentObservation != null && currentObservation.owner() == globalPar
                        ? currentObservation
                        : null;
        MultiTaskContext batchContext =
                MultiTaskContext.resolve(options, taskCount, parent, observation, runtime.identity(), displayName);
        return executeGlobal(list, item -> () -> function.apply(item), batchContext);
    }

    private <T, R> AsyncBatchResult<R> executeGlobal(
            @Nullable List<T> list, Function<T, Callable<R>> callableMapper, MultiTaskContext batchContext) {
        if (list == null || list.isEmpty()) return emptyBatchResult();
        TaskEdge edge = new TaskEdge(
                batchContext.effectiveParallelism(),
                batchContext.taskType(),
                batchContext.executorIdentity(),
                batchContext.parent() == null ? null : batchContext.parent().executorIdentity(),
                batchContext.parLabel(),
                batchContext.parent() == null ? "NA" : batchContext.parent().parLabel(),
                list.size(),
                batchContext.remaining().toMillis(),
                runtime.blockingRisk() == BlockingRisk.BOUNDED_PLATFORM_POOL);
        logForking(batchContext, edge);
        com.google.common.base.Ticker ticker = com.google.common.base.Ticker.systemTicker();
        List<ExecutionPhaseHintFuture<R>> tasks = java.util.stream.IntStream.range(0, list.size())
                .mapToObj(index -> TaskSubmissions.prepare(
                        new TaskExecutionContext(batchContext, index, ticker.read()),
                        callableMapper.apply(list.get(index)),
                        globalPar.executionPolicyFor(displayName).taskListeners(),
                        runtime.phaseObserver()))
                .collect(toImmutableList());
        AsyncBatchResult<R> result = new ConcurrentLimitExecutor<R>(
                        runtime.submissionExecutor(), batchContext, globalPar.submitterPool())
                .submitAll(tasks);
        batchContext.cancellationToken().bind(result.results(), result.submitCanceller(), globalPar.timeoutScheduler());
        globalPar.retainUntilComplete(result.results());
        return result;
    }

    /**
     * Records one parent-to-child batch edge. Batch IDs, rather than reusable task names, preserve
     * graph correctness when the same named operation is invoked concurrently.
     */
    private static void logForking(MultiTaskContext context, TaskEdge edge) {
        MultiTaskContext parent = context.parent();
        TaskGraphObservationContext.logTaskPair(
                parent == null ? null : parent.batchId(),
                parent == null ? null : parent.taskName(),
                context.batchId(),
                context.taskName(),
                edge);
    }

    private static <T> AsyncBatchResult<T> emptyBatchResult() {
        return AsyncBatchResult.of(ImmutableList.of());
    }
}
