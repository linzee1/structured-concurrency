package io.github.huatalk.parallelinscope.internal;

import com.alibaba.ttl.TtlCallable;
import io.github.huatalk.parallelinscope.context.SubmissionScope;
import io.github.huatalk.parallelinscope.scope.MultiTaskContext;
import io.github.huatalk.parallelinscope.spi.ExecutionPhase;
import io.github.huatalk.parallelinscope.spi.TaskListener;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Shared single-task preparation and submission used by both entry points.
 *
 * <p>{@code Par.map} and {@code TaskGroup} must not each grow their own copy of the
 * scoped-task pipeline. This class owns the two pieces that are identical for every task:
 *
 * <ul>
 *   <li>Wrapping a user {@link Callable} with {@link ScopedCallable} lifecycle instrumentation and
 *       a TTL snapshot, and presenting it as an {@link ExecutionPhaseHintFuture}
 *   <li>Submitting a prepared future inside the {@link SubmissionScope} of its batch, with the
 *       CPU-bound inline fallback on executor rejection
 * </ul>
 *
 * <p>The entry points keep their distinct topologies on top of this: the batch path drives a
 * sliding window through {@link SlidingWindowSubmitter}, while the task group freezes every
 * prepared member before submitting any of them.
 */
public final class TaskSubmissions {

    private TaskSubmissions() {}

    /**
     * Wraps {@code callable} with {@link ScopedCallable} instrumentation and captures the current
     * thread's TTL context for replay on the worker thread.
     */
    public static <V> Callable<V> wrapScoped(
            TaskExecutionContext taskContext, Callable<V> callable, List<TaskListener> taskListeners) {
        return TtlCallable.get(new ScopedCallable<>(taskContext, callable, taskListeners), true, true);
    }

    /**
     * Prepares one scoped task as an {@link ExecutionPhaseHintFuture}. The returned future is not
     * running yet; the caller decides when and where to submit it.
     *
     * @param taskContext per-task execution context carrying the batch and task index
     * @param callable user task
     * @param taskListeners SPI listeners notified when the task completes
     * @param phaseObserver consumer of execution-phase hints for queue maintenance
     * @return the prepared future, still in {@code SUBMITTED} phase
     */
    public static <V> ExecutionPhaseHintFuture<V> prepare(
            TaskExecutionContext taskContext,
            Callable<V> callable,
            List<TaskListener> taskListeners,
            Consumer<? super ExecutionPhase> phaseObserver) {
        return ExecutionPhaseHintFuture.create(wrapScoped(taskContext, callable, taskListeners), phaseObserver);
    }

    /**
     * Submits a prepared future to {@code executor} with the unit's {@link SubmissionScope}
     * installed, so enqueue policies see the submitting unit. A rejected CPU-bound task runs
     * inline; any other rejection fails the future with a {@link SubmissionException} without
     * running user code.
     */
    public static void submitScoped(
            ExecutionPhaseHintFuture<?> future, MultiTaskContext unit, Executor executor, boolean cpuBound) {
        MultiTaskContext previous = SubmissionScope.install(unit);
        try {
            future.submitPrepared(executor, cpuBound);
        } finally {
            SubmissionScope.restore(previous);
        }
    }
}
