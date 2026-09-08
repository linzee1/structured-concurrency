package io.github.huatalk.parallelinscope.internal;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import io.github.huatalk.parallelinscope.context.SubmissionScope;
import io.github.huatalk.parallelinscope.scope.MultiTaskContext;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Sliding-window concurrency limiter for task execution.
 *
 * <p>Implements a "submit one when one completes" pattern:
 *
 * <ol>
 *   <li>Submits an initial batch equal to {@code parallelism}
 *   <li>Uses a blocking queue populated by {@link ListenableCompletionService} to detect completion
 *       events
 *   <li>Fills freed slots incrementally with remaining tasks
 * </ol>
 *
 * @param <V> the result type of tasks
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public class SlidingWindowSubmitter<V> {

    private final ListenableCompletionService<V> cs;
    private final BlockingQueue<ListenableFuture<V>> blockingQueue = new LinkedBlockingQueue<>();
    private final MultiTaskContext batchContext;
    private final ListeningExecutorService submitterPool;

    /** Creates a submitter for the new immutable batch runtime context. */
    public SlidingWindowSubmitter(
            ListeningExecutorService pool, MultiTaskContext batchContext, ListeningExecutorService submitterPool) {
        this.batchContext = Objects.requireNonNull(batchContext, "batchContext cannot be null");
        this.submitterPool = Objects.requireNonNull(submitterPool, "submitterPool cannot be null");
        this.cs = new ListenableCompletionService<>(pool, blockingQueue);
    }

    /**
     * Submits all tasks and returns the batch result immediately.
     *
     * <p>Each returned future is the exact {@link ExecutionPhaseHintFuture} passed in: the caller
     * prepares tasks via {@link TaskSubmissions}, and this executor only coordinates when each
     * prepared future enters the worker pool.
     *
     * @param tasks prepared task futures to execute
     * @return TaskBatchResult containing individual task futures
     */
    public TaskBatchResult<V> submitAll(List<? extends ExecutionPhaseHintFuture<V>> tasks) {
        if (tasks.isEmpty()) {
            return TaskBatchResult.of(ImmutableList.of());
        }

        ImmutableList.Builder<ListenableFuture<V>> resultBuilder = ImmutableList.builderWithExpectedSize(tasks.size());

        int start = Math.min(tasks.size(), parallelism());

        // Submit initial batch
        for (int i = 0; i < start; i++) {
            try {
                resultBuilder.add(fallbackSubmit(tasks, i));
            } catch (RuntimeException failure) {
                resultBuilder.add(Futures.immediateFailedFuture(failure));
                for (int pending = i + 1; pending < tasks.size(); pending++) {
                    resultBuilder.add(Futures.immediateFailedFuture(failure));
                }
                return TaskBatchResult.of(resultBuilder.build());
            }
        }

        int remaining = tasks.size() - start;
        if (remaining <= 0) {
            ImmutableList<ListenableFuture<V>> results = resultBuilder.build();
            return TaskBatchResult.of(results);
        }

        // Async submit remaining tasks
        List<SettableFuture<V>> others = IntStream.range(0, remaining)
                .mapToObj(ignore -> SettableFuture.<V>create())
                .collect(toImmutableList());

        ImmutableList<ListenableFuture<V>> results =
                resultBuilder.addAll(others).build();
        AtomicInteger nextIndex = new AtomicInteger(start);
        ListenableFuture<?> submittingFuture = submitterPool.submit(() -> submitRemaining(tasks, results, nextIndex));
        // A cancellation may win before the submitter thread starts. In that case the callable
        // never gets a chance to abandon its placeholders, so close them from the cancellation
        // callback as well. The submitter loop remains responsible for normal interruption.
        submittingFuture.addListener(
                () -> {
                    if (submittingFuture.isCancelled()) {
                        abandonRemaining(
                                results,
                                nextIndex.get(),
                                new InterruptedException("remaining task submission cancelled"));
                    }
                },
                directExecutor());

        return TaskBatchResult.of(submittingFuture, results);
    }

    private ListenableFuture<V> fallbackSubmit(List<? extends ExecutionPhaseHintFuture<V>> tasks, int i) {
        ExecutionPhaseHintFuture<V> task = tasks.get(i);
        MultiTaskContext previous = SubmissionScope.install(batchContext);
        try {
            return TaskType.CPU_BOUND == taskType() ? cs.submitOrRunInline(task) : cs.submit(task);
        } finally {
            SubmissionScope.restore(previous);
        }
    }

    private int parallelism() {
        return batchContext.effectiveParallelism();
    }

    private TaskType taskType() {
        return batchContext.taskType();
    }

    private int submitRemaining(
            List<? extends ExecutionPhaseHintFuture<V>> tasks,
            List<ListenableFuture<V>> result,
            AtomicInteger nextIndex) {
        int index = nextIndex.get();
        int size = tasks.size();
        int submitted = 0;
        while (index < size) {
            nextIndex.set(index);
            ListenableFuture<V> completed;
            try {
                completed = blockingQueue.take();
            } catch (InterruptedException e) {
                abandonRemaining(result, index, e);
                Thread.currentThread().interrupt();
                return submitted;
            }
            if (completed.isCancelled() || result.get(index).isCancelled()) {
                abandonRemaining(result, index, null);
                return submitted;
            }
            if (Thread.currentThread().isInterrupted()) {
                abandonRemaining(
                        result,
                        index,
                        new InterruptedException("submitter thread interrupted while scheduling remaining tasks"));
                Thread.currentThread().interrupt();
                return submitted;
            }
            try {
                ((SettableFuture<V>) result.get(index)).setFuture(fallbackSubmit(tasks, index));
            } catch (RuntimeException e) {
                abandonRemaining(result, index, e);
                throw e;
            }
            submitted++;
            index++;
            nextIndex.set(index);
        }
        return submitted;
    }

    /**
     * Completes every future that will never receive a submission so the batch always reaches a
     * terminal state. Direct placeholder cancellation produces {@code CANCELLED}; an interrupted
     * submitter or rejected submission records its cause. Without this cleanup, {@link
     * Futures#allAsList} could wait forever and hide the reason in {@link TaskBatchResult#report()}.
     *
     * @param result the batch futures
     * @param fromIndex the first never-submitted future index (inclusive)
     * @param reason the failure reported for the abandoned futures, or {@code null} to cancel them
     *     when the batch is already being canceled
     */
    private static <V> void abandonRemaining(
            List<ListenableFuture<V>> result, int fromIndex, @Nullable Throwable reason) {
        for (int i = fromIndex; i < result.size(); i++) {
            SettableFuture<V> future = (SettableFuture<V>) result.get(i);
            if (reason != null) {
                future.setException(reason);
            } else {
                future.cancel(true);
            }
        }
    }
}
