package io.github.huatalk.parallelinscope.internal;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import io.github.huatalk.parallelinscope.spi.ExecutionPhase;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link CompletionService} implementation backed by Guava {@link ListenableFutureTask} instances.
 *
 * <p>Each submitted task is both the future returned to the caller and the runnable passed to the
 * executor. Completion listeners add that same object to the completion queue, so cancellation is
 * directly visible to queue maintenance such as {@code ThreadPoolExecutor.purge()}.
 *
 * @param <V> the result type of submitted tasks
 */
final class ListenableCompletionService<V> implements CompletionService<V> {

    private static final Consumer<ExecutionPhase> NOOP = phase -> {};

    private final Executor executor;
    private final BlockingQueue<ListenableFuture<V>> completionQueue;
    private final Consumer<? super ExecutionPhase> phaseObserver;

    /**
     * Creates a completion service with an unbounded completion queue.
     *
     * @param executor executor used to run submitted tasks
     */
    ListenableCompletionService(Executor executor) {
        this(executor, new LinkedBlockingQueue<>(), NOOP);
    }

    /**
     * Creates a completion service using the supplied completion queue.
     *
     * @param executor executor used to run submitted tasks
     * @param completionQueue queue that receives completed futures
     */
    ListenableCompletionService(Executor executor, BlockingQueue<ListenableFuture<V>> completionQueue) {
        this(executor, completionQueue, NOOP);
    }

    /**
     * Creates a completion service that observes execution-phase hints.
     *
     * @param executor executor used to run submitted tasks
     * @param completionQueue queue that receives completed futures
     * @param phaseObserver phase hint consumer
     */
    ListenableCompletionService(
            Executor executor,
            BlockingQueue<ListenableFuture<V>> completionQueue,
            Consumer<? super ExecutionPhase> phaseObserver) {
        this.executor = Objects.requireNonNull(executor);
        this.completionQueue = Objects.requireNonNull(completionQueue);
        this.phaseObserver = Objects.requireNonNull(phaseObserver);
    }

    /**
     * Creates a completion service that reports cancellation of submitted tasks.
     *
     * @param executor executor used to run submitted tasks
     * @param completionQueue queue that receives completed futures
     * @param queuedCancellationObserver callback for tasks canceled before run
     */
    ListenableCompletionService(
            Executor executor,
            BlockingQueue<ListenableFuture<V>> completionQueue,
            Runnable queuedCancellationObserver) {
        this(executor, completionQueue, phaseObserverFor(queuedCancellationObserver));
    }

    private static Consumer<ExecutionPhase> phaseObserverFor(Runnable queuedCancellationObserver) {
        Objects.requireNonNull(queuedCancellationObserver);
        return phase -> {
            if (phase == ExecutionPhase.CANCELLED_BEFORE_RUN) {
                queuedCancellationObserver.run();
            }
        };
    }

    /**
     * Submits a value-producing task and returns the exact future passed to the executor.
     *
     * @param task task to execute
     * @return the submitted listenable future
     */
    @Override
    public ListenableFuture<V> submit(Callable<V> task) {
        return submit(ExecutionPhaseHintFuture.create(task, phaseObserver));
    }

    /**
     * Submits an already-prepared future and returns the exact future passed to the executor.
     *
     * @param future prepared task future
     * @return the submitted listenable future
     */
    ListenableFuture<V> submit(ExecutionPhaseHintFuture<V> future) {
        future.addListener(() -> completionQueue.add(future), directExecutor());
        executor.execute(future);
        return future;
    }

    /**
     * Submits a value-producing task and runs it inline when the executor rejects it.
     *
     * @param task task to execute
     * @return the submitted or inline-executed listenable future
     */
    ListenableFuture<V> submitOrRunInline(Callable<V> task) {
        return submitOrRunInline(ExecutionPhaseHintFuture.create(task, phaseObserver));
    }

    /**
     * Submits an already-prepared future and runs it inline when the executor rejects it.
     *
     * <p>The returned future is registered with the completion queue before either execution path
     * begins. This preserves the completion-service contract for callers that use completion
     * events to drive a sliding submission window.
     *
     * @param future prepared task future
     * @return the submitted or inline-executed listenable future
     */
    ListenableFuture<V> submitOrRunInline(ExecutionPhaseHintFuture<V> future) {
        future.addListener(() -> completionQueue.add(future), directExecutor());
        try {
            executor.execute(future);
        } catch (RejectedExecutionException rejected) {
            directExecutor().execute(future);
        }
        return future;
    }

    /**
     * Submits a runnable and returns the exact future passed to the executor.
     *
     * @param task task to execute
     * @param result value returned after successful execution, possibly {@code null}
     * @return the submitted listenable future
     */
    @Override
    public ListenableFuture<V> submit(Runnable task, @Nullable V result) {
        return submit(ExecutionPhaseHintFuture.create(task, result, phaseObserver));
    }

    /**
     * Waits for and removes the next completed future.
     *
     * @return the next completed future
     * @throws InterruptedException if interrupted while waiting
     */
    @Override
    public ListenableFuture<V> take() throws InterruptedException {
        return completionQueue.take();
    }

    /**
     * Removes and returns the next completed future when one is immediately available.
     *
     * @return the next completed future, or {@code null} when the queue is empty
     */
    @Override
    public ListenableFuture<V> poll() {
        return completionQueue.poll();
    }

    /**
     * Waits up to the supplied timeout for the next completed future.
     *
     * @param timeout maximum time to wait
     * @param unit unit of the timeout
     * @return the next completed future, or {@code null} when the timeout expires
     * @throws InterruptedException if interrupted while waiting
     */
    @Override
    public ListenableFuture<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return completionQueue.poll(timeout, unit);
    }
}
