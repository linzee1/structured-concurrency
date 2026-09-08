package io.github.huatalk.parallelinscope.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.github.huatalk.parallelinscope.spi.ExecutionPhase;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Tests the completion, cancellation, and queue-identity contracts of ListenableCompletionService.
 */
public class ListenableCompletionServiceTest {

    /** Verifies cancellation runs the observer once on the cancelling thread. */
    @Test
    public void cancellationRunsObserverOnCancellingThread() throws Exception {
        LinkedBlockingQueue<ListenableFuture<Integer>> completions = new LinkedBlockingQueue<>();
        AtomicInteger observations = new AtomicInteger();
        AtomicReference<Thread> observerThread = new AtomicReference<>();
        ListenableCompletionService<Integer> service =
                new ListenableCompletionService<>(command -> {}, completions, () -> {
                    observations.incrementAndGet();
                    observerThread.set(Thread.currentThread());
                });

        ListenableFuture<Integer> task = service.submit(() -> 1);
        Thread cancellingThread = Thread.currentThread();

        assertThat(task.cancel(false)).isTrue();
        assertThat(service.take()).isSameAs(task);
        assertThat(observations).hasValue(1);
        assertThat(observerThread).hasValue(cancellingThread);
        assertThat(task.cancel(false)).isFalse();
        assertThat(observations).hasValue(1);
    }

    /** Verifies cancellation after run starts is not classified as worker-queue garbage. */
    @Test
    public void runningCancellationDoesNotNotifyQueueCancellationObserver() throws Exception {
        AtomicInteger observations = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        ListenableCompletionService<Integer> service =
                new ListenableCompletionService<>(pool, new LinkedBlockingQueue<>(), observations::incrementAndGet);

        try {
            ListenableFuture<Integer> task = service.submit(() -> {
                started.countDown();
                while (true) {
                    try {
                        release.await();
                        return 1;
                    } catch (InterruptedException ignored) {
                        // Keep the task running until the test releases it.
                    }
                }
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(task.cancel(true)).isTrue();
            assertThat(observations).hasValue(0);
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    /** Verifies concurrent cancel and run never classify one task into both lifecycle branches. */
    @Test
    public void cancelAndRunRaceHasOneLifecycleClassification() throws Exception {
        for (int attempt = 0; attempt < 100; attempt++) {
            AtomicReference<Runnable> submitted = new AtomicReference<>();
            AtomicInteger observations = new AtomicInteger();
            AtomicInteger calls = new AtomicInteger();
            AtomicBoolean cancelled = new AtomicBoolean();
            ListenableCompletionService<Integer> service = new ListenableCompletionService<>(
                    submitted::set, new LinkedBlockingQueue<>(), observations::incrementAndGet);
            ListenableFuture<Integer> future = service.submit(calls::incrementAndGet);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(2);
            Thread runner = new Thread(() -> {
                awaitUninterruptibly(start);
                submitted.get().run();
                done.countDown();
            });
            Thread canceller = new Thread(() -> {
                awaitUninterruptibly(start);
                cancelled.set(future.cancel(false));
                done.countDown();
            });

            runner.start();
            canceller.start();
            start.countDown();
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(observations.get()).isBetween(0, 1);
            assertThat(calls.get()).isBetween(0, 1);
            assertThat(observations.get() + calls.get()).isLessThanOrEqualTo(1);
            if (!cancelled.get()) {
                assertThat(calls).hasValue(1);
            }
        }
    }

    /** Verifies futures release the callback once queue-residency classification is complete. */
    @Test
    public void terminalFutureReleasesQueuedCancellationObserver() throws Exception {
        AtomicReference<Runnable> submitted = new AtomicReference<>();
        Runnable observer = () -> {};
        ListenableCompletionService<Integer> service =
                new ListenableCompletionService<>(submitted::set, new LinkedBlockingQueue<>(), observer);
        ListenableFuture<Integer> completed = service.submit(() -> 1);
        submitted.get().run();

        assertThat(phaseObserver(completed)).isNotSameAs(observer);

        ListenableFuture<Integer> cancelled = service.submit(() -> 2);
        assertThat(cancelled.cancel(false)).isTrue();

        assertThat(phaseObserver(cancelled)).isNotSameAs(observer);

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ListenableFuture<Integer> running = service.submit(() -> {
            started.countDown();
            release.await();
            return 3;
        });
        Thread runner = new Thread(submitted.get());
        runner.start();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        assertThat(phaseObserver(running)).isNotSameAs(observer);

        release.countDown();
        runner.join(5000);
        assertThat(runner.isAlive()).isFalse();
    }

    /** Reads the phase observer field to verify reference release without relying on GC. */
    private static Object phaseObserver(ListenableFuture<?> future) throws Exception {
        Field field = ExecutionPhaseHintFuture.class.getDeclaredField("phaseObserver");
        field.setAccessible(true);
        return field.get(future);
    }

    /** Verifies that consumers can observe phases other than queued cancellation. */
    @Test
    public void phaseObserverReceivesExecutionHints() throws Exception {
        List<ExecutionPhase> completedPhases = new java.util.concurrent.CopyOnWriteArrayList<>();
        ExecutionPhaseHintFuture<Integer> completed = ExecutionPhaseHintFuture.create(() -> 1, completedPhases::add);

        completed.run();

        assertThat(completedPhases).containsExactly(ExecutionPhase.RUNNING, ExecutionPhase.TERMINAL);

        List<ExecutionPhase> cancelledPhases = new java.util.concurrent.CopyOnWriteArrayList<>();
        ExecutionPhaseHintFuture<Integer> cancelled = ExecutionPhaseHintFuture.create(() -> 2, cancelledPhases::add);

        assertThat(cancelled.cancel(false)).isTrue();
        assertThat(cancelledPhases).containsExactly(ExecutionPhase.CANCELLED_BEFORE_RUN);

        List<ExecutionPhase> runningCancellationPhases = new java.util.concurrent.CopyOnWriteArrayList<>();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutionPhaseHintFuture<Integer> running = ExecutionPhaseHintFuture.create(
                () -> {
                    started.countDown();
                    release.await();
                    return 3;
                },
                runningCancellationPhases::add);
        Thread runner = new Thread(running);
        runner.start();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        assertThat(running.cancel(true)).isTrue();
        release.countDown();
        runner.join(5000);
        assertThat(runner.isAlive()).isFalse();
        assertThat(runningCancellationPhases)
                .containsExactly(
                        ExecutionPhase.RUNNING, ExecutionPhase.CANCEL_REQUESTED_RUNNING, ExecutionPhase.TERMINAL);
    }

    /** Verifies observer failures do not alter task execution or cancellation. */
    @Test
    public void phaseObserverFailureDoesNotAlterFutureLifecycle() throws Exception {
        ExecutionPhaseHintFuture<Integer> completed = ExecutionPhaseHintFuture.create(() -> 1, phase -> {
            throw new IllegalStateException("observer failed");
        });

        completed.run();

        assertThat(completed.get()).isEqualTo(1);

        ExecutionPhaseHintFuture<Integer> cancelled = ExecutionPhaseHintFuture.create(() -> 2, phase -> {
            throw new IllegalStateException("observer failed");
        });

        assertThat(cancelled.cancel(false)).isTrue();
        assertThat(cancelled.isCancelled()).isTrue();
    }

    /**
     * Regression: a cancel that lands while the task is running must always surface
     * CANCEL_REQUESTED_RUNNING, even when run() has already finished the body and advanced the
     * phase to TERMINAL. Previously run() set TERMINAL unconditionally and afterDone() dropped the
     * signal when it observed TERMINAL, losing the phase under a narrow race.
     */
    @Test
    public void cancelWhileRunningAlwaysSurfacesCancelRequestedRunning() throws Exception {
        for (int attempt = 0; attempt < 200; attempt++) {
            List<ExecutionPhase> phases = new java.util.concurrent.CopyOnWriteArrayList<>();
            CountDownLatch bodyEntered = new CountDownLatch(1);
            CountDownLatch bodyRelease = new CountDownLatch(1);
            ExecutionPhaseHintFuture<Integer> future = ExecutionPhaseHintFuture.create(
                    () -> {
                        bodyEntered.countDown();
                        bodyRelease.await();
                        return 1;
                    },
                    phases::add);

            Thread runner = new Thread(future);
            runner.start();
            assertThat(bodyEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(future.cancel(true)).isTrue();
            bodyRelease.countDown();
            runner.join(5000);
            assertThat(runner.isAlive()).isFalse();

            assertThat(phases)
                    .as(
                            "attempt %s must report the cancel-while-running phase; got %s; isCancelled=%s; future=%s",
                            attempt, phases, future.isCancelled(), future)
                    .contains(ExecutionPhase.CANCEL_REQUESTED_RUNNING)
                    .contains(ExecutionPhase.TERMINAL);
        }
    }

    /** Waits for a test gate while preserving the thread's eventual interrupt status. */
    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Verifies that cancellation is visible on the exact runnable held by the executor queue. */
    @Test
    public void submittedFutureIsTheQueuedRunnableAndCanBePurged() throws Exception {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        ListeningExecutorService listeningPool = MoreExecutors.listeningDecorator(pool);
        ListenableCompletionService<Integer> service = new ListenableCompletionService<>(listeningPool);
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);

        try {
            service.submit(() -> {
                workerStarted.countDown();
                releaseWorker.await();
                return 0;
            });
            assertThat(workerStarted.await(5, TimeUnit.SECONDS)).isTrue();

            ListenableFuture<Integer> queued = service.submit(() -> 1);

            assertThat(pool.getQueue()).containsExactly((Runnable) queued);
            assertThat(queued.cancel(false)).isTrue();
            assertThat(service.take()).isSameAs(queued);

            pool.purge();
            assertThat(pool.getQueue()).isEmpty();
        } finally {
            releaseWorker.countDown();
            pool.shutdownNow();
        }
    }

    /** Verifies both submission forms and all completion-queue retrieval methods. */
    @Test
    public void callableAndRunnableResultsEnterTheSuppliedCompletionQueue() throws Exception {
        LinkedBlockingQueue<ListenableFuture<Integer>> completions = new LinkedBlockingQueue<>();
        ListenableCompletionService<Integer> service = new ListenableCompletionService<>(Runnable::run, completions);

        ListenableFuture<Integer> callable = service.submit(() -> 42);
        ListenableFuture<Integer> runnable = service.submit(() -> {}, 7);
        ListenableFuture<Integer> nullResult = service.submit(() -> {}, null);

        assertThat(service.take()).isSameAs(callable);
        assertThat(service.poll()).isSameAs(runnable);
        assertThat(service.poll(10, TimeUnit.MILLISECONDS)).isSameAs(nullResult);
        assertThat(service.poll()).isNull();
        assertThat(callable.get()).isEqualTo(42);
        assertThat(runnable.get()).isEqualTo(7);
        assertThat(nullResult.get()).isNull();
        assertThat(completions).isEmpty();
    }

    /** Verifies that rejected tasks are not reported as completed. */
    @Test
    public void rejectedSubmissionDoesNotEnterTheCompletionQueue() {
        Executor rejectingExecutor = command -> {
            throw new RejectedExecutionException("rejected");
        };
        ListenableCompletionService<Integer> service = new ListenableCompletionService<>(rejectingExecutor);

        assertThatThrownBy(() -> service.submit(() -> 1))
                .isInstanceOf(RejectedExecutionException.class)
                .hasMessage("rejected");
        assertThat(service.poll()).isNull();
    }
}
