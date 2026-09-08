package io.github.huatalk.parallelinscope.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.github.huatalk.parallelinscope.context.SubmissionScope;
import io.github.huatalk.parallelinscope.scope.MultiTaskContext;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ConcurrentLimitExecutorBatchContextTest {
    @Test
    void installsBatchScopeForInitialAndSlidingWindowSubmissions() throws Exception {
        ConcurrentLinkedQueue<MultiTaskContext> submittedBatches = new ConcurrentLinkedQueue<>();
        ThreadPoolExecutor worker =
                new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()) {
                    @Override
                    public void execute(Runnable command) {
                        submittedBatches.add(SubmissionScope.currentBatch());
                        super.execute(command);
                    }
                };
        ListeningExecutorService workers = MoreExecutors.listeningDecorator(worker);
        ListeningExecutorService submitter = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        try {
            MultiTaskContext batch = context(2, 1, TaskType.IO_BOUND);
            ConcurrentLimitExecutor<Integer> executor = new ConcurrentLimitExecutor<>(workers, batch, submitter);

            assertThat(executor.submitAll(futures(() -> 1, () -> 2)).results())
                    .extracting(future -> future.get(1, TimeUnit.SECONDS))
                    .containsExactly(1, 2);
            await().atMost(1, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(submittedBatches).hasSize(2));
            assertThat(submittedBatches).containsOnly(batch);
            assertThat(SubmissionScope.currentBatch()).isNull();
        } finally {
            workers.shutdownNow();
            submitter.shutdownNow();
        }
    }

    @Test
    void emptyBatchCompletesWithoutSubmitting() {
        ListeningExecutorService workers = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListeningExecutorService submitter = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        try {
            ConcurrentLimitExecutor<Integer> executor =
                    new ConcurrentLimitExecutor<>(workers, context(0, 1, TaskType.IO_BOUND), submitter);
            assertThat(executor.submitAll(java.util.Collections.emptyList()).results())
                    .isEmpty();
        } finally {
            workers.shutdownNow();
            submitter.shutdownNow();
        }
    }

    @Test
    void submitsEveryTaskWhenWindowIsLarge() throws Exception {
        ListeningExecutorService workers = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(3));
        ListeningExecutorService submitter = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        try {
            ConcurrentLimitExecutor<Integer> executor =
                    new ConcurrentLimitExecutor<>(workers, context(3, 3, TaskType.IO_BOUND), submitter);
            assertThat(executor.submitAll(futures(() -> 1, () -> 2, () -> 3)).results())
                    .extracting(f -> f.get(1, TimeUnit.SECONDS))
                    .containsExactly(1, 2, 3);
        } finally {
            workers.shutdownNow();
            submitter.shutdownNow();
        }
    }

    @Test
    void cancellingSubmitterAbandonsRemainingPlaceholders() throws Exception {
        ListeningExecutorService workers = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListeningExecutorService submitter = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        CountDownLatch release = new CountDownLatch(1);
        try {
            ConcurrentLimitExecutor<Integer> executor =
                    new ConcurrentLimitExecutor<>(workers, context(3, 1, TaskType.IO_BOUND), submitter);
            io.github.huatalk.parallelinscope.scope.AsyncBatchResult<Integer> batch = executor.submitAll(futures(
                    () -> {
                        release.await(2, TimeUnit.SECONDS);
                        return 1;
                    },
                    () -> 2,
                    () -> 3));
            assertThat(batch.submitCanceller().cancel(true)).isTrue();
            release.countDown();
            for (com.google.common.util.concurrent.ListenableFuture<Integer> result : batch.results()) {
                try {
                    result.get(2, TimeUnit.SECONDS);
                } catch (java.util.concurrent.ExecutionException | java.util.concurrent.CancellationException ignored) {
                    // Abandoned placeholders may fail or cancel, but must not remain pending.
                }
                assertThat(result.isDone()).isTrue();
            }
        } finally {
            workers.shutdownNow();
            submitter.shutdownNow();
        }
    }

    @Test
    void ioBatchReportsEveryInitialSubmissionRejection() {
        ListeningExecutorService rejected = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListeningExecutorService submitter = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        rejected.shutdownNow();
        try {
            ConcurrentLimitExecutor<Integer> executor =
                    new ConcurrentLimitExecutor<>(rejected, context(3, 2, TaskType.IO_BOUND), submitter);
            io.github.huatalk.parallelinscope.scope.AsyncBatchResult<Integer> batch =
                    executor.submitAll(futures(() -> 1, () -> 2, () -> 3));
            assertThat(batch.results()).hasSize(3);
            for (com.google.common.util.concurrent.ListenableFuture<Integer> result : batch.results()) {
                assertThatThrownBy(result::get).isInstanceOf(java.util.concurrent.ExecutionException.class);
            }
        } finally {
            submitter.shutdownNow();
        }
    }

    @Test
    void cancelledPlaceholderStopsSlidingWindowAndCancelsLaterPlaceholders() throws Exception {
        ListeningExecutorService workers = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListeningExecutorService submitter = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        CountDownLatch release = new CountDownLatch(1);
        try {
            ConcurrentLimitExecutor<Integer> executor =
                    new ConcurrentLimitExecutor<>(workers, context(3, 1, TaskType.IO_BOUND), submitter);
            io.github.huatalk.parallelinscope.scope.AsyncBatchResult<Integer> batch = executor.submitAll(futures(
                    () -> {
                        release.await(2, TimeUnit.SECONDS);
                        return 1;
                    },
                    () -> 2,
                    () -> 3));
            assertThat(batch.results().get(1).cancel(true)).isTrue();
            release.countDown();
            assertThat(batch.results().get(0).get(2, TimeUnit.SECONDS)).isEqualTo(1);
            await().atMost(2, TimeUnit.SECONDS).until(batch.results().get(2)::isDone);
            assertThat(batch.results().get(2).isCancelled()).isTrue();
        } finally {
            workers.shutdownNow();
            submitter.shutdownNow();
        }
    }

    @Test
    void honorsBatchParallelism() throws Exception {
        ListeningExecutorService workers = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(3));
        ListeningExecutorService submitter = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        try {
            MultiTaskContext context = context(3, 1, TaskType.IO_BOUND);
            AtomicInteger active = new AtomicInteger();
            AtomicInteger maximum = new AtomicInteger();
            CountDownLatch release = new CountDownLatch(1);
            ConcurrentLimitExecutor<Integer> executor = new ConcurrentLimitExecutor<>(workers, context, submitter);

            assertThat(executor.submitAll(futures(
                                    () -> runTracked(active, maximum, release, 1),
                                    () -> runTracked(active, maximum, release, 2),
                                    () -> runTracked(active, maximum, release, 3)))
                            .results())
                    .hasSize(3);
            assertThat(awaitMaximum(maximum, 1)).isTrue();
            assertThat(maximum.get()).isEqualTo(1);
            release.countDown();
        } finally {
            workers.shutdownNow();
            submitter.shutdownNow();
        }
    }

    @Test
    void cpuBatchFallsBackToDirectExecutionAfterRejection() throws Exception {
        ListeningExecutorService rejected = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListeningExecutorService submitter = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        rejected.shutdownNow();
        try {
            ConcurrentLimitExecutor<Integer> executor =
                    new ConcurrentLimitExecutor<>(rejected, context(1, 1, TaskType.CPU_BOUND), submitter);
            assertThat(executor.submitAll(futures(() -> 7)).results().get(0).get())
                    .isEqualTo(7);
        } finally {
            submitter.shutdownNow();
        }
    }

    @Test
    void cpuInlineFallbackPublishesCompletionForSlidingWindow() throws Exception {
        ListeningExecutorService rejected = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListeningExecutorService submitter = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        rejected.shutdownNow();
        try {
            ConcurrentLimitExecutor<Integer> executor =
                    new ConcurrentLimitExecutor<>(rejected, context(2, 1, TaskType.CPU_BOUND), submitter);

            io.github.huatalk.parallelinscope.scope.AsyncBatchResult<Integer> batch =
                    executor.submitAll(futures(() -> 1, () -> 2));

            assertThat(batch.results())
                    .extracting(future -> future.get(1, TimeUnit.SECONDS))
                    .containsExactly(1, 2);
            assertThat(batch.submitCanceller().get(1, TimeUnit.SECONDS)).isEqualTo(1);
        } finally {
            submitter.shutdownNow();
        }
    }

    @Test
    void failedCpuInlineFallbackStillAdvancesSlidingWindow() throws Exception {
        ListeningExecutorService rejected = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        ListeningExecutorService submitter = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        rejected.shutdownNow();
        try {
            ConcurrentLimitExecutor<Integer> executor =
                    new ConcurrentLimitExecutor<>(rejected, context(2, 1, TaskType.CPU_BOUND), submitter);

            io.github.huatalk.parallelinscope.scope.AsyncBatchResult<Integer> batch = executor.submitAll(futures(
                    () -> {
                        throw new IllegalStateException("expected failure");
                    },
                    () -> 2));

            assertThatThrownBy(() -> batch.results().get(0).get(1, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class)
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThat(batch.results().get(1).get(1, TimeUnit.SECONDS)).isEqualTo(2);
            assertThat(batch.submitCanceller().get(1, TimeUnit.SECONDS)).isEqualTo(1);
        } finally {
            submitter.shutdownNow();
        }
    }

    @Test
    void slidingWindowRejectionPropagatesOriginalFailure() throws Exception {
        AtomicInteger submissions = new AtomicInteger();
        ExecutorService firstThenReject = new AbstractExecutorService() {
            private volatile boolean shutdown;

            @Override
            public void shutdown() {
                shutdown = true;
            }

            @Override
            public java.util.List<Runnable> shutdownNow() {
                shutdown = true;
                return java.util.Collections.emptyList();
            }

            @Override
            public boolean isShutdown() {
                return shutdown;
            }

            @Override
            public boolean isTerminated() {
                return shutdown;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) {
                return shutdown;
            }

            @Override
            public void execute(Runnable command) {
                if (submissions.getAndIncrement() == 0) command.run();
                else throw new RejectedExecutionException("full");
            }
        };
        ListeningExecutorService workers = MoreExecutors.listeningDecorator(firstThenReject);
        ListeningExecutorService submitter = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        try {
            ConcurrentLimitExecutor<Integer> executor =
                    new ConcurrentLimitExecutor<>(workers, context(2, 1, TaskType.IO_BOUND), submitter);
            io.github.huatalk.parallelinscope.scope.AsyncBatchResult<Integer> batch =
                    executor.submitAll(futures(() -> 1, () -> 2));

            assertThat(batch.results().get(0).get(1, TimeUnit.SECONDS)).isEqualTo(1);
            assertThatThrownBy(() -> batch.results().get(1).get(1, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class)
                    .hasCauseInstanceOf(RejectedExecutionException.class);
            assertThatThrownBy(() -> batch.submitCanceller().get(1, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class)
                    .hasCauseInstanceOf(RejectedExecutionException.class);
        } finally {
            workers.shutdownNow();
            submitter.shutdownNow();
        }
    }

    @SafeVarargs
    private static List<ExecutionPhaseHintFuture<Integer>> futures(Callable<Integer>... tasks) {
        return Arrays.stream(tasks)
                .map(task -> ExecutionPhaseHintFuture.create(task, phase -> {}))
                .collect(java.util.stream.Collectors.toList());
    }

    private static MultiTaskContext context(int tasks, int parallelism, TaskType type) {
        return MultiTaskContext.resolve(
                MultiTaskOptions.of("batch")
                        .parallelism(parallelism)
                        .taskType(type)
                        .timeout(Duration.ofSeconds(30))
                        .build(),
                tasks,
                null);
    }

    private static int runTracked(AtomicInteger active, AtomicInteger maximum, CountDownLatch release, int value)
            throws InterruptedException {
        int now = active.incrementAndGet();
        maximum.updateAndGet(previous -> Math.max(previous, now));
        try {
            release.await(2, TimeUnit.SECONDS);
            return value;
        } finally {
            active.decrementAndGet();
        }
    }

    private static boolean awaitMaximum(AtomicInteger maximum, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (maximum.get() < expected && System.nanoTime() < deadline) Thread.sleep(10);
        return maximum.get() >= expected;
    }
}
