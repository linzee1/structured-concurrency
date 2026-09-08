package io.github.huatalk.parallelinscope.scope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huatalk.parallelinscope.internal.ScopedCallable;
import io.github.huatalk.parallelinscope.internal.TaskExecutionContext;
import io.github.huatalk.parallelinscope.spi.TaskListener;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Behavior pins for small scope primitives: {@link ExecutorIdentity} identity semantics,
 * {@link GlobalExecutionPolicy} defaults, {@link BatchExecutionContext#resolve} boundary matrix,
 * {@link GlobalPar} topology shutdown states with its scheduler adapter, and {@link ScopedCallable}
 * timing bookkeeping.
 */
class ScopePrimitivesTest {

    // ==================== ExecutorIdentity ====================

    @Test
    void executorIdentityBindsToTheExactSuppliedObject() {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            ExecutorIdentity identity = new ExecutorIdentity(pool);
            assertThat(identity.hashCode()).isEqualTo(System.identityHashCode(pool));
            assertThat(identity.suppliedExecutor()).isSameAs(pool);
            assertThat(identity).isEqualTo(new ExecutorIdentity(pool));
            assertThat(identity).hasSameHashCodeAs(new ExecutorIdentity(pool));

            ExecutorService otherPool = Executors.newCachedThreadPool();
            try {
                assertThat(identity).isNotEqualTo(new ExecutorIdentity(otherPool));
            } finally {
                otherPool.shutdownNow();
            }
            assertThat(identity.toString())
                    .contains("@")
                    .contains(pool.getClass().getSimpleName());
            assertThatThrownBy(() -> new ExecutorIdentity(null)).isInstanceOf(NullPointerException.class);
        } finally {
            pool.shutdownNow();
        }
    }

    // ==================== GlobalExecutionPolicy ====================

    @Test
    void executionPolicyDefaultsTimeoutAndSnapshotSemantics() {
        GlobalExecutionPolicy policy = GlobalExecutionPolicy.builder().build();
        assertThat(policy.defaultTimeoutMillis()).isEqualTo(60_000L);
        assertThat(policy.taskListeners()).isEmpty();

        GlobalExecutionPolicy custom =
                GlobalExecutionPolicy.builder().defaultTimeoutMillis(250L).build();
        assertThat(custom.defaultTimeoutMillis()).isEqualTo(250L);

        GlobalExecutionPolicy.Builder builder = GlobalExecutionPolicy.builder();
        assertThatThrownBy(() -> builder.defaultTimeoutMillis(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.defaultTimeoutMillis(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.taskListener(null)).isInstanceOf(NullPointerException.class);

        TaskListener listener = event -> {};
        GlobalExecutionPolicy snapshotted =
                GlobalExecutionPolicy.builder().taskListener(listener).build();
        assertThat(snapshotted.taskListeners()).containsExactly(listener);
        assertThatThrownBy(() -> snapshotted.taskListeners().add(listener))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // ==================== BatchExecutionContext.resolve ====================

    private static BatchExecutionContext resolve(
            int parallelism, Duration timeout, int taskCount, BatchExecutionContext parent) {
        BatchExecutionOptions.Builder options = BatchExecutionOptions.of("batch");
        if (parallelism > 0) {
            options.parallelism(parallelism);
        }
        if (timeout != null) {
            options.timeout(timeout);
        }
        return BatchExecutionContext.resolve(
                GlobalExecutionPolicy.builder().build(), options.build(), taskCount, parent);
    }

    @Test
    void resolveNormalizesParallelismAgainstTaskCount() {
        assertThat(resolve(0, null, 4, null).effectiveParallelism()).isEqualTo(4);
        assertThat(resolve(-1, null, 3, null).effectiveParallelism()).isEqualTo(3);
        assertThat(resolve(9, null, 3, null).effectiveParallelism()).isEqualTo(3);
        assertThat(resolve(2, null, 5, null).effectiveParallelism()).isEqualTo(2);
        assertThat(resolve(2, null, 5, null).taskCount()).isEqualTo(5);
        assertThatThrownBy(() -> resolve(1, null, -1, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void resolveAppliesExplicitTimeoutAndOverflowGuard() {
        long before = System.nanoTime();
        BatchExecutionContext timed = resolve(0, Duration.ofMillis(150), 1, null);
        long deadline = timed.deadlineNanos();
        long expectedLow = before + TimeUnit.MILLISECONDS.toNanos(140);
        long expectedHigh = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(160);
        assertThat(deadline).isBetween(expectedLow, expectedHigh);
        assertThat(timed.remaining().toMillis()).isLessThanOrEqualTo(160);
        assertThat(timed.remaining()).isGreaterThanOrEqualTo(Duration.ZERO);

        BatchExecutionContext overflow = resolve(0, Duration.ofNanos(Long.MAX_VALUE), 1, null);
        assertThat(overflow.deadlineNanos()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void childDeadlineNeverExceedsParentDeadline() {
        BatchExecutionContext parent = resolve(0, Duration.ofMillis(50), 1, null);
        BatchExecutionContext child = resolve(0, Duration.ofHours(10), 1, parent);
        assertThat(child.deadlineNanos()).isLessThanOrEqualTo(parent.deadlineNanos());
        assertThat(child.parent()).isSameAs(parent);
        assertThat(child.cancellationToken()).isNotSameAs(parent.cancellationToken());
    }

    @Test
    void resolveRejectsNullPolicyAndOptions() {
        BatchExecutionOptions options = BatchExecutionOptions.of("x").build();
        assertThatThrownBy(() -> BatchExecutionContext.resolve(null, options, 1, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> BatchExecutionContext.resolve(
                        GlobalExecutionPolicy.builder().build(), null, 1, null))
                .isInstanceOf(NullPointerException.class);
    }

    // ==================== ScopedCallable timing ====================

    @Test
    void scopedCallableRecordsPositiveWaitAndExecutionDurations() throws Exception {
        BatchExecutionContext context = resolve(0, Duration.ofSeconds(30), 1, null);
        ScopedCallable<Integer> callable = new ScopedCallable<>(
                task(context, 0),
                () -> {
                    Thread.sleep(4);
                    return 42;
                },
                java.util.Collections.emptyList());

        Thread.sleep(6); // Simulate queue wait between construction and start.
        assertThat(callable.call()).isEqualTo(42);
        assertThat(callable.waitTime()).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(4));
        assertThat(callable.executionTime()).isGreaterThan(0L);
        assertThat(callable.totalTime()).isEqualTo(callable.waitTime() + callable.executionTime());
        assertThat(callable.getCancellationToken()).isNotNull();

        ScopedCallable<Integer> unlabelled = new ScopedCallable<>(task(context, 0), () -> 1, null);
        unlabelled.call();
        assertThat(unlabelled.getExecutorName()).isNotEmpty();
    }

    @Test
    void scopedCallableExecutorNameUsesParLabelElseNA() throws Exception {
        ExecutorService supplied = Executors.newSingleThreadExecutor();
        try {
            ExecutorIdentity identity = new ExecutorIdentity(supplied);
            BatchExecutionContext labelled = BatchExecutionContext.resolve(
                    GlobalExecutionPolicy.builder().build(),
                    BatchExecutionOptions.of("n").build(),
                    1,
                    null,
                    null,
                    identity,
                    "par-label");
            ScopedCallable<String> labelledCall =
                    new ScopedCallable<>(task(labelled, 0), () -> "ok", java.util.Collections.emptyList());
            assertThat(labelledCall.getExecutorName()).isEqualTo("par-label");

            BatchExecutionContext anonymous = BatchExecutionContext.resolve(
                    GlobalExecutionPolicy.builder().build(),
                    BatchExecutionOptions.of("n").build(),
                    1,
                    null,
                    null,
                    identity,
                    null);
            ScopedCallable<String> anonymousCall =
                    new ScopedCallable<>(task(anonymous, 0), () -> "ok", java.util.Collections.emptyList());
            assertThat(anonymousCall.getExecutorName()).isEqualTo("NA");

            assertThatThrownBy(() -> new ScopedCallable<>(null, () -> "ok", java.util.Collections.emptyList()))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> new ScopedCallable<>(task(labelled, 0), null, java.util.Collections.emptyList()))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> new ScopedCallable<>(null, () -> "ok", java.util.Collections.emptyList()))
                    .isInstanceOf(NullPointerException.class);
        } finally {
            supplied.shutdownNow();
        }
    }

    private static TaskExecutionContext task(BatchExecutionContext context, int index) {
        return new TaskExecutionContext(
                context, index, com.google.common.base.Ticker.systemTicker().read());
    }

    // ==================== GlobalPar lifecycle & scheduler adapter ====================

    @Test
    void runtimeBindingsExposeTheExactRegisteredExecutorAndStayDistinct() {
        ExecutorService poolA = Executors.newSingleThreadExecutor();
        ExecutorService poolB = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register("a", poolA).register("b", poolB).build();
        try {
            ExecutorRuntime runtimeA = global.par("a").runtime();
            ExecutorRuntime runtimeB = global.par("b").runtime();
            assertThat(runtimeA).isNotNull();
            assertThat(runtimeB).isNotNull();
            assertThat(runtimeA).isNotSameAs(runtimeB);
            assertThat(runtimeA.suppliedExecutor()).isSameAs(poolA);
            assertThat(runtimeB.suppliedExecutor()).isSameAs(poolB);
            assertThat(global.runtimesByIdentity().keySet())
                    .contains(new ExecutorIdentity(poolA), new ExecutorIdentity(poolB));
        } finally {
            poolA.shutdownNow();
            poolB.shutdownNow();
            global.close();
        }
    }

    @Test
    void globalParReportsClosedOnlyAfterCloseAndIsIdempotent() throws Exception {
        GlobalPar global = GlobalPar.builder().build();
        assertThat(global.isClosed()).isFalse();
        global.close();
        assertThat(global.isClosed()).isTrue();
        global.close(); // idempotent
        assertThat(global.isClosed()).isTrue();
        assertThatThrownBy(global::openTaskGraphObservation).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void timeoutSchedulerDelegatesRunnablesAndLifecycleStates() throws Exception {
        GlobalPar global = GlobalPar.builder().build();
        java.util.concurrent.ScheduledExecutorService scheduler = global.timeoutScheduler();

        java.util.concurrent.CountDownLatch ran = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.ScheduledFuture<?> scheduled =
                scheduler.schedule(ran::countDown, 5, TimeUnit.MILLISECONDS);
        org.junit.jupiter.api.Assertions.assertNotNull(scheduled);
        assertThat(scheduler.isShutdown()).isFalse();
        assertThat(scheduler.isTerminated()).isFalse();

        java.util.concurrent.atomic.AtomicBoolean executed = new java.util.concurrent.atomic.AtomicBoolean(false);
        scheduler.execute(() -> executed.set(true));
        awaitTrue(executed);

        assertThatThrownBy(() -> scheduler.schedule(() -> "v", 1, TimeUnit.MILLISECONDS))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> scheduler.scheduleAtFixedRate(ran::countDown, 1, 1, TimeUnit.MILLISECONDS))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> scheduler.scheduleWithFixedDelay(ran::countDown, 1, 1, TimeUnit.MILLISECONDS))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(scheduler::shutdown).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(scheduler::shutdownNow).isInstanceOf(UnsupportedOperationException.class);

        global.close();
        awaitTrue(scheduler::isShutdown);
        assertThat(scheduler.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        assertThat(scheduler.isTerminated()).isTrue();
        assertThatThrownBy(() -> scheduler.execute(ran::countDown)).isInstanceOf(RejectedExecutionException.class);
        assertThat(ran.getCount()).isGreaterThanOrEqualTo(0);
    }

    private static void awaitTrue(java.util.concurrent.atomic.AtomicBoolean flag) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!flag.get() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(flag.get()).isTrue();
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
