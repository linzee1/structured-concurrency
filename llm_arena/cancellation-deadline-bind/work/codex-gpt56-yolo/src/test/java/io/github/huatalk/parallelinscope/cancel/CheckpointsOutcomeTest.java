package io.github.huatalk.parallelinscope.cancel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Outcome-focused additions over {@link CheckpointsTest}: every blocking adapter must report the
 * primitive's {@code false} result (not only {@code true}), and the supplier wrapper must surface
 * plain failures and sneakily-thrown checked throwables.
 */
class CheckpointsOutcomeTest {

    @AfterEach
    void clearContextAndInterrupt() {
        Thread.interrupted();
    }

    private static void holdPermits(Semaphore semaphore, int permits) {
        assertThat(semaphore.tryAcquire(permits)).isTrue();
    }

    @Test
    void tryAcquireDurationOverloadReportsTimeoutFalse() {
        Semaphore semaphore = new Semaphore(0);
        holdNothingButAssertBusy(semaphore);
        long start = System.nanoTime();
        assertThat(Checkpoints.checkTryAcquire(semaphore, Duration.ofMillis(30)))
                .isFalse();
        assertThat(System.nanoTime() - start).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(5));
    }

    private static void holdNothingButAssertBusy(Semaphore semaphore) {
        // A zero-permit semaphore is already exhausted; nothing further to arrange.
    }

    @Test
    void tryAcquireTimeoutOverloadReportsTimeoutFalse() {
        Semaphore semaphore = new Semaphore(0);
        assertThat(Checkpoints.checkTryAcquire(semaphore, 25, TimeUnit.MILLISECONDS))
                .isFalse();
    }

    @Test
    void tryAcquirePermitsAndDurationOverloadReportsTimeoutFalse() {
        Semaphore semaphore = new Semaphore(0);
        assertThat(Checkpoints.checkTryAcquire(semaphore, 2, Duration.ofMillis(25)))
                .isFalse();
    }

    @Test
    void tryAcquirePermitsTimeoutUnitReportsTimeoutFalse() {
        Semaphore semaphore = new Semaphore(1);
        holdPermits(semaphore, 1);
        assertThat(Checkpoints.checkTryAcquire(semaphore, 2, 20, TimeUnit.MILLISECONDS))
                .isFalse();
        semaphore.release();
        assertThat(Checkpoints.checkTryAcquire(semaphore, 1, 20, TimeUnit.MILLISECONDS))
                .isTrue();
    }

    @Test
    void tryLockBothOverloadsReportTimeoutFalseWhileHeldElsewhere() throws Exception {
        ReentrantLock heldByOther = new ReentrantLock();
        ExecutorService blocker = Executors.newSingleThreadExecutor();
        try {
            blocker.submit(() -> {
                        heldByOther.lock();
                        return null;
                    })
                    .get(2, TimeUnit.SECONDS);

            assertThat(Checkpoints.checkTryLock(heldByOther, Duration.ofMillis(30)))
                    .isFalse();
            assertThat(Checkpoints.checkTryLock(heldByOther, 30, TimeUnit.MILLISECONDS))
                    .isFalse();

            ReentrantLock free = new ReentrantLock();
            assertThat(Checkpoints.checkTryLock(free, Duration.ofMillis(20))).isTrue();
            free.unlock();
        } finally {
            blocker.shutdownNow();
        }
    }

    @Test
    void awaitTerminationReportsFalseForRunningExecutorViaEveryOverload() throws Exception {
        ExecutorService busy = Executors.newSingleThreadExecutor();
        try {
            busy.submit(() -> {
                Thread.sleep(300);
                return null;
            });
            assertThat(Checkpoints.checkAwaitTermination(busy, 20, TimeUnit.MILLISECONDS))
                    .isFalse();
            assertThat(Checkpoints.checkAwaitTermination(busy, Duration.ofMillis(20)))
                    .isFalse();

            busy.shutdown();
            assertThat(Checkpoints.checkAwaitTermination(busy, Duration.ofSeconds(2)))
                    .isTrue();
        } finally {
            busy.shutdownNow();
        }
    }

    @Test
    void checkSupplierRejectsBadArgumentsBeforeRunningTheSupplier() {
        Semaphore untouched = new Semaphore(1);
        assertThat(Checkpoints.checkTryAcquire(untouched, Duration.ofMillis(1))).isTrue();

        assertThatThrownBy(() -> Checkpoints.checkSupplier(null, IllegalStateException.class))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Checkpoints.<Object, IllegalStateException>checkSupplier(() -> new Object(), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void checkSupplierRethrowsPlainRuntimeFailuresUnwrapped() {
        RuntimeException boom = new IllegalArgumentException("plain");
        assertThatThrownBy(() -> Checkpoints.checkSupplier(
                        () -> {
                            throw boom;
                        },
                        IllegalStateException.class))
                .isSameAs(boom);

        Error error = new AssertionError("error-path");
        assertThatThrownBy(() -> Checkpoints.checkSupplier(
                        () -> {
                            throw error;
                        },
                        IllegalStateException.class))
                .isSameAs(error);
    }

    @Test
    void checkSupplierWrapsSneakyCheckedThrowablesInAssertionError() {
        assertThatThrownBy(() -> Checkpoints.checkSupplier(
                        () -> {
                            throw SneakyThrow.sneak(new Exception("checked"));
                        },
                        IllegalStateException.class))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("checked Throwable")
                .hasCauseInstanceOf(Exception.class);
    }

    @Test
    void latchAwaitDurationOverloadReportsBothOutcomes() throws Exception {
        java.util.concurrent.CountDownLatch openLatch = new java.util.concurrent.CountDownLatch(1);
        openLatch.countDown();
        assertThat(Checkpoints.checkAwait(openLatch, Duration.ofMillis(10))).isTrue();

        java.util.concurrent.CountDownLatch held = new java.util.concurrent.CountDownLatch(1);
        long start = System.nanoTime();
        assertThat(Checkpoints.checkAwait(held, Duration.ofMillis(20))).isFalse();
        assertThat(System.nanoTime() - start).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(5));
    }

    @Test
    void latchAwaitTimeoutUnitOverloadReportsBothOutcomes() throws Exception {
        java.util.concurrent.CountDownLatch openLatch = new java.util.concurrent.CountDownLatch(2);
        openLatch.countDown();
        openLatch.countDown();
        assertThat(Checkpoints.checkAwait(openLatch, 10, TimeUnit.MILLISECONDS)).isTrue();

        java.util.concurrent.CountDownLatch held = new java.util.concurrent.CountDownLatch(1);
        assertThat(Checkpoints.checkAwait(held, 15, TimeUnit.MILLISECONDS)).isFalse();
        held.countDown();
        assertThat(Checkpoints.checkAwait(held, 15, TimeUnit.MILLISECONDS)).isTrue();
    }

    @Test
    void conditionAwaitSignalledOverloadReturnsTrueOnceReleased() throws Exception {
        ReentrantLock lock = new ReentrantLock();
        java.util.concurrent.locks.Condition condition = lock.newCondition();
        java.util.concurrent.atomic.AtomicBoolean done = new java.util.concurrent.atomic.AtomicBoolean(false);
        Thread signaller = new Thread(() -> {
            while (!done.get()) {
                lock.lock();
                try {
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });
        signaller.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        boolean result = false;
        while (!result && System.nanoTime() < deadline) {
            lock.lock();
            try {
                result = Checkpoints.checkAwait(condition, 50, TimeUnit.MILLISECONDS);
            } finally {
                lock.unlock();
            }
        }
        done.set(true);
        signaller.join(TimeUnit.SECONDS.toMillis(2));
        assertThat(result).isTrue();
    }

    /** Helper that converts any throwable into an unchecked throw site for lambda bodies. */
    private static final class SneakyThrow {

        private SneakyThrow() {}

        @SuppressWarnings("unchecked")
        private static <T extends Throwable> RuntimeException sneak(Throwable throwable) throws T {
            throw (T) throwable;
        }
    }
}
