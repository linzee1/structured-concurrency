package io.github.huatalk.parallelinscope.cancel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.ListenableFutureTask;
import io.github.huatalk.parallelinscope.queue.SmartBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Tests lazy expiry of historical cancellation estimates. */
public class HeuristicPurgerExpiryTest {

    private ThreadPoolExecutor executor;

    /** Stops the dormant test executor after each test. */
    @AfterEach
    public void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    /** A signal after the expiry window starts a fresh garbage estimate. */
    @Test
    public void expiredCancellationEstimateDoesNotTriggerPurge() throws Exception {
        AtomicLong now = new AtomicLong(1L);
        AtomicInteger purgeCount = new AtomicInteger();
        executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SmartBlockingQueue<>(10)) {
            @Override
            public void purge() {
                purgeCount.incrementAndGet();
                super.purge();
            }
        };
        HeuristicPurger purger = new HeuristicPurger(
                new AtomicBoolean(true), new AtomicDouble(0.80), new AtomicDouble(0.20), now::get, 100L);
        Runnable observer = purger.cancellationObserverFor(executor);
        for (int i = 0; i < 8; i++) {
            executor.getQueue().put(ListenableFutureTask.create(() -> null));
        }

        cancelOne(observer);
        now.set(102L);
        cancelOne(observer);

        await().during(100, TimeUnit.MILLISECONDS)
                .atMost(1, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(purgeCount).hasValue(0));

        cancelOne(observer);
        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(purgeCount).hasValue(1));
    }

    /** Concurrent first signals after expiry remain unsettled while only the old marker expires. */
    @Test
    public void expiryDoesNotSettleConcurrentNewSignals() throws Exception {
        ControlledClock clock = new ControlledClock(2, 1L, 102L);
        AtomicInteger purgeCount = new AtomicInteger();
        executor = countingExecutor(purgeCount);
        HeuristicPurger purger = new HeuristicPurger(
                new AtomicBoolean(true), new AtomicDouble(0.80), new AtomicDouble(0.30), clock::getAsLong, 100L);
        Runnable observer = purger.cancellationObserverFor(executor);
        enqueue(8);

        observer.run();
        Thread delayed = new Thread(observer::run);
        delayed.start();
        assertThat(clock.blocked.await(5, TimeUnit.SECONDS)).isTrue();
        observer.run();
        clock.release.countDown();
        delayed.join(5_000L);

        await().during(100, TimeUnit.MILLISECONDS)
                .atMost(1, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(purgeCount).hasValue(0));

        observer.run();
        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(purgeCount).hasValue(1));
    }

    /** A callback paused across disable and re-enable cannot contribute to the new generation. */
    @Test
    public void disableGenerationSettlesPausedCallback() throws Exception {
        ControlledClock clock = new ControlledClock(1, 1L, 1L);
        AtomicInteger purgeCount = new AtomicInteger();
        AtomicBoolean enabled = new AtomicBoolean(true);
        executor = countingExecutor(purgeCount);
        HeuristicPurger purger =
                new HeuristicPurger(enabled, new AtomicDouble(0.80), new AtomicDouble(0.20), clock::getAsLong, 100L);
        Runnable observer = purger.cancellationObserverFor(executor);
        enqueue(8);

        Thread paused = new Thread(observer::run);
        paused.start();
        assertThat(clock.blocked.await(5, TimeUnit.SECONDS)).isTrue();
        enabled.set(false);
        purger.clearPendingCancellations();
        enabled.set(true);
        clock.release.countDown();
        paused.join(5_000L);

        observer.run();
        await().during(100, TimeUnit.MILLISECONDS)
                .atMost(1, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(purgeCount).hasValue(0));

        observer.run();
        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(purgeCount).hasValue(1));
    }

    @Test
    public void finestDiagnosticsCanBeEnabledForPurgeDecisions() throws Exception {
        Logger logger = Logger.getLogger(HeuristicPurger.class.getName());
        Level previous = logger.getLevel();
        logger.setLevel(Level.FINEST);
        try {
            AtomicInteger purgeCount = new AtomicInteger();
            executor = countingExecutor(purgeCount);
            HeuristicPurger purger = new HeuristicPurger(new AtomicDouble(0.50), new AtomicDouble(0.05));
            Runnable observer = purger.cancellationObserverFor(executor);
            enqueue(8);
            cancelOne(observer);
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(purgeCount).hasValue(1));
            purger.clearPendingCancellations();
            purger.close();
        } finally {
            logger.setLevel(previous);
        }
    }

    /** Creates an executor that counts normal purge scans. */
    private ThreadPoolExecutor countingExecutor(AtomicInteger purgeCount) {
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SmartBlockingQueue<>(10)) {
            @Override
            public void purge() {
                purgeCount.incrementAndGet();
                super.purge();
            }
        };
    }

    /** Adds the requested number of inert queue entries. */
    private void enqueue(int count) throws InterruptedException {
        for (int i = 0; i < count; i++) {
            executor.getQueue().put(ListenableFutureTask.create(() -> null));
        }
    }

    /** Cancels one live queue entry and emits its pre-run cancellation signal. */
    private void cancelOne(Runnable observer) {
        for (Runnable queued : executor.getQueue()) {
            ListenableFutureTask<?> task = (ListenableFutureTask<?>) queued;
            if (!task.isCancelled()) {
                task.cancel(false);
                observer.run();
                return;
            }
        }
        throw new AssertionError("No live queued task available");
    }

    /** Blocks one selected clock read while returning deterministic timestamps. */
    private static final class ControlledClock {
        private final int blockedCall;
        private final long firstValue;
        private final long laterValue;
        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch blocked = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        /** Creates a clock with one controlled invocation. */
        private ControlledClock(int blockedCall, long firstValue, long laterValue) {
            this.blockedCall = blockedCall;
            this.firstValue = firstValue;
            this.laterValue = laterValue;
        }

        /** Returns the configured time after pausing the selected invocation. */
        private long getAsLong() {
            int call = calls.incrementAndGet();
            if (call == blockedCall) {
                blocked.countDown();
                awaitLatch(release);
            }
            return call == 1 ? firstValue : laterValue;
        }

        /** Waits through interruption and restores the interrupt status afterward. */
        private static void awaitLatch(CountDownLatch latch) {
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
    }
}
