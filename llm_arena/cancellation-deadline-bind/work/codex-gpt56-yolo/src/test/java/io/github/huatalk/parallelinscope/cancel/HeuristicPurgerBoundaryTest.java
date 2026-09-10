package io.github.huatalk.parallelinscope.cancel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.ListenableFutureTask;
import io.github.huatalk.parallelinscope.queue.SmartBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Threshold-equality and skip-path complements to {@link HeuristicPurgerExpiryTest}: a pressure
 * reading exactly equal to the threshold counts as met, while either boundary below its threshold
 * suppresses maintenance.
 */
class HeuristicPurgerBoundaryTest {

    private ThreadPoolExecutor executor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private ThreadPoolExecutor countingExecutor(AtomicInteger purgeCount) {
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SmartBlockingQueue<>(10)) {
            @Override
            public void purge() {
                purgeCount.incrementAndGet();
                super.purge();
            }
        };
    }

    private void enqueue(int count) throws InterruptedException {
        for (int i = 0; i < count; i++) {
            executor.getQueue().put(ListenableFutureTask.create(() -> null));
        }
    }

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

    @Test
    void pressureExactlyAtThresholdStillTriggersMaintenance() throws Exception {
        AtomicInteger purgeCount = new AtomicInteger();
        executor = countingExecutor(purgeCount);
        // 8 occupied of capacity 10 == 0.80 pressure: equal, not strictly below.
        HeuristicPurger purger = new HeuristicPurger(new AtomicDouble(0.80), new AtomicDouble(0.10));
        try {
            Runnable observer = purger.cancellationObserverFor(executor);
            enqueue(8);
            cancelOne(observer);
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(purgeCount).hasValue(1));
        } finally {
            purger.close();
        }
    }

    @Test
    void pressureBelowThresholdSuppressesPurgeEntirely() throws Exception {
        AtomicInteger purgeCount = new AtomicInteger();
        executor = countingExecutor(purgeCount);
        HeuristicPurger purger = new HeuristicPurger(new AtomicDouble(0.80), new AtomicDouble(0.10));
        try {
            Runnable observer = purger.cancellationObserverFor(executor);
            enqueue(2); // Pressure 0.2 << 0.8 despite a high cancelled ratio.
            cancelOne(observer);
            Thread.sleep(200);
            assertThat(purgeCount).hasValue(0);
        } finally {
            purger.close();
        }
    }

    @Test
    void cancelledRatioBelowThresholdSuppressesPurgeEvenUnderPressure() throws Exception {
        AtomicInteger purgeCount = new AtomicInteger();
        executor = countingExecutor(purgeCount);
        HeuristicPurger purger = new HeuristicPurger(new AtomicDouble(0.50), new AtomicDouble(0.90));
        try {
            Runnable observer = purger.cancellationObserverFor(executor);
            enqueue(9); // Pressure 0.9 >= 0.50
            cancelOne(observer); // Ratio min(1,9)/10 == 0.1 << 0.90
            Thread.sleep(200);
            assertThat(purgeCount).hasValue(0);
        } finally {
            purger.close();
        }
    }
}
