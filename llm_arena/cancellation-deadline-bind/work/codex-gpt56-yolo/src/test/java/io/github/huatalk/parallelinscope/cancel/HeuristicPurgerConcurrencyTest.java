package io.github.huatalk.parallelinscope.cancel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.ListenableFutureTask;
import io.github.huatalk.parallelinscope.queue.SmartBlockingQueue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Proves the per-executor purge state machine under deterministic races. */
public class HeuristicPurgerConcurrencyTest {

    private final List<ThreadPoolExecutor> executors = new ArrayList<>();

    /** Stops all dormant test executors after each test. */
    @AfterEach
    public void tearDown() {
        executors.forEach(ThreadPoolExecutor::shutdownNow);
    }

    /**
     * Active callbacks increment sequence state but do not inspect the queue or submit duplicates.
     */
    @Test
    public void callbacksAreLogicalNoopWhilePurgeIsRunning() throws Exception {
        CountingQueue queue = new CountingQueue(20);
        BlockingPurgeExecutor executor = executor(queue);
        executor.blockBeforeSuper = true;
        enqueue(queue, 16);
        HeuristicPurger purger = new HeuristicPurger(new AtomicDouble(0.80), new AtomicDouble(0.05));
        Runnable observer = purger.cancellationObserverFor(executor);

        observer.run();
        assertThat(executor.purgeEntered.await(5, TimeUnit.SECONDS)).isTrue();
        queue.resetAccessCounts();

        CountDownLatch ready = new CountDownLatch(32);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(32);
        for (int i = 0; i < 32; i++) {
            new Thread(() -> {
                        ready.countDown();
                        awaitLatch(start);
                        observer.run();
                        done.countDown();
                    })
                    .start();
        }
        assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
        start.countDown();
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();

        assertThat(queue.accessCount).hasValue(0);
        assertThat(executor.purgeCount).hasValue(1);
        executor.releasePurge.countDown();
    }

    /** Signals arriving after one purge scan are retained and cause exactly one follow-up scan. */
    @Test
    public void cancellationAfterScanRunsInNextRound() throws Exception {
        CountingQueue queue = new CountingQueue(20);
        BlockingPurgeExecutor executor = executor(queue);
        executor.blockAfterSuper = true;
        List<ListenableFutureTask<Void>> tasks = enqueue(queue, 16);
        HeuristicPurger purger = new HeuristicPurger(new AtomicDouble(0.50), new AtomicDouble(0.05));
        Runnable observer = purger.cancellationObserverFor(executor);

        cancel(tasks, 0, 2, observer);
        assertThat(executor.afterSuper.await(5, TimeUnit.SECONDS)).isTrue();
        cancel(tasks, 2, 4, observer);
        executor.releasePurge.countDown();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(executor.purgeCount).hasValue(2);
            assertThat(queue).hasSize(12);
        });
    }

    /** A continuous cancellation stream cannot keep postponing an already submitted purge. */
    @Test
    public void continuousCancellationDoesNotSlideTheDelay() throws Exception {
        CountingQueue queue = new CountingQueue(20);
        BlockingPurgeExecutor executor = executor(queue);
        executor.blockBeforeSuper = true;
        enqueue(queue, 16);
        HeuristicPurger purger = new HeuristicPurger(new AtomicDouble(0.50), new AtomicDouble(0.05));
        Runnable observer = purger.cancellationObserverFor(executor);
        observer.run();

        CountDownLatch producerStarted = new CountDownLatch(1);
        CountDownLatch stopProducer = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            producerStarted.countDown();
            while (stopProducer.getCount() > 0) {
                observer.run();
            }
        });
        producer.start();
        assertThat(producerStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.purgeEntered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.purgeCount).hasValue(1);

        stopProducer.countDown();
        producer.join(5_000L);
        executor.releasePurge.countDown();
    }

    /** A failed purge preserves demand and retries only after a new cancellation signal. */
    @Test
    public void failedPurgeRetriesOnlyAfterNewSignal() throws Exception {
        CountingQueue queue = new CountingQueue(20);
        BlockingPurgeExecutor executor = executor(queue);
        executor.failFirst = true;
        List<ListenableFutureTask<Void>> tasks = enqueue(queue, 16);
        HeuristicPurger purger = new HeuristicPurger(new AtomicDouble(0.50), new AtomicDouble(0.05));
        Runnable observer = purger.cancellationObserverFor(executor);

        cancel(tasks, 0, 2, observer);

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(executor.purgeCount).hasValue(1));
        await().during(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertThat(executor.purgeCount).hasValue(1));

        cancel(tasks, 2, 4, observer);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(executor.purgeCount).hasValue(2);
            assertThat(queue).hasSize(12);
        });
    }

    /** A failed purge stops without retrying the unchanged cancellation sequence. */
    @Test
    public void repeatedPurgeFailureStopsWithoutNewSignals() throws Exception {
        CountingQueue queue = new CountingQueue(20);
        BlockingPurgeExecutor executor = executor(queue);
        executor.failAlways = true;
        List<ListenableFutureTask<Void>> tasks = enqueue(queue, 16);
        HeuristicPurger purger = new HeuristicPurger(new AtomicDouble(0.50), new AtomicDouble(0.05));
        Runnable observer = purger.cancellationObserverFor(executor);

        cancel(tasks, 0, 2, observer);

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(executor.purgeCount).hasValue(1));
        await().during(1_500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertThat(executor.purgeCount).hasValue(1));
    }

    /** Creates and tracks a dormant executor backed by the supplied queue. */
    private BlockingPurgeExecutor executor(CountingQueue queue) {
        BlockingPurgeExecutor executor = new BlockingPurgeExecutor(queue);
        executors.add(executor);
        return executor;
    }

    /** Adds inert futures directly to the queue so worker execution cannot change the fixture. */
    private static List<ListenableFutureTask<Void>> enqueue(CountingQueue queue, int count)
            throws InterruptedException {
        List<ListenableFutureTask<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ListenableFutureTask<Void> task = ListenableFutureTask.create(() -> null);
            queue.put(task);
            tasks.add(task);
        }
        return tasks;
    }

    /** Cancels a task range and emits one matching cancellation signal per task. */
    private static void cancel(List<ListenableFutureTask<Void>> tasks, int from, int to, Runnable observer) {
        for (int i = from; i < to; i++) {
            assertThat(tasks.get(i).cancel(false)).isTrue();
            observer.run();
        }
    }

    /** Waits without allowing an interrupted test thread to skip its callback. */
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

    /** Counts queue snapshots so active callback reads are directly observable. */
    private static final class CountingQueue extends SmartBlockingQueue<Runnable> {
        private final AtomicInteger accessCount = new AtomicInteger();

        /** Creates a counted smart queue. */
        private CountingQueue(int capacity) {
            super(capacity);
        }

        /** Counts queue-size snapshots. */
        @Override
        public int size() {
            accessCount.incrementAndGet();
            return super.size();
        }

        /** Counts capacity snapshots. */
        @Override
        public int capacity() {
            accessCount.incrementAndGet();
            return super.capacity();
        }

        /** Resets the counter after purge has entered its controlled blocking point. */
        private void resetAccessCounts() {
            accessCount.set(0);
        }
    }

    /** Controls purge progress so each race has an explicit happens-before boundary. */
    private static final class BlockingPurgeExecutor extends ThreadPoolExecutor {
        private final AtomicInteger purgeCount = new AtomicInteger();
        private final CountDownLatch purgeEntered = new CountDownLatch(1);
        private final CountDownLatch afterSuper = new CountDownLatch(1);
        private final CountDownLatch releasePurge = new CountDownLatch(1);
        private volatile boolean blockBeforeSuper;
        private volatile boolean blockAfterSuper;
        private volatile boolean failFirst;
        private volatile boolean failAlways;

        /** Creates a dormant one-worker executor. */
        private BlockingPurgeExecutor(CountingQueue queue) {
            super(1, 1, 0L, TimeUnit.MILLISECONDS, queue);
        }

        /** Records purge calls and applies one selected blocking/failure behavior. */
        @Override
        public void purge() {
            int call = purgeCount.incrementAndGet();
            if (failAlways) {
                throw new IllegalStateException("test purge failure");
            }
            if (call == 1 && failFirst) {
                throw new IllegalStateException("test purge failure");
            }
            if (call == 1 && blockBeforeSuper) {
                purgeEntered.countDown();
                awaitLatch(releasePurge);
            }
            super.purge();
            if (call == 1 && blockAfterSuper) {
                afterSuper.countDown();
                awaitLatch(releasePurge);
            }
        }
    }
}
