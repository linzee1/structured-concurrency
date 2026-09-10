package io.github.huatalk.parallelinscope.cancel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.ListenableFutureTask;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Verifies purger binding across {@link BlockingQueue} implementations beyond SmartBlockingQueue. */
class HeuristicPurgerQueueCompatibilityTest {

    private final List<ThreadPoolExecutor> executors = new ArrayList<>();
    private final List<HeuristicPurger> purgers = new ArrayList<>();

    /** Stops all dormant test executors and purgers after each test. */
    @AfterEach
    void tearDown() {
        purgers.forEach(HeuristicPurger::close);
        executors.forEach(ThreadPoolExecutor::shutdownNow);
    }

    /** A bounded plain LinkedBlockingQueue purges cancelled tasks like SmartBlockingQueue. */
    @Test
    void boundedLinkedBlockingQueuePurgesCancelledTasks() throws Exception {
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(20);
        CountingExecutor executor = executor(queue);
        List<ListenableFutureTask<Void>> tasks = enqueue(queue, 16);
        HeuristicPurger purger = purger();
        Runnable observer = purger.cancellationObserverFor(executor);

        cancel(tasks, 0, 2, observer);

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(executor.purgeCount).hasValue(1));
        assertThat(queue).hasSize(14);
    }

    /** An unbounded plain LinkedBlockingQueue has no pressure concept and receives a no-op. */
    @Test
    void unboundedLinkedBlockingQueueReceivesNoopObserver() throws Exception {
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        CountingExecutor executor = executor(queue);
        List<ListenableFutureTask<Void>> tasks = enqueue(queue, 16);
        HeuristicPurger purger = purger();
        Runnable observer = purger.cancellationObserverFor(executor);

        cancel(tasks, 0, 2, observer);

        await().during(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertThat(executor.purgeCount).hasValue(0));
    }

    /** A SynchronousQueue retains nothing and receives a no-op observer. */
    @Test
    void synchronousQueueReceivesNoopObserver() {
        CountingExecutor executor = executor(new SynchronousQueue<>());
        HeuristicPurger purger = purger();

        purger.cancellationObserverFor(executor).run();

        assertThat(executor.purgeCount).hasValue(0);
    }

    /** Creates and tracks a dormant executor backed by the supplied queue. */
    private CountingExecutor executor(BlockingQueue<Runnable> queue) {
        CountingExecutor executor = new CountingExecutor(queue);
        executors.add(executor);
        return executor;
    }

    /** Creates and tracks a purger with low thresholds so few signals trigger maintenance. */
    private HeuristicPurger purger() {
        HeuristicPurger purger = new HeuristicPurger(new AtomicDouble(0.50), new AtomicDouble(0.05));
        purgers.add(purger);
        return purger;
    }

    /** Adds inert futures directly to the queue so worker execution cannot change the fixture. */
    private static List<ListenableFutureTask<Void>> enqueue(BlockingQueue<Runnable> queue, int count)
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

    /** Counts purge calls while retaining normal ThreadPoolExecutor removal behavior. */
    private static final class CountingExecutor extends ThreadPoolExecutor {
        private final AtomicInteger purgeCount = new AtomicInteger();

        /** Creates a dormant one-worker executor. */
        private CountingExecutor(BlockingQueue<Runnable> queue) {
            super(1, 1, 0L, TimeUnit.MILLISECONDS, queue);
        }

        /** Records and performs one queue purge. */
        @Override
        public void purge() {
            purgeCount.incrementAndGet();
            super.purge();
        }
    }
}
