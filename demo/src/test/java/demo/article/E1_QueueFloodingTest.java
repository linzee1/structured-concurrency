package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * E1. 一次性提交打满队列
 *
 * <p>演示问题：一次性向 FixedThreadPool 提交大量任务导致队列堆积。
 *
 * <p>演示解决：Par.map() 滑动窗口调度，队列深度始终受控。
 */
class E1_QueueFloodingTest {

    private static final int TASK_COUNT = 100;

    /**
     * 问题复现：一次性提交大量任务，队列瞬间被打满。
     *
     * <p>使用 FixedThreadPool(2) 提交 100 个任务，所有任务立即入队， 队列深度接近 taskCount - poolSize。
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void problem_queueFloodingWithBulkSubmit() throws Exception {
        int poolSize = 2;
        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(poolSize);

        try {
            // Gate keeps all tasks blocked so the queue fills up
            CountDownLatch gate = new CountDownLatch(1);

            // Submit all tasks at once — this is the problematic pattern
            for (int i = 0; i < TASK_COUNT; i++) {
                pool.submit(() -> {
                    try {
                        gate.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            // Give the pool threads time to pick up their tasks from the queue
            Thread.sleep(500);

            // The queue is flooded: TASK_COUNT - poolSize tasks are waiting
            int queueSize = pool.getQueue().size();
            assertThat(queueSize)
                    .as("Queue should be flooded: %d tasks submitted, pool size %d", TASK_COUNT, poolSize)
                    .isGreaterThan(TASK_COUNT - poolSize - 5);

            // Release all tasks so the test can finish
            gate.countDown();
            pool.shutdown();
            pool.awaitTermination(10, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * 解决方法：Par.map() 滑动窗口调度，并发度始终受控。
     *
     * <p>使用 parallelism=2 提交 100 个任务，通过滑动窗口机制， 每次只有 parallelism 个任务在线程池中执行，其余等待前一个完成后才提交。 最大并发度不超过
     * parallelism (+1 调度开销)。
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void solution_slidingWindowBoundedConcurrency() throws Exception {
        int poolSize = 4;
        int parallelism = 2;

        ExecutorService pool = Executors.newFixedThreadPool(poolSize);
        GlobalPar config = GlobalPar.builder()
                .register("test-pool", pool)
                .defaultPar("test-pool")
                .build();
        Par par = config.defaultPar();

        try {
            AtomicInteger concurrency = new AtomicInteger(0);
            AtomicInteger maxConcurrency = new AtomicInteger(0);

            // Gate blocks all tasks so we can observe peak concurrency
            CountDownLatch gate = new CountDownLatch(1);

            List<Integer> input = IntStream.range(0, TASK_COUNT).boxed().collect(Collectors.toList());

            MultiTaskOptions options = MultiTaskOptions.of("queue-flood-test")
                    .parallelism(parallelism)
                    .timeout(java.time.Duration.ofMillis(30000))
                    .taskType(TaskType.IO_BOUND)
                    .build();

            TaskBatchResult<Void> result = par.map(
                    input,
                    item -> {
                        int cur = concurrency.incrementAndGet();
                        maxConcurrency.updateAndGet(prev -> Math.max(prev, cur));
                        try {
                            gate.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            concurrency.decrementAndGet();
                        }
                        return null;
                    },
                    options);

            // Wait for initial batch to start running
            Thread.sleep(500);

            // Max concurrency should be bounded by parallelism (+ 1 scheduling overlap)
            assertThat(maxConcurrency.get())
                    .as("Max concurrency should be bounded by parallelism=%d", parallelism)
                    .isLessThanOrEqualTo(parallelism + 1);

            // Release all tasks
            gate.countDown();

            // Wait for all futures to complete
            for (com.google.common.util.concurrent.ListenableFuture<Void> f : result.results()) {
                f.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
