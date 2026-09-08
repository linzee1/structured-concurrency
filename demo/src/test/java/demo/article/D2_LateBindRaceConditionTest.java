package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.github.huatalk.parallelinscope.scope.AsyncBatchResult;
import io.github.huatalk.parallelinscope.scope.BatchExecutionOptions;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * D2. 为什么使用统一的超时时间 — 问题复现与解决方案
 *
 * <p>场景：滑动窗口调度下，如果每个 Future 在提交时就绑定超时（per-task timeout）， 先提交的任务超时计时器先启动，后提交的任务超时计时器后启动。
 * 当线程池被占满时，后提交的任务在队列中等待，等轮到它执行时， 它的超时计时器可能已经过期了——任务还没开始就被取消。
 *
 * <p>Test 1: 用 Guava FluentFuture.withTimeout 模拟 per-task timeout，展示后提交任务的不公平超时
 *
 * <p>Test 2: 用 Par.map() 的 lateBind 机制，展示整个批次共享一个超时截止时间
 */
class D2_LateBindRaceConditionTest {

    private static final int TASK_COUNT = 8;
    private static final int PARALLELISM = 2;
    private static final long TASK_SLEEP_MS = 1200;

    /**
     * Test 1 — 问题复现：per-task timeout 导致后提交任务超时
     *
     * <p>模拟原生做法：每个 Future 在提交时通过 FluentFuture.withTimeout() 绑定超时。 8 个任务提交到 3 线程的池（parallelism=2，预留 1
     * 个调度线程）， 每个任务需要 1200ms，超时 2000ms。
     *
     * <p>时间线：
     *
     * <pre>
     * task 0-1: submitted T=0/100ms,   timeout@2000/2100ms, run immediately,     finish@1200/1300ms → SUCCEED
     * task 2:   submitted T=200ms,     timeout@2200ms,      queued, starts@1200, finish@2400ms     → SUCCEED (barely)
     * task 3:   submitted T=300ms,     timeout@2300ms,      queued, starts@2400, needs until 3600  → FAIL (timeout fires at 2300)
     * task 4-7: submitted T=400-700ms, timeout@2400-2700ms, queued much longer   → FAIL
     * </pre>
     *
     * <p>关键问题：每个任务都有自己的 2000ms 超时窗口，但窗口从"提交"开始计时， 而非从"开始执行"开始计时。后续任务在队列中等待的时间消耗了超时预算， 导致实际可用执行时间不足。
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void problem_perTaskTimeoutStarvesLateTasks() throws Exception {
        // Pool with 3 threads: 2 for parallel tasks + 1 spare for scheduling
        ExecutorService rawPool = Executors.newFixedThreadPool(PARALLELISM + 1);
        ListeningExecutorService pool = MoreExecutors.listeningDecorator(rawPool);
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();

        long perTaskTimeoutMs = 2000;

        try {
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger timeoutCount = new AtomicInteger(0);
            List<ListenableFuture<String>> futures = new ArrayList<>();

            // Submit all tasks with staggered timing, each with its own per-task timeout
            for (int i = 0; i < TASK_COUNT; i++) {
                final int taskId = i;

                ListenableFuture<String> raw = pool.submit(() -> {
                    Thread.sleep(TASK_SLEEP_MS);
                    return "task-" + taskId;
                });

                // Per-task timeout: timer starts NOW at submission time
                FluentFuture<String> withTimeout =
                        FluentFuture.from(raw).withTimeout(perTaskTimeoutMs, TimeUnit.MILLISECONDS, timer);
                futures.add(withTimeout);

                // Small stagger to simulate realistic submission pattern
                Thread.sleep(100);
            }

            // Collect results
            for (ListenableFuture<String> f : futures) {
                try {
                    f.get(15, TimeUnit.SECONDS);
                    successCount.incrementAndGet();
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof TimeoutException) {
                        timeoutCount.incrementAndGet();
                    }
                } catch (TimeoutException e) {
                    timeoutCount.incrementAndGet();
                }
            }

            // Early tasks succeed, but later tasks hit per-task timeout before getting scheduled
            assertThat(successCount.get())
                    .as("Only early tasks should succeed; later tasks timeout before execution starts")
                    .isGreaterThan(0)
                    .isLessThan(TASK_COUNT);

            assertThat(timeoutCount.get())
                    .as("Later tasks should fail with TimeoutException due to per-task timeout starvation")
                    .isGreaterThan(0);

        } finally {
            timer.shutdownNow();
            pool.shutdownNow();
        }
    }

    /**
     * Test 2 — 解决方案：Par.map() 的 lateBind 机制确保全批次统一超时
     *
     * <p>Par.map() 先提交初始窗口并为剩余逻辑任务创建 Future 槽位，然后通过 CancellationToken.lateBind() 将这些 Future
     * 和异步提交循环绑定到统一超时。
     *
     * <p>关键特性：
     *
     * <ul>
     *   <li>所有任务共享同一个超时截止时间
     *   <li>超时约束的是整个批次的总执行时间，而非单个任务的执行时间
     *   <li>如果整个批次在超时前完成，所有任务都成功；否则所有未完成的任务被取消
     * </ul>
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void solution_lateBindGivesUnifiedBatchTimeout() throws Exception {
        // Total time for sliding window: (TASK_COUNT / PARALLELISM) * TASK_SLEEP_MS ≈ 4800ms
        // Use 8000ms to give comfortable margin
        long batchTimeoutMs = 8000;

        ExecutorService rawPool = Executors.newFixedThreadPool(PARALLELISM + 1);
        GlobalPar config = GlobalPar.builder()
                .register("test-pool", rawPool)
                .defaultPar("test-pool")
                .build();
        Par par = config.defaultPar();

        try {
            List<Integer> items = IntStream.range(0, TASK_COUNT).boxed().collect(Collectors.toList());

            BatchExecutionOptions options = BatchExecutionOptions.of("late-bind-test")
                    .parallelism(PARALLELISM)
                    .timeout(java.time.Duration.ofMillis(batchTimeoutMs))
                    .taskType(TaskType.IO_BOUND)
                    .build();

            long startTime = System.currentTimeMillis();

            // Par.map() internally:
            // 1. Submits the initial window and creates Future slots for remaining tasks
            // 2. Calls CancellationToken.lateBind() on the complete logical batch
            // 3. The aggregate timeout gives the batch one shared deadline
            AsyncBatchResult<String> result = par.map(
                    items,
                    taskId -> {
                        try {
                            Thread.sleep(TASK_SLEEP_MS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        return "task-" + taskId;
                    },
                    options);

            // Wait for all futures to complete
            for (int i = 0; i < result.results().size(); i++) {
                result.results().get(i).get(15, TimeUnit.SECONDS);
            }

            long totalElapsed = System.currentTimeMillis() - startTime;

            // With lateBind, the batch timeout of 8000ms applies to the entire batch.
            // All 8 tasks with parallelism=2 and sleep=1200ms need ~4800ms total.
            // Since 4800ms < 8000ms, ALL tasks succeed — no task is unfairly starved.
            String report = result.reportString();
            assertThat(report)
                    .as("All tasks should succeed with batch timeout %dms: %s", batchTimeoutMs, report)
                    .contains("SUCCESS:" + TASK_COUNT);

            // Verify total time is within the batch timeout
            assertThat(totalElapsed)
                    .as("Total elapsed time (%dms) should be within batch timeout (%dms)", totalElapsed, batchTimeoutMs)
                    .isLessThan(batchTimeoutMs);

        } finally {
            rawPool.shutdownNow();
        }
    }
}
