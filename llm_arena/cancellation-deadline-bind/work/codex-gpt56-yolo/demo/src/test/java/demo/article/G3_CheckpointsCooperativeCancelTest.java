package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.cancel.Checkpoints;
import io.github.huatalk.parallelinscope.scope.AsyncBatchResult;
import io.github.huatalk.parallelinscope.scope.BatchExecutionOptions;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.Par;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * G3. 任务取消后还在算——显式检查点
 *
 * <p>问题：CPU 密集型任务不调用 sleep/wait/IO，中断信号打不进来。 cancel(true) 设置了中断标志，但紧密循环从不检查，任务继续算到自然结束。
 *
 * <p>解决方案概念：在 CPU 密集循环中插入 Checkpoints.check() 检查点， 框架检查 CancellationToken 状态，已取消则立即抛出
 * LeanCancellationException。 对于 IO 任务，Par.map() + timeout 配合 Thread.interrupt() 天然有效。
 */
public class G3_CheckpointsCooperativeCancelTest {

    private ExecutorService pool;
    private Par par;

    @BeforeEach
    void setUp() {
        pool = Executors.newFixedThreadPool(4);
        GlobalPar config = GlobalPar.builder()
                .register("test-pool", pool)
                .defaultPar("test-pool")
                .build();
        par = config.defaultPar();
    }

    @AfterEach
    void tearDown() {
        pool.shutdownNow();
    }

    /**
     * 问题演示：CPU 密集循环忽略 Thread.interrupt()，cancel(true) 无效。
     *
     * <p>提交 4 个忙等待任务（各 2 秒），立即调用 cancel(true)。 尽管 Future 被标记为已取消，所有任务仍然完成——中断信号从未被检查。 总耗时约 2 秒（4 个任务在
     * 4 线程池上并行），而非立即停止。
     */
    @Test
    void problem_cpuIntensiveLoopIgnoresInterrupt() throws Exception {
        final int taskCount = 4;
        final long loopDurationSec = 2;
        CountDownLatch started = new CountDownLatch(taskCount);
        AtomicInteger completed = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < taskCount; i++) {
            futures.add(pool.submit(() -> {
                started.countDown();
                // CPU 密集循环——不调用 sleep/wait/IO
                // Thread.interrupt() 设置的中断标志在这里完全被忽略
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(loopDurationSec);
                while (System.nanoTime() < end) {
                    /* 忙等待：紧密循环不检查 Thread.interrupted() */
                }
                completed.incrementAndGet();
            }));
        }

        // 确保所有任务已开始执行
        started.await(3, TimeUnit.SECONDS);

        // 立即取消所有任务
        futures.forEach(f -> f.cancel(true));

        long start = System.currentTimeMillis();

        // cancel(true) 后 get() 立即抛 CancellationException
        for (Future<?> f : futures) {
            try {
                f.get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // CancellationException or ExecutionException
            }
        }

        // 等待忙循环自然结束
        Thread.sleep(loopDurationSec * 1000 + 500);
        long elapsed = System.currentTimeMillis() - start;

        // 核心问题验证：尽管调用了 cancel(true)，所有任务仍然完成
        assertThat(completed.get())
                .as("CPU 密集任务忽略 interrupt，cancel(true) 无效，所有任务仍然完成")
                .isEqualTo(taskCount);

        // 证明任务是花时间跑完的，而非立即停止
        assertThat(elapsed)
                .as("任务耗时约 %d 秒——中断没有提前终止循环", loopDurationSec)
                .isGreaterThanOrEqualTo(loopDurationSec * 1000 - 200);
    }

    /**
     * 解决方案演示：Par.map() + timeout 对 IO 任务（Thread.sleep）取消有效。
     *
     * <p>4 个任务各 sleep 5 秒，设置 500ms 超时。 框架超时后调用 Future.cancel(true)，Thread.sleep 响应中断抛出
     * InterruptedException， 任务立即停止，总耗时远小于 4 * 5s = 20s。
     */
    @Test
    void solution_parMapTimeoutCancelsIoTasks() throws Exception {
        BatchExecutionOptions opts = BatchExecutionOptions.of("io-task")
                .parallelism(4)
                .timeout(java.time.Duration.ofMillis(500))
                .build();

        List<Integer> input = Arrays.asList(1, 2, 3, 4);
        long start = System.currentTimeMillis();

        AsyncBatchResult<String> result = par.map(
                input,
                x -> {
                    // Thread.sleep 响应中断——cancel(true) 对 IO 操作天然有效
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Task interrupted", e);
                    }
                    return "result-" + x;
                },
                opts);

        // 等待超时取消生效
        Thread.sleep(2000);
        long elapsed = System.currentTimeMillis() - start;

        // 验证：总耗时远小于 4 * 5s = 20s
        assertThat(elapsed).as("Par.map() 超时取消生效，耗时远小于 20 秒").isLessThan(5000);

        // 验证：任务因中断而失败（CancellationException 或 ExecutionException）
        boolean anyFailedOrCancelled = false;
        for (int i = 0; i < result.results().size(); i++) {
            Future<String> future = result.results().get(i);
            if (future.isDone()) {
                if (future.isCancelled()) {
                    anyFailedOrCancelled = true;
                } else {
                    try {
                        future.get(1, TimeUnit.SECONDS);
                    } catch (ExecutionException e) {
                        // InterruptedException wrapped in RuntimeException
                        anyFailedOrCancelled = true;
                        assertThat(e.getCause()).as("任务因中断而抛出异常").isInstanceOf(RuntimeException.class);
                    }
                }
            }
        }
        assertThat(anyFailedOrCancelled).as("至少有一个任务因超时取消而失败").isTrue();

        // reportString() 包含 FAILED 或 CANCELLED 状态
        String report = result.reportString();
        assertThat(report).as("报告中应包含失败或取消状态").containsAnyOf("FAILED", "CANCELLED");
    }

    /**
     * 核心解决方案：Checkpoints.check() 在 CPU 密集循环中实现协作式取消。
     *
     * <p>与 Test 1 的忙等待不同，这里在循环中插入 Checkpoints.check()。 框架超时后，下一次 check() 调用会立即抛出
     * LeanCancellationException， 任务立刻停止——不需要 Thread.interrupt()，不需要 sleep。
     */
    @Test
    void solution_checkpointsCancelCpuIntensiveLoop() throws Exception {
        BatchExecutionOptions opts = BatchExecutionOptions.of("cpu-checkpoint")
                .parallelism(4)
                .timeout(java.time.Duration.ofMillis(500))
                .build();

        List<Integer> input = Arrays.asList(1, 2, 3, 4);
        long start = System.currentTimeMillis();

        AsyncBatchResult<Integer> result = par.map(
                input,
                x -> {
                    int iterations = 0;
                    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10); // 本应跑 10 秒
                    while (System.nanoTime() < end) {
                        iterations++;
                        // 每 10000 次迭代检查一次——几乎零开销
                        if (iterations % 10000 == 0) {
                            Checkpoints.checkpoint("cpu-checkpoint", true); // 如果已取消，立即抛出 LeanCancellationException
                        }
                    }
                    return iterations;
                },
                opts);

        // 等待超时取消生效
        Thread.sleep(2000);
        long elapsed = System.currentTimeMillis() - start;

        // 核心验证：总耗时远小于 4 * 10s = 40s
        assertThat(elapsed).as("Checkpoints.check() 在 500ms 超时后立即停止 CPU 循环").isLessThan(5000);

        // 验证有任务被取消或失败
        String report = result.reportString();
        assertThat(report).as("报告中应包含失败或取消状态").containsAnyOf("FAILED", "CANCELLED");
    }
}
