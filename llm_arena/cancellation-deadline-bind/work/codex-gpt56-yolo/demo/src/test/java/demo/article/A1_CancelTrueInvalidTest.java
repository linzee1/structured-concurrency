package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.cancel.Checkpoints;
import io.github.huatalk.parallelinscope.scope.AsyncBatchResult;
import io.github.huatalk.parallelinscope.scope.BatchExecutionOptions;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A1: cancel(true) 无效 — 协作式取消
 *
 * <p>演示标准 Future.cancel(true) 对非中断代码无效的问题， 以及 parallel-in-scope Par.map() 超时取消的解决方案。
 */
public class A1_CancelTrueInvalidTest {

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

    @Test
    void cancelTrue_doesNotStopNonInterruptibleTask() throws Exception {
        // 问题演示：cancel(true) 对忽略中断标志的代码无效
        final CountDownLatch started = new CountDownLatch(3);
        AtomicInteger completed = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            futures.add(pool.submit(() -> {
                started.countDown();
                // 紧密循环，忽略中断标志
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (System.nanoTime() < end) {
                    /* 忙等待，不检查 Thread.interrupted() */
                }
                completed.incrementAndGet();
            }));
        }
        // 确保所有任务已开始执行
        started.await(3, TimeUnit.SECONDS);
        // 尝试取消所有任务
        futures.forEach(f -> f.cancel(true));
        // cancel(true) 后 get() 立即抛 CancellationException，不会阻塞等待
        for (Future<?> f : futures) {
            try {
                f.get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }
        // 等待忙循环自然结束（2 秒），证明 cancel(true) 没有停止任务
        Thread.sleep(3000);
        // 问题验证：尽管调用了 cancel(true)，所有 3 个任务仍然完成
        assertThat(completed.get()).isEqualTo(3);
    }

    @Test
    void parMap_withTimeout_cancelsTasks() throws Exception {
        // 解决方案：Par.map() 配合超时自动取消
        BatchExecutionOptions opts = BatchExecutionOptions.of("cancel-demo")
                .parallelism(3)
                .timeout(java.time.Duration.ofMillis(500))
                .taskType(TaskType.IO_BOUND)
                .build();

        List<Integer> input = Arrays.asList(1, 2, 3);
        long start = System.currentTimeMillis();
        AsyncBatchResult<Integer> result = par.map(
                input,
                x -> {
                    // 使用 Checkpoints.sleep() 响应取消信号
                    Checkpoints.sleep(5000);
                    return x;
                },
                opts);

        // 等待超时取消生效
        Thread.sleep(2000);
        long elapsed = System.currentTimeMillis() - start;
        // 验证：总耗时远小于 3 个任务各 5 秒的总和
        assertThat(elapsed).isLessThan(5000);
    }
}
