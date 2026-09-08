package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.scope.AsyncBatchResult;
import io.github.huatalk.parallelinscope.scope.BatchExecutionOptions;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * D1: get(timeout) 后任务还在跑
 *
 * <p>演示标准 Future.get(timeout) 超时后任务仍在运行的问题， 以及 parallel-in-scope Par.map() 超时自动取消的解决方案。
 */
public class D1_GetTimeoutStillRunningTest {

    private ExecutorService pool;
    private Par par;

    @BeforeEach
    void setUp() {
        pool = Executors.newFixedThreadPool(2);
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
    void futureGet_timeout_taskStillRunning() throws Exception {
        // 问题演示：Future.get(timeout) 超时后，任务仍在运行
        AtomicBoolean task1Completed = new AtomicBoolean(false);
        AtomicBoolean task2Completed = new AtomicBoolean(false);

        Future<?> future1 = pool.submit(() -> {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            task1Completed.set(true);
            return "task1";
        });

        Future<?> future2 = pool.submit(() -> {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            task2Completed.set(true);
            return "task2";
        });

        // 调用者等待 500ms 后超时
        try {
            future1.get(500, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // 预期：调用者收到超时异常
        }

        try {
            future2.get(500, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // 预期：调用者收到超时异常
        }

        // 验证：此时任务仍在运行，尚未完成
        assertThat(task1Completed.get()).isFalse();
        assertThat(task2Completed.get()).isFalse();

        // 等待任务自然完成
        // 说明：虽然调用者已超时返回，但任务继续占用线程直到完成
        future1.get(5, TimeUnit.SECONDS);
        future2.get(5, TimeUnit.SECONDS);

        // 验证：任务最终完成了（但调用者早已超时离开）
        assertThat(task1Completed.get()).isTrue();
        assertThat(task2Completed.get()).isTrue();
    }

    @Test
    void parMap_withTimeout_cancelsTasks() throws Exception {
        // 解决方案：Par.map() 配合超时自动取消
        AtomicBoolean task1Completed = new AtomicBoolean(false);
        AtomicBoolean task2Completed = new AtomicBoolean(false);

        BatchExecutionOptions opts = BatchExecutionOptions.of("cancel-demo")
                .parallelism(2)
                .timeout(java.time.Duration.ofMillis(500))
                .taskType(TaskType.IO_BOUND)
                .build();

        List<Integer> input = Arrays.asList(1, 2);
        AsyncBatchResult<Integer> result = par.map(
                input,
                x -> {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        // 被中断，记录状态后退出
                        if (x == 1) task1Completed.set(false);
                        else task2Completed.set(false);
                        throw new RuntimeException("Task cancelled", e);
                    }
                    if (x == 1) task1Completed.set(true);
                    else task2Completed.set(true);
                    return x;
                },
                opts);

        // 等待超时取消生效
        Thread.sleep(2000);

        // 验证：任务被取消，不会完成
        // task1Completed 和 task2Completed 保持初始值 false
        assertThat(task1Completed.get()).isFalse();
        assertThat(task2Completed.get()).isFalse();

        // 验证：所有 Future 都已取消
        for (Future<Integer> future : result.results()) {
            assertThat(future.isCancelled() || future.isDone()).isTrue();
        }
    }
}
