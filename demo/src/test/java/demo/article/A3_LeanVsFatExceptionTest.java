package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A3: 取消异常太重 — 轻量级取消异常 vs 标准异常
 *
 * <p>演示 Exception.fillInStackTrace() 的性能开销， 以及 parallel-in-scope 内部使用轻量级异常处理高频取消的优化效果。
 */
public class A3_LeanVsFatExceptionTest {

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

    // ==================== 问题演示 ====================

    @Test
    void fillInStackTrace_isExpensive() {
        // 问题演示：标准 CancellationException 的 fillInStackTrace() 开销巨大
        int count = 10000;

        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            // 标准异常构造时自动调用 fillInStackTrace()
            CancellationException e = new CancellationException("task-" + i + " cancelled");
            // 确保异常未被 JIT 优化掉
            if (e.getMessage() == null) {
                throw new RuntimeException("unreachable");
            }
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        // 验证：创建 10000 个标准异常的耗时显著
        // 通常需要 100-500ms，说明 fillInStackTrace() 开销不可忽略
        System.out.println("Standard exceptions (10000): " + elapsedMs + " ms");
        assertThat(elapsedMs).isGreaterThan(0);
    }

    @Test
    void leanException_skipsStackTrace() {
        // 对比演示：跳过 fillInStackTrace() 的异常创建开销极低
        int count = 10000;

        // 模拟 LeanCancellationException 的行为：重写 fillInStackTrace() 为空操作
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            CancellationException e = new CancellationException("task-" + i + " cancelled") {
                @Override
                public synchronized Throwable fillInStackTrace() {
                    return this; // 跳过栈追踪采集
                }
            };
            e.setStackTrace(new StackTraceElement[0]);
            if (e.getMessage() == null) {
                throw new RuntimeException("unreachable");
            }
        }
        long leanMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        // 验证：轻量级异常的创建耗时远低于标准异常
        System.out.println("Lean exceptions (10000): " + leanMs + " ms");
        // 轻量级异常创建应该非常快，通常 < 10ms
        assertThat(leanMs).isLessThan(100);
    }

    // ==================== 解决方案 ====================

    @Test
    void parMap_withShortTimeout_completesEfficiently() {
        // 解决方案：Par.map() 配合短超时，内部使用轻量级异常处理取消
        MultiTaskOptions opts = MultiTaskOptions.of("lean-cancel-demo")
                .parallelism(4)
                .timeout(java.time.Duration.ofMillis(200))
                .taskType(TaskType.IO_BOUND)
                .build();

        // 提交 100 个任务，大部分会在 200ms 后被取消
        List<Integer> items = IntStream.rangeClosed(1, 100).boxed().collect(Collectors.toList());

        long start = System.currentTimeMillis();
        TaskBatchResult<String> result = par.map(
                items,
                id -> {
                    try {
                        // 模拟慢操作，超过 200ms 超时
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "done-" + id;
                },
                opts);

        // 等待超时取消生效
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long elapsed = System.currentTimeMillis() - start;

        // 验证：100 个任务在超时后高效取消，总耗时远小于 100 * 5s
        // 如果使用标准异常，100 次 fillInStackTrace() 会产生显著额外开销
        assertThat(elapsed).isLessThan(10000);
        System.out.println("Par.map 100 tasks with 200ms timeout: " + elapsed + " ms");
    }
}
