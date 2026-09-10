package demo.article;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * C2. CachedThreadPool 不会死锁 — 问题复现与对比验证
 *
 * <p>场景：同一个嵌套并行模式，分别用 FixedThreadPool 和 CachedThreadPool 执行。 FixedThreadPool 因为有界队列 + 固定线程数导致死锁；
 * CachedThreadPool 因为 SynchronousQueue + 按需创建线程，天然不会死锁。
 *
 * <p>Test 1: FixedThreadPool 嵌套并行导致死锁
 *
 * <p>Test 2: CachedThreadPool 同样的嵌套模式正常完成
 */
class C2_CachedVsFixedPoolTest {

    /**
     * Test 1 — 问题复现：FixedThreadPool 嵌套并行导致死锁
     *
     * <p>2 线程的固定池，外层提交 2 个任务占满线程池。 每个外层任务内部向同一个池提交内层子任务并等待结果。 内层子任务排队等线程，外层任务等内层完成——循环等待死锁。
     *
     * <p>使用 Future.get(timeout) 捕获 TimeoutException 来证明死锁发生。
     */
    @Test
    void testFixedThreadPoolNestedParallelismDeadlocks() throws Exception {
        // 2 线程的固定池——嵌套调用时确定性死锁
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CyclicBarrier outerTasksStarted = new CyclicBarrier(2);

        try {
            // 外层：提交 2 个任务，正好占满 2 个线程
            Future<String> outer1 = pool.submit(() -> {
                outerTasksStarted.await(2, TimeUnit.SECONDS);
                // 内层：向同一个池提交子任务并等待
                Future<String> inner = pool.submit(() -> "inner-result");
                // 阻塞等待内层结果——但 2 个线程已被外层占满，内层永远无法执行
                return inner.get(2, TimeUnit.SECONDS);
            });

            Future<String> outer2 = pool.submit(() -> {
                outerTasksStarted.await(2, TimeUnit.SECONDS);
                Future<String> inner = pool.submit(() -> "inner-result");
                return inner.get(2, TimeUnit.SECONDS);
            });

            // 内层 get(2s) 超时后，外层 Callable 抛出 TimeoutException
            // Future.get() 将其包装为 ExecutionException(TimeoutException)
            // 至少一个外层任务应该因为内层超时而失败
            boolean anyFailed = false;
            try {
                outer1.get(5, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                assertThat(e).hasCauseInstanceOf(TimeoutException.class);
                anyFailed = true;
            }
            try {
                outer2.get(5, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                assertThat(e).hasCauseInstanceOf(TimeoutException.class);
                anyFailed = true;
            }
            assertThat(anyFailed).as("At least one outer task should deadlock").isTrue();

        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Test 2 — 对比验证：CachedThreadPool 同样的嵌套模式正常完成
     *
     * <p>与 Test 1 完全相同的嵌套结构，但使用 CachedThreadPool。 CachedThreadPool 底层使用 SynchronousQueue，没有任务队列，
     * 每当有新任务提交时自动创建新线程。内层子任务总能获得线程执行，不会死锁。
     */
    @Test
    void testCachedThreadPoolNestedParallelismNoDeadlock() {
        // CachedThreadPool：按需创建线程，无界——不会死锁
        ExecutorService pool = Executors.newCachedThreadPool();

        try {
            // 外层：提交 2 个任务，与 Test 1 相同的结构
            Future<String> outer1 = pool.submit(() -> {
                // 内层：向同一个 CachedThreadPool 提交子任务
                // CachedThreadPool 自动创建新线程，内层子任务立即执行 → 无死锁
                Future<String> inner = pool.submit(() -> "inner-result");
                return inner.get(5, TimeUnit.SECONDS);
            });

            Future<String> outer2 = pool.submit(() -> {
                Future<String> inner = pool.submit(() -> "inner-result");
                return inner.get(5, TimeUnit.SECONDS);
            });

            // 验证所有任务正常完成，无死锁
            assertThatCode(() -> {
                        String result1 = outer1.get(10, TimeUnit.SECONDS);
                        String result2 = outer2.get(10, TimeUnit.SECONDS);
                        assertThat(result1).isEqualTo("inner-result");
                        assertThat(result2).isEqualTo("inner-result");
                    })
                    .doesNotThrowAnyException();

        } finally {
            pool.shutdownNow();
        }
    }
}
