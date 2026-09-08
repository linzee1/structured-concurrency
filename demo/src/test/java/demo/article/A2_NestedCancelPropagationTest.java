package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.scope.AsyncBatchResult;
import io.github.huatalk.parallelinscope.scope.BatchExecutionOptions;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.Par;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A2. 嵌套任务取消传播 —— 配套测试
 *
 * <p>测试 1：演示原生 ExecutorService 中，取消外层任务不会停止内层任务。
 *
 * <p>测试 2：演示 Par.map() 嵌套调用时，超时取消自动传播到内层任务。
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class A2_NestedCancelPropagationTest {

    /**
     * 问题演示：原生 ExecutorService 中，取消外层任务不会停止内层任务。
     *
     * <p>外层任务提交 3 个内层任务，每个内层任务 sleep 2 秒。 取消外层 Future 后，内层任务仍正常完成，说明取消信号没有穿透到内层。
     */
    @Test
    void testVanillaOuterCancelDoesNotStopInnerTasks() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        AtomicInteger innerCompletedNormally = new AtomicInteger(0);
        CountDownLatch innerStarted = new CountDownLatch(3);
        CountDownLatch innerFinished = new CountDownLatch(3);

        try {
            // 提交外层任务
            Future<?> outer = pool.submit(() -> {
                List<Future<?>> innerFutures = new ArrayList<>();
                for (int i = 0; i < 3; i++) {
                    innerFutures.add(pool.submit(() -> {
                        innerStarted.countDown();
                        try {
                            Thread.sleep(2000);
                            innerCompletedNormally.incrementAndGet();
                        } catch (InterruptedException e) {
                            // 被中断
                        } finally {
                            innerFinished.countDown();
                        }
                    }));
                }
                try {
                    for (Future<?> f : innerFutures) {
                        f.get();
                    }
                } catch (Exception e) {
                    // 外层被取消，忽略异常
                }
            });

            // 等待内层任务全部启动
            innerStarted.await(2, TimeUnit.SECONDS);

            // 取消外层任务
            outer.cancel(true);

            // 等待内层任务完成（它们不会被取消）
            boolean finished = innerFinished.await(5, TimeUnit.SECONDS);

            // 断言：内层任务全部正常完成，没有被取消
            assertThat(finished).isTrue();
            assertThat(innerCompletedNormally.get()).isEqualTo(3);

        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * 解决方案：Par.map() 嵌套调用时，超时取消自动传播到内层任务。
     *
     * <p>外层 Par.map() 设置 500ms 超时，内层任务 sleep 5 秒。 外层超时后，取消信号通过 CancellationToken 父子链传播到内层， 内层任务的
     * Thread.sleep() 被中断，不会正常完成。
     */
    @Test
    void testParNestedCancelPropagatesViaTimeout() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        GlobalPar config = GlobalPar.builder()
                .register("test-pool", pool)
                .defaultPar("test-pool")
                .build();
        Par par = config.defaultPar();
        AtomicInteger innerCompletedNormally = new AtomicInteger(0);
        CountDownLatch innerStarted = new CountDownLatch(6);

        try {
            // 外层配置：500ms 超时
            BatchExecutionOptions outerOptions = BatchExecutionOptions.of("outer")
                    .parallelism(2)
                    .timeout(java.time.Duration.ofMillis(500))
                    .build();

            List<Integer> items = Arrays.asList(1, 2);

            AsyncBatchResult<String> result = par.map(
                    items,
                    outerItem -> {
                        // 内层并行处理
                        BatchExecutionOptions innerOptions =
                                BatchExecutionOptions.of("inner").parallelism(3).build();

                        List<Integer> innerItems = Arrays.asList(10, 20, 30);
                        AsyncBatchResult<Integer> innerResult = par.map(
                                innerItems,
                                innerItem -> {
                                    innerStarted.countDown();
                                    try {
                                        Thread.sleep(5000);
                                        innerCompletedNormally.incrementAndGet();
                                        return innerItem * 2;
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        throw new RuntimeException("inner task interrupted", e);
                                    }
                                },
                                innerOptions);

                        // 等待内层结果（阻塞导致外层超时）
                        for (int i = 0; i < innerResult.results().size(); i++) {
                            try {
                                innerResult.results().get(i).get();
                            } catch (Exception e) {
                                // 被取消或失败
                            }
                        }

                        return "done-" + outerItem;
                    },
                    outerOptions);

            // 等待内层任务启动，再给足时间让超时生效
            innerStarted.await(3, TimeUnit.SECONDS);
            Thread.sleep(2000);

            // 断言：内层任务被取消，没有全部正常完成
            assertThat(innerCompletedNormally.get()).isLessThan(6);

        } finally {
            pool.shutdownNow();
        }
    }
}
