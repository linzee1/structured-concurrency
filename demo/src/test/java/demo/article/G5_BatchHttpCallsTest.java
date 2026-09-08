package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * G5: 批量 HTTP 调用——一个超时其余继续
 *
 * <p>演示问题：CompletableFuture.allOf() 没有并发控制、没有 fail-fast、超时后任务还在跑。
 *
 * <p>演示解决：Par.map() + MultiTaskOptions 一行搞定 parallelism + timeout + fail-fast。
 */
public class G5_BatchHttpCallsTest {

    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        pool = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    void tearDown() {
        pool.shutdownNow();
    }

    /**
     * 问题复现：CompletableFuture.allOf() 没有 fail-fast。
     *
     * <p>"payment" 在 10ms 内就抛了异常，但 allOf 等所有任务跑完才返回。 其余 9 个任务各 sleep 300ms，全部执行完毕后才拿到第一个异常。
     * 没有任何任务被取消——资源白白浪费。
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void allOf_noFailFast_allTasksRunDespiteEarlyFailure() throws Exception {
        List<String> services = Arrays.asList(
                "order",
                "user",
                "payment",
                "inventory",
                "notification",
                "billing",
                "shipping",
                "review",
                "recommendation",
                "analytics");

        AtomicInteger completedCount = new AtomicInteger(0);

        // 一次性提交所有任务——没有并发控制
        List<CompletableFuture<String>> futures = services.stream()
                .map(svc -> CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                if ("payment".equals(svc)) {
                                    // payment 快速失败
                                    throw new RuntimeException("payment service unavailable");
                                }
                                // 模拟慢 HTTP 调用，每个耗时 300ms
                                Thread.sleep(300);
                                completedCount.incrementAndGet();
                                return svc + ":ok";
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return svc + ":interrupted";
                            }
                        },
                        pool))
                .collect(Collectors.toList());

        // allOf 等所有任务完成——即使 payment 早就失败了
        long start = System.currentTimeMillis();
        Throwable caught = null;
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            caught = (e instanceof java.util.concurrent.CompletionException) ? e.getCause() : e;
        }
        long elapsed = System.currentTimeMillis() - start;

        // 验证：确实拿到了 payment 的异常
        assertThat(caught).isInstanceOf(RuntimeException.class);
        assertThat(caught.getMessage()).contains("payment service unavailable");

        // 验证：其余 9 个任务全部跑完了——没有 fail-fast
        assertThat(completedCount.get())
                .as("allOf has no fail-fast: all 9 non-failing tasks still completed")
                .isEqualTo(9);

        // 验证：总耗时约 300ms（所有任务并行跑），不是 9*300ms
        assertThat(elapsed)
                .as("All tasks ran in parallel (4-thread pool), but no cancellation")
                .isLessThan(2000);
    }

    /**
     * 解决方法：Par.map() + MultiTaskOptions，并发控制 + fail-fast。
     *
     * <p>parallelism=4 限制最多 4 个并发，滑动窗口调度。
     *
     * <p>"payment" 失败后，框架自动取消剩余未完成任务（fail-fast）。
     *
     * <p>reportString() 一行查看状态分布。
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void parMap_batchHttpCalls_failFastCancelsSiblings() throws Exception {
        GlobalPar config = GlobalPar.builder()
                .register("test-pool", pool)
                .defaultPar("test-pool")
                .build();
        Par par = config.defaultPar();

        List<String> services = Arrays.asList(
                "order",
                "user",
                "payment",
                "inventory",
                "notification",
                "billing",
                "shipping",
                "review",
                "recommendation",
                "analytics");

        MultiTaskOptions opts = MultiTaskOptions.of("batch-http")
                .parallelism(4)
                .timeout(java.time.Duration.ofMillis(5000))
                .taskType(TaskType.IO_BOUND)
                .build();

        AtomicInteger completedCount = new AtomicInteger(0);

        TaskBatchResult<String> result = par.map(
                services,
                svc -> {
                    try {
                        if ("payment".equals(svc)) {
                            // payment 快速失败——触发 fail-fast，取消兄弟任务
                            throw new RuntimeException("payment service unavailable");
                        }
                        // 模拟慢 HTTP 调用
                        Thread.sleep(500);
                        completedCount.incrementAndGet();
                        return svc + ":ok";
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return svc + ":interrupted";
                    }
                },
                opts);

        // 等待足够时间让 fail-fast 生效并取消剩余任务
        Thread.sleep(3000);

        // 验证：fail-fast 生效，不是所有任务都完成了
        assertThat(completedCount.get())
                .as("Par.map() fail-fast: not all tasks completed")
                .isLessThan(services.size());

        // 验证：report 包含 CANCELLED（fail-fast 取消了兄弟任务）和 FAILED
        String report = result.reportString();
        assertThat(report).as("Report should show USER_FAILURE task (payment)").contains("USER_FAILURE");
        assertThat(report)
                .as("Report should show MEMBER_CANCELED tasks from fail-fast")
                .contains("MEMBER_CANCELED");
    }
}
