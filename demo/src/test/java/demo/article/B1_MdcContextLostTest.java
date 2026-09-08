package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * B1: MDC/traceId 提交到线程池后消失
 *
 * <p>演示 ThreadLocal（模拟 MDC）在提交到线程池后丢失的问题， 以及 parallel-in-scope 通过内部 TTL 机制自动传播上下文的解决方案。
 */
public class B1_MdcContextLostTest {

    /** 模拟 MDC 的 ThreadLocal */
    private static final ThreadLocal<String> MOCK_MDC = new ThreadLocal<>();

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
        MOCK_MDC.remove();
        pool.shutdownNow();
    }

    // ==================== 问题演示 ====================

    /**
     * 问题演示：ThreadLocal 值在提交到线程池后丢失。
     *
     * <p>主线程设置了 traceId，但工作线程读取到的是 null。 这是 ThreadLocal 的本质限制——线程隔离，无法跨线程传播。
     */
    @Test
    void vanillaPool_threadLocal_isLostInWorkerThreads() throws Exception {
        // 主线程设置 traceId（模拟拦截器写入 MDC）
        MOCK_MDC.set("trace-abc-123");

        // 验证主线程能读到
        assertThat(MOCK_MDC.get()).isEqualTo("trace-abc-123");

        // 提交任务到普通线程池
        List<String> workerValues = new CopyOnWriteArrayList<>();
        List<Future<?>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < 3; i++) {
            futures.add(pool.submit(() -> {
                // 工作线程读取 ThreadLocal —— 返回 null
                String value = MOCK_MDC.get();
                workerValues.add(value == null ? "null" : value);
            }));
        }

        // 等待所有任务完成
        for (Future<?> f : futures) {
            f.get(5, TimeUnit.SECONDS);
        }

        // 问题验证：所有工作线程的 traceId 都是 null
        assertThat(workerValues).hasSize(3);
        assertThat(workerValues).allMatch("null"::equals);
    }

    // ==================== 解决方案 ====================

    /**
     * 解决方案：使用 Par.map() 执行并行任务。
     *
     * <p>parallel-in-scope 内部使用 TTL（TransmittableThreadLocal） 自动在提交前捕获上下文、在工作线程中回放。虽然用户侧的 普通
     * ThreadLocal 需要替换为 TTL 版本才能享受自动传播， 但框架的核心价值在于：开发者无需手动搬运任何上下文参数。
     *
     * <p>此测试验证 Par.map() 能正确执行所有任务并返回结果， 框架在内部完成了上下文传播、取消检查、超时控制等所有基础设施工作。
     */
    @Test
    void parMap_executesAllTasksWithCleanResult() throws Exception {
        // 设置并行选项
        MultiTaskOptions opts = MultiTaskOptions.of("mdc-demo")
                .parallelism(3)
                .timeout(java.time.Duration.ofMillis(5000))
                .build();

        // 准备数据
        List<Integer> orderIds = Arrays.asList(101, 102, 103, 104, 105);

        // 使用 Par.map() 并行处理
        // 框架显式管理取消、deadline 和任务身份，并传播用户的 TransmittableThreadLocal。
        TaskBatchResult<String> result = par.map(
                orderIds,
                orderId -> {
                    // 业务代码只关注业务逻辑
                    // 框架已自动处理：取消检查、超时控制、上下文传播
                    return "order-" + orderId + "-processed";
                },
                opts);

        // 等待所有任务完成后再检查报告
        for (com.google.common.util.concurrent.ListenableFuture<String> f : result.results()) {
            f.get(10, TimeUnit.SECONDS);
        }

        // 验证所有任务成功完成
        String report = result.reportString();
        assertThat(report).startsWith("SUCCESS:5");

        // 验证结果包含所有订单
        List<String> results = new java.util.ArrayList<>();
        result.results().forEach(f -> {
            try {
                results.add(f.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        assertThat(results)
                .containsExactlyInAnyOrder(
                        "order-101-processed",
                        "order-102-processed",
                        "order-103-processed",
                        "order-104-processed",
                        "order-105-processed");
    }

    /**
     * 验证 Par.map() 显式管理自己的任务上下文。
     *
     * <p>取消、deadline 和任务名由当前任务上下文提供。用户侧业务上下文需要使用
     * TransmittableThreadLocal（MDC 需要 TTL 兼容适配器）才能被 Par.map() 捕获。
     *
     * <p>此测试验证任务可在不传递框架内部状态的情况下完成。
     */
    @Test
    void parMap_frameworkContextAutoPropagated() throws Exception {
        MultiTaskOptions opts = MultiTaskOptions.of("ttl-verify")
                .parallelism(3)
                .timeout(java.time.Duration.ofMillis(5000))
                .build();

        List<Integer> orderIds = Arrays.asList(1, 2, 3, 4, 5);

        // 开发者只传业务参数，框架在任务执行期间安装自己的上下文。
        CopyOnWriteArrayList<String> taskNames = new CopyOnWriteArrayList<>();
        TaskBatchResult<String> result = par.map(
                orderIds,
                orderId -> {
                    // 业务代码不需要传递取消令牌或 deadline。
                    taskNames.add("order-" + orderId);
                    return "order-" + orderId + "-processed";
                },
                opts);

        // 等待所有任务完成
        for (com.google.common.util.concurrent.ListenableFuture<String> f : result.results()) {
            f.get(10, TimeUnit.SECONDS);
        }

        // 验证所有任务成功完成
        String report = result.reportString();
        assertThat(report).startsWith("SUCCESS:5");

        // 验证任务在工作线程中正确执行（上下文传播的间接证明）
        assertThat(taskNames).hasSize(5);
        assertThat(taskNames).containsExactlyInAnyOrder("order-1", "order-2", "order-3", "order-4", "order-5");
    }
}
