package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.Futures;
import io.github.huatalk.parallelinscope.scope.AsyncBatchResult;
import io.github.huatalk.parallelinscope.scope.BatchExecutionOptions;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.Par;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * B2: 函数签名膨胀 — 上下文传播导致参数污染
 *
 * <p>演示当需要传递 traceId、timeout、cancelFlag 等基础设施参数时， 业务函数签名被"管道参数"淹没的问题，以及 parallel-in-scope 通过 TTL
 * 隐式传播上下文的解决方案。
 */
public class B2_FunctionSignatureBloatTest {

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

    /** 模拟膨胀的函数签名：一个简单的数据获取操作， 被迫接收 traceId、timeout、cancelFlag 三个基础设施参数。 */
    private static String fetchWithBloat(String url, String traceId, long timeoutMs, boolean[] cancelFlag) {
        // 手动检查取消
        if (cancelFlag[0]) {
            return "CANCELED";
        }
        // 手动传递 traceId（模拟 MDC 设置）
        String oldTrace = Thread.currentThread().getName();
        try {
            return "fetched-" + url + "-trace-" + traceId;
        } finally {
            // cleanup
        }
    }

    /** 模拟更极端的膨胀：批量处理函数需要搬运 6 个基础设施参数。 */
    private static List<String> batchFetchWithBloat(
            List<String> urls,
            String traceId,
            String userId,
            String orgId,
            long timeoutMs,
            boolean[] cancelFlag,
            int retryCount) {
        List<String> results = new ArrayList<>();
        for (String url : urls) {
            if (cancelFlag[0]) break;
            for (int retry = 0; retry <= retryCount; retry++) {
                try {
                    results.add("fetched-" + url + "-org-" + orgId + "-user-" + userId);
                    break;
                } catch (Exception e) {
                    if (retry == retryCount) throw new RuntimeException(e);
                }
            }
        }
        return results;
    }

    @Test
    void bloatedSignature_requiresPlumbingParameters() {
        // 问题演示：每个调用都必须传递基础设施参数
        String traceId = "trace-abc-123";
        long timeoutMs = 5000;
        boolean[] cancelFlag = new boolean[] {false};

        // 单次调用：4 个参数，只有 1 个是业务参数
        String result1 = fetchWithBloat("http://api.example.com/users", traceId, timeoutMs, cancelFlag);
        assertThat(result1).contains("fetched-");
        assertThat(result1).contains("trace-trace-abc-123");

        // 批量调用：7 个参数，只有 1 个是业务参数
        List<String> urls = Arrays.asList("url1", "url2", "url3");
        List<String> results = batchFetchWithBloat(urls, traceId, "user-1", "org-1", timeoutMs, cancelFlag, 3);
        assertThat(results).hasSize(3);
        assertThat(results.get(0)).contains("org-org-1");

        // 验证：函数签名中基础设施参数占比过高
        // fetchWithBloat: 4 参数，3 个是管道参数（75%）
        // batchFetchWithBloat: 7 参数，6 个是管道参数（86%）
    }

    // ==================== 解决方案 ====================

    @Test
    void parMap_cleanSignature_onlyBusinessParam() throws Exception {
        // 解决方案：Par.map() 隐式传播上下文，lambda 只需业务参数
        BatchExecutionOptions opts = BatchExecutionOptions.of("fetch-data")
                .parallelism(3)
                .timeout(java.time.Duration.ofMillis(5000))
                .build();

        List<String> urls = Arrays.asList(
                "http://api.example.com/users", "http://api.example.com/orders", "http://api.example.com/products");

        // lambda 只需要 url 这一个业务参数
        // CancellationToken、超时、并发控制全部由框架隐式处理
        AsyncBatchResult<String> result = par.map(
                urls,
                url -> {
                    // 只关注业务逻辑，无需关心 traceId、timeout、cancelFlag
                    return "fetched-" + url;
                },
                opts);

        // Par.map() returns immediately; wait for the terminal states before reporting.
        Futures.allAsList(result.results()).get();

        // 验证所有任务成功完成
        String report = result.reportString();
        assertThat(report).startsWith("SUCCESS:3");

        // 验证函数签名对比：
        // 膨胀版: fetch(url, traceId, userId, orgId, timeout, cancelFlag, retryCount) = 7 参数
        // 清洁版: url -> fetched(url) = 1 参数（业务参数）
        // 基础设施参数从 86% 降至 0%
    }

    @Test
    void parMap_multipleContextParams_allHandledImplicitly() {
        // 验证：即使有多种上下文需求，Par.map() 的 lambda 签名依然干净
        AtomicInteger processedCount = new AtomicInteger(0);

        BatchExecutionOptions opts = BatchExecutionOptions.of("complex-task")
                .parallelism(2)
                .timeout(java.time.Duration.ofMillis(3000))
                .build();

        List<Integer> orderIds = Arrays.asList(101, 102, 103, 104);

        AsyncBatchResult<String> result = par.map(
                orderIds,
                orderId -> {
                    // 框架已自动处理以下所有基础设施需求：
                    //   - 取消检查（ScopedCallable 内部 checkpoint）
                    //   - 超时控制（CancellationToken.lateBind）
                    //   - 并发限制（滑动窗口调度）
                    //   - 任务生命周期（TaskListener SPI 回调）
                    //
                    // 开发者只需写纯业务代码，无需任何管道参数
                    processedCount.incrementAndGet();
                    return "order-" + orderId + "-processed";
                },
                opts);

        // 等待任务完成
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 验证所有任务都被处理
        assertThat(processedCount.get()).isEqualTo(4);

        String report = result.reportString();
        assertThat(report).startsWith("SUCCESS:4");
    }
}
