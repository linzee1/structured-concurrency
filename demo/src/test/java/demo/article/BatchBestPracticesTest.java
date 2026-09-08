package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * 批量调用最佳实践：HTTP/DB/RPC 完整方案。
 *
 * <p>三个真实场景：批量 HTTP 调用、数据库分片查询、混合 IO 调用。 每个场景都演示 parallelism 控制 + timeout 保护 + fail-fast +
 * reportString() 统计。
 */
public class BatchBestPracticesTest {

    private ExecutorService pool;
    private Par par;

    @BeforeEach
    void setUp() {
        pool = Executors.newFixedThreadPool(8);
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

    // ==================== 场景一：批量 HTTP 调用 ====================

    /**
     * 模拟调用 10 个下游微服务，parallelism=5 保护下游，3 秒超时，fail-fast。
     *
     * <p>"payment" 服务快速失败（50ms），触发 fail-fast 取消剩余任务。 reportString() 应包含 FAILED 和 CANCELLED 状态。
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void scenario1_batchHttpCalls() throws Exception {
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
                .taskType(TaskType.IO_BOUND)
                .parallelism(5)
                .timeout(java.time.Duration.ofMillis(3000))
                .build();

        AtomicInteger completedCount = new AtomicInteger(0);

        TaskBatchResult<String> result = par.map(
                services,
                svc -> {
                    if ("payment".equals(svc)) {
                        // payment 快速失败，触发 fail-fast
                        throw new RuntimeException("payment service unavailable");
                    }
                    // 模拟 HTTP 调用耗时 500ms
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return svc + ":interrupted";
                    }
                    completedCount.incrementAndGet();
                    return svc + ":ok";
                },
                opts);

        // 等待 fail-fast 生效并取消剩余任务
        Thread.sleep(3500);

        // 验证：不是所有任务都完成了（fail-fast 取消了部分）
        assertThat(completedCount.get()).as("fail-fast 应取消部分任务，不是全部完成").isLessThan(services.size());

        // 验证：report 包含 FAILED（payment）和 CANCELLED（被取消的兄弟任务）
        String report = result.reportString();
        assertThat(report).as("report 应包含 payment 的 FAILED 状态").contains("USER_FAILURE");
        assertThat(report).as("report 应包含被 fail-fast 取消的任务").contains("MEMBER_CANCELED");
    }

    // ==================== 场景二：数据库分片查询 ====================

    /**
     * 模拟 10000 条 ID 分 10 片并行查询，parallelism=3 匹配 DB 连接池。
     *
     * <p>每片模拟查询耗时 50ms，串行需要 500ms，并行后约 ceil(10/3)*50=200ms。 最终合并结果验证完整性。
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void scenario2_dbShardQuery() throws Exception {
        int totalIds = 10000;
        int shardSize = 1000;
        int parallelism = 3;

        // 构造 10000 个 ID
        List<Long> allIds =
                IntStream.rangeClosed(1, totalIds).mapToObj(Long::valueOf).collect(Collectors.toList());

        // 分片：每 1000 个 ID 一片
        List<List<Long>> shards = new ArrayList<>();
        for (int i = 0; i < allIds.size(); i += shardSize) {
            shards.add(new ArrayList<>(allIds.subList(i, Math.min(i + shardSize, allIds.size()))));
        }
        assertThat(shards).hasSize(10);

        MultiTaskOptions opts = MultiTaskOptions.of("db-batch-query")
                .taskType(TaskType.IO_BOUND)
                .parallelism(parallelism)
                .timeout(java.time.Duration.ofMillis(30000))
                .build();

        long start = System.currentTimeMillis();

        // 并行分片查询
        TaskBatchResult<List<Long>> result = par.map(
                shards,
                shard -> {
                    // 模拟 DB 查询耗时 50ms
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return shard;
                },
                opts);

        // 合并分片结果
        List<Long> queriedIds = new ArrayList<>();
        for (int i = 0; i < result.results().size(); i++) {
            queriedIds.addAll(result.results().get(i).get(30, TimeUnit.SECONDS));
        }

        long elapsed = System.currentTimeMillis() - start;

        // 验证结果完整性
        assertThat(queriedIds).hasSize(totalIds);

        // 验证并行比串行快：串行 10*50=500ms，并行 ceil(10/3)*50≈200ms
        assertThat(elapsed).as("并行分片查询 (%dms) 应快于串行 500ms", elapsed).isLessThan(500);

        // 验证 report 全部成功
        String report = result.reportString();
        assertThat(report).as("所有分片查询应全部成功").contains("SUCCESS:" + shards.size());
    }

    // ==================== 场景三：混合 IO 调用 ====================

    /**
     * 模拟混合 IO：HTTP + DB + 缓存，统一 5 秒超时，fail-fast。
     *
     * <p>5 个任务中第 3 个（模拟 HTTP 调用）快速失败， 触发 fail-fast 取消剩余未完成任务。
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void scenario3_mixedIoCalls() throws Exception {
        // 模拟混合任务：0=DB, 1=Cache, 2=HTTP(会失败), 3=DB, 4=Cache
        List<String> tasks = Arrays.asList("db-1", "cache-1", "http-1", "db-2", "cache-2");

        MultiTaskOptions opts = MultiTaskOptions.of("mixed-io")
                .taskType(TaskType.IO_BOUND)
                .parallelism(3)
                .timeout(java.time.Duration.ofMillis(5000))
                .build();

        AtomicInteger completedCount = new AtomicInteger(0);

        TaskBatchResult<String> result = par.map(
                tasks,
                task -> {
                    if (task.startsWith("http")) {
                        // HTTP 调用快速失败
                        throw new RuntimeException("HTTP 503 Service Unavailable");
                    }
                    // DB 和 Cache 模拟 800ms 延迟
                    try {
                        Thread.sleep(800);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return task + ":interrupted";
                    }
                    completedCount.incrementAndGet();
                    return task + ":ok";
                },
                opts);

        // 等待 fail-fast 生效
        Thread.sleep(3000);

        // 验证：不是全部完成
        assertThat(completedCount.get()).as("fail-fast 应取消部分任务").isLessThan(tasks.size());

        // 验证 report 包含 FAILED 和 CANCELLED
        String report = result.reportString();
        assertThat(report).as("report 应包含 HTTP 调用的 FAILED 状态").contains("USER_FAILURE");
        assertThat(report).as("report 应包含被取消的任务").contains("MEMBER_CANCELED");
    }
}
