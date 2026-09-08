package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.util.ArrayList;
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
 * G6. 数据库批量查询——分片并行
 *
 * <p>问题：单次 SELECT 查询 10000 条数据太慢，想分片并行但手动管理线程池、分片、 结果合并代码量大且容易出错。
 *
 * <p>解决：Par.map() 天然适合分片并行场景——把 ID 列表切片后交给 Par.map()， 每片并行查询数据库，结果自动聚合，parallelism 控制并发度。
 */
class G6_BatchDbQueryTest {

    private static final int TOTAL_IDS = 10000;
    private static final int SHARD_SIZE = 1000;
    private static final int SHARD_COUNT = TOTAL_IDS / SHARD_SIZE; // 10 片
    private static final long SIMULATED_QUERY_MS = 50; // 每片查询模拟耗时

    private ExecutorService pool;
    private Par par;

    @BeforeEach
    void setUp() {
        pool = Executors.newFixedThreadPool(8);
        GlobalPar config = GlobalPar.builder()
                .register("db-pool", pool)
                .defaultPar("db-pool")
                .build();
        par = config.defaultPar();
    }

    @AfterEach
    void tearDown() {
        pool.shutdownNow();
    }

    /** 模拟数据库分片查询：给定一批 ID，返回对应的 User 对象。 内部使用 Thread.sleep 模拟数据库查询延迟。 */
    private List<User> simulateDbQuery(List<Long> ids) {
        try {
            Thread.sleep(SIMULATED_QUERY_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return ids.stream().map(id -> new User(id, "user_" + id)).collect(Collectors.toList());
    }

    /** 手动分片：将列表按指定大小切分。 */
    private <T> List<List<T>> partition(List<T> list, int size) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(new ArrayList<>(list.subList(i, Math.min(i + size, list.size()))));
        }
        return partitions;
    }

    /**
     * 问题复现：串行分片查询，10 个分片依次执行，总耗时 = 10 * 50ms = ~500ms。
     *
     * <p>每次调用 selectByIds 都是同步阻塞，分片之间没有任何并行。 当分片数量多、每片查询慢时，总耗时线性增长。
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void problem_sequentialShardQuery() {
        List<Long> allIds =
                IntStream.rangeClosed(1, TOTAL_IDS).mapToObj(Long::valueOf).collect(Collectors.toList());

        List<List<Long>> shards = partition(allIds, SHARD_SIZE);
        assertThat(shards).hasSize(SHARD_COUNT);

        long start = System.currentTimeMillis();

        // 串行查询：每个分片依次执行
        List<User> allUsers = new ArrayList<>();
        for (List<Long> shard : shards) {
            allUsers.addAll(simulateDbQuery(shard));
        }

        long elapsed = System.currentTimeMillis() - start;

        // 验证结果完整性
        assertThat(allUsers).hasSize(TOTAL_IDS);

        // 串行耗时 >= 10 * 50ms = 500ms
        assertThat(elapsed)
                .as("串行分片查询耗时应 >= %dms（10 片 * %dms/片）", SHARD_COUNT * SIMULATED_QUERY_MS, SIMULATED_QUERY_MS)
                .isGreaterThanOrEqualTo(SHARD_COUNT * SIMULATED_QUERY_MS);
    }

    /**
     * 解决方案：Par.map() 分片并行查询，parallelism=5，最多 5 个分片同时执行。
     *
     * <p>10 个分片通过 Par.map() 并行执行，parallelism=5 保证最多 5 个分片同时查询。 理论耗时 = ceil(10/5) * 50ms =
     * ~100ms，相比串行的 ~500ms 提升约 5 倍。
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void solution_parMapParallelShardQuery() throws Exception {
        int parallelism = 5;

        List<Long> allIds =
                IntStream.rangeClosed(1, TOTAL_IDS).mapToObj(Long::valueOf).collect(Collectors.toList());

        List<List<Long>> shards = partition(allIds, SHARD_SIZE);
        assertThat(shards).hasSize(SHARD_COUNT);

        MultiTaskOptions options = MultiTaskOptions.of("db-batch-query")
                .taskType(TaskType.IO_BOUND)
                .parallelism(parallelism)
                .timeout(java.time.Duration.ofMillis(30000))
                .build();

        long start = System.currentTimeMillis();

        // 并行查询：最多 parallelism 个分片同时执行
        TaskBatchResult<List<User>> result = par.map(
                shards,
                shard -> {
                    return simulateDbQuery(shard);
                },
                options);

        // 收集所有分片结果
        List<User> allUsers = new ArrayList<>();
        for (int i = 0; i < result.results().size(); i++) {
            allUsers.addAll(result.results().get(i).get(30, TimeUnit.SECONDS));
        }

        long elapsed = System.currentTimeMillis() - start;

        // 验证结果完整性
        assertThat(allUsers).hasSize(TOTAL_IDS);

        // 并行耗时应明显低于串行：ceil(10/5) * 50ms = 100ms，留余量取 400ms
        long sequentialTime = SHARD_COUNT * SIMULATED_QUERY_MS;
        assertThat(elapsed)
                .as("并行分片查询耗时 (%dms) 应明显低于串行耗时 (%dms)", elapsed, sequentialTime)
                .isLessThan(sequentialTime);

        // 验证 reportString 表示全部成功
        String report = result.reportString();
        assertThat(report).as("所有分片查询应全部成功").contains("SUCCESS");
    }

    /** 额外验证：parallelism 控制并发度，峰值并发不超过设定值。 */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void solution_concurrencyControlled() throws Exception {
        int parallelism = 3;

        List<Long> allIds =
                IntStream.rangeClosed(1, TOTAL_IDS).mapToObj(Long::valueOf).collect(Collectors.toList());

        List<List<Long>> shards = partition(allIds, SHARD_SIZE);

        AtomicInteger concurrency = new AtomicInteger(0);
        AtomicInteger maxConcurrency = new AtomicInteger(0);

        MultiTaskOptions options = MultiTaskOptions.of("db-batch-query")
                .taskType(TaskType.IO_BOUND)
                .parallelism(parallelism)
                .timeout(java.time.Duration.ofMillis(30000))
                .build();

        TaskBatchResult<List<User>> result = par.map(
                shards,
                shard -> {
                    int cur = concurrency.incrementAndGet();
                    maxConcurrency.updateAndGet(prev -> Math.max(prev, cur));
                    try {
                        return simulateDbQuery(shard);
                    } finally {
                        concurrency.decrementAndGet();
                    }
                },
                options);

        // 等待所有分片完成
        for (int i = 0; i < result.results().size(); i++) {
            result.results().get(i).get(30, TimeUnit.SECONDS);
        }

        // 并发峰值受控
        assertThat(maxConcurrency.get())
                .as("并发峰值 (%d) 应受 parallelism (%d) 控制", maxConcurrency.get(), parallelism)
                .isLessThanOrEqualTo(parallelism + 1);
    }

    /** 简单的 User 数据模型。 */
    static class User {
        final long id;
        final String name;

        User(long id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String toString() {
            return "User{id=" + id + ", name='" + name + "'}";
        }
    }
}
