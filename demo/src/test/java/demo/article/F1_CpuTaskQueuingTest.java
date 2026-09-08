package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * F1. CPU 密集任务排队白等
 *
 * <p>问题：CPU 密集任务在线程池队列中排队等待毫无意义——它们不需要等待外部资源， 只需要 CPU 时间。排队只增加延迟，不提升吞吐。
 *
 * <p>解决：Par.map() + MultiTaskOptions.taskType(TaskType.CPU_BOUND) 通过滑动窗口调度控制并发度， 并利用 CallerRunsPolicy 确保 CPU 任务不排队。
 */
class F1_CpuTaskQueuingTest {

    private static final int POOL_SIZE = 2;
    private static final int TASK_COUNT = 20;
    private static final int COMPUTE_ITERATIONS = 3000;

    /** 模拟 CPU 密集计算：确定性哈希迭代。 */
    private long cpuIntensiveWork(int input) {
        long hash = input;
        for (int i = 0; i < COMPUTE_ITERATIONS; i++) {
            hash = hash * 31 + i;
            hash ^= (hash >>> 16);
        }
        return hash;
    }

    /**
     * 问题复现：原生 FixedThreadPool 提交 CPU 任务，全部排队等待。
     *
     * <p>2 个线程的固定池提交 20 个 CPU 任务，使用 CountDownLatch 模拟同时提交。 所有任务在 2 个池线程上轮流执行，18 个任务必须在队列中排队。
     * 主线程不参与任务执行，证明任务全部排在队列中白等。
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void problem_cpuTasksQueuedInFixedThreadPool() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(POOL_SIZE);
        try {
            long mainThreadId = Thread.currentThread().getId();
            AtomicInteger poolThreadTaskCount = new AtomicInteger(0);
            AtomicInteger mainThreadTaskCount = new AtomicInteger(0);

            // 所有任务通过同一个 latch 同时放行，确保队列积压
            CountDownLatch started = new CountDownLatch(TASK_COUNT);
            CountDownLatch gate = new CountDownLatch(1);

            List<Future<Long>> futures = IntStream.range(0, TASK_COUNT)
                    .mapToObj(i -> pool.submit(() -> {
                        started.countDown();
                        // 等待所有任务都提交后一起执行——暴露队列积压
                        try {
                            gate.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        long tid = Thread.currentThread().getId();
                        if (tid == mainThreadId) {
                            mainThreadTaskCount.incrementAndGet();
                        } else {
                            poolThreadTaskCount.incrementAndGet();
                        }
                        return cpuIntensiveWork(i);
                    }))
                    .collect(Collectors.toList());

            // 等待所有任务进入线程池（2 个运行 + 18 个排队）
            started.await(10, TimeUnit.SECONDS);
            Thread.sleep(200); // 让线程池稳定

            // 队列深度验证：18 个任务在队列中白等
            int queueSize = ((ThreadPoolExecutor) pool).getQueue().size();
            assertThat(queueSize)
                    .as("CPU 任务全部堆积在队列中，队列深度 ≈ %d", TASK_COUNT - POOL_SIZE)
                    .isGreaterThan(TASK_COUNT - POOL_SIZE - 2);

            // 所有任务在池线程执行，主线程不参与
            gate.countDown();
            for (Future<Long> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }

            assertThat(poolThreadTaskCount.get()).as("所有 CPU 任务都在池线程上排队执行").isEqualTo(TASK_COUNT);
            assertThat(mainThreadTaskCount.get()).as("主线程空等——CPU 任务全在队列里排队").isZero();
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * 解决方案：Par.map() + taskType(TaskType.CPU_BOUND)，滑动窗口控制并发，CPU 任务不排队。
     *
     * <p>使用 MultiTaskOptions.taskType(TaskType.CPU_BOUND) 标记 CPU 密集任务。Par.map() 通过滑动窗口调度 确保只有 parallelism
     * 个任务在执行中，其余任务等前一个完成后才提交。 当线程池满时，fallback 机制让 CPU 任务在提交线程上直接执行，避免排队。
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void solution_cpuTaskWithParMapSlidingWindow() throws Exception {
        int poolSize = 4;
        int parallelism = 2;

        ExecutorService pool = Executors.newFixedThreadPool(poolSize);
        GlobalPar config = GlobalPar.builder()
                .register("cpu-pool", pool)
                .defaultPar("cpu-pool")
                .build();
        Par par = config.defaultPar();

        try {
            AtomicInteger concurrency = new AtomicInteger(0);
            AtomicInteger maxConcurrency = new AtomicInteger(0);

            List<Integer> input = IntStream.range(0, TASK_COUNT).boxed().collect(Collectors.toList());

            // taskType(TaskType.CPU_BOUND) 标记 CPU 密集任务
            MultiTaskOptions options = MultiTaskOptions.of("cpu-compute")
                    .taskType(TaskType.CPU_BOUND)
                    .parallelism(parallelism)
                    .timeout(java.time.Duration.ofMillis(30000))
                    .build();

            TaskBatchResult<Long> result = par.map(
                    input,
                    i -> {
                        int cur = concurrency.incrementAndGet();
                        // 记录峰值并发度
                        maxConcurrency.updateAndGet(prev -> Math.max(prev, cur));
                        try {
                            return cpuIntensiveWork(i);
                        } finally {
                            concurrency.decrementAndGet();
                        }
                    },
                    options);

            // 等待所有任务完成
            for (int i = 0; i < result.results().size(); i++) {
                result.results().get(i).get(30, TimeUnit.SECONDS);
            }

            // 滑动窗口确保并发度受控
            assertThat(maxConcurrency.get())
                    .as("CPU 任务并发度受控: parallelism=%d, 最大并发=%d", parallelism, maxConcurrency.get())
                    .isLessThanOrEqualTo(parallelism + 1);

            // 验证任务类型为 CPU_BOUND
            assertThat(options.taskType()).as("taskType(TaskType.CPU_BOUND) 标记任务为 CPU_BOUND").isEqualTo(TaskType.CPU_BOUND);
        } finally {
            pool.shutdownNow();
        }
    }
}
