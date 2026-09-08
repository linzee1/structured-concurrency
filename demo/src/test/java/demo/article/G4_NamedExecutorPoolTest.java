package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * G4. 多个线程池怎么管理——命名注册
 *
 * <p>问题：多个 ExecutorService 实例类型相同，传错池编译器不报错，运行时才发现。
 *
 * <p>解决：GlobalPar.builder().register("name", pool) 命名注册，par.map(...) 按名引用。
 */
class G4_NamedExecutorPoolTest {

    /**
     * 问题复现：多个 ExecutorService 引用类型相同，传错不报错。
     *
     * <p>两个方法各需要一个 ExecutorService，但参数类型完全一样。 如果调用方不小心传错了池，编译器不会有任何提示，任务静默地跑在错误的池上。 这里模拟"IO 任务误用 CPU
     * 池"的场景——验证错误池被使用时，代码仍能正常运行， 正因为"不报错"才危险。
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void problem_wrongPoolPassedSilently_noCompileError() throws Exception {
        ExecutorService ioPool = Executors.newFixedThreadPool(16, new java.util.concurrent.ThreadFactory() {
            private int count = 0;

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "io-pool-thread-" + count++);
            }
        });
        ExecutorService cpuPool = Executors.newFixedThreadPool(4, new java.util.concurrent.ThreadFactory() {
            private int count = 0;

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "cpu-pool-thread-" + count++);
            }
        });

        try {
            List<String> items = Arrays.asList("a", "b", "c", "d", "e");

            // 应该用 ioPool，但不小心传了 cpuPool —— 编译器不报错！
            List<java.util.concurrent.Future<String>> futures = new java.util.ArrayList<>();
            for (String item : items) {
                futures.add(cpuPool.submit(() -> {
                    // 通过线程名可以发现跑错了池，但代码层面完全没有提示
                    return Thread.currentThread().getName() + ":" + item;
                }));
            }

            // 验证：所有任务都跑在了 cpuPool 上（本应跑在 ioPool）
            for (java.util.concurrent.Future<String> f : futures) {
                String result = f.get(5, TimeUnit.SECONDS);
                assertThat(result).as("IO 任务误用了 cpuPool，线程名应包含 'cpu-pool'").contains("cpu-pool");
            }

            // 危险信号：传错了池，编译通过、运行正常、结果看起来也对——直到线上出问题
        } finally {
            ioPool.shutdownNow();
            cpuPool.shutdownNow();
        }
    }

    /**
     * 解决方案：GlobalPar 命名注册，按名引用，不可能传错。
     *
     * <p>IO 池和 CPU 池分别注册为 "io-pool" 和 "cpu-pool"，通过不同的 Par 实例调用 map。名字不匹配会直接抛出 IllegalArgumentException，而不是静默地跑在错误的池上。
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void solution_namedExecutors_cleanSeparation() throws Exception {
        ExecutorService ioPool = Executors.newFixedThreadPool(8, new java.util.concurrent.ThreadFactory() {
            private int count = 0;

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "io-pool-thread-" + count++);
            }
        });
        ExecutorService cpuPool = Executors.newFixedThreadPool(4, new java.util.concurrent.ThreadFactory() {
            private int count = 0;

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "cpu-pool-thread-" + count++);
            }
        });

        try {
            // 命名注册：一个 GlobalPar 统一管理多个线程池
            GlobalPar config = GlobalPar.builder()
                    .register("io-pool", ioPool)
                    .register("cpu-pool", cpuPool)
                    .defaultPar("io-pool")
                    .build();
            Par par = config.par("io-pool");
            Par cpuPar = config.par("cpu-pool");

            List<String> items = Arrays.asList("a", "b", "c", "d", "e");

            // IO 任务：按名引用 io-pool
            MultiTaskOptions ioOpts = MultiTaskOptions.of("fetch-data")
                    .taskType(TaskType.IO_BOUND)
                    .parallelism(4)
                    .timeout(java.time.Duration.ofMillis(5000))
                    .build();
            TaskBatchResult<String> ioResult = par.map(
                    items,
                    item -> {
                        return "io:" + Thread.currentThread().getName() + ":" + item;
                    },
                    ioOpts);

            // CPU 任务：按名引用 cpu-pool
            MultiTaskOptions cpuOpts = MultiTaskOptions.of("compute")
                    .taskType(TaskType.CPU_BOUND)
                    .parallelism(4)
                    .timeout(java.time.Duration.ofMillis(5000))
                    .build();
            TaskBatchResult<String> cpuResult = cpuPar.map(
                    items,
                    item -> {
                        return "cpu:" + Thread.currentThread().getName() + ":" + item;
                    },
                    cpuOpts);

            // 验证：IO 任务确实跑在 io-pool 上
            for (int i = 0; i < ioResult.results().size(); i++) {
                String result = ioResult.results().get(i).get(5, TimeUnit.SECONDS);
                assertThat(result).as("IO 任务应跑在 io-pool 上").contains("io-pool");
            }

            // 验证：CPU 任务确实跑在 cpu-pool 上
            for (int i = 0; i < cpuResult.results().size(); i++) {
                String result = cpuResult.results().get(i).get(5, TimeUnit.SECONDS);
                assertThat(result).as("CPU 任务应跑在 cpu-pool 上").contains("cpu-pool");
            }

            // 验证 report
            assertThat(ioResult.reportString()).contains("SUCCESS:5");
            assertThat(cpuResult.reportString()).contains("SUCCESS:5");

            // 验证名字不匹配时直接报错，而不是静默地跑在错误的池上
            try {
                config.par("nonexistent-pool").map(items, item -> item, ioOpts);
                // 应该抛异常，不会走到这里
                assertThat(false).as("应抛出 IllegalArgumentException").isTrue();
            } catch (IllegalArgumentException e) {
                assertThat(e.getMessage()).contains("nonexistent-pool");
            }

        } finally {
            ioPool.shutdownNow();
            cpuPool.shutdownNow();
        }
    }
}
