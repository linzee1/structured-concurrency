package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.scope.AsyncBatchResult;
import io.github.huatalk.parallelinscope.scope.BatchExecutionOptions;
import io.github.huatalk.parallelinscope.scope.GlobalExecutionPolicy;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskType;
import io.github.huatalk.parallelinscope.spi.TaskListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * G1: 任务执行看不见——接入监控
 *
 * <p>演示问题：标准 ExecutorService 没有任务监控钩子，只能在 lambda 里手动埋点。
 *
 * <p>演示解决：Par.map() 配合 TaskListener SPI，零侵入地捕获每个任务的执行事件。
 */
public class G1_TaskListenerMonitoringTest {

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
     * 问题复现：标准 ExecutorService 没有监控钩子。
     *
     * <p>只能在 lambda 内部手动埋点，且拿不到等待时间（提交到开始执行的间隔）。 监控逻辑和业务逻辑耦合，容易遗漏。
     */
    @Test
    void vanillaExecutorService_hasNoMonitoringHook() throws Exception {
        // 模拟手动埋点：在 lambda 里记录开始/结束时间
        List<Long> manualTimings = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger failureCount = new AtomicInteger(0);

        List<String> inputs = Arrays.asList("a", "b", "c", "d", "e");
        List<java.util.concurrent.Future<String>> futures = new ArrayList<>();

        for (String input : inputs) {
            futures.add(pool.submit(() -> {
                long start = System.nanoTime(); // 手动记录开始时间
                try {
                    Thread.sleep(50);
                    return input.toUpperCase();
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                    throw e;
                } finally {
                    // 手动记录执行耗时——但拿不到等待时间！
                    manualTimings.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                }
            }));
        }

        // 等待所有任务完成
        for (java.util.concurrent.Future<String> f : futures) {
            f.get(5, TimeUnit.SECONDS);
        }

        // 验证：手动埋点能记录执行耗时
        assertThat(manualTimings).hasSize(5);
        for (Long timing : manualTimings) {
            assertThat(timing).isGreaterThanOrEqualTo(40); // ~50ms sleep
        }
        // 但等待时间（queue wait）完全拿不到，这是标准 API 的盲区
        assertThat(failureCount.get()).isEqualTo(0);
    }

    /**
     * 解决方法：Par.map() 配合 TaskListener，零侵入监控。
     *
     * <p>注册 TaskListener 后，每个任务完成时自动回调 onTaskComplete(TaskEvent)， 包含
     * taskName、executionTime()、totalTime()、exception 等完整信息。 业务 lambda 无需任何监控代码。
     */
    @Test
    void parMap_withTaskListener_capturesSuccessfulTaskEvents() throws Exception {
        // 注册 TaskListener，收集所有事件
        CopyOnWriteArrayList<TaskListener.TaskEvent> events = new CopyOnWriteArrayList<>();

        GlobalPar config = GlobalPar.builder()
                .register("test-pool", pool)
                .executionPolicy(GlobalExecutionPolicy.builder()
                        .taskListener(events::add)
                        .build())
                .defaultPar("test-pool")
                .build();
        Par par = config.defaultPar();

        List<Integer> input = Arrays.asList(1, 2, 3, 4, 5);
        // parallelism=5 确保所有任务同时启动，避免被取消
        BatchExecutionOptions opts = BatchExecutionOptions.of("monitor-demo")
                .parallelism(5)
                .timeout(java.time.Duration.ofMillis(5000))
                .taskType(TaskType.IO_BOUND)
                .build();

        // 业务代码：纯逻辑，不碰监控
        AsyncBatchResult<String> result = par.map(
                input,
                x -> {
                    // 模拟 50ms 业务计算（忙等待，不响应中断）
                    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50);
                    while (System.nanoTime() < deadline) {
                        /* busy wait */
                    }
                    return "result-" + x;
                },
                opts);

        // 等待所有任务完成
        Thread.sleep(2000);

        // 验证：TaskListener 捕获了所有 5 个任务的事件
        assertThat(events).hasSize(5);

        // 验证：每个事件都有正确的 taskName
        for (TaskListener.TaskEvent event : events) {
            assertThat(event.taskName()).isEqualTo("monitor-demo");
        }

        // 验证：执行耗时 >= 40ms（因为我们忙等了 50ms）
        for (TaskListener.TaskEvent event : events) {
            assertThat(event.executionTime().toMillis())
                    .as("Task %s execution time", event.taskName())
                    .isGreaterThanOrEqualTo(40);
        }

        // 验证：总耗时 >= 执行耗时（total = wait + execution）
        for (TaskListener.TaskEvent event : events) {
            assertThat(event.totalTime().toNanos())
                    .as("Task %s total time >= execution time", event.taskName())
                    .isGreaterThanOrEqualTo(event.executionTime().toNanos());
        }

        // 验证：所有任务成功，没有异常
        for (TaskListener.TaskEvent event : events) {
            assertThat(event.exception())
                    .as("Task %s should not have exception", event.taskName())
                    .isNull();
        }

        // 验证：report 确认全部成功
        String report = result.reportString();
        assertThat(report).contains("SUCCESS:5");
    }

    /**
     * TaskListener 同样能捕获失败任务的异常信息。
     *
     * <p>当任务抛出异常时，TaskEvent.exception() 返回对应的 Throwable， 无需在业务代码中手动 try-catch。
     */
    @Test
    void parMap_withTaskListener_capturesFailedTaskException() throws Exception {
        CopyOnWriteArrayList<TaskListener.TaskEvent> events = new CopyOnWriteArrayList<>();

        GlobalPar config = GlobalPar.builder()
                .register("test-pool", pool)
                .executionPolicy(GlobalExecutionPolicy.builder()
                        .taskListener(events::add)
                        .build())
                .defaultPar("test-pool")
                .build();
        Par par = config.defaultPar();

        // 只有 2 个任务，parallelism=2 确保同时启动
        List<Integer> input = Arrays.asList(1, 2);
        BatchExecutionOptions opts = BatchExecutionOptions.of("fail-demo")
                .parallelism(2)
                .timeout(java.time.Duration.ofMillis(5000))
                .taskType(TaskType.IO_BOUND)
                .build();

        AsyncBatchResult<String> result = par.map(
                input,
                x -> {
                    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50);
                    while (System.nanoTime() < deadline) {
                        /* busy wait */
                    }
                    if (x == 2) {
                        throw new RuntimeException("item " + x + " failed");
                    }
                    return "result-" + x;
                },
                opts);

        Thread.sleep(2000);

        // 验证：TaskListener 捕获了 2 个事件
        assertThat(events).hasSize(2);

        // 验证：捕获到了失败任务的异常
        TaskListener.TaskEvent failedEvent = events.stream()
                .filter(e -> e.exception() != null)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No failed event found"));
        assertThat(failedEvent.exception().getMessage()).contains("item 2 failed");

        // 验证：失败事件也有 taskName
        assertThat(failedEvent.taskName()).isEqualTo("fail-demo");

        // 验证：成功任务没有异常
        long successCount =
                events.stream().filter(e -> e.exception() == null).count();
        assertThat(successCount).isEqualTo(1);

        // 验证：report 确认结果
        String report = result.reportString();
        assertThat(report).contains("SUCCESS:1");
        assertThat(report).contains("FAILED:1");
    }
}
