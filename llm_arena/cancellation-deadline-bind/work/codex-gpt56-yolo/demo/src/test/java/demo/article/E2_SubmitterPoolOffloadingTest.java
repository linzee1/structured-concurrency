package demo.article;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huatalk.parallelinscope.scope.AsyncBatchResult;
import io.github.huatalk.parallelinscope.scope.BatchExecutionOptions;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.Par;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * E2: 提交循环占用业务线程 — SubmitterPool 线程分离
 *
 * <p>演示朴素滑动窗口实现中，提交循环占用业务线程池的问题， 以及 parallel-in-scope 通过专用 SubmitterPool 解决此问题的方案。
 */
public class E2_SubmitterPoolOffloadingTest {

    /** 问题演示：朴素滑动窗口的提交循环运行在业务线程池上， 占用一个业务线程，导致线程池为 1 时死锁。 */
    @Test
    void naiveSlidingWindow_deadlocksWhenPoolSizeEqualsParallelism() throws Exception {
        // 使用 1 个线程的业务线程池
        ExecutorService pool = Executors.newFixedThreadPool(1);
        try {
            ExecutorCompletionService<Integer> ecs = new ExecutorCompletionService<>(pool);

            Callable<Integer> task = () -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return 1;
            };

            // 提交第一个任务到业务线程池
            ecs.submit(task);

            // 朴素实现：将提交循环也提交到同一个业务线程池
            Future<?> scheduler = pool.submit(() -> {
                try {
                    for (int i = 1; i < 3; i++) {
                        ecs.take(); // 阻塞等待任务完成
                        ecs.submit(task); // 提交下一个任务
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            // 问题验证：提交循环占用唯一线程，导致死锁
            // task0 完成后，提交循环在唯一线程上运行，
            // 它调用 take() 等待 task1 完成，但 task1 无法运行（线程被占用）
            assertThatThrownBy(() -> scheduler.get(2, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);

        } finally {
            pool.shutdownNow();
        }
    }

    /** 解决方案：Par.map() 将提交循环运行在专用的 Par-Submitter 守护线程上， 业务线程只执行实际任务，不会被调度逻辑占用。 */
    @Test
    void parMap_schedulingOffloadedToSubmitterPool() throws Exception {
        // 同样使用 1 个线程的业务线程池
        ExecutorService pool = Executors.newFixedThreadPool(1);
        try {
            GlobalPar config = GlobalPar.builder()
                    .register("test-pool", pool)
                    .defaultPar("test-pool")
                    .build();
            Par par = config.defaultPar();

            BatchExecutionOptions opts =
                    BatchExecutionOptions.of("offload-demo").parallelism(1).build();

            List<Integer> input = Arrays.asList(1, 2, 3);

            // Par.map() 不会死锁，因为提交循环运行在 Par-Submitter 线程上
            AsyncBatchResult<Integer> result = par.map(
                    input,
                    x -> {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return x * 2;
                    },
                    opts);

            // 收集结果
            List<Integer> results = new ArrayList<>();
            for (Future<Integer> f : result.results()) {
                results.add(f.get(5, TimeUnit.SECONDS));
            }

            // 验证：所有任务正常完成
            assertThat(results).containsExactlyInAnyOrder(2, 4, 6);

        } finally {
            pool.shutdownNow();
        }
    }
}
