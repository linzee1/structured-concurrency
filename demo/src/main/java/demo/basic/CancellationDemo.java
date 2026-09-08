package demo.basic;

import com.google.common.util.concurrent.Futures;
import io.github.huatalk.parallelinscope.cancel.Checkpoints;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 取消机制示例：演示协作式取消（cooperative cancellation）
 *
 * <p>这个示例展示了 parallel-in-scope 的核心取消机制：
 *
 * <ul>
 *   <li>使用 {@link Checkpoints#sleep(long)} 替代 Thread.sleep — 可被取消中断
 *   <li>配置 {@link MultiTaskOptions} 超时时间触发取消
 *   <li>被取消的任务抛出 {@code LeanCancellationException}
 *   <li>通过 {@code reportString()} 查看最终状态
 * </ul>
 *
 * <p>关键区别：普通 Thread.sleep() 不响应取消信号，而 Checkpoints.sleep() 会检查 CancellationToken 状态并抛出异常，实现协作式取消。
 */
public class CancellationDemo {

    public static void main(String[] args) throws Exception {
        System.out.println("=== CancellationDemo ===");
        System.out.println("演示任务超时取消机制\n");

        ExecutorService pool = Executors.newFixedThreadPool(4);
        GlobalPar global = GlobalPar.builder()
                .register("cancel-demo", pool)
                .defaultPar("cancel-demo")
                .build();
        Par par = global.par("cancel-demo");

        try {
            List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

            // 1. 配置并行选项（设置超时）
            MultiTaskOptions options = MultiTaskOptions.of("cancel-demo")
                    .parallelism(3)
                    .timeout(java.time.Duration.ofMillis(2000))
                    .build();

            System.out.println("开始并行处理（2秒超时）...");
            System.out.println("并行度: " + options.parallelism());

            // 2. 执行并行处理
            long startTime = System.currentTimeMillis();
            TaskBatchResult<Integer> result = par.map(
                    numbers,
                    n -> {
                        String threadName = Thread.currentThread().getName();

                        System.out.println("  [" + threadName + "] 处理: " + n);

                        // 使用 Checkpoints.sleep() — 当超时触发取消时，这里会抛出 LeanCancellationException
                        Checkpoints.sleep(1500);
                        return n * n;
                    },
                    options);

            // Wait until every task has succeeded, failed, or been cancelled. Unlike allAsList,
            // successfulAsList itself completes normally when individual tasks are cancelled.
            Futures.successfulAsList(result.results()).get();
            long endTime = System.currentTimeMillis();

            // 3. 查看结果
            System.out.println("\n处理完成!");
            System.out.println("耗时: " + (endTime - startTime) + " 毫秒");
            System.out.println("执行报告: " + result.reportString());
            System.out.println("（超时触发后，Checkpoints.sleep() 抛出 LeanCancellationException 实现协作式取消）");

        } finally {
            global.close();
            pool.shutdownNow();
        }
    }
}
