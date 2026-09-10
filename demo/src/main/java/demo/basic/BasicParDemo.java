package demo.basic;

import io.github.monadrome.parallelinscope.GlobalPar;
import io.github.monadrome.parallelinscope.BatchOptions;
import io.github.monadrome.parallelinscope.Par;
import io.github.monadrome.parallelinscope.ParName;
import io.github.monadrome.parallelinscope.TaskBatchResult;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 基础示例：演示 Par.map() 的基本用法
 *
 * <p>这个示例展示了如何使用 parallel-in-scope 进行并行数据处理：
 *
 * <ul>
 *   <li>创建并配置 Par 实例
 *   <li>使用 BatchOptions 设置并行度
 *   <li>使用 Par.map() 并行处理集合
 *   <li>获取和处理结果
 * </ul>
 */
public class BasicParDemo {

    public static void main(String[] args) {
        System.out.println("=== BasicParDemo ===");
        System.out.println("演示 Par.map() 基本用法\n");

        // 1. 创建线程池
        ExecutorService pool = Executors.newFixedThreadPool(4);

        // 2. 创建 GlobalPar 并注册执行器
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("demo-pool"), pool)
                .defaultPar(ParName.of("demo-pool"))
                .build();

        // 3. 创建 Par 实例
        Par par = global.par(ParName.of("demo-pool"));

        try {
            // 4. 准备数据
            List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            System.out.println("输入数据: " + numbers);

            // 5. 配置并行选项
            BatchOptions options = BatchOptions.timeout("basic-demo", java.time.Duration.ofSeconds(30)).parallelism(3);

            System.out.println("并行度: " + options.parallelism());

            // 6. 执行并行处理
            System.out.println("\n开始并行处理...");
            TaskBatchResult<Integer> result = par.map(
                    numbers,
                    n -> {
                        String threadName = Thread.currentThread().getName();
                        System.out.println("  [" + threadName + "] 处理: " + n);
                        return n * n;
                    },
                    options);

            // 7. 获取结果
            System.out.println("\n处理完成!");
            System.out.println("结果: " + result.reportString());

        } finally {
            // 8. 清理资源
            global.close();
            pool.shutdownNow();
        }
    }
}
