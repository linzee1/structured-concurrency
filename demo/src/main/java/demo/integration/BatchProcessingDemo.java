package demo.integration;

import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 集成示例：演示批量数据处理
 *
 * <p>这个示例展示了如何使用 parallel-in-scope 进行批量数据处理：
 *
 * <ul>
 *   <li>大数据集分批处理
 *   <li>并行度控制
 *   <li>错误处理
 *   <li>性能监控
 * </ul>
 */
public class BatchProcessingDemo {

    public static void main(String[] args) {
        System.out.println("=== BatchProcessingDemo ===");
        System.out.println("演示批量数据处理\n");

        // 1. 创建大量数据
        List<Integer> largeDataset = generateDataset(100);
        System.out.println("数据集大小: " + largeDataset.size() + " 个元素");

        ExecutorService pool = Executors.newFixedThreadPool(4);
        GlobalPar global = GlobalPar.builder()
                .register("batch-demo", pool)
                .defaultPar("batch-demo")
                .build();
        Par par = global.par("batch-demo");

        try {
            // 2. 配置批处理参数
            MultiTaskOptions options = MultiTaskOptions.of("batch-demo")
                    .parallelism(4)
                    .timeout(java.time.Duration.ofMillis(30000))
                    .build();

            System.out.println("并行度: " + options.parallelism());
            System.out.println("超时: 30秒\n");

            // 3. 执行批量处理
            System.out.println("开始批量处理...");
            long startTime = System.currentTimeMillis();

            TaskBatchResult<Integer> result = par.map(
                    largeDataset,
                    n -> {
                        // 模拟复杂计算
                        return expensiveComputation(n);
                    },
                    options);

            long endTime = System.currentTimeMillis();

            // 4. 统计结果
            System.out.println("\n处理完成!");
            System.out.println("耗时: " + (endTime - startTime) + " 毫秒");
            System.out.println(
                    "平均每个任务: " + String.format("%.2f", (double) (endTime - startTime) / largeDataset.size()) + " 毫秒");

            // 5. 查看结果
            System.out.println("执行报告: " + result.reportString());

            // 6. 性能分析（使用实际计算结果，而非输入数据）
            List<Integer> computedResults = new ArrayList<>();
            for (java.util.concurrent.Future<Integer> future : result.results()) {
                try {
                    computedResults.add(future.get());
                } catch (Exception e) {
                    computedResults.add(-1);
                }
            }
            analyzePerformance(computedResults);

        } finally {
            global.close();
            pool.shutdownNow();
        }
    }

    /** 生成测试数据集 */
    private static List<Integer> generateDataset(int size) {
        List<Integer> dataset = new ArrayList<>();
        for (int i = 1; i <= size; i++) {
            dataset.add(i);
        }
        return dataset;
    }

    /** 模拟复杂计算 */
    private static Integer expensiveComputation(int n) {
        // 模拟计算密集型任务
        try {
            TimeUnit.MILLISECONDS.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return -1;
        }
        return n * n + n;
    }

    /** 分析性能 */
    private static void analyzePerformance(List<Integer> results) {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        long sum = 0;

        for (int value : results) {
            if (value < min) min = value;
            if (value > max) max = value;
            sum += value;
        }

        System.out.println("\n性能分析:");
        System.out.println("  最小值: " + min);
        System.out.println("  最大值: " + max);
        System.out.println("  平均值: " + (sum / results.size()));
    }
}
