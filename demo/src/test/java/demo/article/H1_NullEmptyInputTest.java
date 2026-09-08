package demo.article;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * H1: null 和空列表——防御性编程
 *
 * <p>演示标准 ExecutorService 对 null/空列表输入的脆弱性， 以及 Par.map() 内置防御处理的安全性。
 */
public class H1_NullEmptyInputTest {

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

    /** 问题演示：标准 ExecutorService 面对 null 列表时，开发者必须手动防御。 如果忘记 null 检查，for-each 循环直接 NPE。 */
    @Test
    void vanillaExecutor_nullList_throwsNPE() {
        List<String> nullList = null;

        // 不加防御的写法：for-each 直接 NPE
        assertThatThrownBy(() -> {
                    List<Future<?>> futures = new ArrayList<>();
                    for (String item : nullList) { // NPE here
                        futures.add(pool.submit(() -> item.toUpperCase()));
                    }
                })
                .isInstanceOf(NullPointerException.class);
    }

    /** 问题演示：空列表不会 NPE，但提交了一轮无意义的循环和列表创建开销。 在高 QPS 场景下，这种空轮询是不必要的资源浪费。 */
    @Test
    void vanillaExecutor_emptyList_submitsNothing() {
        List<String> emptyList = Collections.emptyList();
        List<Future<?>> futures = new ArrayList<>();
        for (String item : emptyList) {
            futures.add(pool.submit(() -> item.toUpperCase()));
        }
        // 不会 NPE，但 futures 为空——上面的循环和列表创建全是浪费
        assertThat(futures).isEmpty();
    }

    /** 解决方案：Par.map() 对 null 列表做了防御处理，直接返回空的 TaskBatchResult。 调用方无需任何 null 检查，不会 NPE，不会提交任何任务。 */
    @Test
    void parMap_nullList_returnsEmptyResult() {
        MultiTaskOptions opts = MultiTaskOptions.of("null-demo")
                .timeout(java.time.Duration.ofMillis(5000))
                .build();

        List<String> nullList = null;
        TaskBatchResult<String> result = par.map(nullList, item -> item.toUpperCase(), opts);

        // 安全返回，结果列表为空
        assertThat(result.results()).isEmpty();
        // report() 也不会抛异常，状态统计为空
        assertThat(result.report().stateCounts()).isEmpty();
    }

    /** 解决方案：Par.map() 对空列表同样安全返回，零开销。 */
    @Test
    void parMap_emptyList_returnsEmptyResult() {
        MultiTaskOptions opts = MultiTaskOptions.of("empty-demo")
                .timeout(java.time.Duration.ofMillis(5000))
                .build();

        List<String> emptyList = Collections.emptyList();
        TaskBatchResult<String> result = par.map(emptyList, item -> item.toUpperCase(), opts);

        assertThat(result.results()).isEmpty();
        assertThat(result.report().stateCounts()).isEmpty();
    }

    /** 正常输入不受影响：Par.map() 对非空列表正常并行执行。 */
    @Test
    void parMap_normalList_worksAsExpected() {
        MultiTaskOptions opts = MultiTaskOptions.of("normal-demo")
                .timeout(java.time.Duration.ofMillis(5000))
                .build();

        List<String> input = Arrays.asList("hello", "world", "java");
        TaskBatchResult<String> result = par.map(input, item -> item.toUpperCase(), opts);

        // 3 个任务全部提交
        assertThat(result.results()).hasSize(3);
        // 验证结果值
        List<String> values = new ArrayList<>();
        for (com.google.common.util.concurrent.ListenableFuture<String> future : result.results()) {
            try {
                values.add(future.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        assertThat(values).containsExactlyInAnyOrder("HELLO", "WORLD", "JAVA");
    }
}
