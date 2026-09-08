package demo.article;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.TaskBatchResult;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Demonstrates truthful batch reporting for success and fail-fast outcomes. */
class G2_BatchResultReportTest {

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

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void report_allSuccessfulTasksHasExactCountAndNoException() throws Exception {
        List<Integer> items = Arrays.asList(1, 2, 3, 4, 5, 6);
        TaskBatchResult<Integer> result = par.map(items, value -> value * 2, options("all-success", 3));

        awaitTerminalStates(result);
        TaskBatchResult.BatchReport report = result.report();

        assertThat(totalStateCount(report)).isEqualTo(items.size());
        assertThat(report.firstException()).isNull();
        assertThat(result.reportString()).isEqualTo("SUCCESS:" + items.size());
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void report_failFastAccountsForEveryTaskAndPreservesRootFailure() throws Exception {
        int taskCount = 6;
        CountDownLatch siblingStarted = new CountDownLatch(1);
        CountDownLatch blockSibling = new CountDownLatch(1);
        RuntimeException rootFailure = new RuntimeException("root failure");

        TaskBatchResult<Integer> result = par.map(
                Arrays.asList(0, 1, 2, 3, 4, 5),
                value -> {
                    if (value == 0) {
                        awaitStartedSibling(siblingStarted);
                        throw rootFailure;
                    }
                    if (value == 1) {
                        siblingStarted.countDown();
                        try {
                            blockSibling.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return value * 2;
                },
                options("fail-fast-report", 2));

        awaitTerminalStates(result);
        TaskBatchResult.BatchReport report = result.report();
        String reportString = result.reportString();

        assertThat(totalStateCount(report)).isEqualTo(taskCount);
        assertThat(report.firstException()).isSameAs(rootFailure);
        assertThat(reportString)
                .contains("USER_FAILURE:")
                .contains("MEMBER_CANCELED:")
                .contains("firstException=root failure");
    }

    private static MultiTaskOptions options(String taskName, int parallelism) {
        return MultiTaskOptions.of(taskName)
                .parallelism(parallelism)
                .timeout(java.time.Duration.ofMillis(5000))
                .build();
    }

    private static void awaitStartedSibling(CountDownLatch siblingStarted) {
        try {
            if (!siblingStarted.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Sibling task did not start");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while arranging fail-fast scenario", e);
        }
    }

    private static void awaitTerminalStates(TaskBatchResult<?> result) throws Exception {
        for (com.google.common.util.concurrent.ListenableFuture<?> future : result.results()) {
            try {
                future.get(5, TimeUnit.SECONDS);
            } catch (ExecutionException | CancellationException ignored) {
                // Both are terminal states represented by BatchReport.
            }
        }
    }

    private static int totalStateCount(TaskBatchResult.BatchReport report) {
        return report.stateCounts().values().stream()
                .mapToInt(Integer::intValue)
                .sum();
    }
}
