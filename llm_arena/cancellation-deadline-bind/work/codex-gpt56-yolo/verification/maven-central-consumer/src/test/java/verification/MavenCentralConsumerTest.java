package verification;

import io.github.huatalk.parallelinscope.scope.AsyncBatchResult;
import io.github.huatalk.parallelinscope.scope.Par;
import io.github.huatalk.parallelinscope.scope.ParConfig;
import io.github.huatalk.parallelinscope.scope.ParOptions;
import io.github.huatalk.parallelinscope.scope.TaskType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 15, unit = TimeUnit.SECONDS)
class MavenCentralConsumerTest {

    @Test
    void publishedArtifactCanBeResolvedAndUsed() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            ParConfig config = ParConfig.builder()
                    .executor("consumer-pool", executor)
                    .build();
            ParOptions options = ParOptions.ioTask("consumer-smoke")
                    .parallelism(2)
                    .timeout(5)
                    .timeUnit(TimeUnit.SECONDS)
                    .build();

            AsyncBatchResult<Integer> result = new Par(config).map(
                    "consumer-pool",
                    Arrays.asList(1, 2),
                    value -> value * 2,
                    options);

            assertEquals(2, result.results().get(0).get());
            assertEquals(4, result.results().get(1).get());
            assertEquals("consumer-smoke", options.taskName());
            assertEquals(2, options.parallelism());
            assertEquals(5, options.getTimeout());
            assertEquals(TimeUnit.SECONDS, options.getTimeUnit());
            assertEquals(TaskType.IO_BOUND, options.taskType());
            assertEquals(config, new Par(config).getConfig());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void timeoutCancelsRunningTasks() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch interrupted = new CountDownLatch(2);
        try {
            Par par = new Par(ParConfig.builder().executor("timeout-pool", executor).build());
            AsyncBatchResult<Integer> result = par.map(
                    "timeout-pool",
                    Arrays.asList(1, 2),
                    value -> awaitInterruption(started, interrupted, value),
                    ParOptions.ioTask("timeout-contract")
                            .parallelism(2)
                            .timeout(200)
                            .build());

            assertTrue(started.await(5, TimeUnit.SECONDS), "tasks did not start");
            assertThrows(CancellationException.class,
                    () -> result.results().get(0).get(5, TimeUnit.SECONDS));
            assertTrue(interrupted.await(5, TimeUnit.SECONDS), "timeout did not interrupt running tasks");
            assertTrue(result.results().stream().allMatch(future -> future.isCancelled()));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void failureCancelsSiblingTasks() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch siblingsStarted = new CountDownLatch(2);
        CountDownLatch siblingsInterrupted = new CountDownLatch(2);
        try {
            Par par = new Par(ParConfig.builder().executor("fail-fast-pool", executor).build());
            AsyncBatchResult<Integer> result = par.map(
                    "fail-fast-pool",
                    Arrays.asList(1, 2, 3),
                    value -> {
                        if (value == 1) {
                            if (!await(siblingsStarted, 5, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("siblings did not start");
                            }
                            throw new IllegalArgumentException("expected failure");
                        }
                        return awaitInterruption(siblingsStarted, siblingsInterrupted, value);
                    },
                    ParOptions.ioTask("fail-fast-contract")
                            .parallelism(3)
                            .timeout(10)
                            .timeUnit(TimeUnit.SECONDS)
                            .build());

            assertThrows(ExecutionException.class,
                    () -> result.results().get(0).get(5, TimeUnit.SECONDS));
            assertTrue(siblingsInterrupted.await(5, TimeUnit.SECONDS), "failure did not interrupt sibling tasks");
            assertTrue(result.results().get(1).isCancelled());
            assertTrue(result.results().get(2).isCancelled());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void outerTimeoutPropagatesToNestedTasks() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch innerStarted = new CountDownLatch(2);
        CountDownLatch innerInterrupted = new CountDownLatch(2);
        AtomicReference<AsyncBatchResult<Integer>> innerResult = new AtomicReference<>();
        try {
            Par par = new Par(ParConfig.builder().executor("nested-pool", executor).build());
            AsyncBatchResult<Integer> outerResult = par.map(
                    "nested-pool",
                    Arrays.asList(1),
                    outerValue -> {
                        AsyncBatchResult<Integer> nested = par.map(
                                "nested-pool",
                                Arrays.asList(10, 20),
                                value -> awaitInterruption(innerStarted, innerInterrupted, value),
                                ParOptions.ioTask("inner-contract")
                                        .parallelism(2)
                                        .timeout(10)
                                        .timeUnit(TimeUnit.SECONDS)
                                        .build());
                        innerResult.set(nested);
                        try {
                            for (int i = 0; i < nested.results().size(); i++) {
                                nested.results().get(i).get();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("nested task failed", e);
                        }
                        return outerValue;
                    },
                    ParOptions.ioTask("outer-contract")
                            .parallelism(1)
                            .timeout(300)
                            .build());

            assertTrue(innerStarted.await(5, TimeUnit.SECONDS), "nested tasks did not start");
            assertThrows(CancellationException.class,
                    () -> outerResult.results().get(0).get(5, TimeUnit.SECONDS));
            assertTrue(innerInterrupted.await(5, TimeUnit.SECONDS), "outer timeout did not interrupt nested tasks");
            assertTrue(innerResult.get().results().stream().allMatch(future -> future.isCancelled()));
        } finally {
            executor.shutdownNow();
        }
    }

    private static int awaitInterruption(
            CountDownLatch started, CountDownLatch interrupted, int value) {
        started.countDown();
        try {
            new CountDownLatch(1).await(10, TimeUnit.SECONDS);
            return value;
        } catch (InterruptedException e) {
            interrupted.countDown();
            Thread.currentThread().interrupt();
            throw new RuntimeException("task interrupted", e);
        }
    }

    private static boolean await(CountDownLatch latch, long timeout, TimeUnit unit) {
        try {
            return latch.await(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("task interrupted while awaiting peers", e);
        }
    }
}
