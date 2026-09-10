package verification;

import io.github.monadrome.parallelinscope.BatchOptions;
import io.github.monadrome.parallelinscope.GlobalPar;
import io.github.monadrome.parallelinscope.Par;
import io.github.monadrome.parallelinscope.ParName;
import io.github.monadrome.parallelinscope.TaskBatchResult;
import io.github.monadrome.parallelinscope.TaskFuture;
import io.github.monadrome.parallelinscope.TaskOutcome;
import io.github.monadrome.parallelinscope.TaskType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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

    private static final ParName CONSUMER = ParName.of("consumer-pool");

    @Test
    void publishedArtifactCanBeResolvedAndUsed() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global = GlobalPar.builder().register(CONSUMER, executor).build();
        try {
            BatchOptions options = BatchOptions.timeout("consumer-smoke", Duration.ofSeconds(5))
                    .parallelism(2)
                    .taskType(TaskType.IO_BOUND);

            Par par = global.par(CONSUMER);
            TaskBatchResult<Integer> result = par.map(Arrays.asList(1, 2), value -> value * 2, options);

            List<TaskFuture<Integer>> results = result.results();
            assertEquals(2, results.size());
            assertEquals(2, results.get(0).get());
            assertEquals(4, results.get(1).get());
            assertEquals("consumer-smoke", results.get(0).taskName());
            assertEquals(TaskOutcome.SUCCESS, results.get(0).outcome());

            assertEquals("consumer-smoke", options.name());
            assertEquals(2, options.parallelism());
            assertEquals(Optional.of(Duration.ofSeconds(5)), options.timeout());
            assertEquals(TaskType.IO_BOUND, options.taskType());
            assertEquals(CONSUMER, par.name());
            assertEquals(global, par.globalPar());
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void timeoutCancelsRunningTasks() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch interrupted = new CountDownLatch(2);
        GlobalPar global = GlobalPar.builder().register(CONSUMER, executor).build();
        try {
            Par par = global.par(CONSUMER);
            TaskBatchResult<Integer> result = par.map(
                    Arrays.asList(1, 2),
                    value -> awaitInterruption(started, interrupted, value),
                    BatchOptions.timeout("timeout-contract", Duration.ofMillis(200))
                            .parallelism(2)
                            .taskType(TaskType.IO_BOUND));

            assertTrue(started.await(5, TimeUnit.SECONDS), "tasks did not start");
            assertThrows(
                    CancellationException.class, () -> result.results().get(0).get(5, TimeUnit.SECONDS));
            assertTrue(interrupted.await(5, TimeUnit.SECONDS), "timeout did not interrupt running tasks");
            assertTrue(result.results().stream().allMatch(future -> future.isCancelled()));
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void failureCancelsSiblingTasks() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch siblingsStarted = new CountDownLatch(2);
        CountDownLatch siblingsInterrupted = new CountDownLatch(2);
        GlobalPar global = GlobalPar.builder().register(CONSUMER, executor).build();
        try {
            Par par = global.par(CONSUMER);
            TaskBatchResult<Integer> result = par.map(
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
                    BatchOptions.timeout("fail-fast-contract", Duration.ofSeconds(10))
                            .parallelism(3)
                            .taskType(TaskType.IO_BOUND));

            assertThrows(
                    ExecutionException.class, () -> result.results().get(0).get(5, TimeUnit.SECONDS));
            assertTrue(siblingsInterrupted.await(5, TimeUnit.SECONDS), "failure did not interrupt sibling tasks");
            assertTrue(result.results().get(1).isCancelled());
            assertTrue(result.results().get(2).isCancelled());
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void outerTimeoutPropagatesToNestedTasks() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch innerStarted = new CountDownLatch(2);
        CountDownLatch innerInterrupted = new CountDownLatch(2);
        AtomicReference<TaskBatchResult<Integer>> innerResult = new AtomicReference<>();
        GlobalPar global = GlobalPar.builder().register(CONSUMER, executor).build();
        try {
            Par par = global.par(CONSUMER);
            TaskBatchResult<Integer> outerResult = par.map(
                    Arrays.asList(1),
                    outerValue -> {
                        TaskBatchResult<Integer> nested = par.map(
                                Arrays.asList(10, 20),
                                value -> awaitInterruption(innerStarted, innerInterrupted, value),
                                BatchOptions.timeout("inner-contract", Duration.ofSeconds(10))
                                        .parallelism(2)
                                        .taskType(TaskType.IO_BOUND));
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
                    BatchOptions.timeout("outer-contract", Duration.ofMillis(300))
                            .parallelism(1)
                            .taskType(TaskType.IO_BOUND));

            assertTrue(innerStarted.await(5, TimeUnit.SECONDS), "nested tasks did not start");
            assertThrows(
                    CancellationException.class, () -> outerResult.results().get(0).get(5, TimeUnit.SECONDS));
            assertTrue(innerInterrupted.await(5, TimeUnit.SECONDS), "outer timeout did not interrupt nested tasks");
            assertTrue(innerResult.get().results().stream().allMatch(future -> future.isCancelled()));
        } finally {
            global.close();
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
