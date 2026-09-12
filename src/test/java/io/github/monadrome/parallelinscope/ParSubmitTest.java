package io.github.monadrome.parallelinscope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Contract tests for {@link Par#submit(String, java.util.concurrent.Callable, TaskOptions)}. */
class ParSubmitTest {

    @Test
    void submitRunsOneScopedTaskWithNameAndDeadline() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<TaskCompletion<?>> listenerCompletion = new AtomicReference<>();
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("worker"), executor)
                .taskListener(listenerCompletion::set)
                .build();
        try {
            TaskFuture<String> task = global.par(ParName.of("worker"))
                    .submit(
                            "single",
                            () -> {
                                MultiTaskContext unit =
                                        TaskExecutionContext.current().multiTaskContext();
                                assertThat(unit.name()).isEqualTo("single");
                                return "done";
                            },
                            TaskOptions.timeout(Duration.ofSeconds(30)));

            assertThat(task.get(2, TimeUnit.SECONDS)).isEqualTo("done");
            assertThat(task.taskName()).isEqualTo("single");
            assertThat(task.outcome()).isEqualTo(TaskOutcome.SUCCESS);
            assertThat(listenerCompletion.get().taskName()).isEqualTo("single");
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void submitRequiresAnExplicitTimeoutWithoutAnEnclosingScope() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        try {
            Par par = global.par(ParName.of("worker"));
            assertThatThrownBy(() -> par.submit("single", () -> "x", TaskOptions.inheritTimeout()))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void submitDeadlineExpiresAndCancelsTheTask() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        try {
            CountDownLatch started = new CountDownLatch(1);
            TaskFuture<String> task = global.par(ParName.of("worker"))
                    .submit(
                            "slow",
                            () -> {
                                started.countDown();
                                while (true) {
                                    Checkpoints.checkpoint();
                                }
                            },
                            TaskOptions.timeout(Duration.ofMillis(50)).taskType(TaskType.IO_BOUND));

            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> task.get(2, TimeUnit.SECONDS)).isInstanceOf(CancellationException.class);
            assertThat(task.outcome()).isEqualTo(TaskOutcome.TIMEOUT);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void submitInsideABatchInheritsCancellation() throws Exception {
        ExecutorService outer = Executors.newSingleThreadExecutor();
        ExecutorService inner = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("outer"), outer)
                .register(ParName.of("inner"), inner)
                .build();
        try {
            TaskBatchResult<String> batch = global.par(ParName.of("outer"))
                    .map(
                            Arrays.asList("a"),
                            ignored -> com.google.common.util.concurrent.Futures.getUnchecked(
                                    global.par(ParName.of("inner"))
                                            .submit("nested", () -> "nested-value", TaskOptions.inheritTimeout())),
                            BatchOptions.timeout("outer-batch", Duration.ofSeconds(30)));

            assertThat(batch.valuesOrThrow()).containsExactly("nested-value");
        } finally {
            global.close();
            outer.shutdownNow();
            inner.shutdownNow();
        }
    }

    @Test
    void submitRejectsAfterClose() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        global.close();
        try {
            Par par = global.par(ParName.of("worker"));
            assertThatThrownBy(() -> par.submit("single", () -> "x", TaskOptions.timeout(Duration.ofSeconds(1))))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            executor.shutdownNow();
        }
    }
}
