package io.github.monadrome.parallelinscope.scope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.monadrome.parallelinscope.internal.TaskExecutionContext;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class TaskGroupCombineTest {

    private static MultiTaskOptions options(String name) {
        return MultiTaskOptions.of(name).timeout(Duration.ofSeconds(30)).build();
    }

    @Test
    void combineRunsOnceOnItsOwnExecutorAfterAllMembersSucceed() throws Exception {
        ExecutorService io = Executors.newFixedThreadPool(2);
        ExecutorService cpu = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("io"), io)
                .register(ParName.of("cpu"), cpu)
                .build();
        try {
            AtomicInteger combineRuns = new AtomicInteger();
            AtomicReference<String> combineThread = new AtomicReference<>();
            AtomicReference<String> combineTask = new AtomicReference<>();
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("page"));
            TaskKey<String> user =
                    definition.task(new TaskKey<>("user") {}, ParName.of("io"), () -> "alice", options("user"));
            TaskKey<List<String>> orders = definition.task(
                    new TaskKey<List<String>>("orders") {},
                    ParName.of("io"),
                    () -> java.util.Collections.singletonList("order-1"),
                    options("orders"));
            TaskKey<String> page = definition.combine(
                    new TaskKey<>("assemble") {},
                    ParName.of("cpu"),
                    values -> {
                        combineRuns.incrementAndGet();
                        combineThread.set(Thread.currentThread().getName());
                        combineTask.set(TaskExecutionContext.current()
                                .multiTaskContext()
                                .name());
                        return values.value(user) + ":" + values.value(orders).get(0);
                    },
                    options("assemble-options"));

            TaskGroup group = TaskGroup.submit(global, definition.build());

            assertThat(group.future(page).get(2, TimeUnit.SECONDS)).isEqualTo("alice:order-1");
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);
            assertThat(combineRuns).hasValue(1);
            assertThat(combineThread.get()).isNotEqualTo(Thread.currentThread().getName());
            assertThat(combineTask.get()).isEqualTo("assemble");
            assertThat(result.outcome()).isEqualTo(TaskOutcome.SUCCESS);
            assertThat(result.members().keySet()).containsExactly("user", "orders");
            assertThat(result.terminal()).isNotNull();
            assertThat(result.terminal().taskName()).isEqualTo("assemble");
            assertThat(result.terminal().outcome()).isEqualTo(TaskOutcome.SUCCESS);
            assertThat(result.failedTaskName()).isNull();
        } finally {
            global.close();
            io.shutdownNow();
            cpu.shutdownNow();
        }
    }

    @Test
    void memberFailureSkipsCombineAndTerminatesTerminalFuture() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        try {
            AtomicInteger combineRuns = new AtomicInteger();
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("page"));
            definition.task(
                    new TaskKey<>("failure") {},
                    ParName.of("worker"),
                    () -> {
                        throw new IllegalStateException("boom");
                    },
                    options("failure"));
            TaskKey<String> page = definition.combine(
                    new TaskKey<>("assemble") {},
                    ParName.of("worker"),
                    values -> {
                        combineRuns.incrementAndGet();
                        return "unreachable";
                    },
                    options("assemble"));

            TaskGroup group = TaskGroup.submit(global, definition.build());
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(combineRuns).hasValue(0);
            assertThat(group.future(page).isCancelled()).isTrue();
            assertThat(result.outcome()).isEqualTo(TaskOutcome.USER_FAILURE);
            assertThat(result.failedTaskName()).isEqualTo("failure");
            assertThat(result.terminal().outcome()).isEqualTo(TaskOutcome.FAIL_FAST);
            assertThat(result.terminal().startTimeNanos()).isZero();
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void combineFailureIsAttributedToTheCombineNotAMember() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("page"));
            definition.task(new TaskKey<>("user") {}, ParName.of("worker"), () -> "alice", options("user"));
            TaskKey<String> page = definition.combine(
                    new TaskKey<>("assemble") {},
                    ParName.of("worker"),
                    values -> {
                        throw new java.io.IOException("assemble failed");
                    },
                    options("assemble"));

            TaskGroup group = TaskGroup.submit(global, definition.build());
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(result.outcome()).isEqualTo(TaskOutcome.USER_FAILURE);
            assertThat(result.failedTaskName()).isEqualTo("assemble");
            assertThat(result.members().get("user").outcome()).isEqualTo(TaskOutcome.SUCCESS);
            assertThat(result.terminal().outcome()).isEqualTo(TaskOutcome.USER_FAILURE);
            assertThat(result.terminal().failure()).isInstanceOf(java.io.IOException.class);
            assertThatThrownBy(() -> group.future(page).get())
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOf(java.io.IOException.class);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void rejectedCombineFailsAsSubmissionFailureWithoutInlineExecution() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ExecutorService rejecting = new AbstractExecutorService() {
            private volatile boolean shutdown;

            @Override
            public void shutdown() {
                shutdown = true;
            }

            @Override
            public List<Runnable> shutdownNow() {
                shutdown = true;
                return java.util.Collections.emptyList();
            }

            @Override
            public boolean isShutdown() {
                return shutdown;
            }

            @Override
            public boolean isTerminated() {
                return shutdown;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) {
                return true;
            }

            @Override
            public void execute(Runnable command) {
                throw new RejectedExecutionException("full");
            }
        };
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("worker"), executor)
                .register(ParName.of("rejecting"), rejecting)
                .build();
        try {
            AtomicInteger combineRuns = new AtomicInteger();
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("page"));
            definition.task(new TaskKey<>("user") {}, ParName.of("worker"), () -> "alice", options("user"));
            definition.combine(
                    new TaskKey<>("assemble") {},
                    ParName.of("rejecting"),
                    values -> {
                        combineRuns.incrementAndGet();
                        return "unreachable";
                    },
                    options("assemble"));

            TaskGroupResult result = TaskGroup.submit(global, definition.build())
                    .completionFuture()
                    .get(2, TimeUnit.SECONDS);

            assertThat(combineRuns).hasValue(0);
            assertThat(result.outcome()).isEqualTo(TaskOutcome.SUBMISSION_FAILURE);
            assertThat(result.failedTaskName()).isEqualTo("assemble");
            assertThat(result.terminal().outcome()).isEqualTo(TaskOutcome.SUBMISSION_FAILURE);
        } finally {
            global.close();
            executor.shutdownNow();
            rejecting.shutdownNow();
        }
    }

    @Test
    void groupCancelSkipsUnstartedCombine() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        CountDownLatch running = new CountDownLatch(1);
        try {
            AtomicInteger combineRuns = new AtomicInteger();
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("page"));
            definition.task(
                    new TaskKey<>("slow") {},
                    ParName.of("worker"),
                    () -> {
                        running.countDown();
                        Thread.sleep(10_000);
                        return 1;
                    },
                    options("slow"));
            TaskKey<String> page = definition.combine(
                    new TaskKey<>("assemble") {},
                    ParName.of("worker"),
                    values -> {
                        combineRuns.incrementAndGet();
                        return "unreachable";
                    },
                    options("assemble"));

            TaskGroup group = TaskGroup.submit(global, definition.build());
            running.await(2, TimeUnit.SECONDS);
            group.cancel();
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(combineRuns).hasValue(0);
            assertThat(group.future(page).isCancelled()).isTrue();
            assertThat(result.outcome()).isEqualTo(TaskOutcome.GROUP_CANCELED);
            assertThat(result.terminal().outcome()).isEqualTo(TaskOutcome.GROUP_CANCELED);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void combineOwnDeadlineEscalatesToGroupTimeout() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("page"));
            definition.task(new TaskKey<>("user") {}, ParName.of("worker"), () -> "alice", options("user"));
            definition.combine(
                    new TaskKey<>("assemble") {},
                    ParName.of("worker"),
                    values -> {
                        Thread.sleep(10_000);
                        return "unreachable";
                    },
                    MultiTaskOptions.of("assemble")
                            .timeout(Duration.ofMillis(100))
                            .build());

            TaskGroupResult result = TaskGroup.submit(global, definition.build())
                    .completionFuture()
                    .get(5, TimeUnit.SECONDS);

            assertThat(result.outcome()).isEqualTo(TaskOutcome.TIMEOUT);
            assertThat(result.terminal().outcome()).isEqualTo(TaskOutcome.TIMEOUT);
            assertThat(result.members().get("user").outcome()).isEqualTo(TaskOutcome.SUCCESS);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void emptyGroupSubmitsCombineInsideTheSubmitFlow() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("empty"));
            TaskKey<String> page = definition.combine(
                    new TaskKey<>("assemble") {}, ParName.of("worker"), values -> "assembled", options("assemble"));

            TaskGroup group = TaskGroup.submit(global, definition.build());

            assertThat(group.future(page).get(2, TimeUnit.SECONDS)).isEqualTo("assembled");
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);
            assertThat(result.outcome()).isEqualTo(TaskOutcome.SUCCESS);
            assertThat(result.members()).isEmpty();
            assertThat(result.terminal().outcome()).isEqualTo(TaskOutcome.SUCCESS);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void completedTaskValuesEnforcesRefContract() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        try {
            AtomicReference<Throwable> unknownRef = new AtomicReference<>();
            AtomicReference<Throwable> selfRef = new AtomicReference<>();
            AtomicReference<Throwable> wrongType = new AtomicReference<>();
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("page"));
            TaskKey<String> user =
                    definition.task(new TaskKey<>("user") {}, ParName.of("worker"), () -> null, options("user"));
            definition.task(new TaskKey<>("count") {}, ParName.of("worker"), () -> 41, options("count"));
            TaskKey<String> page = definition.combine(
                    new TaskKey<>("assemble") {},
                    ParName.of("worker"),
                    values -> {
                        assertThat(values.value(user)).isNull();
                        assertThat(values.<Integer>value(new TaskKey<>("count") {}))
                                .isEqualTo(41);
                        unknownRef.set(catchIllegal(() -> values.value(new TaskKey<>("missing") {})));
                        selfRef.set(catchIllegal(() -> values.value(new TaskKey<>("assemble") {})));
                        wrongType.set(catchIllegal(() -> values.value(new TaskKey<StringBuilder>("count") {})));
                        return "ok";
                    },
                    options("assemble"));

            TaskGroup group = TaskGroup.submit(global, definition.build());

            assertThat(group.future(page).get(2, TimeUnit.SECONDS)).isEqualTo("ok");
            assertThat(unknownRef.get()).isInstanceOf(IllegalArgumentException.class);
            assertThat(selfRef.get()).isInstanceOf(IllegalArgumentException.class);
            assertThat(wrongType.get()).isInstanceOf(IllegalArgumentException.class);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void combineConfigurationIsValidatedEarly() {
        TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("page"));
        definition.task(new TaskKey<>("user") {}, ParName.of("worker"), () -> "alice", options("user"));
        TaskKey<String> page = new TaskKey<>("assemble") {};

        assertThatThrownBy(() -> definition.combine(
                        new TaskKey<>("user") {}, ParName.of("worker"), values -> "x", options("dup")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> definition.combine(null, ParName.of("worker"), values -> "x", options("null")))
                .isInstanceOf(NullPointerException.class);
        definition.combine(page, ParName.of("worker"), values -> "x", options("assemble"));
        assertThatThrownBy(() -> definition.combine(
                        new TaskKey<>("second") {}, ParName.of("worker"), values -> "y", options("second")))
                .isInstanceOf(IllegalArgumentException.class);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        try {
            TaskGroupDefinition.Builder unknownExecutor = TaskGroupDefinition.builder(options("page"));
            unknownExecutor.combine(
                    new TaskKey<>("assemble") {}, ParName.of("missing"), values -> "x", options("assemble"));
            assertThatThrownBy(() -> TaskGroup.submit(global, unknownExecutor.build()))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    private static Throwable catchIllegal(Runnable action) {
        try {
            action.run();
            return null;
        } catch (IllegalArgumentException failure) {
            return failure;
        }
    }
}
