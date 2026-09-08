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
        GlobalPar global =
                GlobalPar.builder().register("io", io).register("cpu", cpu).build();
        try {
            AtomicInteger combineRuns = new AtomicInteger();
            AtomicReference<String> combineThread = new AtomicReference<>();
            AtomicReference<String> combineTask = new AtomicReference<>();
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("page"));
            TaskRef<String> user = definition.task(new TaskRef<>("user") {}, "io", () -> "alice", options("user"));
            TaskRef<List<String>> orders = definition.task(
                    new TaskRef<List<String>>("orders") {},
                    "io",
                    () -> java.util.Collections.singletonList("order-1"),
                    options("orders"));
            TaskRef<String> page = definition.combine(
                    new TaskRef<>("assemble") {},
                    "cpu",
                    values -> {
                        combineRuns.incrementAndGet();
                        combineThread.set(Thread.currentThread().getName());
                        combineTask.set(TaskExecutionContext.current()
                                .multiTaskContext()
                                .name());
                        return values.value(user) + ":" + values.value(orders).get(0);
                    },
                    options("assemble"));

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
            assertThat(result.failedMemberName()).isNull();
        } finally {
            global.close();
            io.shutdownNow();
            cpu.shutdownNow();
        }
    }

    @Test
    void memberFailureSkipsCombineAndTerminatesTerminalFuture() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        try {
            AtomicInteger combineRuns = new AtomicInteger();
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("page"));
            definition.task(
                    new TaskRef<>("failure") {},
                    "worker",
                    () -> {
                        throw new IllegalStateException("boom");
                    },
                    options("failure"));
            TaskRef<String> page = definition.combine(
                    new TaskRef<>("assemble") {},
                    "worker",
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
            assertThat(result.failedMemberName()).isEqualTo("failure");
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
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("page"));
            definition.task(new TaskRef<>("user") {}, "worker", () -> "alice", options("user"));
            TaskRef<String> page = definition.combine(
                    new TaskRef<>("assemble") {},
                    "worker",
                    values -> {
                        throw new java.io.IOException("assemble failed");
                    },
                    options("assemble"));

            TaskGroup group = TaskGroup.submit(global, definition.build());
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(result.outcome()).isEqualTo(TaskOutcome.USER_FAILURE);
            assertThat(result.failedMemberName()).isEqualTo("assemble");
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
                .register("worker", executor)
                .register("rejecting", rejecting)
                .build();
        try {
            AtomicInteger combineRuns = new AtomicInteger();
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("page"));
            definition.task(new TaskRef<>("user") {}, "worker", () -> "alice", options("user"));
            definition.combine(
                    new TaskRef<>("assemble") {},
                    "rejecting",
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
            assertThat(result.failedMemberName()).isEqualTo("assemble");
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
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        CountDownLatch running = new CountDownLatch(1);
        try {
            AtomicInteger combineRuns = new AtomicInteger();
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("page"));
            definition.task(
                    new TaskRef<>("slow") {},
                    "worker",
                    () -> {
                        running.countDown();
                        Thread.sleep(10_000);
                        return 1;
                    },
                    options("slow"));
            TaskRef<String> page = definition.combine(
                    new TaskRef<>("assemble") {},
                    "worker",
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
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("page"));
            definition.task(new TaskRef<>("user") {}, "worker", () -> "alice", options("user"));
            definition.combine(
                    new TaskRef<>("assemble") {},
                    "worker",
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
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("empty"));
            TaskRef<String> page = definition.combine(
                    new TaskRef<>("assemble") {}, "worker", values -> "assembled", options("assemble"));

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
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        try {
            AtomicReference<Throwable> unknownRef = new AtomicReference<>();
            AtomicReference<Throwable> selfRef = new AtomicReference<>();
            AtomicReference<Throwable> wrongType = new AtomicReference<>();
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options("page"));
            TaskRef<String> user = definition.task(new TaskRef<>("user") {}, "worker", () -> null, options("user"));
            definition.task(new TaskRef<>("count") {}, "worker", () -> 41, options("count"));
            TaskRef<String> page = definition.combine(
                    new TaskRef<>("assemble") {},
                    "worker",
                    values -> {
                        assertThat(values.value(user)).isNull();
                        assertThat(values.<Integer>value(new TaskRef<>("count") {}))
                                .isEqualTo(41);
                        unknownRef.set(catchIllegal(() -> values.value(new TaskRef<>("missing") {})));
                        selfRef.set(catchIllegal(() -> values.value(new TaskRef<>("assemble") {})));
                        wrongType.set(catchIllegal(() -> values.value(new TaskRef<StringBuilder>("count") {})));
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
        definition.task(new TaskRef<>("user") {}, "worker", () -> "alice", options("user"));
        TaskRef<String> page = new TaskRef<>("assemble") {};

        assertThatThrownBy(() -> definition.combine(new TaskRef<>("user") {}, "worker", values -> "x", options("dup")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> definition.combine(null, "worker", values -> "x", options("null")))
                .isInstanceOf(NullPointerException.class);
        definition.combine(page, "worker", values -> "x", options("assemble"));
        assertThatThrownBy(() ->
                        definition.combine(new TaskRef<>("second") {}, "worker", values -> "y", options("second")))
                .isInstanceOf(IllegalArgumentException.class);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        try {
            TaskGroupDefinition.Builder unknownExecutor = TaskGroupDefinition.builder(options("page"));
            unknownExecutor.combine(new TaskRef<>("assemble") {}, "missing", values -> "x", options("assemble"));
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
