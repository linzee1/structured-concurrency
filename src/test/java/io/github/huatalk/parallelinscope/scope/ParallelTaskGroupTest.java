package io.github.huatalk.parallelinscope.scope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.alibaba.ttl.TransmittableThreadLocal;
import com.google.common.util.concurrent.MoreExecutors;
import io.github.huatalk.parallelinscope.context.SubmissionScope;
import io.github.huatalk.parallelinscope.internal.TaskExecutionContext;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ParallelTaskGroupTest {
    @Test
    void buildsAndSubmitsHeterogeneousMembersAtOneBoundary() throws Exception {
        ExecutorService first = Executors.newSingleThreadExecutor();
        ExecutorService second = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register("first", first)
                .register("second", second)
                .build();
        try {
            AtomicInteger executions = new AtomicInteger();
            ParallelTaskGroup.Builder builder =
                    global.taskGroupBuilder(TaskGroupOptions.of("page").build());
            ParallelTaskGroup.TaskHandle<String> text = builder.addTask(
                    "text",
                    global.par("first"),
                    () -> "value-" + executions.incrementAndGet(),
                    BatchExecutionOptions.of("text").build());
            ParallelTaskGroup.TaskHandle<Integer> number = builder.addTask(
                    "number",
                    global.par("second"),
                    () -> 40 + executions.incrementAndGet(),
                    BatchExecutionOptions.of("number").build());

            assertThatThrownBy(text::future).isInstanceOf(IllegalStateException.class);

            ParallelTaskGroup group = builder.buildAndSubmitAll();
            assertThat(text.future().get(2, TimeUnit.SECONDS)).startsWith("value-");
            assertThat(number.future().get(2, TimeUnit.SECONDS)).isBetween(41, 42);
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(executions).hasValue(2);
            assertThat(result.completionReason()).isEqualTo(TaskGroupCompletionReason.SUCCESS);
            assertThat(result.members().keySet()).containsExactly("text", "number");
            assertThat(group.members().keySet()).containsExactly("text", "number");
            assertThat(group.findMember("text")).contains(text.future());
            assertThatThrownBy(builder::buildAndSubmitAll).isInstanceOf(IllegalStateException.class);
        } finally {
            global.close();
            first.shutdownNow();
            second.shutdownNow();
        }
    }

    @Test
    void addTaskIsConfigurationOnlyAndTtlSnapshotIsTakenAtBuild() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        TransmittableThreadLocal<String> ttl = new TransmittableThreadLocal<>();
        try {
            AtomicInteger calls = new AtomicInteger();
            ttl.set("add");
            ParallelTaskGroup.Builder builder =
                    global.taskGroupBuilder(TaskGroupOptions.of("ttl").build());
            ParallelTaskGroup.TaskHandle<String> handle = builder.addTask(
                    "member",
                    global.par("worker"),
                    () -> {
                        calls.incrementAndGet();
                        return ttl.get();
                    },
                    BatchExecutionOptions.of("member").build());

            assertThat(calls).hasValue(0);
            ttl.set("build");
            builder.buildAndSubmitAll();

            assertThat(handle.future().get(2, TimeUnit.SECONDS)).isEqualTo("build");
            assertThat(calls).hasValue(1);
        } finally {
            ttl.remove();
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void failureIsFailFastAndCancelsUnfinishedSibling() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        CountDownLatch running = new CountDownLatch(1);
        try {
            ParallelTaskGroup.Builder builder =
                    global.taskGroupBuilder(TaskGroupOptions.of("fail-fast").build());
            builder.addTask(
                    "slow",
                    global.par("worker"),
                    () -> {
                        running.countDown();
                        Thread.sleep(10_000);
                        return 1;
                    },
                    BatchExecutionOptions.of("slow").build());
            builder.addTask(
                    "failure",
                    global.par("worker"),
                    () -> {
                        running.await(2, TimeUnit.SECONDS);
                        throw new IllegalStateException("boom");
                    },
                    BatchExecutionOptions.of("failure").build());

            TaskGroupResult result =
                    builder.buildAndSubmitAll().completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(result.completionReason()).isEqualTo(TaskGroupCompletionReason.FAILED);
            assertThat(result.failedMemberName()).isEqualTo("failure");
            assertThat(result.members().get("failure").completionReason())
                    .isEqualTo(TaskOutcome.USER_FAILURE);
            assertThat(result.members().get("slow").completionReason()).isEqualTo(TaskOutcome.FAIL_FAST);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void groupAndMemberDeadlinesConvergeAsTimeout() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        try {
            ParallelTaskGroup.Builder groupDeadline = global.taskGroupBuilder(TaskGroupOptions.of("group-timeout")
                    .timeout(Duration.ofMillis(30))
                    .build());
            groupDeadline.addTask(
                    "slow",
                    global.par("worker"),
                    () -> {
                        Thread.sleep(10_000);
                        return 1;
                    },
                    BatchExecutionOptions.of("slow")
                            .timeout(Duration.ofSeconds(2))
                            .build());
            TaskGroupResult first =
                    groupDeadline.buildAndSubmitAll().completionFuture().get(2, TimeUnit.SECONDS);
            assertThat(first.completionReason()).isEqualTo(TaskGroupCompletionReason.TIMEOUT);
            assertThat(first.members().get("slow").completionReason()).isEqualTo(TaskOutcome.TIMEOUT);

            ParallelTaskGroup.Builder memberDeadline = global.taskGroupBuilder(TaskGroupOptions.of("member-timeout")
                    .timeout(Duration.ofSeconds(2))
                    .build());
            memberDeadline.addTask(
                    "slow",
                    global.par("worker"),
                    () -> {
                        Thread.sleep(10_000);
                        return 1;
                    },
                    BatchExecutionOptions.of("slow")
                            .timeout(Duration.ofMillis(30))
                            .build());
            TaskGroupResult second =
                    memberDeadline.buildAndSubmitAll().completionFuture().get(2, TimeUnit.SECONDS);
            assertThat(second.completionReason()).isEqualTo(TaskGroupCompletionReason.TIMEOUT);
            assertThat(second.members().get("slow").completionReason()).isEqualTo(TaskOutcome.TIMEOUT);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void directMemberCancellationDoesNotCancelSibling() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        CountDownLatch release = new CountDownLatch(1);
        try {
            ParallelTaskGroup.Builder builder =
                    global.taskGroupBuilder(TaskGroupOptions.of("member-cancel").build());
            ParallelTaskGroup.TaskHandle<Integer> canceled = builder.addTask(
                    "canceled",
                    global.par("worker"),
                    () -> {
                        release.await();
                        return 1;
                    },
                    BatchExecutionOptions.of("canceled").build());
            ParallelTaskGroup.TaskHandle<Integer> sibling = builder.addTask(
                    "sibling",
                    global.par("worker"),
                    () -> 2,
                    BatchExecutionOptions.of("sibling").build());
            ParallelTaskGroup group = builder.buildAndSubmitAll();
            canceled.future().cancel(true);
            release.countDown();

            assertThat(sibling.future().get(2, TimeUnit.SECONDS)).isEqualTo(2);
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);
            assertThat(result.completionReason()).isEqualTo(TaskGroupCompletionReason.CANCELED);
            assertThat(result.members().get("canceled").completionReason())
                    .isEqualTo(TaskOutcome.MEMBER_CANCELED);
            assertThat(result.members().get("sibling").completionReason()).isEqualTo(TaskOutcome.SUCCESS);
        } finally {
            release.countDown();
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void explicitGroupCancellationClassifiesEveryUnfinishedMember() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        CountDownLatch started = new CountDownLatch(2);
        try {
            ParallelTaskGroup.Builder builder =
                    global.taskGroupBuilder(TaskGroupOptions.of("cancel").build());
            for (String name : Arrays.asList("one", "two")) {
                builder.addTask(
                        name,
                        global.par("worker"),
                        () -> {
                            started.countDown();
                            Thread.sleep(10_000);
                            return name;
                        },
                        BatchExecutionOptions.of(name).build());
            }
            ParallelTaskGroup group = builder.buildAndSubmitAll();
            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();

            group.cancel();
            group.cancel();
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(result.completionReason()).isEqualTo(TaskGroupCompletionReason.CANCELED);
            assertThat(result.members().values())
                    .extracting(TaskGroupMemberResult::completionReason)
                    .containsOnly(TaskOutcome.GROUP_CANCELED);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void inlineTaskObservesCompleteFrozenRegistry() throws Exception {
        ExecutorService direct = MoreExecutors.newDirectExecutorService();
        GlobalPar global = GlobalPar.builder().register("direct", direct).build();
        AtomicReference<ParallelTaskGroup> published = new AtomicReference<>();
        AtomicReference<ParallelTaskGroup.Builder> builderRef = new AtomicReference<>();
        try {
            ParallelTaskGroup.Builder builder =
                    global.taskGroupBuilder(TaskGroupOptions.of("inline").build());
            builderRef.set(builder);
            AtomicReference<ParallelTaskGroup.TaskHandle<Integer>> second = new AtomicReference<>();
            builder.addTask(
                    "first",
                    global.par("direct"),
                    () -> {
                        assertThat(second.get().future()).isNotNull();
                        return 1;
                    },
                    BatchExecutionOptions.of("first").build());
            second.set(builder.addTask(
                    "second",
                    global.par("direct"),
                    () -> 2,
                    BatchExecutionOptions.of("second").build()));

            published.set(builder.buildAndSubmitAll());

            assertThat(published.get().members().keySet()).containsExactly("first", "second");
            assertThat(published.get().completionFuture().get().completionReason())
                    .isEqualTo(TaskGroupCompletionReason.SUCCESS);
        } finally {
            global.close();
            direct.shutdownNow();
        }
    }

    @Test
    void rejectionIsSubmissionFailureAndLaterPreparedTaskNeverRuns() throws Exception {
        ExecutorService rejecting = new RejectingExecutor();
        ExecutorService normal = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register("reject", rejecting)
                .register("normal", normal)
                .build();
        AtomicInteger calls = new AtomicInteger();
        try {
            ParallelTaskGroup.Builder builder =
                    global.taskGroupBuilder(TaskGroupOptions.of("rejection").build());
            builder.addTask(
                    "rejected",
                    global.par("reject"),
                    () -> 1,
                    BatchExecutionOptions.of("rejected")
                            .taskType(TaskType.IO_BOUND)
                            .build());
            builder.addTask(
                    "later",
                    global.par("normal"),
                    calls::incrementAndGet,
                    BatchExecutionOptions.of("later").build());

            TaskGroupResult result =
                    builder.buildAndSubmitAll().completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(result.completionReason()).isEqualTo(TaskGroupCompletionReason.FAILED);
            assertThat(result.members().get("rejected").completionReason())
                    .isEqualTo(TaskOutcome.SUBMISSION_FAILURE);
            assertThat(result.members().get("later").completionReason()).isEqualTo(TaskOutcome.FAIL_FAST);
            assertThat(calls).hasValue(0);
            assertThat(SubmissionScope.currentBatch()).isNull();
        } finally {
            global.close();
            rejecting.shutdownNow();
            normal.shutdownNow();
        }
    }

    @Test
    void listenerReceivesSnapshotOutsideCurrentTaskAndIsolatedFromFailure() throws Exception {
        ExecutorService direct = MoreExecutors.newDirectExecutorService();
        AtomicReference<TaskGroupResult> observed = new AtomicReference<>();
        AtomicReference<TaskExecutionContext> current = new AtomicReference<>();
        GlobalPar global = GlobalPar.builder().register("direct", direct).build();
        try {
            TaskGroupOptions options = TaskGroupOptions.of("listener")
                    .listener(event -> {
                        observed.set(event.result());
                        current.set(TaskExecutionContext.current());
                        throw new IllegalStateException("ignored");
                    })
                    .build();
            ParallelTaskGroup.Builder builder = global.taskGroupBuilder(options);
            builder.addTask(
                    "one",
                    global.par("direct"),
                    () -> 1,
                    BatchExecutionOptions.of("one").build());

            TaskGroupResult result =
                    builder.buildAndSubmitAll().completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(observed.get()).isSameAs(result);
            assertThat(current.get()).isNull();
        } finally {
            global.close();
            direct.shutdownNow();
        }
    }

    @Test
    void validatesDefinitionsAndEmptyGroupCompletesImmediately() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        GlobalPar foreign = GlobalPar.builder().register("foreign", executor).build();
        try {
            ParallelTaskGroup.Builder builder =
                    global.taskGroupBuilder(TaskGroupOptions.of("validation").build());
            builder.addTask(
                    "one",
                    global.par("worker"),
                    () -> 1,
                    BatchExecutionOptions.of("one").build());
            assertThatThrownBy(() -> builder.addTask(
                            "one",
                            global.par("worker"),
                            () -> 2,
                            BatchExecutionOptions.of("two").build()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> builder.addTask(
                            "foreign",
                            foreign.par("foreign"),
                            () -> 2,
                            BatchExecutionOptions.of("foreign").build()))
                    .isInstanceOf(IllegalArgumentException.class);

            ParallelTaskGroup empty = global.taskGroupBuilder(
                            TaskGroupOptions.of("empty").build())
                    .buildAndSubmitAll();
            assertThat(empty.completionFuture().get().completionReason()).isEqualTo(TaskGroupCompletionReason.SUCCESS);

            ParallelTaskGroup.Builder existing =
                    global.taskGroupBuilder(TaskGroupOptions.of("existing").build());
            global.close();
            assertThatThrownBy(() -> global.taskGroupBuilder(
                            TaskGroupOptions.of("closed").build()))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(existing::buildAndSubmitAll).isInstanceOf(IllegalStateException.class);
        } finally {
            foreign.close();
            executor.shutdownNow();
        }
    }

    @Test
    void nestedGroupCapturesOuterTaskAsStructuralParent() throws Exception {
        ExecutorService outer = Executors.newSingleThreadExecutor();
        ExecutorService inner = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register("outer", outer)
                .register("inner", inner)
                .build();
        try {
            AsyncBatchResult<BatchExecutionContext> result = global.par("outer")
                    .map(
                            Arrays.asList(1),
                            ignored -> {
                                BatchExecutionContext expectedParent =
                                        TaskExecutionContext.current().batchContext();
                                ParallelTaskGroup.Builder builder = global.taskGroupBuilder(
                                        TaskGroupOptions.of("nested").build());
                                ParallelTaskGroup.TaskHandle<BatchExecutionContext> child = builder.addTask(
                                        "child",
                                        global.par("inner"),
                                        () -> TaskExecutionContext.current()
                                                .batchContext()
                                                .parent(),
                                        BatchExecutionOptions.of("child").build());
                                builder.buildAndSubmitAll();
                                try {
                                    assertThat(child.future().get(2, TimeUnit.SECONDS))
                                            .isSameAs(expectedParent);
                                    return expectedParent;
                                } catch (Exception failure) {
                                    throw new RuntimeException(failure);
                                }
                            },
                            BatchExecutionOptions.of("outer").build());
            assertThat(result.results().get(0).get(2, TimeUnit.SECONDS)).isNotNull();
        } finally {
            global.close();
            outer.shutdownNow();
            inner.shutdownNow();
        }
    }

    private static final class RejectingExecutor extends AbstractExecutorService {
        private volatile boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public java.util.List<Runnable> shutdownNow() {
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
            return shutdown;
        }

        @Override
        public void execute(Runnable command) {
            throw new RejectedExecutionException("rejected");
        }
    }
}
