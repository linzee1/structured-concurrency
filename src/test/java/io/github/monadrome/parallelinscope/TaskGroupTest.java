package io.github.monadrome.parallelinscope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.alibaba.ttl.TransmittableThreadLocal;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class TaskGroupTest {

    private static MultiTaskOptions groupOptions(String name) {
        return MultiTaskOptions.of(name).timeout(Duration.ofSeconds(30)).build();
    }

    private static MultiTaskOptions memberOptions(String name) {
        return MultiTaskOptions.of(name).timeout(Duration.ofSeconds(30)).build();
    }

    @Test
    void buildsAndSubmitsHeterogeneousMembersAtOneBoundary() throws Exception {
        ExecutorService first = Executors.newSingleThreadExecutor();
        ExecutorService second = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("first"), first)
                .register(ParName.of("second"), second)
                .build();
        try {
            AtomicInteger executions = new AtomicInteger();
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("page"));
            TaskKey<String> text = definition.task(
                    new TaskKey<>("text") {},
                    ParName.of("first"),
                    () -> "value-" + executions.incrementAndGet(),
                    memberOptions("text"));
            TaskKey<Integer> number = definition.task(
                    new TaskKey<>("number") {},
                    ParName.of("second"),
                    () -> 40 + executions.incrementAndGet(),
                    memberOptions("number"));

            TaskGroup group = TaskGroup.submit(global, definition.build());
            assertThat(group.future(text).get(2, TimeUnit.SECONDS)).startsWith("value-");
            assertThat(group.future(number).get(2, TimeUnit.SECONDS)).isBetween(41, 42);
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(executions).hasValue(2);
            assertThat(result.outcome()).isEqualTo(TaskOutcome.SUCCESS);
            assertThat(result.members().keySet()).containsExactly("text", "number");
            assertThat(group.members().keySet()).containsExactly("text", "number");
            assertThat(group.findMember("text")).contains(group.future(text));
            assertThatThrownBy(() -> group.future(new TaskKey<>("missing") {}))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            global.close();
            first.shutdownNow();
            second.shutdownNow();
        }
    }

    @Test
    void memberKeyNameOwnsExecutionDiagnosticsWhenOptionsNameDiffers() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<TaskCompletion<?>> listenerCompletion = new AtomicReference<>();
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("worker"), executor)
                .taskListener(listenerCompletion::set)
                .build();
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("page"));
            TaskKey<String> user = definition.task(
                    new TaskKey<>("user") {},
                    ParName.of("worker"),
                    () -> {
                        MultiTaskContext context =
                                TaskExecutionContext.current().multiTaskContext();
                        context.cancellationToken().cancel(false);
                        Checkpoints.checkpoint("load-user", true);
                        assertThatThrownBy(() -> Checkpoints.checkpoint("user", true))
                                .isInstanceOf(CancellationException.class);
                        return "alice";
                    },
                    MultiTaskOptions.of("load-user").inheritTimeout().build());

            TaskGroup group = TaskGroup.submit(global, definition.build());
            assertThat(group.future(user).get(2, TimeUnit.SECONDS)).isEqualTo("alice");
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(listenerCompletion.get().taskName()).isEqualTo("user");
            assertThat(result.members().get("user").taskName()).isEqualTo("user");
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void specConfigurationRunsNoTaskAndTtlSnapshotIsTakenAtSubmit() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        TransmittableThreadLocal<String> ttl = new TransmittableThreadLocal<>();
        try {
            AtomicInteger calls = new AtomicInteger();
            ttl.set("configure");
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("ttl"));
            TaskKey<String> member = definition.task(
                    new TaskKey<>("member") {},
                    ParName.of("worker"),
                    () -> {
                        calls.incrementAndGet();
                        return ttl.get();
                    },
                    memberOptions("member"));

            assertThat(calls).hasValue(0);
            ttl.set("submit");
            TaskGroup group = TaskGroup.submit(global, definition.build());

            assertThat(group.future(member).get(2, TimeUnit.SECONDS)).isEqualTo("submit");
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
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        CountDownLatch running = new CountDownLatch(1);
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("fail-fast"));
            definition.task(
                    new TaskKey<>("slow") {},
                    ParName.of("worker"),
                    () -> {
                        running.countDown();
                        Thread.sleep(10_000);
                        return 1;
                    },
                    memberOptions("slow"));
            definition.task(
                    new TaskKey<>("failure") {},
                    ParName.of("worker"),
                    () -> {
                        running.await(2, TimeUnit.SECONDS);
                        throw new IllegalStateException("boom");
                    },
                    memberOptions("failure"));

            TaskGroupResult result = TaskGroup.submit(global, definition.build())
                    .completionFuture()
                    .get(2, TimeUnit.SECONDS);

            assertThat(result.outcome()).isEqualTo(TaskOutcome.USER_FAILURE);
            assertThat(result.failedTaskName()).isEqualTo("failure");
            assertThat(result.members().get("failure").outcome()).isEqualTo(TaskOutcome.USER_FAILURE);
            assertThat(result.members().get("slow").outcome()).isEqualTo(TaskOutcome.FAIL_FAST);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void groupAndMemberDeadlinesConvergeAsTimeout() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        try {
            TaskGroupDefinition.Builder groupDeadline = TaskGroupDefinition.builder(MultiTaskOptions.of("group-timeout")
                    .timeout(Duration.ofMillis(30))
                    .build());
            groupDeadline.task(
                    new TaskKey<>("slow") {},
                    ParName.of("worker"),
                    () -> {
                        Thread.sleep(10_000);
                        return 1;
                    },
                    MultiTaskOptions.of("slow").timeout(Duration.ofSeconds(2)).build());
            TaskGroupResult first = TaskGroup.submit(global, groupDeadline.build())
                    .completionFuture()
                    .get(2, TimeUnit.SECONDS);
            assertThat(first.outcome()).isEqualTo(TaskOutcome.TIMEOUT);
            assertThat(first.members().get("slow").outcome()).isEqualTo(TaskOutcome.TIMEOUT);

            TaskGroupDefinition.Builder memberDeadline =
                    TaskGroupDefinition.builder(MultiTaskOptions.of("member-timeout")
                            .timeout(Duration.ofSeconds(2))
                            .build());
            memberDeadline.task(
                    new TaskKey<>("slow") {},
                    ParName.of("worker"),
                    () -> {
                        Thread.sleep(10_000);
                        return 1;
                    },
                    MultiTaskOptions.of("slow").timeout(Duration.ofMillis(30)).build());
            TaskGroupResult second = TaskGroup.submit(global, memberDeadline.build())
                    .completionFuture()
                    .get(2, TimeUnit.SECONDS);
            assertThat(second.outcome()).isEqualTo(TaskOutcome.TIMEOUT);
            assertThat(second.members().get("slow").outcome()).isEqualTo(TaskOutcome.TIMEOUT);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void directMemberCancellationCascadesToUnfinishedSibling() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        CountDownLatch release = new CountDownLatch(1);
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("member-cancel"));
            TaskKey<Integer> canceled = definition.task(
                    new TaskKey<>("canceled") {},
                    ParName.of("worker"),
                    () -> {
                        release.await();
                        return 1;
                    },
                    memberOptions("canceled"));
            definition.task(
                    new TaskKey<>("sibling") {},
                    ParName.of("worker"),
                    () -> {
                        release.await(10, TimeUnit.SECONDS);
                        return 2;
                    },
                    memberOptions("sibling"));
            TaskGroup group = TaskGroup.submit(global, definition.build());
            group.future(canceled).cancel(true);

            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);
            assertThat(result.outcome()).isEqualTo(TaskOutcome.GROUP_CANCELED);
            assertThat(result.members().get("canceled").outcome()).isEqualTo(TaskOutcome.MEMBER_CANCELED);
            assertThat(result.members().get("sibling").outcome()).isEqualTo(TaskOutcome.GROUP_CANCELED);
        } finally {
            release.countDown();
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void memberTimeoutEscalatesToGroupTimeoutAndCancelsSibling() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(MultiTaskOptions.of("member-timeout")
                    .timeout(Duration.ofSeconds(2))
                    .build());
            definition.task(
                    new TaskKey<>("slow") {},
                    ParName.of("worker"),
                    () -> {
                        Thread.sleep(10_000);
                        return 1;
                    },
                    MultiTaskOptions.of("slow").timeout(Duration.ofMillis(50)).build());
            definition.task(
                    new TaskKey<>("sibling") {},
                    ParName.of("worker"),
                    () -> {
                        Thread.sleep(10_000);
                        return 2;
                    },
                    memberOptions("sibling"));

            TaskGroupResult result = TaskGroup.submit(global, definition.build())
                    .completionFuture()
                    .get(2, TimeUnit.SECONDS);

            assertThat(result.outcome()).isEqualTo(TaskOutcome.TIMEOUT);
            assertThat(result.members().get("slow").outcome()).isEqualTo(TaskOutcome.TIMEOUT);
            assertThat(result.members().get("sibling").outcome()).isEqualTo(TaskOutcome.TIMEOUT);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void mixedMemberDeadlinesAttributeBoundAndSkippedPathsCorrectly() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        try {
            // "tight" binds its own stricter deadline; "shared" resolves to exactly the group
            // deadline and skips the member bind. Both paths must still attribute TIMEOUT.
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(MultiTaskOptions.of("mixed-deadlines")
                    .timeout(Duration.ofSeconds(2))
                    .build());
            definition.task(
                    new TaskKey<>("tight") {},
                    ParName.of("worker"),
                    () -> {
                        Thread.sleep(10_000);
                        return 1;
                    },
                    MultiTaskOptions.of("tight").timeout(Duration.ofMillis(50)).build());
            definition.task(
                    new TaskKey<>("shared") {},
                    ParName.of("worker"),
                    () -> {
                        Thread.sleep(10_000);
                        return 2;
                    },
                    MultiTaskOptions.of("shared").timeout(Duration.ofSeconds(2)).build());

            TaskGroupResult result = TaskGroup.submit(global, definition.build())
                    .completionFuture()
                    .get(2, TimeUnit.SECONDS);

            assertThat(result.outcome()).isEqualTo(TaskOutcome.TIMEOUT);
            assertThat(result.members().get("tight").outcome()).isEqualTo(TaskOutcome.TIMEOUT);
            assertThat(result.members().get("shared").outcome()).isEqualTo(TaskOutcome.TIMEOUT);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void groupTimeoutPropagatesThroughSkippedMemberBindIntoNestedBatch() throws Exception {
        ExecutorService outer = Executors.newSingleThreadExecutor();
        ExecutorService inner = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("outer"), outer)
                .register(ParName.of("inner"), inner)
                .build();
        AtomicReference<CancellationToken> memberToken = new AtomicReference<>();
        AtomicReference<CancellationToken> nestedToken = new AtomicReference<>();
        AtomicReference<ListenableFuture<?>> nestedFuture = new AtomicReference<>();
        CountDownLatch nestedRunning = new CountDownLatch(1);
        try {
            // The member inherits the group deadline, so its token is never bound; propagation to
            // the nested batch rides the token constructor listener chain alone.
            TaskGroupDefinition.Builder definition =
                    TaskGroupDefinition.builder(MultiTaskOptions.of("nested-propagation")
                            .timeout(Duration.ofMillis(50))
                            .build());
            definition.task(
                    new TaskKey<>("member") {},
                    ParName.of("outer"),
                    () -> {
                        memberToken.set(TaskExecutionContext.current()
                                .multiTaskContext()
                                .cancellationToken());
                        TaskBatchResult<Integer> nested = global.par(ParName.of("inner"))
                                .map(
                                        Arrays.asList(1),
                                        ignored -> {
                                            nestedToken.set(TaskExecutionContext.current()
                                                    .multiTaskContext()
                                                    .cancellationToken());
                                            nestedRunning.countDown();
                                            try {
                                                new CountDownLatch(1).await(10, TimeUnit.SECONDS);
                                            } catch (InterruptedException interrupted) {
                                                Thread.currentThread().interrupt();
                                            }
                                            return 1;
                                        },
                                        MultiTaskOptions.of("nested")
                                                .inheritTimeout()
                                                .build());
                        nestedFuture.set(nested.results().get(0));
                        try {
                            return nested.results().get(0).get(10, TimeUnit.SECONDS);
                        } catch (Exception failure) {
                            throw new RuntimeException(failure);
                        }
                    },
                    MultiTaskOptions.of("member").inheritTimeout().build());

            TaskGroup group = TaskGroup.submit(global, definition.build());
            assertThat(nestedRunning.await(2, TimeUnit.SECONDS)).isTrue();
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            // Two timers share the group deadline: the group token's own and the nested batch's
            // (both inherit the same instant). If the group timer wins, the group converges as
            // TIMEOUT; if the nested timer wins, the member surfaces the nested cancellation as a
            // failure while the group token is still RUNNING, and the group converges as
            // MEMBER_CANCELED. Either way the deadline reached every level (asserted below).
            assertThat(result.outcome()).isIn(TaskOutcome.TIMEOUT, TaskOutcome.MEMBER_CANCELED);
            // The member token is never bound, so only constructor-listener propagation from the
            // group token can move it; await the cascade, which runs after the group converges.
            org.awaitility.Awaitility.await()
                    .atMost(2, TimeUnit.SECONDS)
                    .until(() -> memberToken.get().state() == CancellationToken.State.PROPAGATED_CANCELED
                            && nestedToken.get().state() != CancellationToken.State.RUNNING
                            && nestedFuture.get().isCancelled());
            // The nested batch inherits the group deadline, so its own timer races the propagated
            // cancellation; either terminal state attests the deadline reached the nested batch.
            assertThat(nestedToken.get().state())
                    .isIn(CancellationToken.State.PROPAGATED_CANCELED, CancellationToken.State.TIMEOUT);
        } finally {
            global.close();
            outer.shutdownNow();
            inner.shutdownNow();
        }
    }

    @Test
    void explicitGroupCancellationClassifiesEveryUnfinishedMember() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        CountDownLatch started = new CountDownLatch(2);
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("cancel"));
            for (String name : Arrays.asList("one", "two")) {
                definition.task(
                        new TaskKey<>(name) {},
                        ParName.of("worker"),
                        () -> {
                            started.countDown();
                            Thread.sleep(10_000);
                            return name;
                        },
                        memberOptions(name));
            }
            TaskGroup group = TaskGroup.submit(global, definition.build());
            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();

            group.cancel();
            group.cancel();
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(result.outcome()).isEqualTo(TaskOutcome.GROUP_CANCELED);
            assertThat(result.members().values())
                    .extracting(TaskCompletion::outcome)
                    .containsOnly(TaskOutcome.GROUP_CANCELED);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void inlineMembersRunDuringSubmitOverACompleteFrozenRegistry() throws Exception {
        ExecutorService direct = MoreExecutors.newDirectExecutorService();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("direct"), direct).build();
        try {
            Thread submitThread = Thread.currentThread();
            AtomicReference<Thread> firstThread = new AtomicReference<>();
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("inline"));
            TaskKey<Integer> first = definition.task(
                    new TaskKey<>("first") {},
                    ParName.of("direct"),
                    () -> {
                        firstThread.set(Thread.currentThread());
                        return 1;
                    },
                    memberOptions("first"));
            TaskKey<Integer> second =
                    definition.task(new TaskKey<>("second") {}, ParName.of("direct"), () -> 2, memberOptions("second"));

            TaskGroup group = TaskGroup.submit(global, definition.build());

            assertThat(firstThread.get()).isSameAs(submitThread);
            assertThat(group.members().keySet()).containsExactly("first", "second");
            assertThat(group.future(first).get()).isEqualTo(1);
            assertThat(group.future(second).get()).isEqualTo(2);
            assertThat(group.completionFuture().get().outcome()).isEqualTo(TaskOutcome.SUCCESS);
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
                .register(ParName.of("reject"), rejecting)
                .register(ParName.of("normal"), normal)
                .build();
        AtomicInteger calls = new AtomicInteger();
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("rejection"));
            definition.task(
                    new TaskKey<>("rejected") {},
                    ParName.of("reject"),
                    () -> 1,
                    MultiTaskOptions.of("rejected")
                            .taskType(TaskType.IO_BOUND)
                            .timeout(Duration.ofSeconds(30))
                            .build());
            definition.task(
                    new TaskKey<>("later") {}, ParName.of("normal"), calls::incrementAndGet, memberOptions("later"));

            TaskGroupResult result = TaskGroup.submit(global, definition.build())
                    .completionFuture()
                    .get(2, TimeUnit.SECONDS);

            assertThat(result.outcome()).isEqualTo(TaskOutcome.SUBMISSION_FAILURE);
            assertThat(result.members().get("rejected").outcome()).isEqualTo(TaskOutcome.SUBMISSION_FAILURE);
            assertThat(result.members().get("later").outcome()).isEqualTo(TaskOutcome.FAIL_FAST);
            assertThat(calls).hasValue(0);
            assertThat(SubmissionScope.current()).isNull();
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
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("direct"), direct).build();
        try {
            MultiTaskOptions options = MultiTaskOptions.of("listener")
                    .listener(event -> {
                        observed.set(event.result());
                        current.set(TaskExecutionContext.current());
                        throw new IllegalStateException("ignored");
                    })
                    .timeout(Duration.ofSeconds(30))
                    .build();
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(options);
            definition.task(new TaskKey<>("one") {}, ParName.of("direct"), () -> 1, memberOptions("one"));

            TaskGroupResult result = TaskGroup.submit(global, definition.build())
                    .completionFuture()
                    .get(2, TimeUnit.SECONDS);

            assertThat(observed.get()).isSameAs(result);
            assertThat(current.get()).isNull();
        } finally {
            global.close();
            direct.shutdownNow();
        }
    }

    @Test
    void definitionValidatesTasksAndIsReusableAcrossSubmits() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("definition"));
            definition.task(new TaskKey<>("one") {}, ParName.of("worker"), () -> 1, memberOptions("one"));
            assertThatThrownBy(() -> definition.task(
                            new TaskKey<>("one") {}, ParName.of("worker"), () -> 2, memberOptions("two")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> definition.task(
                            new TaskKey<>(" ") {}, ParName.of("worker"), () -> 2, memberOptions("blank")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> definition.task(null, ParName.of("worker"), () -> 2, memberOptions("null")))
                    .isInstanceOf(NullPointerException.class);

            TaskGroupDefinition.Builder unknownExecutor = TaskGroupDefinition.builder(groupOptions("unknown"));
            unknownExecutor.task(new TaskKey<>("member") {}, ParName.of("missing"), () -> 1, memberOptions("member"));
            assertThatThrownBy(() -> TaskGroup.submit(global, unknownExecutor.build()))
                    .isInstanceOf(IllegalArgumentException.class);

            TaskGroupDefinition reusable = definition.build();
            assertThat(reusable.tasks())
                    .extracting(TaskGroupDefinition.TaskDefinition::name)
                    .containsExactly("one");
            TaskGroup first = TaskGroup.submit(global, reusable);
            TaskGroup second = TaskGroup.submit(global, reusable);
            assertThat(first.groupId()).isNotEqualTo(second.groupId());
            assertThat(first.completionFuture().get(2, TimeUnit.SECONDS).outcome())
                    .isEqualTo(TaskOutcome.SUCCESS);
            assertThat(second.completionFuture().get(2, TimeUnit.SECONDS).outcome())
                    .isEqualTo(TaskOutcome.SUCCESS);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void keyCapturesTheParameterizedResultType() {
        TaskKey<java.util.List<String>> key = new TaskKey<java.util.List<String>>("orders") {};
        assertThat(key.name()).isEqualTo("orders");
        assertThat(key.resultType().getType().getTypeName()).isEqualTo("java.util.List<java.lang.String>");
        assertThat(key.resultType().getRawType()).isEqualTo(java.util.List.class);
        assertThatThrownBy(() -> new TaskKey<Object>(" ") {}).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TaskKey<Object>(null) {}).isInstanceOf(NullPointerException.class);
    }

    @Test
    void futureRejectsKeyWhoseRawTypeDoesNotCoverTheRegisteredType() throws Exception {
        ExecutorService direct = MoreExecutors.newDirectExecutorService();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("direct"), direct).build();
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("typed"));
            TaskKey<Integer> member = definition.task(
                    new TaskKey<Integer>("member") {}, ParName.of("direct"), () -> 1, memberOptions("member"));

            TaskGroup group = TaskGroup.submit(global, definition.build());

            assertThat(group.future(member).get(2, TimeUnit.SECONDS)).isEqualTo(1);
            // A supertype key is sound: the member result is assignable to it.
            assertThat(group.future(new TaskKey<Number>("member") {}).get(2, TimeUnit.SECONDS))
                    .isEqualTo(1);
            assertThatThrownBy(() -> group.future(new TaskKey<String>("member") {}))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("member");
        } finally {
            global.close();
            direct.shutdownNow();
        }
    }

    @Test
    void emptyGroupCompletesImmediatelyAndSubmitAfterCloseIsRejected() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        try {
            TaskGroup empty = TaskGroup.submit(
                    global, TaskGroupDefinition.builder(groupOptions("empty")).build());
            assertThat(empty.completionFuture().get().outcome()).isEqualTo(TaskOutcome.SUCCESS);

            global.close();
            assertThatThrownBy(() -> TaskGroup.submit(
                            global,
                            TaskGroupDefinition.builder(groupOptions("closed")).build()))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void memberWithInheritedTimeoutResolvesToTheGroupDeadline() throws Exception {
        ExecutorService direct = MoreExecutors.newDirectExecutorService();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("direct"), direct).build();
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("inherit-member"));
            TaskKey<Long> member = definition.task(
                    new TaskKey<>("member") {},
                    ParName.of("direct"),
                    () -> TaskExecutionContext.current().multiTaskContext().deadlineNanos(),
                    MultiTaskOptions.of("member").inheritTimeout().build());

            TaskGroup group = TaskGroup.submit(global, definition.build());
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(group.future(member).get(2, TimeUnit.SECONDS)).isEqualTo(result.deadlineNanos());
        } finally {
            global.close();
            direct.shutdownNow();
        }
    }

    @Test
    void memberWithTighterExplicitTimeoutKeepsItsOwnDeadline() throws Exception {
        ExecutorService direct = MoreExecutors.newDirectExecutorService();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("direct"), direct).build();
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("tight-member"));
            TaskKey<Long> member = definition.task(
                    new TaskKey<>("member") {},
                    ParName.of("direct"),
                    () -> TaskExecutionContext.current().multiTaskContext().deadlineNanos(),
                    MultiTaskOptions.of("member")
                            .timeout(Duration.ofMillis(100))
                            .build());

            TaskGroup group = TaskGroup.submit(global, definition.build());
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(group.future(member).get(2, TimeUnit.SECONDS)).isLessThan(result.deadlineNanos());
        } finally {
            global.close();
            direct.shutdownNow();
        }
    }

    @Test
    void nestedBatchInsideAMemberWithInheritedTimeoutUsesTheMemberDeadline() throws Exception {
        ExecutorService outer = Executors.newSingleThreadExecutor();
        ExecutorService inner = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("outer"), outer)
                .register(ParName.of("inner"), inner)
                .build();
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("nested-batch"));
            TaskKey<long[]> member = definition.task(
                    new TaskKey<>("member") {},
                    ParName.of("outer"),
                    () -> {
                        long memberDeadline = TaskExecutionContext.current()
                                .multiTaskContext()
                                .deadlineNanos();
                        TaskBatchResult<Long> nested = global.par(ParName.of("inner"))
                                .map(
                                        Arrays.asList(1),
                                        ignored -> TaskExecutionContext.current()
                                                .multiTaskContext()
                                                .deadlineNanos(),
                                        MultiTaskOptions.of("nested")
                                                .inheritTimeout()
                                                .build());
                        try {
                            return new long[] {
                                memberDeadline, nested.results().get(0).get(2, TimeUnit.SECONDS)
                            };
                        } catch (Exception failure) {
                            throw new RuntimeException(failure);
                        }
                    },
                    MultiTaskOptions.of("member").inheritTimeout().build());

            TaskGroup group = TaskGroup.submit(global, definition.build());
            long[] deadlines = group.future(member).get(2, TimeUnit.SECONDS);

            assertThat(deadlines[1]).isEqualTo(deadlines[0]);
            assertThat(deadlines[0])
                    .isEqualTo(group.completionFuture().get(2, TimeUnit.SECONDS).deadlineNanos());
        } finally {
            global.close();
            outer.shutdownNow();
            inner.shutdownNow();
        }
    }

    @Test
    void groupWithInheritedTimeoutRequiresAnEnclosingScopedTask() throws Exception {
        ExecutorService outer = Executors.newSingleThreadExecutor();
        ExecutorService inner = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("outer"), outer)
                .register(ParName.of("inner"), inner)
                .build();
        try {
            assertThatThrownBy(() -> TaskGroup.submit(
                            global,
                            TaskGroupDefinition.builder(MultiTaskOptions.of("orphan")
                                            .inheritTimeout()
                                            .build())
                                    .build()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("no enclosing deadline to inherit");

            TaskBatchResult<Long> batch = global.par(ParName.of("outer"))
                    .map(
                            Arrays.asList(1),
                            ignored -> {
                                long outerDeadline = TaskExecutionContext.current()
                                        .multiTaskContext()
                                        .deadlineNanos();
                                TaskGroupDefinition.Builder definition =
                                        TaskGroupDefinition.builder(MultiTaskOptions.of("nested-group")
                                                .inheritTimeout()
                                                .build());
                                definition.task(
                                        new TaskKey<>("child") {},
                                        ParName.of("inner"),
                                        () -> 1,
                                        MultiTaskOptions.of("child")
                                                .inheritTimeout()
                                                .build());
                                try {
                                    TaskGroupResult result = TaskGroup.submit(global, definition.build())
                                            .completionFuture()
                                            .get(2, TimeUnit.SECONDS);
                                    assertThat(result.deadlineNanos()).isEqualTo(outerDeadline);
                                    return result.deadlineNanos();
                                } catch (Exception failure) {
                                    throw new RuntimeException(failure);
                                }
                            },
                            groupOptions("outer"));

            assertThat(batch.results().get(0).get(2, TimeUnit.SECONDS)).isNotNull();
        } finally {
            global.close();
            outer.shutdownNow();
            inner.shutdownNow();
        }
    }

    @Test
    void nestedGroupCapturesOuterTaskAsStructuralParent() throws Exception {
        ExecutorService outer = Executors.newSingleThreadExecutor();
        ExecutorService inner = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("outer"), outer)
                .register(ParName.of("inner"), inner)
                .build();
        try {
            TaskBatchResult<MultiTaskContext> result = global.par(ParName.of("outer"))
                    .map(
                            Arrays.asList(1),
                            ignored -> {
                                MultiTaskContext expectedParent =
                                        TaskExecutionContext.current().multiTaskContext();
                                TaskGroupDefinition.Builder definition =
                                        TaskGroupDefinition.builder(groupOptions("nested"));
                                TaskKey<MultiTaskContext> child = definition.task(
                                        new TaskKey<>("child") {},
                                        ParName.of("inner"),
                                        () -> TaskExecutionContext.current()
                                                .multiTaskContext()
                                                .structuralParent(),
                                        memberOptions("child"));
                                TaskGroup group = TaskGroup.submit(global, definition.build());
                                try {
                                    assertThat(group.future(child).get(2, TimeUnit.SECONDS))
                                            .isSameAs(expectedParent);
                                    return expectedParent;
                                } catch (Exception failure) {
                                    throw new RuntimeException(failure);
                                }
                            },
                            groupOptions("outer"));
            assertThat(result.results().get(0).get(2, TimeUnit.SECONDS)).isNotNull();
        } finally {
            global.close();
            outer.shutdownNow();
            inner.shutdownNow();
        }
    }

    @Test
    void ancestorTimeoutPropagatesAsTimeoutIntoNestedGroup() throws Exception {
        ExecutorService outer = Executors.newSingleThreadExecutor();
        ExecutorService inner = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("outer"), outer)
                .register(ParName.of("inner"), inner)
                .build();
        AtomicReference<TaskGroup> nestedGroup = new AtomicReference<>();
        try {
            TaskBatchResult<Object> result = global.par(ParName.of("outer"))
                    .map(
                            Arrays.asList(1),
                            ignored -> {
                                TaskGroupDefinition.Builder definition =
                                        TaskGroupDefinition.builder(groupOptions("nested"));
                                definition.task(
                                        new TaskKey<>("child") {},
                                        ParName.of("inner"),
                                        () -> {
                                            Thread.sleep(10_000);
                                            return 1;
                                        },
                                        memberOptions("child"));
                                nestedGroup.set(TaskGroup.submit(global, definition.build()));
                                try {
                                    new CountDownLatch(1).await(10, TimeUnit.SECONDS);
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                }
                                return null;
                            },
                            MultiTaskOptions.of("outer")
                                    .timeout(Duration.ofMillis(50))
                                    .build());

            // The outer batch deadline cancels the outer task and propagates into the nested
            // group's token tree; the group keeps the originating timeout reason.
            org.awaitility.Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> nestedGroup.get() != null);
            TaskGroupResult nested = nestedGroup.get().completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(result.results().get(0)).isCancelled();
            assertThat(nested.outcome()).isEqualTo(TaskOutcome.TIMEOUT);
            assertThat(nested.members().get("child").outcome()).isEqualTo(TaskOutcome.TIMEOUT);
        } finally {
            global.close();
            outer.shutdownNow();
            inner.shutdownNow();
        }
    }

    @Test
    void groupIdentifiersExposeConfiguredAndGeneratedValues() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        try {
            TaskGroup group = TaskGroup.submit(
                    global, TaskGroupDefinition.builder(groupOptions("named")).build());
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(group.groupName()).isEqualTo("named");
            assertThat(result.groupName()).isEqualTo("named");
            assertThat(group.groupId()).isNotBlank();
            assertThat(result.groupId()).isEqualTo(group.groupId());
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void closeAfterCompletionIsNoopAndCloseCancelsUnfinishedMembers() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), executor).build();
        CountDownLatch started = new CountDownLatch(1);
        try {
            TaskGroup completed = TaskGroup.submit(
                    global, TaskGroupDefinition.builder(groupOptions("done")).build());
            completed.completionFuture().get(2, TimeUnit.SECONDS);
            completed.close(); // must not disturb the recorded result
            assertThat(completed.completionFuture().get().outcome()).isEqualTo(TaskOutcome.SUCCESS);

            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("close-cancel"));
            definition.task(
                    new TaskKey<>("slow") {},
                    ParName.of("worker"),
                    () -> {
                        started.countDown();
                        Thread.sleep(10_000);
                        return 1;
                    },
                    memberOptions("slow"));
            TaskGroup unfinished = TaskGroup.submit(global, definition.build());
            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
            unfinished.close();
            TaskGroupResult result = unfinished.completionFuture().get(2, TimeUnit.SECONDS);
            assertThat(result.outcome()).isEqualTo(TaskOutcome.GROUP_CANCELED);
            assertThat(result.members().get("slow").outcome()).isEqualTo(TaskOutcome.GROUP_CANCELED);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void outerBatchCancellationPropagatesIntoGroupAsGroupCancellation() throws Exception {
        ExecutorService outer = Executors.newSingleThreadExecutor();
        ExecutorService inner = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("outer"), outer)
                .register(ParName.of("inner"), inner)
                .build();
        try {
            AtomicReference<CancellationToken> outerToken = new AtomicReference<>();
            AtomicReference<TaskGroup> publishedGroup = new AtomicReference<>();
            AtomicReference<String> observedReason = new AtomicReference<>();
            CountDownLatch groupBuilt = new CountDownLatch(1);
            TaskBatchResult<String> outerBatch = global.par(ParName.of("outer"))
                    .map(
                            Arrays.asList("x"),
                            ignored -> {
                                outerToken.set(TaskExecutionContext.current()
                                        .multiTaskContext()
                                        .cancellationToken());
                                TaskGroupDefinition.Builder definition =
                                        TaskGroupDefinition.builder(groupOptions("outer-cancel"));
                                definition.task(
                                        new TaskKey<>("slow") {},
                                        ParName.of("inner"),
                                        () -> {
                                            Thread.sleep(10_000);
                                            return 1;
                                        },
                                        memberOptions("slow"));
                                TaskGroup group = TaskGroup.submit(global, definition.build());
                                publishedGroup.set(group);
                                groupBuilt.countDown();
                                // Stay inside the outer task until the group converges, so the
                                // outer batch token is still RUNNING when the test cancels it.
                                while (true) {
                                    try {
                                        String reason = group.completionFuture()
                                                .get()
                                                .outcome()
                                                .name();
                                        // Record what the running task observed instead of
                                        // asserting on the outer future: the cancel cascade
                                        // hard-cancels bound futures right after the group
                                        // converges, so the task's return value races the
                                        // cancellation and is not a stable signal.
                                        observedReason.set(reason);
                                        return reason;
                                    } catch (InterruptedException interrupted) {
                                        // cancellation reached this task before the group settled;
                                        // keep waiting for the group's terminal reason
                                    } catch (java.util.concurrent.ExecutionException failure) {
                                        throw new RuntimeException(failure);
                                    }
                                }
                            },
                            groupOptions("outer"));
            assertThat(groupBuilt.await(2, TimeUnit.SECONDS)).isTrue();
            outerToken.get().cancel(true);
            TaskGroup group = publishedGroup.get();

            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);
            assertThat(result.outcome()).isEqualTo(TaskOutcome.GROUP_CANCELED);
            assertThat(result.members().get("slow").outcome()).isEqualTo(TaskOutcome.GROUP_CANCELED);
            org.awaitility.Awaitility.await()
                    .atMost(2, TimeUnit.SECONDS)
                    .until(() -> observedReason.get() != null
                            && outerBatch.results().get(0).isDone());
            assertThat(observedReason.get()).isEqualTo("GROUP_CANCELED");
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
