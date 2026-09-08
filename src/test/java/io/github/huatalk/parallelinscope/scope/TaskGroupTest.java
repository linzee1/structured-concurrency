package io.github.huatalk.parallelinscope.scope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.alibaba.ttl.TransmittableThreadLocal;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.github.huatalk.parallelinscope.cancel.CancellationToken;
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
                .register("first", first)
                .register("second", second)
                .build();
        try {
            AtomicInteger executions = new AtomicInteger();
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("page"));
            TaskRef<String> text = spec.task(
                    new TaskRef<>("text") {},
                    "first",
                    () -> "value-" + executions.incrementAndGet(),
                    memberOptions("text"));
            TaskRef<Integer> number = spec.task(
                    new TaskRef<>("number") {},
                    "second",
                    () -> 40 + executions.incrementAndGet(),
                    memberOptions("number"));

            TaskGroup group = TaskGroup.submit(global, spec.build());
            assertThat(group.future(text).get(2, TimeUnit.SECONDS)).startsWith("value-");
            assertThat(group.future(number).get(2, TimeUnit.SECONDS)).isBetween(41, 42);
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(executions).hasValue(2);
            assertThat(result.outcome()).isEqualTo(TaskOutcome.SUCCESS);
            assertThat(result.members().keySet()).containsExactly("text", "number");
            assertThat(group.members().keySet()).containsExactly("text", "number");
            assertThat(group.findMember("text")).contains(group.future(text));
            assertThatThrownBy(() -> group.future(new TaskRef<>("missing") {}))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            global.close();
            first.shutdownNow();
            second.shutdownNow();
        }
    }

    @Test
    void specConfigurationRunsNoTaskAndTtlSnapshotIsTakenAtSubmit() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        TransmittableThreadLocal<String> ttl = new TransmittableThreadLocal<>();
        try {
            AtomicInteger calls = new AtomicInteger();
            ttl.set("configure");
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("ttl"));
            TaskRef<String> member = spec.task(
                    new TaskRef<>("member") {},
                    "worker",
                    () -> {
                        calls.incrementAndGet();
                        return ttl.get();
                    },
                    memberOptions("member"));

            assertThat(calls).hasValue(0);
            ttl.set("submit");
            TaskGroup group = TaskGroup.submit(global, spec.build());

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
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        CountDownLatch running = new CountDownLatch(1);
        try {
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("fail-fast"));
            spec.task(
                    new TaskRef<>("slow") {},
                    "worker",
                    () -> {
                        running.countDown();
                        Thread.sleep(10_000);
                        return 1;
                    },
                    memberOptions("slow"));
            spec.task(
                    new TaskRef<>("failure") {},
                    "worker",
                    () -> {
                        running.await(2, TimeUnit.SECONDS);
                        throw new IllegalStateException("boom");
                    },
                    memberOptions("failure"));

            TaskGroupResult result =
                    TaskGroup.submit(global, spec.build()).completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(result.outcome()).isEqualTo(TaskOutcome.USER_FAILURE);
            assertThat(result.failedMemberName()).isEqualTo("failure");
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
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        try {
            TaskGroupSpec.Builder groupDeadline = TaskGroupSpec.builder(MultiTaskOptions.of("group-timeout")
                    .timeout(Duration.ofMillis(30))
                    .build());
            groupDeadline.task(
                    new TaskRef<>("slow") {},
                    "worker",
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

            TaskGroupSpec.Builder memberDeadline = TaskGroupSpec.builder(MultiTaskOptions.of("member-timeout")
                    .timeout(Duration.ofSeconds(2))
                    .build());
            memberDeadline.task(
                    new TaskRef<>("slow") {},
                    "worker",
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
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        CountDownLatch release = new CountDownLatch(1);
        try {
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("member-cancel"));
            TaskRef<Integer> canceled = spec.task(
                    new TaskRef<>("canceled") {},
                    "worker",
                    () -> {
                        release.await();
                        return 1;
                    },
                    memberOptions("canceled"));
            spec.task(
                    new TaskRef<>("sibling") {},
                    "worker",
                    () -> {
                        release.await(10, TimeUnit.SECONDS);
                        return 2;
                    },
                    memberOptions("sibling"));
            TaskGroup group = TaskGroup.submit(global, spec.build());
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
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        try {
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(MultiTaskOptions.of("member-timeout")
                    .timeout(Duration.ofSeconds(2))
                    .build());
            spec.task(
                    new TaskRef<>("slow") {},
                    "worker",
                    () -> {
                        Thread.sleep(10_000);
                        return 1;
                    },
                    MultiTaskOptions.of("slow").timeout(Duration.ofMillis(50)).build());
            spec.task(
                    new TaskRef<>("sibling") {},
                    "worker",
                    () -> {
                        Thread.sleep(10_000);
                        return 2;
                    },
                    memberOptions("sibling"));

            TaskGroupResult result =
                    TaskGroup.submit(global, spec.build()).completionFuture().get(2, TimeUnit.SECONDS);

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
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        try {
            // "tight" binds its own stricter deadline; "shared" resolves to exactly the group
            // deadline and skips the member bind. Both paths must still attribute TIMEOUT.
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(MultiTaskOptions.of("mixed-deadlines")
                    .timeout(Duration.ofSeconds(2))
                    .build());
            spec.task(
                    new TaskRef<>("tight") {},
                    "worker",
                    () -> {
                        Thread.sleep(10_000);
                        return 1;
                    },
                    MultiTaskOptions.of("tight").timeout(Duration.ofMillis(50)).build());
            spec.task(
                    new TaskRef<>("shared") {},
                    "worker",
                    () -> {
                        Thread.sleep(10_000);
                        return 2;
                    },
                    MultiTaskOptions.of("shared").timeout(Duration.ofSeconds(2)).build());

            TaskGroupResult result =
                    TaskGroup.submit(global, spec.build()).completionFuture().get(2, TimeUnit.SECONDS);

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
                .register("outer", outer)
                .register("inner", inner)
                .build();
        AtomicReference<CancellationToken> memberToken = new AtomicReference<>();
        AtomicReference<CancellationToken> nestedToken = new AtomicReference<>();
        AtomicReference<ListenableFuture<?>> nestedFuture = new AtomicReference<>();
        CountDownLatch nestedRunning = new CountDownLatch(1);
        try {
            // The member inherits the group deadline, so its token is never bound; propagation to
            // the nested batch rides the token constructor listener chain alone.
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(MultiTaskOptions.of("nested-propagation")
                    .timeout(Duration.ofMillis(50))
                    .build());
            spec.task(
                    new TaskRef<>("member") {},
                    "outer",
                    () -> {
                        memberToken.set(
                                TaskExecutionContext.current().batchContext().cancellationToken());
                        AsyncBatchResult<Integer> nested = global.par("inner")
                                .map(
                                        Arrays.asList(1),
                                        ignored -> {
                                            nestedToken.set(TaskExecutionContext.current()
                                                    .batchContext()
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

            TaskGroup group = TaskGroup.submit(global, spec.build());
            assertThat(nestedRunning.await(2, TimeUnit.SECONDS)).isTrue();
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(result.outcome()).isEqualTo(TaskOutcome.TIMEOUT);
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
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        CountDownLatch started = new CountDownLatch(2);
        try {
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("cancel"));
            for (String name : Arrays.asList("one", "two")) {
                spec.task(
                        new TaskRef<>(name) {},
                        "worker",
                        () -> {
                            started.countDown();
                            Thread.sleep(10_000);
                            return name;
                        },
                        memberOptions(name));
            }
            TaskGroup group = TaskGroup.submit(global, spec.build());
            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();

            group.cancel();
            group.cancel();
            TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(result.outcome()).isEqualTo(TaskOutcome.GROUP_CANCELED);
            assertThat(result.members().values())
                    .extracting(TaskGroupMemberResult::outcome)
                    .containsOnly(TaskOutcome.GROUP_CANCELED);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void inlineMembersRunDuringSubmitOverACompleteFrozenRegistry() throws Exception {
        ExecutorService direct = MoreExecutors.newDirectExecutorService();
        GlobalPar global = GlobalPar.builder().register("direct", direct).build();
        try {
            Thread submitThread = Thread.currentThread();
            AtomicReference<Thread> firstThread = new AtomicReference<>();
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("inline"));
            TaskRef<Integer> first = spec.task(
                    new TaskRef<>("first") {},
                    "direct",
                    () -> {
                        firstThread.set(Thread.currentThread());
                        return 1;
                    },
                    memberOptions("first"));
            TaskRef<Integer> second = spec.task(new TaskRef<>("second") {}, "direct", () -> 2, memberOptions("second"));

            TaskGroup group = TaskGroup.submit(global, spec.build());

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
                .register("reject", rejecting)
                .register("normal", normal)
                .build();
        AtomicInteger calls = new AtomicInteger();
        try {
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("rejection"));
            spec.task(
                    new TaskRef<>("rejected") {},
                    "reject",
                    () -> 1,
                    MultiTaskOptions.of("rejected")
                            .taskType(TaskType.IO_BOUND)
                            .timeout(Duration.ofSeconds(30))
                            .build());
            spec.task(new TaskRef<>("later") {}, "normal", calls::incrementAndGet, memberOptions("later"));

            TaskGroupResult result =
                    TaskGroup.submit(global, spec.build()).completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(result.outcome()).isEqualTo(TaskOutcome.SUBMISSION_FAILURE);
            assertThat(result.members().get("rejected").outcome()).isEqualTo(TaskOutcome.SUBMISSION_FAILURE);
            assertThat(result.members().get("later").outcome()).isEqualTo(TaskOutcome.FAIL_FAST);
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
            MultiTaskOptions options = MultiTaskOptions.of("listener")
                    .listener(event -> {
                        observed.set(event.result());
                        current.set(TaskExecutionContext.current());
                        throw new IllegalStateException("ignored");
                    })
                    .timeout(Duration.ofSeconds(30))
                    .build();
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(options);
            spec.task(new TaskRef<>("one") {}, "direct", () -> 1, memberOptions("one"));

            TaskGroupResult result =
                    TaskGroup.submit(global, spec.build()).completionFuture().get(2, TimeUnit.SECONDS);

            assertThat(observed.get()).isSameAs(result);
            assertThat(current.get()).isNull();
        } finally {
            global.close();
            direct.shutdownNow();
        }
    }

    @Test
    void specValidatesMembersAndIsReusableAcrossSubmits() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        try {
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("spec"));
            spec.task(new TaskRef<>("one") {}, "worker", () -> 1, memberOptions("one"));
            assertThatThrownBy(() -> spec.task(new TaskRef<>("one") {}, "worker", () -> 2, memberOptions("two")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> spec.task(new TaskRef<>(" ") {}, "worker", () -> 2, memberOptions("blank")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> spec.task(null, "worker", () -> 2, memberOptions("null")))
                    .isInstanceOf(NullPointerException.class);

            TaskGroupSpec.Builder unknownExecutor = TaskGroupSpec.builder(groupOptions("unknown"));
            unknownExecutor.task(new TaskRef<>("member") {}, "missing", () -> 1, memberOptions("member"));
            assertThatThrownBy(() -> TaskGroup.submit(global, unknownExecutor.build()))
                    .isInstanceOf(IllegalArgumentException.class);

            TaskGroupSpec reusable = spec.build();
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
    void refCapturesTheParameterizedResultType() {
        TaskRef<java.util.List<String>> ref = new TaskRef<java.util.List<String>>("orders") {};
        assertThat(ref.memberName()).isEqualTo("orders");
        assertThat(ref.resultType().getType().getTypeName()).isEqualTo("java.util.List<java.lang.String>");
        assertThat(ref.resultType().getRawType()).isEqualTo(java.util.List.class);
        assertThatThrownBy(() -> new TaskRef<Object>(" ") {}).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TaskRef<Object>(null) {}).isInstanceOf(NullPointerException.class);
    }

    @Test
    void futureRejectsRefWhoseRawTypeDoesNotCoverTheRegisteredType() throws Exception {
        ExecutorService direct = MoreExecutors.newDirectExecutorService();
        GlobalPar global = GlobalPar.builder().register("direct", direct).build();
        try {
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("typed"));
            TaskRef<Integer> member =
                    spec.task(new TaskRef<Integer>("member") {}, "direct", () -> 1, memberOptions("member"));

            TaskGroup group = TaskGroup.submit(global, spec.build());

            assertThat(group.future(member).get(2, TimeUnit.SECONDS)).isEqualTo(1);
            // A supertype ref is sound: the member result is assignable to it.
            assertThat(group.future(new TaskRef<Number>("member") {}).get(2, TimeUnit.SECONDS))
                    .isEqualTo(1);
            assertThatThrownBy(() -> group.future(new TaskRef<String>("member") {}))
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
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        try {
            TaskGroup empty = TaskGroup.submit(
                    global, TaskGroupSpec.builder(groupOptions("empty")).build());
            assertThat(empty.completionFuture().get().outcome()).isEqualTo(TaskOutcome.SUCCESS);

            global.close();
            assertThatThrownBy(() -> TaskGroup.submit(
                            global,
                            TaskGroupSpec.builder(groupOptions("closed")).build()))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void memberWithInheritedTimeoutResolvesToTheGroupDeadline() throws Exception {
        ExecutorService direct = MoreExecutors.newDirectExecutorService();
        GlobalPar global = GlobalPar.builder().register("direct", direct).build();
        try {
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("inherit-member"));
            TaskRef<Long> member = spec.task(
                    new TaskRef<>("member") {},
                    "direct",
                    () -> TaskExecutionContext.current().batchContext().deadlineNanos(),
                    MultiTaskOptions.of("member").inheritTimeout().build());

            TaskGroup group = TaskGroup.submit(global, spec.build());
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
        GlobalPar global = GlobalPar.builder().register("direct", direct).build();
        try {
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("tight-member"));
            TaskRef<Long> member = spec.task(
                    new TaskRef<>("member") {},
                    "direct",
                    () -> TaskExecutionContext.current().batchContext().deadlineNanos(),
                    MultiTaskOptions.of("member")
                            .timeout(Duration.ofMillis(100))
                            .build());

            TaskGroup group = TaskGroup.submit(global, spec.build());
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
                .register("outer", outer)
                .register("inner", inner)
                .build();
        try {
            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("nested-batch"));
            TaskRef<long[]> member = spec.task(
                    new TaskRef<>("member") {},
                    "outer",
                    () -> {
                        long memberDeadline =
                                TaskExecutionContext.current().batchContext().deadlineNanos();
                        AsyncBatchResult<Long> nested = global.par("inner")
                                .map(
                                        Arrays.asList(1),
                                        ignored -> TaskExecutionContext.current()
                                                .batchContext()
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

            TaskGroup group = TaskGroup.submit(global, spec.build());
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
                .register("outer", outer)
                .register("inner", inner)
                .build();
        try {
            assertThatThrownBy(() -> TaskGroup.submit(
                            global,
                            TaskGroupSpec.builder(MultiTaskOptions.of("orphan")
                                            .inheritTimeout()
                                            .build())
                                    .build()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("no enclosing deadline to inherit");

            AsyncBatchResult<Long> batch = global.par("outer")
                    .map(
                            Arrays.asList(1),
                            ignored -> {
                                long outerDeadline = TaskExecutionContext.current()
                                        .batchContext()
                                        .deadlineNanos();
                                TaskGroupSpec.Builder spec = TaskGroupSpec.builder(MultiTaskOptions.of("nested-group")
                                        .inheritTimeout()
                                        .build());
                                spec.task(
                                        new TaskRef<>("child") {},
                                        "inner",
                                        () -> 1,
                                        MultiTaskOptions.of("child")
                                                .inheritTimeout()
                                                .build());
                                try {
                                    TaskGroupResult result = TaskGroup.submit(global, spec.build())
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
                .register("outer", outer)
                .register("inner", inner)
                .build();
        try {
            AsyncBatchResult<MultiTaskContext> result = global.par("outer")
                    .map(
                            Arrays.asList(1),
                            ignored -> {
                                MultiTaskContext expectedParent =
                                        TaskExecutionContext.current().batchContext();
                                TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("nested"));
                                TaskRef<MultiTaskContext> child = spec.task(
                                        new TaskRef<>("child") {},
                                        "inner",
                                        () -> TaskExecutionContext.current()
                                                .batchContext()
                                                .parent(),
                                        memberOptions("child"));
                                TaskGroup group = TaskGroup.submit(global, spec.build());
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
                .register("outer", outer)
                .register("inner", inner)
                .build();
        AtomicReference<TaskGroup> nestedGroup = new AtomicReference<>();
        try {
            AsyncBatchResult<Object> result = global.par("outer")
                    .map(
                            Arrays.asList(1),
                            ignored -> {
                                TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("nested"));
                                spec.task(
                                        new TaskRef<>("child") {},
                                        "inner",
                                        () -> {
                                            Thread.sleep(10_000);
                                            return 1;
                                        },
                                        memberOptions("child"));
                                nestedGroup.set(TaskGroup.submit(global, spec.build()));
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
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        try {
            TaskGroup group = TaskGroup.submit(
                    global, TaskGroupSpec.builder(groupOptions("named")).build());
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
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        CountDownLatch started = new CountDownLatch(1);
        try {
            TaskGroup completed = TaskGroup.submit(
                    global, TaskGroupSpec.builder(groupOptions("done")).build());
            completed.completionFuture().get(2, TimeUnit.SECONDS);
            completed.close(); // must not disturb the recorded result
            assertThat(completed.completionFuture().get().outcome()).isEqualTo(TaskOutcome.SUCCESS);

            TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("close-cancel"));
            spec.task(
                    new TaskRef<>("slow") {},
                    "worker",
                    () -> {
                        started.countDown();
                        Thread.sleep(10_000);
                        return 1;
                    },
                    memberOptions("slow"));
            TaskGroup unfinished = TaskGroup.submit(global, spec.build());
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
                .register("outer", outer)
                .register("inner", inner)
                .build();
        try {
            AtomicReference<CancellationToken> outerToken = new AtomicReference<>();
            AtomicReference<TaskGroup> publishedGroup = new AtomicReference<>();
            AtomicReference<String> observedReason = new AtomicReference<>();
            CountDownLatch groupBuilt = new CountDownLatch(1);
            AsyncBatchResult<String> outerBatch = global.par("outer")
                    .map(
                            Arrays.asList("x"),
                            ignored -> {
                                outerToken.set(TaskExecutionContext.current()
                                        .batchContext()
                                        .cancellationToken());
                                TaskGroupSpec.Builder spec = TaskGroupSpec.builder(groupOptions("outer-cancel"));
                                spec.task(
                                        new TaskRef<>("slow") {},
                                        "inner",
                                        () -> {
                                            Thread.sleep(10_000);
                                            return 1;
                                        },
                                        memberOptions("slow"));
                                TaskGroup group = TaskGroup.submit(global, spec.build());
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
