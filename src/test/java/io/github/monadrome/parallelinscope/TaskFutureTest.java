package io.github.monadrome.parallelinscope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Contract tests for the {@link TaskFuture} view the library delivers: delivery completeness,
 * outcome attribution, race stability, placeholder bridging, delegation transparency, and deadline
 * reporting.
 */
class TaskFutureTest {

    private static final Duration SCOPE_TIMEOUT = Duration.ofSeconds(30);

    // ==================== delivery completeness ====================

    @Test
    void batchDeliversEveryElementAsATaskFutureInsideAndOutsideTheWindow() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        CountDownLatch block = new CountDownLatch(1);
        try {
            TaskBatchResult<String> batch = global.par(ParName.of("worker"))
                    .map(
                            Arrays.asList("first", "second", "third"),
                            item -> hold(block, item),
                            BatchOptions.timeout("orders", SCOPE_TIMEOUT).parallelism(1));

            // Element 0 occupies the only slot; elements 1 and 2 are still window placeholders, yet
            // already answer with their task identity.
            assertThat(batch.results()).allMatch(future -> future instanceof TaskFuture);
            TaskFuture<String> pending = batch.results().get(2);
            assertThat(pending.taskName()).isEqualTo("orders");
            assertThat(pending.outcome()).isEqualTo(TaskOutcome.RUNNING);
            assertThat(pending.deadlineNanos()).isNotEqualTo(Long.MAX_VALUE);

            block.countDown();
            assertThat(batch.results())
                    .extracting(future -> future.get(2, TimeUnit.SECONDS))
                    .containsExactly("first", "second", "third");
        } finally {
            block.countDown();
            global.close();
            pool.shutdownNow();
        }
    }

    @Test
    void batchRejectedAtSubmitDeliversIdentifiedTaskFutures() {
        ExecutorService rejected = Executors.newSingleThreadExecutor();
        rejected.shutdownNow();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), rejected).build();
        try {
            TaskBatchResult<String> batch = global.par(ParName.of("worker"))
                    .map(
                            Arrays.asList("first", "second"),
                            item -> item,
                            BatchOptions.timeout("rejected", SCOPE_TIMEOUT)
                                    .parallelism(2)
                                    .taskType(TaskType.IO_BOUND));

            assertThat(batch.results()).allMatch(future -> future instanceof TaskFuture);
            for (TaskFuture<String> element : batch.results()) {
                assertThat(element.taskName()).isEqualTo("rejected");
                assertThat(element.outcome()).isEqualTo(TaskOutcome.SUBMISSION_FAILURE);
                assertThat(element.failure()).isInstanceOf(SubmissionException.class);
            }
            assertThat(batch.report().stateCounts())
                    .containsOnlyKeys(TaskOutcome.SUBMISSION_FAILURE)
                    .containsEntry(TaskOutcome.SUBMISSION_FAILURE, 2);
        } finally {
            global.close();
        }
    }

    @Test
    void groupDeliversMembersCombineAndCompletionAsTaskFutures() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("page"));
            TaskKey<String> user =
                    definition.task(new TaskKey<>("user") {}, ParName.of("worker"), () -> "alice", memberOptions());
            TaskKey<Integer> orders =
                    definition.task(new TaskKey<>("orders") {}, ParName.of("worker"), () -> 7, memberOptions());
            TaskKey<String> page = new TaskKey<>("assemble") {};
            TaskGroup group = TaskGroup.submit(
                    global,
                    definition.buildWithCombiner(
                            page, ParName.of("worker"), values -> values.value(user) + ":" + values.value(orders)));

            assertThat(group.future(user)).isInstanceOf(TaskFuture.class);
            assertThat(group.future(user).taskName()).isEqualTo("user");
            assertThat(group.future(page)).isInstanceOf(TaskFuture.class);
            assertThat(group.findMember("user")).contains(group.future(user));
            assertThat(group.members().values()).allMatch(future -> future instanceof TaskFuture);
            assertThat(group.completionFuture()).isInstanceOf(TaskFuture.class);
            assertThat(group.completionFuture().get(2, TimeUnit.SECONDS).outcome())
                    .isEqualTo(TaskOutcome.SUCCESS);
        } finally {
            global.close();
            pool.shutdownNow();
        }
    }

    @Test
    void submitCancellerStaysAPlainFuture() {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        CountDownLatch block = new CountDownLatch(1);
        try {
            TaskBatchResult<String> batch = global.par(ParName.of("worker"))
                    .map(
                            Arrays.asList("first", "second"),
                            item -> hold(block, item),
                            BatchOptions.timeout("orders", SCOPE_TIMEOUT).parallelism(1));

            assertThat(batch.submitCanceller()).isNotInstanceOf(TaskFuture.class);
            assertThat(batch.results().get(1)).isInstanceOf(TaskFuture.class);
        } finally {
            block.countDown();
            global.close();
            pool.shutdownNow();
        }
    }

    // ==================== outcome attribution ====================

    @Test
    void succeededTaskReadsSuccessWithoutAFailure() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        try {
            TaskBatchResult<String> batch = global.par(ParName.of("worker"))
                    .map(Collections.singletonList("x"), item -> "done", BatchOptions.timeout("orders", SCOPE_TIMEOUT));

            TaskFuture<String> element = batch.results().get(0);
            assertThat(element.get(2, TimeUnit.SECONDS)).isEqualTo("done");
            assertThat(element.outcome()).isEqualTo(TaskOutcome.SUCCESS);
            assertThat(element.failure()).isNull();
        } finally {
            global.close();
            pool.shutdownNow();
        }
    }

    @Test
    void failedTaskReadsUserFailureAndExposesTheCause() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        IllegalStateException boom = new IllegalStateException("boom");
        try {
            TaskBatchResult<String> batch = global.par(ParName.of("worker"))
                    .map(
                            Collections.singletonList("x"),
                            item -> {
                                throw boom;
                            },
                            BatchOptions.timeout("orders", SCOPE_TIMEOUT));

            TaskFuture<String> element = batch.results().get(0);
            assertThat(element.outcome()).isEqualTo(TaskOutcome.USER_FAILURE);
            assertThat(element.failure()).isSameAs(boom);
            assertThatThrownBy(element::get)
                    .isInstanceOf(ExecutionException.class)
                    .hasCause(boom);
        } finally {
            global.close();
            pool.shutdownNow();
        }
    }

    @Test
    void aMemberCancelledDirectlyKeepsItsInitiatorAttributionInTheSnapshot() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch block = new CountDownLatch(1);
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("page"));
            TaskKey<String> slow = definition.task(
                    new TaskKey<>("slow") {},
                    ParName.of("worker"),
                    () -> startedThenHold(started, block, "never"),
                    memberOptions());
            TaskGroup group = TaskGroup.submit(global, definition.build());
            TaskFuture<String> member = group.future(slow);
            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();

            assertThat(member.cancel(true)).isTrue();
            TaskGroupResult snapshot = group.completionFuture().get(2, TimeUnit.SECONDS);

            // The member snapshot records the reason before the group cancels itself for the other
            // members, so it keeps the initiating member distinct from the fall-out.
            assertThat(snapshot.members().get("slow").outcome()).isEqualTo(TaskOutcome.MEMBER_CANCELED);
            // The group-level outcome is the post-hoc view: the group token was canceled to protect
            // the remaining members, so the group reads GROUP_CANCELED.
            assertThat(snapshot.outcome()).isEqualTo(TaskOutcome.GROUP_CANCELED);
            // A future reads the token chain only. The member token is wired to the group token, so
            // once the group's own cancel lands it reports the group's cancellation too — the
            // transient MEMBER_CANCELED a caller may see right after its own cancel() is not stable.
            await().atMost(2, TimeUnit.SECONDS).until(() -> member.outcome() == TaskOutcome.GROUP_CANCELED);
        } finally {
            block.countDown();
            global.close();
            pool.shutdownNow();
        }
    }

    @Test
    void groupCancellationReadsGroupCanceledOnEveryMember() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch block = new CountDownLatch(1);
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("page"));
            definition.task(
                    new TaskKey<>("first") {},
                    ParName.of("worker"),
                    () -> startedThenHold(started, block, "never"),
                    memberOptions());
            definition.task(
                    new TaskKey<>("second") {},
                    ParName.of("worker"),
                    () -> startedThenHold(started, block, "never"),
                    memberOptions());
            TaskGroup group = TaskGroup.submit(global, definition.build());

            group.cancel();
            assertThat(group.completionFuture().get(2, TimeUnit.SECONDS).outcome())
                    .isEqualTo(TaskOutcome.GROUP_CANCELED);

            assertThat(group.findMember("first").get().outcome()).isEqualTo(TaskOutcome.GROUP_CANCELED);
            assertThat(group.findMember("second").get().outcome()).isEqualTo(TaskOutcome.GROUP_CANCELED);
        } finally {
            block.countDown();
            global.close();
            pool.shutdownNow();
        }
    }

    @Test
    void siblingFailureReadsFailFastOnTheCancelledElement() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        CountDownLatch block = new CountDownLatch(1);
        try {
            TaskBatchResult<String> batch = global.par(ParName.of("worker"))
                    .map(
                            Arrays.asList("boom", "victim"),
                            item -> "boom".equals(item) ? boom() : hold(block, item),
                            BatchOptions.timeout("orders", SCOPE_TIMEOUT).parallelism(2));

            await().atMost(2, TimeUnit.SECONDS)
                    .until(() -> batch.results().get(1).isDone());
            assertThat(batch.results().get(0).outcome()).isEqualTo(TaskOutcome.USER_FAILURE);
            assertThat(batch.results().get(1).outcome()).isEqualTo(TaskOutcome.FAIL_FAST);
        } finally {
            block.countDown();
            global.close();
            pool.shutdownNow();
        }
    }

    @Test
    void expiredDeadlineReadsTimeoutAndClampsTheRemainingBudget() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        CountDownLatch block = new CountDownLatch(1);
        try {
            TaskBatchResult<String> batch = global.par(ParName.of("worker"))
                    .map(
                            Collections.singletonList("x"),
                            item -> hold(block, item),
                            BatchOptions.timeout("orders", Duration.ofMillis(50)));

            await().atMost(2, TimeUnit.SECONDS)
                    .until(() -> batch.results().get(0).isDone());
            TaskFuture<String> element = batch.results().get(0);
            assertThat(element.outcome()).isEqualTo(TaskOutcome.TIMEOUT);
            assertThat(element.remaining()).isEqualTo(Duration.ZERO);
            assertThat(element.failure()).isNull();
        } finally {
            block.countDown();
            global.close();
            pool.shutdownNow();
        }
    }

    @Test
    void nestedScopeCancellationReadsTheOriginatingCause() throws Exception {
        ExecutorService outerPool = Executors.newSingleThreadExecutor();
        ExecutorService innerPool = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("outer"), outerPool)
                .register(ParName.of("inner"), innerPool)
                .build();
        AtomicReference<CancellationToken> outerToken = new AtomicReference<>();
        AtomicReference<TaskFuture<String>> innerMember = new AtomicReference<>();
        AtomicReference<TaskGroup> innerGroup = new AtomicReference<>();
        CountDownLatch memberStarted = new CountDownLatch(1);
        CountDownLatch block = new CountDownLatch(1);
        try {
            global.par(ParName.of("outer"))
                    .map(
                            Collections.singletonList("x"),
                            ignored -> {
                                outerToken.set(TaskExecutionContext.current()
                                        .multiTaskContext()
                                        .cancellationToken());
                                TaskGroupDefinition.Builder inner = TaskGroupDefinition.builder(groupOptions("inner"));
                                TaskKey<String> slow = inner.task(
                                        new TaskKey<>("slow") {},
                                        ParName.of("inner"),
                                        () -> startedThenHold(memberStarted, block, "never"),
                                        memberOptions());
                                TaskGroup group = TaskGroup.submit(global, inner.build());
                                innerGroup.set(group);
                                innerMember.set(group.future(slow));
                                return awaitQuietly(group.completionFuture()) == null ? "cancelled" : "done";
                            },
                            BatchOptions.timeout("outer", SCOPE_TIMEOUT));

            assertThat(memberStarted.await(2, TimeUnit.SECONDS)).isTrue();
            outerToken.get().cancel(true);

            await().atMost(2, TimeUnit.SECONDS)
                    .until(() -> innerGroup.get().completionFuture().isDone());
            // The member token only learned about the cancellation by propagation, so the reported
            // cause is the originating state at the root of the chain, not the propagation link.
            assertThat(innerMember.get().outcome()).isEqualTo(TaskOutcome.GROUP_CANCELED);
        } finally {
            block.countDown();
            global.close();
            outerPool.shutdownNow();
            innerPool.shutdownNow();
        }
    }

    // ==================== races and stability ====================

    @Test
    void aCancellationSignalRacingTheCascadeIsAttributedToTheDeadline() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        CountDownLatch block = new CountDownLatch(1);
        try {
            TaskBatchResult<String> batch = global.par(ParName.of("worker"))
                    .map(
                            Collections.singletonList("x"),
                            item -> {
                                try {
                                    block.await();
                                } catch (InterruptedException interrupted) {
                                    // Report the observed cancellation as a failure instead of
                                    // letting the interrupt surface: this checkpoint races the
                                    // cascade cancel on the element future, and must be attributed
                                    // to the deadline either way.
                                    Thread.currentThread().interrupt();
                                    throw new CancellationException("checkpoint observed cancellation");
                                }
                                return item;
                            },
                            BatchOptions.timeout("orders", Duration.ofMillis(50)));

            await().atMost(2, TimeUnit.SECONDS)
                    .until(() -> batch.results().get(0).isDone());
            assertThat(batch.results().get(0).outcome()).isEqualTo(TaskOutcome.TIMEOUT);
        } finally {
            block.countDown();
            global.close();
            pool.shutdownNow();
        }
    }

    @Test
    void recordedSuccessSurvivesALaterDeadlineCommit() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        CountDownLatch block = new CountDownLatch(1);
        try {
            TaskBatchResult<String> batch = global.par(ParName.of("worker"))
                    .map(
                            Arrays.asList("fast", "slow"),
                            item -> "fast".equals(item) ? item : hold(block, item),
                            BatchOptions.timeout("orders", Duration.ofMillis(60))
                                    .parallelism(2));

            assertThat(batch.results().get(0).get(2, TimeUnit.SECONDS)).isEqualTo("fast");
            await().atMost(2, TimeUnit.SECONDS)
                    .until(() -> batch.results().get(1).isDone());
            assertThat(batch.results().get(1).outcome()).isEqualTo(TaskOutcome.TIMEOUT);
            assertThat(batch.results().get(0).outcome()).isEqualTo(TaskOutcome.SUCCESS);
        } finally {
            block.countDown();
            global.close();
            pool.shutdownNow();
        }
    }

    @Test
    void terminalOutcomeIsStableAcrossRepeatedReads() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        CountDownLatch block = new CountDownLatch(1);
        try {
            TaskBatchResult<String> batch = global.par(ParName.of("worker"))
                    .map(
                            Collections.singletonList("x"),
                            item -> hold(block, item),
                            BatchOptions.timeout("orders", Duration.ofMillis(50)));
            TaskFuture<String> element = batch.results().get(0);

            AtomicReference<TaskOutcome> terminal = new AtomicReference<>();
            AtomicBoolean regressed = new AtomicBoolean();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (System.nanoTime() < deadline) {
                TaskOutcome observed = element.outcome();
                if (observed == TaskOutcome.RUNNING) {
                    regressed.set(terminal.get() != null);
                } else {
                    terminal.set(observed);
                    if (element.isDone()) break;
                }
            }

            assertThat(regressed).isFalse();
            assertThat(element.isDone()).isTrue();
            assertThat(element.outcome()).isEqualTo(terminal.get()).isEqualTo(TaskOutcome.TIMEOUT);
        } finally {
            block.countDown();
            global.close();
            pool.shutdownNow();
        }
    }

    // ==================== placeholder bridging ====================

    @Test
    void placeholderDeliversListenersRegisteredBeforeTheBind() {
        Task<String> placeholder = Task.placeholder("orders", new CancellationToken());
        SettableFuture<String> real = SettableFuture.create();
        AtomicReference<String> observed = new AtomicReference<>();

        placeholder.addListener(() -> observed.set(placeholder.outcome().name()), Runnable::run);
        assertThat(placeholder.toString()).contains("placeholder=pending");

        placeholder.bind(real);
        real.set("done");

        assertThat(observed.get()).isEqualTo("SUCCESS");
        assertThat(placeholder.toString()).contains("placeholder=handed-off").doesNotContain("pending");
    }

    @Test
    void cancellingAPlaceholderBeforeTheBindCancelsTheRealFuture() {
        Task<String> placeholder = Task.placeholder("orders", new CancellationToken());
        SettableFuture<String> real = SettableFuture.create();

        assertThat(placeholder.cancel(true)).isTrue();
        placeholder.bind(real);

        assertThat(real.isCancelled()).isTrue();
        assertThat(placeholder.outcome()).isEqualTo(TaskOutcome.MEMBER_CANCELED);
    }

    @Test
    void abandonedPlaceholderAttributesTheAbandonment() {
        Task<String> rejected = Task.placeholder("orders", new CancellationToken());
        Task<String> cancelled = Task.placeholder("orders", new CancellationToken());

        rejected.abandon(new InterruptedException("submitter interrupted"));
        cancelled.abandon(null);

        assertThat(rejected.outcome()).isEqualTo(TaskOutcome.SUBMISSION_FAILURE);
        assertThat(rejected.failure())
                .isInstanceOf(SubmissionException.class)
                .hasCauseInstanceOf(InterruptedException.class);
        assertThat(cancelled.isCancelled()).isTrue();
        assertThat(cancelled.outcome()).isEqualTo(TaskOutcome.MEMBER_CANCELED);
    }

    // ==================== delegation transparency ====================

    @Test
    void cancelInterruptsTheWorkerThroughTheTaskView() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        try {
            TaskBatchResult<String> batch = global.par(ParName.of("worker"))
                    .map(
                            Collections.singletonList("x"),
                            item -> {
                                started.countDown();
                                try {
                                    Thread.sleep(TimeUnit.SECONDS.toMillis(10));
                                } catch (InterruptedException observed) {
                                    interrupted.countDown();
                                    Thread.currentThread().interrupt();
                                    throw new IllegalStateException(observed);
                                }
                                return item;
                            },
                            BatchOptions.timeout("orders", SCOPE_TIMEOUT));

            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(batch.results().get(0).cancel(true)).isTrue();

            assertThat(interrupted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(batch.results().get(0).isCancelled()).isTrue();
        } finally {
            global.close();
            pool.shutdownNow();
        }
    }

    @Test
    void listenersRunOnTheSuppliedExecutor() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "task-worker"));
        ExecutorService listeners =
                Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "listener-thread"));
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        try {
            TaskBatchResult<String> batch = global.par(ParName.of("worker"))
                    .map(Collections.singletonList("x"), item -> "done", BatchOptions.timeout("orders", SCOPE_TIMEOUT));
            TaskFuture<String> element = batch.results().get(0);
            AtomicReference<String> listenerThread = new AtomicReference<>();

            element.addListener(() -> listenerThread.set(Thread.currentThread().getName()), listeners);

            await().atMost(2, TimeUnit.SECONDS).until(() -> listenerThread.get() != null);
            assertThat(listenerThread.get()).isEqualTo("listener-thread");
        } finally {
            global.close();
            pool.shutdownNow();
            listeners.shutdownNow();
        }
    }

    @Test
    void theTaskViewIsStillAPlainListenableFuture() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        try {
            TaskBatchResult<Integer> batch = global.par(ParName.of("worker"))
                    .map(Arrays.asList(1, 2, 3), item -> item + 1, BatchOptions.timeout("orders", SCOPE_TIMEOUT));
            List<ListenableFuture<Integer>> asPlainFutures = new ArrayList<>(batch.results());

            assertThat(Futures.allAsList(asPlainFutures).get(2, TimeUnit.SECONDS))
                    .containsExactly(2, 3, 4);
        } finally {
            global.close();
            pool.shutdownNow();
        }
    }

    // ==================== deadline budget ====================

    @Test
    void deadlineAndRemainingBudgetComeFromTheOwningScope() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        GlobalPar global =
                GlobalPar.builder().register(ParName.of("worker"), pool).build();
        CountDownLatch block = new CountDownLatch(1);
        try {
            TaskGroupDefinition.Builder definition = TaskGroupDefinition.builder(groupOptions("page"));
            TaskKey<String> inherited = definition.task(
                    new TaskKey<>("inherited") {},
                    ParName.of("worker"),
                    () -> hold(block, "x"),
                    TaskOptions.inheritTimeout());
            TaskKey<String> tighter = definition.task(
                    new TaskKey<>("tighter") {},
                    ParName.of("worker"),
                    () -> hold(block, "y"),
                    TaskOptions.timeout(Duration.ofMillis(200)));
            TaskGroup group = TaskGroup.submit(global, definition.build());

            TaskFuture<String> inheritedFuture = group.future(inherited);
            TaskFuture<String> tighterFuture = group.future(tighter);
            long groupDeadline = group.completionFuture().deadlineNanos();

            assertThat(inheritedFuture.deadlineNanos()).isEqualTo(groupDeadline);
            assertThat(tighterFuture.deadlineNanos()).isLessThan(groupDeadline);
            assertThat(group.completionFuture().get(2, TimeUnit.SECONDS).deadlineNanos())
                    .isEqualTo(groupDeadline);

            Duration before = inheritedFuture.remaining();
            Thread.sleep(5);
            assertThat(inheritedFuture.remaining()).isLessThanOrEqualTo(before);
        } finally {
            block.countDown();
            global.close();
            pool.shutdownNow();
        }
    }

    @Test
    void packagePrivateFluentDerivationKeepsTheTaskView() throws Exception {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        CancellationToken token = new CancellationToken(null, System.nanoTime() + TimeUnit.SECONDS.toNanos(30));
        try {
            Task<String> origin = Task.of("orders", token, Futures.immediateFuture("4"));
            Task<Integer> transformed = origin.transform(value -> Integer.valueOf(value) + 1, Runnable::run);
            Task<String> recovered = Task.of(
                            "orders", token, Futures.<String>immediateFailedFuture(new IllegalStateException("boom")))
                    .catching(IllegalStateException.class, failure -> "fallback", Runnable::run);
            Task<String> bounded = Task.of("orders", token, SettableFuture.<String>create())
                    .withTimeout(Duration.ofMillis(20), scheduler);

            // The derived shell keeps the name and token of the task that produced it, and stays a
            // Task rather than decaying into a plain FluentFuture.
            assertThat(transformed.taskName()).isEqualTo("orders");
            assertThat(transformed.deadlineNanos()).isEqualTo(origin.deadlineNanos());
            assertThat(transformed.toString()).doesNotContain("placeholder=");
            assertThat(transformed.get(2, TimeUnit.SECONDS)).isEqualTo(5);
            assertThat(transformed.outcome()).isEqualTo(TaskOutcome.SUCCESS);
            assertThat(recovered.get(2, TimeUnit.SECONDS)).isEqualTo("fallback");
            assertThatThrownBy(() -> bounded.get(2, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void toStringDescribesTheTaskForDiagnostics() {
        Task<String> succeeded = Task.of("orders", new CancellationToken(), Futures.immediateFuture("done"));
        Task<String> timed = Task.of(
                "orders",
                new CancellationToken(null, System.nanoTime() + TimeUnit.SECONDS.toNanos(30)),
                SettableFuture.<String>create());

        assertThat(succeeded.toString()).isEqualTo("Task[name=orders, state=SUCCESS, remaining=unbounded]");
        assertThat(timed.toString()).contains("name=orders", "state=RUNNING", "remaining=PT");
    }

    // ==================== helpers ====================

    private static TaskGroupOptions groupOptions(String name) {
        return TaskGroupOptions.timeout(name, SCOPE_TIMEOUT);
    }

    private static TaskOptions memberOptions() {
        return TaskOptions.timeout(SCOPE_TIMEOUT);
    }

    /** Blocks the worker until the latch opens or the thread is interrupted, then returns {@code value}. */
    private static <T> T hold(CountDownLatch latch, T value) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while holding", interrupted);
        }
        return value;
    }

    /** Signals that the worker reached this point, then blocks until the latch opens. */
    private static <T> T startedThenHold(CountDownLatch started, CountDownLatch latch, T value) {
        started.countDown();
        return hold(latch, value);
    }

    private static String boom() {
        throw new IllegalStateException("boom");
    }

    private static <T> T awaitQuietly(ListenableFuture<T> future) {
        try {
            return future.get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        } catch (ExecutionException failure) {
            throw new IllegalStateException(failure);
        }
    }
}
