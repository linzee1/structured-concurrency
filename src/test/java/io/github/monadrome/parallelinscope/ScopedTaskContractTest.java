package io.github.monadrome.parallelinscope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.alibaba.ttl.TransmittableThreadLocal;
import com.google.common.util.concurrent.ListenableFuture;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Cross-entry contract tests: {@code Par.map} and {@code TaskGroup} prepare and submit
 * every task through the same {@code TaskSubmissions} pipeline, so the single-task semantics —
 * instrumentation, TTL capture, phase hints, and rejection handling — must agree across both
 * entry points.
 */
class ScopedTaskContractTest {

    /** Submission entry point under test. */
    enum Entry {
        BATCH,
        GROUP
    }

    private static Stream<Entry> entries() {
        return Stream.of(Entry.values());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("entries")
    void successRunsOnceWithListenerAndRunningPhase(Entry entry) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        List<TaskCompletion<?>> events = synchronizedEvents();
        ConcurrentLinkedQueue<ExecutionPhase> phases = new ConcurrentLinkedQueue<>();
        GlobalPar global = globalWithListener(executor, events);
        try {
            observePhases(global, phases);
            AtomicInteger executions = new AtomicInteger();

            ListenableFuture<Object> future = submitSingle(global, entry, "task", () -> {
                executions.incrementAndGet();
                return "done";
            });

            assertThat(future.get(2, TimeUnit.SECONDS)).isEqualTo("done");
            assertThat(executions).hasValue(1);
            // set(result) precedes the TERMINAL emission inside the worker, so wait for it.
            org.awaitility.Awaitility.await()
                    .atMost(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> assertThat(phases).containsExactly(ExecutionPhase.RUNNING, ExecutionPhase.TERMINAL));
            assertThat(events).hasSize(1);
            assertThat(events.get(0).successful()).isTrue();
            assertThat(events.get(0).result()).isEqualTo("done");
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("entries")
    void userExceptionIsReportedAsUserFailure(Entry entry) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        List<TaskCompletion<?>> events = synchronizedEvents();
        GlobalPar global = globalWithListener(executor, events);
        try {
            IllegalStateException boom = new IllegalStateException("boom");
            ListenableFuture<Object> future = submitSingle(global, entry, "task", () -> {
                throw boom;
            });

            assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCause(boom);
            if (entry == Entry.GROUP) {
                TaskGroupResult result = lastGroupResult(global);
                // The member observer is registered before the group token bind, so the group
                // converges while its token is still RUNNING: a lone member failure reads as
                // MEMBER_CANCELED, with the failure attributed to the member.
                assertThat(result.outcome()).isEqualTo(TaskOutcome.MEMBER_CANCELED);
                assertThat(result.failedTaskName()).isEqualTo("task");
                assertThat(result.members().get("task").outcome()).isEqualTo(TaskOutcome.USER_FAILURE);
                assertThat(result.members().get("task").failure()).isSameAs(boom);
            }
            assertThat(events).hasSize(1);
            assertThat(events.get(0).successful()).isFalse();
            assertThat(events.get(0).failure()).isSameAs(boom);
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("entries")
    void ttlSnapshotIsVisibleToTheTask(Entry entry) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global = globalWithListener(executor, synchronizedEvents());
        TransmittableThreadLocal<String> ttl = new TransmittableThreadLocal<>();
        try {
            ttl.set("snapshot");
            ListenableFuture<Object> future = submitSingle(global, entry, "task", ttl::get);

            assertThat(future.get(2, TimeUnit.SECONDS)).isEqualTo("snapshot");
        } finally {
            ttl.remove();
            global.close();
            executor.shutdownNow();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("entries")
    void cpuBoundRejectionFallsBackToInlineExecution(Entry entry) throws Exception {
        ExecutorService rejecting = new RejectingExecutor();
        List<TaskCompletion<?>> events = synchronizedEvents();
        ConcurrentLinkedQueue<ExecutionPhase> phases = new ConcurrentLinkedQueue<>();
        GlobalPar global = globalWithListener(rejecting, events);
        try {
            observePhases(global, phases);
            AtomicInteger executions = new AtomicInteger();

            ListenableFuture<Object> future = submitSingle(global, entry, "task", TaskType.CPU_BOUND, () -> {
                executions.incrementAndGet();
                return "inline";
            });

            assertThat(future.get(2, TimeUnit.SECONDS)).isEqualTo("inline");
            assertThat(executions).hasValue(1);
            assertThat(phases).containsExactly(ExecutionPhase.RUNNING, ExecutionPhase.TERMINAL);
            assertThat(events).hasSize(1);
        } finally {
            global.close();
            rejecting.shutdownNow();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("entries")
    void ioBoundRejectionNeverRunsUserCode(Entry entry) throws Exception {
        ExecutorService rejecting = new RejectingExecutor();
        List<TaskCompletion<?>> events = synchronizedEvents();
        ConcurrentLinkedQueue<ExecutionPhase> phases = new ConcurrentLinkedQueue<>();
        GlobalPar global = globalWithListener(rejecting, events);
        try {
            observePhases(global, phases);
            AtomicInteger executions = new AtomicInteger();

            ListenableFuture<Object> future = submitSingle(global, entry, "task", TaskType.IO_BOUND, () -> {
                executions.incrementAndGet();
                return "never";
            });

            assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
            assertThat(executions).hasValue(0);
            assertThat(events).isEmpty();
            assertThat(phases).doesNotContain(ExecutionPhase.RUNNING);
            if (entry == Entry.GROUP) {
                TaskGroupResult result = lastGroupResult(global);
                assertThat(result.members().get("task").outcome()).isEqualTo(TaskOutcome.SUBMISSION_FAILURE);
                assertThat(result.members().get("task").failure()).isInstanceOf(SubmissionException.class);
            }
        } finally {
            global.close();
            rejecting.shutdownNow();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("entries")
    void cancelBeforeRunSkipsUserCodeAndHintsThePhase(Entry entry) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ConcurrentLinkedQueue<ExecutionPhase> phases = new ConcurrentLinkedQueue<>();
        GlobalPar global = globalWithListener(executor, synchronizedEvents());
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger queuedRuns = new AtomicInteger();
        try {
            observePhases(global, phases);
            if (entry == Entry.BATCH) {
                ListenableFuture<Object> queued = global.par(ParName.of("worker"))
                        .map(
                                java.util.Arrays.asList("blocker", "queued"),
                                item -> callUnchecked(() -> runUnlessQueued(item, release, queuedRuns)),
                                BatchOptions.timeout("cancel", Duration.ofSeconds(30))
                                        .parallelism(2))
                        .results()
                        .get(1);
                assertThat(queued.cancel(true)).isTrue();
            } else {
                TaskGroupDefinition.Builder definition =
                        TaskGroupDefinition.builder(TaskGroupOptions.timeout("cancel", Duration.ofSeconds(30)));
                definition.task(
                        new TaskKey<>("blocker") {},
                        ParName.of("worker"),
                        () -> runUnlessQueued("blocker", release, queuedRuns),
                        TaskOptions.timeout(Duration.ofSeconds(30)));
                TaskKey<Object> queued = definition.task(
                        new TaskKey<>("queued") {},
                        ParName.of("worker"),
                        () -> runUnlessQueued("queued", release, queuedRuns),
                        TaskOptions.timeout(Duration.ofSeconds(30)));
                TaskGroup group = TaskGroup.submit(global, definition.build());
                assertThat(group.future(queued).cancel(true)).isTrue();
                TaskGroupResult result = group.completionFuture().get(2, TimeUnit.SECONDS);
                assertThat(result.members().get("queued").outcome()).isEqualTo(TaskOutcome.MEMBER_CANCELED);
            }
            assertThat(phases).contains(ExecutionPhase.CANCELED_BEFORE_RUN);
            assertThat(queuedRuns).hasValue(0);
        } finally {
            release.countDown();
            global.close();
            executor.shutdownNow();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("entries")
    void nestedSubmissionRecordsTaskGraphEdge(Entry entry) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global = globalWithListener(executor, synchronizedEvents());
        try {
            try (TaskGraphObservationScope observation = global.openTaskGraphObservation()) {
                Object value = global.par(ParName.of("worker"))
                        .map(
                                Collections.singletonList("outer"),
                                item -> {
                                    try {
                                        return submitSingle(global, entry, "inner", () -> "inner-value")
                                                .get(2, TimeUnit.SECONDS);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        throw new IllegalStateException(e);
                                    } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
                                        throw new IllegalStateException(e);
                                    }
                                },
                                BatchOptions.timeout("outer", Duration.ofSeconds(30)))
                        .results()
                        .get(0)
                        .get(2, TimeUnit.SECONDS);

                assertThat(value).isEqualTo("inner-value");
                // root->outer plus outer->inner; a missing member/batch edge shows up here.
                assertThat(TaskGraphObservationScope.data().graph().edges()).hasSize(2);
            }
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    private static Object runUnlessQueued(String item, CountDownLatch release, AtomicInteger queuedRuns)
            throws InterruptedException {
        if ("queued".equals(item)) {
            queuedRuns.incrementAndGet();
            return "queued-ran";
        }
        release.await(2, TimeUnit.SECONDS);
        return "blocked";
    }

    private static ListenableFuture<Object> submitSingle(
            GlobalPar global, Entry entry, String name, Callable<Object> task) {
        return submitSingle(global, entry, name, TaskType.CPU_BOUND, task);
    }

    /**
     * Submits one task through the batch or the group path. The two entry points declare their own
     * option types — a batch its {@link BatchOptions}, a group member its {@link TaskOptions} — so
     * the shared task type is the only parameter they can share.
     */
    private static ListenableFuture<Object> submitSingle(
            GlobalPar global, Entry entry, String name, TaskType taskType, Callable<Object> task) {
        if (entry == Entry.BATCH) {
            return global.par(ParName.of("worker"))
                    .map(
                            Collections.singletonList("item"),
                            item -> callUnchecked(task),
                            BatchOptions.timeout(name, Duration.ofSeconds(30)).taskType(taskType))
                    .results()
                    .get(0);
        }
        TaskGroupDefinition.Builder definition =
                TaskGroupDefinition.builder(TaskGroupOptions.timeout("contract", Duration.ofSeconds(30)));
        TaskKey<Object> key = definition.task(
                new TaskKey<>(name) {},
                ParName.of("worker"),
                task,
                TaskOptions.timeout(Duration.ofSeconds(30)).taskType(taskType));
        TaskGroup group = TaskGroup.submit(global, definition.build());
        LAST_GROUP.set(group);
        return group.future(key);
    }

    private static Object callUnchecked(Callable<Object> task) {
        try {
            return task.call();
        } catch (RuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    // Tracks the group built by submitSingle so assertions can inspect the terminal snapshot
    // without changing the submission call sites. Tests are single-threaded per entry case.

    private static final ThreadLocal<TaskGroup> LAST_GROUP = new ThreadLocal<>();

    private static TaskGroupResult lastGroupResult(GlobalPar global) throws Exception {
        TaskGroup group = LAST_GROUP.get();
        if (group == null) {
            throw new IllegalStateException("no group was built");
        }
        LAST_GROUP.remove();
        return group.completionFuture().get(2, TimeUnit.SECONDS);
    }

    private static List<TaskCompletion<?>> synchronizedEvents() {
        return Collections.synchronizedList(new ArrayList<>());
    }

    private static GlobalPar globalWithListener(ExecutorService executor, List<TaskCompletion<?>> events) {
        return GlobalPar.builder()
                .taskListener(events::add)
                .register(ParName.of("worker"), executor)
                .build();
    }

    private static void observePhases(GlobalPar global, ConcurrentLinkedQueue<ExecutionPhase> phases) {
        // The test executors are never raw ThreadPoolExecutor instances, so no purge observer is
        // installed and the phase observer slot is free to claim.
        global.par(ParName.of("worker")).runtime().setPhaseObserver(phases::add);
    }

    private static final class RejectingExecutor extends AbstractExecutorService {
        private volatile boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return Collections.emptyList();
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
