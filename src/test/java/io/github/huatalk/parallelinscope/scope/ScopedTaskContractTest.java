package io.github.huatalk.parallelinscope.scope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.alibaba.ttl.TransmittableThreadLocal;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.huatalk.parallelinscope.context.TaskGraphObservationScope;
import io.github.huatalk.parallelinscope.internal.SubmissionException;
import io.github.huatalk.parallelinscope.spi.ExecutionPhase;
import io.github.huatalk.parallelinscope.spi.TaskListener;
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
        List<TaskListener.TaskEvent<?>> events = synchronizedEvents();
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
        List<TaskListener.TaskEvent<?>> events = synchronizedEvents();
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
                assertThat(result.failedMemberName()).isEqualTo("task");
                assertThat(result.members().get("task").outcome()).isEqualTo(TaskOutcome.USER_FAILURE);
                assertThat(result.members().get("task").failure()).isSameAs(boom);
            }
            assertThat(events).hasSize(1);
            assertThat(events.get(0).successful()).isFalse();
            assertThat(events.get(0).exception()).isSameAs(boom);
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
        List<TaskListener.TaskEvent<?>> events = synchronizedEvents();
        ConcurrentLinkedQueue<ExecutionPhase> phases = new ConcurrentLinkedQueue<>();
        GlobalPar global = globalWithListener(rejecting, events);
        try {
            observePhases(global, phases);
            AtomicInteger executions = new AtomicInteger();
            MultiTaskOptions options = MultiTaskOptions.of("task")
                    .taskType(TaskType.CPU_BOUND)
                    .timeout(Duration.ofSeconds(30))
                    .build();

            ListenableFuture<Object> future = submitSingle(global, entry, "task", options, () -> {
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
        List<TaskListener.TaskEvent<?>> events = synchronizedEvents();
        ConcurrentLinkedQueue<ExecutionPhase> phases = new ConcurrentLinkedQueue<>();
        GlobalPar global = globalWithListener(rejecting, events);
        try {
            observePhases(global, phases);
            AtomicInteger executions = new AtomicInteger();
            MultiTaskOptions options = MultiTaskOptions.of("task")
                    .taskType(TaskType.IO_BOUND)
                    .timeout(Duration.ofSeconds(30))
                    .build();

            ListenableFuture<Object> future = submitSingle(global, entry, "task", options, () -> {
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
                ListenableFuture<Object> queued = global.par("worker")
                        .map(
                                java.util.Arrays.asList("blocker", "queued"),
                                item -> callUnchecked(() -> runUnlessQueued(item, release, queuedRuns)),
                                MultiTaskOptions.of("cancel")
                                        .parallelism(2)
                                        .timeout(Duration.ofSeconds(30))
                                        .build())
                        .results()
                        .get(1);
                assertThat(queued.cancel(true)).isTrue();
            } else {
                TaskGroupSpec.Builder spec = TaskGroupSpec.builder(MultiTaskOptions.of("cancel")
                        .timeout(Duration.ofSeconds(30))
                        .build());
                spec.task(
                        new TaskRef<>("blocker") {},
                        "worker",
                        () -> runUnlessQueued("blocker", release, queuedRuns),
                        MultiTaskOptions.of("blocker")
                                .timeout(Duration.ofSeconds(30))
                                .build());
                TaskRef<Object> queued = spec.task(
                        new TaskRef<>("queued") {},
                        "worker",
                        () -> runUnlessQueued("queued", release, queuedRuns),
                        MultiTaskOptions.of("queued")
                                .timeout(Duration.ofSeconds(30))
                                .build());
                TaskGroup group = TaskGroup.submit(global, spec.build());
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
                Object value = global.par("worker")
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
                                MultiTaskOptions.of("outer")
                                        .timeout(Duration.ofSeconds(30))
                                        .build())
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
        return submitSingle(
                global,
                entry,
                name,
                MultiTaskOptions.of(name).timeout(Duration.ofSeconds(30)).build(),
                task);
    }

    private static ListenableFuture<Object> submitSingle(
            GlobalPar global, Entry entry, String name, MultiTaskOptions options, Callable<Object> task) {
        if (entry == Entry.BATCH) {
            return global.par("worker")
                    .map(Collections.singletonList("item"), item -> callUnchecked(task), options)
                    .results()
                    .get(0);
        }
        TaskGroupSpec.Builder spec = TaskGroupSpec.builder(
                MultiTaskOptions.of("contract").timeout(Duration.ofSeconds(30)).build());
        TaskRef<Object> ref = spec.task(new TaskRef<>(name) {}, "worker", task, options);
        TaskGroup group = TaskGroup.submit(global, spec.build());
        LAST_GROUP.set(group);
        return group.future(ref);
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

    private static List<TaskListener.TaskEvent<?>> synchronizedEvents() {
        return Collections.synchronizedList(new ArrayList<>());
    }

    private static GlobalPar globalWithListener(ExecutorService executor, List<TaskListener.TaskEvent<?>> events) {
        return GlobalPar.builder()
                .taskListener(events::add)
                .register("worker", executor)
                .build();
    }

    private static void observePhases(GlobalPar global, ConcurrentLinkedQueue<ExecutionPhase> phases) {
        // The test executors are never raw ThreadPoolExecutor instances, so no purge observer is
        // installed and the phase observer slot is free to claim.
        global.par("worker").runtime().setPhaseObserver(phases::add);
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
