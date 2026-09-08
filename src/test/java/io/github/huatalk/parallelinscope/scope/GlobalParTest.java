package io.github.huatalk.parallelinscope.scope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.alibaba.ttl.TransmittableThreadLocal;
import io.github.huatalk.parallelinscope.context.TaskGraphObservationContext;
import io.github.huatalk.parallelinscope.context.graph.TaskGraphData;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class GlobalParTest {
    @Test
    void propagatesAndRestoresTransmittableThreadLocalForEveryTask() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        TransmittableThreadLocal<String> context = new TransmittableThreadLocal<>();
        GlobalPar global = GlobalPar.builder().register("worker", executor).build();
        try {
            executor.submit(() -> {}).get(2, TimeUnit.SECONDS);
            context.set("request-42");

            AsyncBatchResult<String> result = global.par("worker")
                    .map(
                            java.util.Arrays.asList(1, 2),
                            ignored -> context.get(),
                            BatchExecutionOptions.of("ttl").parallelism(1).build());

            assertThat(result.results())
                    .extracting(future -> future.get(2, TimeUnit.SECONDS))
                    .containsExactly("request-42", "request-42");

            AtomicReference<String> workerAfterTasks = new AtomicReference<>();
            executor.submit(() -> workerAfterTasks.set(context.get())).get(2, TimeUnit.SECONDS);
            assertThat(workerAfterTasks.get()).isNull();
        } finally {
            context.remove();
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void executorRuntimeIsPackagePrivateImplementationDetail() {
        assertThat(Modifier.isPublic(ExecutorRuntime.class.getModifiers())).isFalse();
    }

    @Test
    void buildsImmutableNamedEntriesAndSharesRuntimeBySuppliedIdentity() {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            GlobalPar global = GlobalPar.builder()
                    .register("one", executor)
                    .register("same", executor)
                    .defaultPar("one")
                    .build();

            assertThat(global.defaultPar()).isSameAs(global.par("one"));
            assertThat(global.par("one").displayName()).isEqualTo("one");
            assertThat(global.par("one").globalPar()).isSameAs(global);
            assertThat(global.par("one").runtime()).isSameAs(global.par("same").runtime());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void rejectsUnknownDefaultAndDuplicateNames() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            assertThatThrownBy(() -> GlobalPar.builder().defaultPar("missing").build())
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> GlobalPar.builder().register("x", executor).register("x", executor))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void executesUsingTheExecutorBoundAtBuildTime() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            GlobalPar global = GlobalPar.builder().register("io", executor).build();

            AsyncBatchResult<Integer> result = global.par("io")
                    .map(
                            Collections.singletonList(2),
                            value -> value + 1,
                            BatchExecutionOptions.of("increment").build());

            assertThat(result.results().get(0).get()).isEqualTo(3);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void nullAndEmptyInputsProduceUsableEmptyBatchResults() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            GlobalPar global = GlobalPar.builder().register("io", executor).build();

            assertThat(global.par("io")
                            .map(
                                    null,
                                    value -> value,
                                    BatchExecutionOptions.of("empty").build())
                            .results())
                    .isEmpty();
            assertThat(global.par("io")
                            .map(
                                    Collections.<Integer>emptyList(),
                                    value -> value,
                                    BatchExecutionOptions.of("empty").build())
                            .results())
                    .isEmpty();
            global.close();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void nestedBatchesAcrossExecutorsShareCancellationContextAndObservationGraph() throws Exception {
        ExecutorService outerExecutor = Executors.newSingleThreadExecutor();
        ExecutorService innerExecutor = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register("outer", outerExecutor)
                .register("inner", innerExecutor)
                .build();
        try (TaskGraphObservationContext ignored = global.openTaskGraphObservation()) {
            TaskGraphData expectedGraph = TaskGraphObservationContext.data();
            AsyncBatchResult<Integer> outer = global.par("outer")
                    .map(
                            Collections.singletonList(2),
                            value -> {
                                AsyncBatchResult<Integer> inner = global.par("inner")
                                        .map(
                                                Collections.singletonList(value),
                                                item -> item + 1,
                                                BatchExecutionOptions.of("inner")
                                                        .build());
                                try {
                                    return inner.results().get(0).get(2, TimeUnit.SECONDS);
                                } catch (Exception failure) {
                                    throw new RuntimeException(failure);
                                }
                            },
                            BatchExecutionOptions.of("outer").build());

            assertThat(outer.results().get(0).get(2, TimeUnit.SECONDS)).isEqualTo(3);
            assertThat(TaskGraphObservationContext.data()).isSameAs(expectedGraph);
            assertThat(expectedGraph.graph().edges()).isNotEmpty();
        } finally {
            global.close();
            outerExecutor.shutdownNow();
            innerExecutor.shutdownNow();
        }
    }

    @Test
    void topLevelBatchInstallsObservationOnPreexistingWorkerAndPropagatesItToNestedBatch() throws Exception {
        ExecutorService outerExecutor = Executors.newSingleThreadExecutor();
        ExecutorService innerExecutor = Executors.newSingleThreadExecutor();
        outerExecutor.submit(() -> {}).get(2, TimeUnit.SECONDS);
        GlobalPar global = GlobalPar.builder()
                .register("outer", outerExecutor)
                .register("inner", innerExecutor)
                .build();
        try (TaskGraphObservationContext ignored = global.openTaskGraphObservation()) {
            TaskGraphData expectedGraph = TaskGraphObservationContext.data();
            java.util.concurrent.atomic.AtomicReference<TaskGraphData> graphOnOuterWorker =
                    new java.util.concurrent.atomic.AtomicReference<>();
            AsyncBatchResult<Integer> outer = global.par("outer")
                    .map(
                            Collections.singletonList(2),
                            value -> {
                                graphOnOuterWorker.set(TaskGraphObservationContext.data());
                                AsyncBatchResult<Integer> inner = global.par("inner")
                                        .map(
                                                Collections.singletonList(value),
                                                item -> item + 1,
                                                BatchExecutionOptions.of("inner")
                                                        .build());
                                try {
                                    return inner.results().get(0).get(2, TimeUnit.SECONDS);
                                } catch (Exception failure) {
                                    throw new RuntimeException(failure);
                                }
                            },
                            BatchExecutionOptions.of("outer").build());

            assertThat(outer.results().get(0).get(2, TimeUnit.SECONDS)).isEqualTo(3);
            assertThat(graphOnOuterWorker.get()).isSameAs(expectedGraph);
            assertThat(expectedGraph.graph().edges()).hasSize(2);
        } finally {
            global.close();
            outerExecutor.shutdownNow();
            innerExecutor.shutdownNow();
        }
    }

    @Test
    void nestedSlidingWindowsDoNotSerializeTheirSubmitterLoops() throws Exception {
        ExecutorService outerExecutor = Executors.newSingleThreadExecutor();
        ExecutorService innerExecutor = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder()
                .register("outer", outerExecutor)
                .register("inner", innerExecutor)
                .build();
        try {
            AsyncBatchResult<Integer> outer = global.par("outer")
                    .map(
                            java.util.Arrays.asList(1, 99),
                            ignored -> {
                                AsyncBatchResult<Integer> inner = global.par("inner")
                                        .map(
                                                java.util.Arrays.asList(1, 2),
                                                value -> value + 1,
                                                BatchExecutionOptions.of("inner")
                                                        .parallelism(1)
                                                        .build());
                                try {
                                    return inner.results().get(1).get(2, TimeUnit.SECONDS);
                                } catch (Exception failure) {
                                    throw new RuntimeException(failure);
                                }
                            },
                            BatchExecutionOptions.of("outer").parallelism(1).build());

            assertThat(outer.results().get(0).get(3, TimeUnit.SECONDS)).isEqualTo(3);
        } finally {
            global.close();
            outerExecutor.shutdownNow();
            innerExecutor.shutdownNow();
        }
    }

    @Test
    void closeFromCpuFallbackTaskDoesNotDeadlockBatchAdmission() throws Exception {
        ExecutorService rejectedExecutor = Executors.newSingleThreadExecutor();
        rejectedExecutor.shutdown();
        GlobalPar global = GlobalPar.builder().register("cpu", rejectedExecutor).build();
        try {
            AsyncBatchResult<Integer> result = global.par("cpu")
                    .map(
                            Collections.singletonList(1),
                            value -> {
                                global.close();
                                return value + 1;
                            },
                            BatchExecutionOptions.of("cpu")
                                    .taskType(TaskType.CPU_BOUND)
                                    .build());

            assertThat(result.results().get(0).get(2, TimeUnit.SECONDS)).isEqualTo(2);
            assertThat(global.closed()).isTrue();
        } finally {
            global.close();
            rejectedExecutor.shutdownNow();
        }
    }

    @Test
    void validatesPoliciesNamesAndStaticGlobalInstallation() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            GlobalExecutionPolicy policy =
                    GlobalExecutionPolicy.builder().defaultTimeoutMillis(42).build();
            GlobalPar.Builder builder =
                    GlobalPar.builder().executionPolicy(policy).register("io", executor);
            assertThatThrownBy(
                            () -> builder.parPolicyOverride("missing", policy).build())
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> GlobalPar.builder()
                            .register("io", executor)
                            .parPolicyOverride("io", policy)
                            .parPolicyOverride("io", policy))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> GlobalPar.builder().register("", executor))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> GlobalPar.builder().register("null", null))
                    .isInstanceOf(NullPointerException.class);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void installsAndReturnsTheProcessGlobalOnlyOnce() {
        GlobalPar installed = GlobalPar.builder().build();
        try {
            GlobalPar.installGlobal(installed);

            assertThat(GlobalPar.global()).isSameAs(installed);
            assertThatThrownBy(() -> GlobalPar.installGlobal(GlobalPar.builder().build()))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            installed.close();
        }
    }

    @Test
    void observationIsOwnedAndClosedExactlyOnce() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder().register("io", executor).build();
        try {
            io.github.huatalk.parallelinscope.context.TaskGraphObservationContext observation =
                    global.openTaskGraphObservation();
            assertThat(observation.owner()).isSameAs(global);
            assertThat(observation.closed()).isFalse();
            assertThat(TaskGraphObservationContext.current()).isSameAs(observation);
            observation.close();
            observation.close();
            assertThat(observation.closed()).isTrue();
            assertThat(TaskGraphObservationContext.current()).isNull();
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void rejectsNewBatchesAndObservationsAfterCloseWithoutClosingBorrowedExecutor() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder().register("io", executor).build();
        try {
            global.close();

            assertThat(global.closed()).isTrue();
            assertThat(executor.isShutdown()).isFalse();
            assertThatThrownBy(() -> global.openTaskGraphObservation()).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> global.par("io")
                            .map(
                                    Collections.singletonList(1),
                                    value -> value + 1,
                                    BatchExecutionOptions.of("closed").build()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("GlobalPar is closed");
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    @Test
    void closeRejectsNewWorkWhileAnAdmittedBatchCompletesSetup() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(2);
        GlobalPar global = GlobalPar.builder().register("io", executor).build();
        CountDownLatch setupEntered = new CountDownLatch(1);
        CountDownLatch releaseSetup = new CountDownLatch(1);
        CountDownLatch closeStarted = new CountDownLatch(1);
        try {
            Future<?> setup = callers.submit(() -> global.whileOpen(() -> {
                setupEntered.countDown();
                awaitLatch(releaseSetup);
                return null;
            }));
            assertThat(setupEntered.await(5, TimeUnit.SECONDS)).isTrue();

            Future<?> close = callers.submit(() -> {
                closeStarted.countDown();
                global.close();
            });
            assertThat(closeStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(global.closed()).isTrue();

            releaseSetup.countDown();
            setup.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
            assertThat(global.closed()).isTrue();
            assertThatThrownBy(() -> global.par("io")
                            .map(
                                    Collections.singletonList(1),
                                    value -> value + 1,
                                    BatchExecutionOptions.of("closed").build()))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            releaseSetup.countDown();
            global.close();
            callers.shutdownNow();
            executor.shutdownNow();
        }
    }

    @Test
    void closeLetsAnAdmittedBatchDrainWithoutClosingItsBorrowedExecutor() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder().register("io", executor).build();
        CountDownLatch firstTaskStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstTask = new CountDownLatch(1);
        try {
            AsyncBatchResult<Integer> result = global.par("io")
                    .map(
                            java.util.Arrays.asList(1, 2, 3),
                            value -> {
                                if (value == 1) {
                                    firstTaskStarted.countDown();
                                    awaitLatch(releaseFirstTask);
                                }
                                return value + 1;
                            },
                            BatchExecutionOptions.of("drain").parallelism(1).build());
            assertThat(firstTaskStarted.await(5, TimeUnit.SECONDS)).isTrue();

            global.close();
            releaseFirstTask.countDown();

            assertThat(executor.isShutdown()).isFalse();
            assertThat(result.results().get(0).get(5, TimeUnit.SECONDS)).isEqualTo(2);
            assertThat(result.results().get(1).get(5, TimeUnit.SECONDS)).isEqualTo(3);
            assertThat(result.results().get(2).get(5, TimeUnit.SECONDS)).isEqualTo(4);
        } finally {
            releaseFirstTask.countDown();
            global.close();
            executor.shutdownNow();
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for test latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for test latch", e);
        }
    }

    @Test
    void purgePolicyAndDeadlockDetectionListenersAreImmutableAndIdentityDeduplicated() {
        AtomicInteger calls = new AtomicInteger();
        io.github.huatalk.parallelinscope.spi.DeadlockDetectionListener listener = event -> calls.incrementAndGet();
        GlobalParDeadlockPolicy deadlock = GlobalParDeadlockPolicy.builder()
                .enabled(true)
                .listener(listener)
                .listener(listener)
                .build();
        assertThat(deadlock.listeners()).hasSize(1);
        assertThatThrownBy(() -> deadlock.listeners().clear()).isInstanceOf(UnsupportedOperationException.class);
        GlobalParPurgePolicy purge = GlobalParPurgePolicy.builder()
                .enabled(true)
                .queuePressureThreshold(1.0)
                .canceledTaskRatioThreshold(0.5)
                .build();
        assertThat(purge.enabled()).isTrue();
        assertThat(purge.queuePressureThreshold()).isEqualTo(1.0);
        assertThat(purge.canceledTaskRatioThreshold()).isEqualTo(0.5);
        assertThat(GlobalParPurgePolicy.builder().build().enabled()).isFalse();
        assertThat(GlobalParDeadlockPolicy.builder().build().enabled()).isFalse();
    }

    @Test
    void exposesImmutableTopologyAndConfiguredPolicies() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            GlobalExecutionPolicy policy =
                    GlobalExecutionPolicy.builder().defaultTimeoutMillis(123).build();
            GlobalParDeadlockPolicy deadlock =
                    GlobalParDeadlockPolicy.builder().enabled(true).build();
            GlobalParPurgePolicy purge =
                    GlobalParPurgePolicy.builder().enabled(true).build();
            GlobalPar global = GlobalPar.builder()
                    .executionPolicy(policy)
                    .deadlockPolicy(deadlock)
                    .purgePolicy(purge)
                    .register("one", executor)
                    .build();

            assertThat(global.executionPolicy()).isSameAs(policy);
            assertThat(global.executionPolicyFor("one")).isSameAs(policy);
            assertThat(global.deadlockPolicy()).isSameAs(deadlock);
            assertThat(global.purgePolicy()).isSameAs(purge);
            assertThat(global.find("one")).contains(global.par("one"));
            assertThat(global.find("missing")).isEmpty();
            assertThat(global.pars()).containsOnlyKeys("one");
            assertThat(global.runtimes()).containsOnlyKeys("one");
            assertThat(global.runtimesByIdentity()).hasSize(1);
            assertThat(global.purger()).isNotNull();
            assertThatThrownBy(() -> global.pars().clear()).isInstanceOf(UnsupportedOperationException.class);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void executorRuntimeKeepsSuppliedIdentityAndCreatesOnlyNeededAdapter() {
        ExecutorService plain = Executors.newSingleThreadExecutor();
        com.google.common.util.concurrent.ListeningExecutorService listening =
                com.google.common.util.concurrent.MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        try {
            ExecutorRuntime plainRuntime = new ExecutorRuntime(plain);
            ExecutorRuntime listeningRuntime = new ExecutorRuntime(listening);
            ExecutorIdentity samePlain = new ExecutorIdentity(plain);
            ExecutorIdentity different = new ExecutorIdentity(listening);

            assertThat(plainRuntime.suppliedExecutor()).isSameAs(plain);
            assertThat(plainRuntime.submissionExecutorIsAdapter()).isTrue();
            assertThat(listeningRuntime.submissionExecutor()).isSameAs(listening);
            assertThat(listeningRuntime.submissionExecutorIsAdapter()).isFalse();
            assertThat(plainRuntime.identity()).isEqualTo(samePlain).isNotEqualTo(different);
            assertThat(plainRuntime.identity().hashCode()).isEqualTo(samePlain.hashCode());
            assertThat(plainRuntime.identity().suppliedExecutor()).isSameAs(plain);
            assertThat(plainRuntime.identity().toString()).contains("@");
            assertThat(plainRuntime.blockingRisk()).isEqualTo(BlockingRisk.UNKNOWN);
        } finally {
            plain.shutdownNow();
            listening.shutdownNow();
        }
    }

    @Test
    void exposesImmutableGlobalTaskListeners() {
        io.github.huatalk.parallelinscope.spi.TaskListener listener = event -> {};
        GlobalExecutionPolicy policy =
                GlobalExecutionPolicy.builder().taskListener(listener).build();

        assertThat(policy.taskListeners()).containsExactly(listener);
        assertThatThrownBy(() -> policy.taskListeners().clear()).isInstanceOf(UnsupportedOperationException.class);
    }
}
