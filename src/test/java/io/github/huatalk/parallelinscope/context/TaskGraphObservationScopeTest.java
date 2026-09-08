package io.github.huatalk.parallelinscope.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.alibaba.ttl.TtlRunnable;
import io.github.huatalk.parallelinscope.context.graph.TaskGraphData;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Lifecycle semantics of {@link TaskGraphObservationScope}: ownership, polarity, idempotence. */
class TaskGraphObservationScopeTest {

    private GlobalPar global;

    @AfterEach
    void cleanUp() {
        TaskGraphObservationScope.restore(null);
        if (global != null) {
            global.close();
        }
    }

    @Test
    void openTaskGraphObservationExposesOwnerDataAndCurrentPolarity() {
        global = GlobalPar.builder().build();
        TaskGraphObservationScope context = global.openTaskGraphObservation();
        try {
            assertThat(context.owner()).isSameAs(global);
            assertThat(context.closed()).isFalse();
            assertThat(TaskGraphObservationScope.current()).isSameAs(context);
            assertThat(TaskGraphObservationScope.data()).isNotNull();
        } finally {
            context.close();
        }

        assertThat(context.closed()).isTrue();
        assertThat(TaskGraphObservationScope.current()).isNull();

        // current() stays null after closing even without a fresh thread-local reset.
        assertThat(TaskGraphObservationScope.current()).isNull();
    }

    @Test
    void nestedObservationsRestoreTheOuterGraphData() throws Exception {
        global = GlobalPar.builder()
                .register("io", Executors.newSingleThreadExecutor())
                .build();
        try (TaskGraphObservationScope outer = global.openTaskGraphObservation()) {
            TaskGraphData outerData = TaskGraphObservationScope.data();
            assertThat(outerData).isNotNull();

            TaskGraphObservationScope inner = global.openTaskGraphObservation();
            assertThat(TaskGraphObservationScope.current()).isSameAs(inner);
            assertThat(TaskGraphObservationScope.data()).isNotSameAs(outerData);
            inner.close();

            assertThat(TaskGraphObservationScope.current()).isSameAs(outer);
            assertThat(TaskGraphObservationScope.data()).isSameAs(outerData);
        }
        assertThat(TaskGraphObservationScope.current()).isNull();
    }

    @Test
    void observationPropagatesAcrossTtlEnhancedSubmission() throws Exception {
        global = GlobalPar.builder().build();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            TaskGraphData expectedData;
            try (TaskGraphObservationScope context = global.openTaskGraphObservation()) {
                expectedData = TaskGraphObservationScope.data();
                AtomicReference<TaskGraphObservationScope> seen = new AtomicReference<>();
                AtomicReference<TaskGraphData> dataSeen = new AtomicReference<>();

                executor.submit(TtlRunnable.get(() -> {
                            seen.set(TaskGraphObservationScope.current());
                            dataSeen.set(TaskGraphObservationScope.data());
                        }))
                        .get(2, TimeUnit.SECONDS);

                assertThat(seen.get()).isSameAs(context);
                assertThat(dataSeen.get()).isSameAs(expectedData);
            }

            // TTL restores the worker's previous value after the task: no scope leaks.
            AtomicReference<TaskGraphObservationScope> afterRestore = new AtomicReference<>();
            executor.submit(TtlRunnable.get(() -> afterRestore.set(TaskGraphObservationScope.current())))
                    .get(2, TimeUnit.SECONDS);
            assertThat(afterRestore.get()).isNull();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void doubleCloseDestroysTheGraphOnlyOnce() throws Exception {
        java.util.concurrent.atomic.AtomicInteger detections = new java.util.concurrent.atomic.AtomicInteger();
        global = GlobalPar.builder()
                .deadlockPolicy(io.github.huatalk.parallelinscope.scope.GlobalParDeadlockPolicy.builder()
                        .enabled(true)
                        .listener(event -> detections.incrementAndGet())
                        .build())
                .build();

        TaskGraphObservationScope context = global.openTaskGraphObservation();
        TaskGraphObservationScope.logTaskPair(
                "a",
                "a",
                "b",
                "b",
                new io.github.huatalk.parallelinscope.context.graph.TaskEdge(
                        1, io.github.huatalk.parallelinscope.scope.TaskType.IO_BOUND, "e1", "e2", 1, 10L));
        TaskGraphObservationScope.logTaskPair(
                "b",
                "b",
                "a",
                "a",
                new io.github.huatalk.parallelinscope.context.graph.TaskEdge(
                        1, io.github.huatalk.parallelinscope.scope.TaskType.IO_BOUND, "e2", "e1", 1, 10L));

        context.close();
        int afterFirstClose = detections.get();
        context.close(); // Idempotent: no second detection pass.
        assertThat(detections.get()).isEqualTo(afterFirstClose);
        assertThat(afterFirstClose).isEqualTo(1);
    }

    @Test
    void constructorRejectsNullOwner() {
        assertThatThrownBy(() -> new TaskGraphObservationScope(null)).isInstanceOf(NullPointerException.class);
    }
}
