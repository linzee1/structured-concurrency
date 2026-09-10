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

/** Lifecycle semantics of {@link TaskGraphObservationContext}: ownership, polarity, idempotence. */
class TaskGraphObservationContextTest {

    private GlobalPar global;

    @AfterEach
    void cleanUp() {
        TaskGraphObservationContext.restore(null);
        if (global != null) {
            global.close();
        }
    }

    @Test
    void openTaskGraphObservationExposesOwnerDataAndCurrentPolarity() {
        global = GlobalPar.builder().build();
        TaskGraphObservationContext context = global.openTaskGraphObservation();
        try {
            assertThat(context.owner()).isSameAs(global);
            assertThat(context.closed()).isFalse();
            assertThat(TaskGraphObservationContext.current()).isSameAs(context);
            assertThat(TaskGraphObservationContext.data()).isNotNull();
        } finally {
            context.close();
        }

        assertThat(context.closed()).isTrue();
        assertThat(TaskGraphObservationContext.current()).isNull();

        // current() stays null after closing even without a fresh thread-local reset.
        assertThat(TaskGraphObservationContext.current()).isNull();
    }

    @Test
    void nestedObservationsRestoreTheOuterGraphData() throws Exception {
        global = GlobalPar.builder()
                .register("io", Executors.newSingleThreadExecutor())
                .build();
        try (TaskGraphObservationContext outer = global.openTaskGraphObservation()) {
            TaskGraphData outerData = TaskGraphObservationContext.data();
            assertThat(outerData).isNotNull();

            TaskGraphObservationContext inner = global.openTaskGraphObservation();
            assertThat(TaskGraphObservationContext.current()).isSameAs(inner);
            assertThat(TaskGraphObservationContext.data()).isNotSameAs(outerData);
            inner.close();

            assertThat(TaskGraphObservationContext.current()).isSameAs(outer);
            assertThat(TaskGraphObservationContext.data()).isSameAs(outerData);
        }
        assertThat(TaskGraphObservationContext.current()).isNull();
    }

    @Test
    void observationPropagatesAcrossTtlEnhancedSubmission() throws Exception {
        global = GlobalPar.builder().build();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            TaskGraphData expectedData;
            try (TaskGraphObservationContext context = global.openTaskGraphObservation()) {
                expectedData = TaskGraphObservationContext.data();
                AtomicReference<TaskGraphObservationContext> seen = new AtomicReference<>();
                AtomicReference<TaskGraphData> dataSeen = new AtomicReference<>();

                executor.submit(TtlRunnable.get(() -> {
                            seen.set(TaskGraphObservationContext.current());
                            dataSeen.set(TaskGraphObservationContext.data());
                        }))
                        .get(2, TimeUnit.SECONDS);

                assertThat(seen.get()).isSameAs(context);
                assertThat(dataSeen.get()).isSameAs(expectedData);
            }

            // TTL restores the worker's previous value after the task: no scope leaks.
            AtomicReference<TaskGraphObservationContext> afterRestore = new AtomicReference<>();
            executor.submit(TtlRunnable.get(() -> afterRestore.set(TaskGraphObservationContext.current())))
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

        TaskGraphObservationContext context = global.openTaskGraphObservation();
        TaskGraphObservationContext.logTaskPair(
                "a",
                "a",
                "b",
                "b",
                new io.github.huatalk.parallelinscope.context.graph.TaskEdge(
                        1, io.github.huatalk.parallelinscope.scope.TaskType.IO_BOUND, "e1", "e2", 1, 10L));
        TaskGraphObservationContext.logTaskPair(
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
        assertThatThrownBy(() -> new TaskGraphObservationContext(null)).isInstanceOf(NullPointerException.class);
    }
}
