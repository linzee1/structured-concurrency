package io.github.huatalk.parallelinscope.context.graph;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.context.TaskGraphObservationContext;
import io.github.huatalk.parallelinscope.scope.ExecutorIdentity;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.GlobalParDeadlockPolicy;
import io.github.huatalk.parallelinscope.scope.MultiTaskContext;
import io.github.huatalk.parallelinscope.scope.MultiTaskOptions;
import io.github.huatalk.parallelinscope.spi.DeadlockDetectionListener;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class TaskGraphBatchIdentityTest {
    @Test
    void sameTaskNameInIndependentBatchesDoesNotCollapseNodes() {
        GlobalPar global = GlobalPar.builder().build();
        try (TaskGraphObservationContext ignored = global.openTaskGraphObservation()) {
            MultiTaskContext first = context();
            MultiTaskContext second = context();
            TaskGraphObservationContext.logTaskPair(null, "root", first.batchId(), first.taskName(), edge());
            TaskGraphObservationContext.logTaskPair(null, "root", second.batchId(), second.taskName(), edge());

            TaskGraphData data = TaskGraphObservationContext.data();
            assertThat(data.graph().nodes()).contains(first.batchId(), second.batchId());
            assertThat(first.batchId()).isNotEqualTo(second.batchId());
            assertThat(data.graph().edges()).hasSize(2);
            assertThat(data.selfLoop()).isFalse();
        } finally {
            global.close();
        }
    }

    @Test
    void detectsTaskCyclesSelfLoopsAndPreservesParallelEdges() {
        GlobalPar global = GlobalPar.builder().build();
        try (TaskGraphObservationContext ignored = global.openTaskGraphObservation()) {
            TaskGraphObservationContext.logTaskPair("a", "task-a", "b", "task-b", edge());
            TaskGraphObservationContext.logTaskPair("b", "task-b", "a", "task-a", edge());
            TaskGraphObservationContext.logTaskPair("a", "task-a", "a", "task-a", edge());
            TaskGraphObservationContext.logTaskPair("a", "task-a", "b", "task-b", edge());

            TaskGraphData data = TaskGraphObservationContext.data();
            assertThat(TaskGraphObservationContext.hasTaskCycle()).isTrue();
            assertThat(TaskGraphObservationContext.hasSelfLoop()).isTrue();
            assertThat(data.graph().edgeValueOrDefault("a", "b", java.util.Collections.emptyList()))
                    .hasSize(2);
            assertThat(data.graph()).isSameAs(data.graph());
        } finally {
            global.close();
        }
    }

    @Test
    void detectsExecutorCyclesAndSkipsNonRiskyEdges() {
        GlobalPar global = GlobalPar.builder().build();
        try (TaskGraphObservationContext ignored = global.openTaskGraphObservation()) {
            TaskGraphObservationContext.logTaskPair("a", "task-a", "b", "task-b", legacyEdge("pool-a", "pool-b", true));
            TaskGraphObservationContext.logTaskPair("b", "task-b", "a", "task-a", legacyEdge("pool-b", "pool-a", true));
            assertThat(TaskGraphObservationContext.hasExecutorCycle()).isTrue();
            assertThat(TaskGraphObservationContext.hasExecutorSelfLoop()).isFalse();
        } finally {
            global.close();
        }

        GlobalPar nonRisky = GlobalPar.builder().build();
        try (TaskGraphObservationContext ignored = nonRisky.openTaskGraphObservation()) {
            TaskGraphObservationContext.logTaskPair("a", "task-a", "a", "task-a", legacyEdge("pool", "pool", false));
            assertThat(TaskGraphObservationContext.hasExecutorCycle()).isFalse();
            assertThat(TaskGraphObservationContext.hasExecutorSelfLoop()).isFalse();
        } finally {
            nonRisky.close();
        }
    }

    @Test
    void detectsCyclesAndSelfLoopsByExecutorObjectIdentity() {
        ExecutorService first = Executors.newSingleThreadExecutor();
        ExecutorService second = Executors.newSingleThreadExecutor();
        GlobalPar global = GlobalPar.builder().build();
        try (TaskGraphObservationContext ignored = global.openTaskGraphObservation()) {
            ExecutorIdentity firstIdentity = new ExecutorIdentity(first);
            ExecutorIdentity secondIdentity = new ExecutorIdentity(second);
            TaskGraphObservationContext.logTaskPair(
                    "a", "task-a", "b", "task-b", identityEdge(firstIdentity, secondIdentity));
            TaskGraphObservationContext.logTaskPair(
                    "b", "task-b", "a", "task-a", identityEdge(secondIdentity, firstIdentity));
            TaskGraphObservationContext.logTaskPair(
                    "self", "self", "self", "self", identityEdge(firstIdentity, firstIdentity));

            assertThat(TaskGraphObservationContext.hasExecutorCycle()).isTrue();
            assertThat(TaskGraphObservationContext.hasExecutorSelfLoop()).isTrue();
        } finally {
            global.close();
            first.shutdownNow();
            second.shutdownNow();
        }
    }

    @Test
    void absentOrRestoredGraphHasNoIssues() {
        TaskGraphObservationContext.restore(null);
        assertThat(TaskGraphObservationContext.data()).isNull();
        assertThat(TaskGraphObservationContext.hasTaskCycle()).isFalse();
        assertThat(TaskGraphObservationContext.hasSelfLoop()).isFalse();
        assertThat(TaskGraphObservationContext.hasExecutorCycle()).isFalse();
        assertThat(TaskGraphObservationContext.hasExecutorSelfLoop()).isFalse();
    }

    @Test
    void closingObservationPublishesDetectionEventAndRestoresOuterGraph() {
        AtomicReference<DeadlockDetectionListener.DeadlockDetectionEvent> event = new AtomicReference<>();
        GlobalPar global = GlobalPar.builder()
                .deadlockPolicy(GlobalParDeadlockPolicy.builder()
                        .enabled(true)
                        .listener(event::set)
                        .build())
                .build();
        try (TaskGraphObservationContext outer = global.openTaskGraphObservation()) {
            TaskGraphData outerData = TaskGraphObservationContext.data();
            TaskGraphObservationContext.logTaskPair("outer", "outer", "outer", "outer", edge());
            try (TaskGraphObservationContext inner = global.openTaskGraphObservation()) {
                TaskGraphObservationContext.logTaskPair("inner", "inner", "inner", "inner", edge());
            }
            assertThat(event.get()).isNotNull();
            assertThat(event.get().hasSelfLoop()).isTrue();
            assertThat(TaskGraphObservationContext.current()).isSameAs(outer);
            assertThat(TaskGraphObservationContext.data()).isSameAs(outerData);
        } finally {
            global.close();
        }
    }

    @Test
    void closingObservationPublishesExecutorCycleEdges() {
        AtomicReference<DeadlockDetectionListener.DeadlockDetectionEvent> event = new AtomicReference<>();
        GlobalPar global = GlobalPar.builder()
                .deadlockPolicy(GlobalParDeadlockPolicy.builder()
                        .enabled(true)
                        .listener(event::set)
                        .build())
                .build();
        try (TaskGraphObservationContext ignored = global.openTaskGraphObservation()) {
            TaskGraphObservationContext.logTaskPair("a", "a", "b", "b", legacyEdge("pool-a", "pool-b", true));
            TaskGraphObservationContext.logTaskPair("b", "b", "a", "a", legacyEdge("pool-b", "pool-a", true));
        } finally {
            global.close();
        }

        assertThat(event.get()).isNotNull();
        assertThat(event.get().executorEdges()).contains("pool-a -> pool-b", "pool-b -> pool-a");
    }

    private static MultiTaskContext context() {
        return MultiTaskContext.resolve(
                MultiTaskOptions.of("same-name").timeout(Duration.ofSeconds(30)).build(), 1, null);
    }

    private static TaskEdge edge() {
        return new TaskEdge(
                1,
                io.github.huatalk.parallelinscope.scope.TaskType.CPU_BOUND,
                null,
                null,
                "executor",
                "parent",
                1,
                0,
                false);
    }

    private static TaskEdge legacyEdge(String source, String target, boolean deadlockProne) {
        return new TaskEdge(
                1, io.github.huatalk.parallelinscope.scope.TaskType.CPU_BOUND, target, source, 1, 0, deadlockProne);
    }

    private static TaskEdge identityEdge(ExecutorIdentity source, ExecutorIdentity target) {
        return new TaskEdge(
                1,
                io.github.huatalk.parallelinscope.scope.TaskType.CPU_BOUND,
                target,
                source,
                "target",
                "source",
                1,
                0,
                true);
    }
}
