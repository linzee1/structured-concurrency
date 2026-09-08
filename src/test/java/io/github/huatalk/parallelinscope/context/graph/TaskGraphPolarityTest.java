package io.github.huatalk.parallelinscope.context.graph;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.context.TaskGraphObservationContext;
import io.github.huatalk.parallelinscope.scope.ExecutorIdentity;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.scope.GlobalParDeadlockPolicy;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Polarity complements to {@link TaskGraphBatchIdentityTest}: clean graphs must evaluate every
 * detection predicate {@code false}, unknown nodes pass through {@code displayNode} unformatted,
 * missing labels fall back to {@code NA}, and a benign graph publishes no deadlock event.
 */
class TaskGraphPolarityTest {

    @AfterEach
    void clearThreadState() {
        TaskGraphObservationContext.restore(null);
    }

    /** Non-deadlock-prone edge so executor-level detection stays clean in polarity tests. */
    private static TaskEdge plainEdge() {
        return new TaskEdge(1, TaskType.IO_BOUND, "child-exec", "parent-exec", 1, 1_000L, false);
    }

    @Test
    void acyclicEdgesEvaluateEveryDetectionPredicateFalse() {
        GlobalPar global = GlobalPar.builder().build();
        try (TaskGraphObservationContext ignored = global.openTaskGraphObservation()) {
            TaskGraphObservationContext.logTaskPair("root", "root-label", "a", "task-a", plainEdge());
            TaskGraphObservationContext.logTaskPair("a", "task-a", "b", "task-b", plainEdge());

            assertThat(TaskGraphObservationContext.hasTaskCycle()).isFalse();
            assertThat(TaskGraphObservationContext.hasSelfLoop()).isFalse();
            assertThat(TaskGraphObservationContext.hasExecutorCycle()).isFalse();
            assertThat(TaskGraphObservationContext.hasExecutorSelfLoop()).isFalse();

            TaskGraphData data = TaskGraphObservationContext.data();
            assertThat(data).isNotNull();
            assertThat(data.executorCycle()).isFalse();
            assertThat(data.executorSelfLoop()).isFalse();

            TaskGraphObservationContext.restore(null);
            assertThat(TaskGraphObservationContext.data()).isNull();
            assertThat(TaskGraphObservationContext.hasTaskCycle()).isFalse();
        } finally {
            global.close();
        }
    }

    @Test
    void displayNodeFormatsLabelledNodesAndPassesUnknownNodesThrough() throws Exception {
        TaskGraphData data = new TaskGraphData();
        Map<String, String> labels = labelsOf(data);
        labels.put("t1", "OrderService");
        assertThat(data.displayNode("t1")).isEqualTo("OrderService[t1]");
        assertThat(data.displayNode("unknown")).isEqualTo("unknown");
        assertThat(labels.get("never-added")).isNull();
    }

    @Test
    void logTaskPairDefaultsMissingParentAndLabelsToRootAndNA() throws Exception {
        GlobalPar global = GlobalPar.builder().build();
        try (TaskGraphObservationContext ignored = global.openTaskGraphObservation()) {
            TaskGraphObservationContext.logTaskPair(null, null, "child", null, plainEdge());

            TaskGraphData data = TaskGraphObservationContext.data();
            assertThat(data).isNotNull();
            Map<String, String> labels = labelsOf(data);
            assertThat(labels).containsEntry("root", "NA");
            assertThat(labels).containsEntry("child", "NA");

            List<TaskEdgeEntry> entries = new java.util.ArrayList<>();
            data.subTaskList.drainTo(entries);
            assertThat(entries).hasSize(1);
            assertThat(entries.get(0).edge().source()).isEqualTo("root");
            assertThat(entries.get(0).edge().target()).isEqualTo("child");
            TaskGraphObservationContext.restore(null);
        } finally {
            global.close();
        }
    }

    @Test
    void benignGraphPublishesNoDetectionEventOnObservationClose() {
        AtomicInteger detections = new AtomicInteger();
        GlobalPar global = GlobalPar.builder()
                .deadlockPolicy(GlobalParDeadlockPolicy.builder()
                        .enabled(true)
                        .listener(event -> detections.incrementAndGet())
                        .build())
                .build();
        try (TaskGraphObservationContext outer = global.openTaskGraphObservation()) {
            // Acyclic chain only: no cycle, no self-loop anywhere.
            TaskGraphObservationContext.logTaskPair("r", "r", "x", "x", plainEdge());
            assertThat(detections.get()).isZero();
        }
        assertThat(detections.get()).isZero();
    }

    @Test
    void taskCycleWithoutExecutorRiskPublishesEventWithFalseExecutorFlags() throws Exception {
        java.util.concurrent.atomic.AtomicReference<
                        io.github.huatalk.parallelinscope.spi.DeadlockDetectionListener.DeadlockDetectionEvent>
                captured = new java.util.concurrent.atomic.AtomicReference<>();
        GlobalPar global = GlobalPar.builder()
                .deadlockPolicy(GlobalParDeadlockPolicy.builder()
                        .enabled(true)
                        .listener(captured::set)
                        .build())
                .build();
        try (TaskGraphObservationContext ignored = global.openTaskGraphObservation()) {
            // Task-level cycle a -> b -> a using NON-deadlock-prone edges: the task cycle is real,
            // but no executor dependency edges exist at all.
            TaskGraphObservationContext.logTaskPair("a", "task-a", "b", "task-b", plainEdge());
            TaskGraphObservationContext.logTaskPair("b", "task-b", "a", "task-a", plainEdge());
        }

        io.github.huatalk.parallelinscope.spi.DeadlockDetectionListener.DeadlockDetectionEvent event = captured.get();
        assertThat(event).isNotNull();
        assertThat(event.hasTaskCycle()).isTrue();
        assertThat(event.hasSelfLoop()).isFalse();
        assertThat(event.hasExecutorCycle()).isFalse();
        assertThat(event.hasExecutorSelfLoop()).isFalse();
        assertThat(event.hasAnyIssue()).isTrue();
        assertThat(event.taskEdges()).contains("task-a[a] -> task-b[b]");
        assertThat(String.valueOf(event)).isNotEmpty();
    }

    @Test
    void distinctExecutorIdentitiesFormNoSelfLoopEvenWhenTheTaskGraphCycles() {
        java.util.concurrent.ExecutorService firstPool = java.util.concurrent.Executors.newSingleThreadExecutor();
        java.util.concurrent.ExecutorService secondPool = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            ExecutorIdentity firstIdentity = new ExecutorIdentity(firstPool);
            ExecutorIdentity secondIdentity = new ExecutorIdentity(secondPool);
            GlobalPar global = GlobalPar.builder().build();
            try (TaskGraphObservationContext ignored = global.openTaskGraphObservation()) {
                TaskEdge forward = new TaskEdge(
                        1, TaskType.IO_BOUND, firstIdentity, secondIdentity, "first", "second", 1, 10L, true);
                TaskEdge back = new TaskEdge(
                        1, TaskType.IO_BOUND, secondIdentity, firstIdentity, "second", "first", 1, 10L, true);

                TaskGraphObservationContext.logTaskPair("a", "a", "b", "b", forward);
                TaskGraphObservationContext.logTaskPair("b", "b", "a", "a", back);

                TaskGraphData data = TaskGraphObservationContext.data();
                assertThat(data).isNotNull();
                assertThat(data.executorCycle()).isTrue(); // Real pool-to-pool cycle.
                assertThat(data.executorSelfLoop()).isFalse(); // But no single-pool loop.
                assertThat(data.executorGraph().nodes()).isNotEmpty();
            } finally {
                global.close();
            }
        } finally {
            firstPool.shutdownNow();
            secondPool.shutdownNow();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> labelsOf(TaskGraphData data) {
        try {
            Field field = TaskGraphData.class.getDeclaredField("nodeLabels");
            field.setAccessible(true);
            return (Map<String, String>) field.get(data);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }
}
