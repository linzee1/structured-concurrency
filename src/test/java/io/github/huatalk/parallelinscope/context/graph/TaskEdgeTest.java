package io.github.huatalk.parallelinscope.context.graph;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.scope.ExecutorIdentity;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class TaskEdgeTest {
    @Test
    void legacyEdgeExposesAllMetadata() {
        TaskEdge edge = new TaskEdge(3, TaskType.IO_BOUND, "child", "parent", 4, Duration.ofMillis(500), false);
        assertThat(edge.parallelism()).isEqualTo(3);
        assertThat(edge.taskType()).isEqualTo(TaskType.IO_BOUND);
        assertThat(edge.executorName()).isEqualTo("child");
        assertThat(edge.sourceExecutorName()).isEqualTo("parent");
        assertThat(edge.taskCount()).isEqualTo(4);
        assertThat(edge.timeout()).isEqualTo(Duration.ofMillis(500));
        assertThat(edge.executorDeadlockProne()).isFalse();
        assertThat(edge.executorIdentity()).isNull();
        assertThat(edge.sourceExecutorIdentity()).isNull();
        assertThat(edge.toString()).contains("p=3", "child", "parent", "count=4");
    }

    @Test
    void identityEdgeRetainsSuppliedExecutorIdentities() {
        ExecutorService source = Executors.newSingleThreadExecutor();
        ExecutorService target = Executors.newSingleThreadExecutor();
        try {
            ExecutorIdentity sourceIdentity = new ExecutorIdentity(source);
            ExecutorIdentity targetIdentity = new ExecutorIdentity(target);
            TaskEdge edge = new TaskEdge(
                    1, TaskType.CPU_BOUND, targetIdentity, sourceIdentity, "target", "source", 1, Duration.ZERO, true);
            assertThat(edge.executorIdentity()).isSameAs(targetIdentity);
            assertThat(edge.sourceExecutorIdentity()).isSameAs(sourceIdentity);
            assertThat(edge.executorDeadlockProne()).isTrue();
        } finally {
            source.shutdownNow();
            target.shutdownNow();
        }
    }
}
