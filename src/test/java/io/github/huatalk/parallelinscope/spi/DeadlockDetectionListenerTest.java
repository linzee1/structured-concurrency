package io.github.huatalk.parallelinscope.spi;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.spi.DeadlockDetectionListener.DeadlockDetectionEvent;
import org.junit.jupiter.api.Test;

/** Contract tests for {@link DeadlockDetectionEvent} diagnostics. */
class DeadlockDetectionListenerTest {

    @Test
    void event_withNoIssues_hasAnyIssueFalse() {
        DeadlockDetectionEvent event = new DeadlockDetectionEvent(false, false, false, false, "tasks", "execs");
        assertThat(event.hasAnyIssue()).isFalse();
        assertThat(event.hasTaskCycle()).isFalse();
        assertThat(event.hasSelfLoop()).isFalse();
        assertThat(event.hasExecutorCycle()).isFalse();
        assertThat(event.hasExecutorSelfLoop()).isFalse();
    }

    @Test
    void event_withSelfLoop_hasAnyIssueTrue() {
        DeadlockDetectionEvent event = new DeadlockDetectionEvent(false, true, false, false, "tasks", "execs");
        assertThat(event.hasAnyIssue()).isTrue();
        assertThat(event.hasSelfLoop()).isTrue();
        assertThat(event.taskEdges()).isEqualTo("tasks");
        assertThat(event.executorEdges()).isEqualTo("execs");
    }

    @Test
    void event_withTaskCycle_hasAnyIssueTrue() {
        DeadlockDetectionEvent event = new DeadlockDetectionEvent(true, false, false, false, "tasks", "execs");
        assertThat(event.hasAnyIssue()).isTrue();
        assertThat(event.hasTaskCycle()).isTrue();
    }

    @Test
    void event_withExecutorCycleOrSelfLoop_hasAnyIssueTrue() {
        assertThat(new DeadlockDetectionEvent(false, false, true, false, "t", "e").hasAnyIssue())
                .isTrue();
        assertThat(new DeadlockDetectionEvent(false, false, false, true, "t", "e").hasAnyIssue())
                .isTrue();
    }

    @Test
    void toString_containsDiagnostics() {
        DeadlockDetectionEvent event = new DeadlockDetectionEvent(true, true, true, true, "task-edges", "exec-edges");
        assertThat(event.toString())
                .contains("taskCycle=true")
                .contains("taskEdges=[task-edges]")
                .contains("executorEdges=[exec-edges]");
    }
}
