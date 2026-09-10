package io.github.huatalk.parallelinscope.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import io.github.huatalk.parallelinscope.scope.BatchExecutionContext;
import io.github.huatalk.parallelinscope.scope.BatchExecutionOptions;
import io.github.huatalk.parallelinscope.scope.GlobalExecutionPolicy;
import org.junit.jupiter.api.Test;

class TaskExecutionContextTest {

    @Test
    void siblingTasksShareBatchButKeepIndependentIdentityAndTiming() {
        BatchExecutionContext batch = BatchExecutionContext.resolve(
                GlobalExecutionPolicy.builder().build(),
                BatchExecutionOptions.of("batch").build(),
                3,
                null);
        TaskExecutionContext first = new TaskExecutionContext(batch, 0, 10L);
        TaskExecutionContext second = new TaskExecutionContext(batch, 1, 20L);

        first.markStarted(30L);
        first.markEnded(40L);

        assertThat(first.batchContext()).isSameAs(batch);
        assertThat(second.batchContext()).isSameAs(batch);
        assertThat(first.taskIndex()).isEqualTo(0);
        assertThat(second.taskIndex()).isEqualTo(1);
        assertThat(first.submitTimeNanos()).isEqualTo(10L);
        assertThat(first.startTimeNanos()).isEqualTo(30L);
        assertThat(first.endTimeNanos()).isEqualTo(40L);
        assertThat(first.executionTimeNanos()).isEqualTo(10L);
        assertThat(first.waitTimeNanos()).isEqualTo(20L);
        assertThat(first.totalTimeNanos()).isEqualTo(30L);
        assertThat(second.startTimeNanos()).isZero();
        assertThat(second.endTimeNanos()).isZero();
    }

    @Test
    void rejectsNegativeTaskIndex() {
        BatchExecutionContext batch = BatchExecutionContext.resolve(
                GlobalExecutionPolicy.builder().build(),
                BatchExecutionOptions.of("batch").build(),
                1,
                null);

        assertThatIllegalArgumentException().isThrownBy(() -> new TaskExecutionContext(batch, -1, 0L));
    }

    @Test
    void installAndRestorePreserveNestedCurrentTask() {
        BatchExecutionContext batch = BatchExecutionContext.resolve(
                GlobalExecutionPolicy.builder().build(),
                BatchExecutionOptions.of("batch").build(),
                2,
                null);
        TaskExecutionContext outer = new TaskExecutionContext(batch, 0, 0L);
        TaskExecutionContext inner = new TaskExecutionContext(batch, 1, 0L);

        TaskExecutionContext previousOuter = TaskExecutionContext.install(outer);
        try {
            assertThat(previousOuter).isNull();
            assertThat(TaskExecutionContext.current()).isSameAs(outer);
            TaskExecutionContext previousInner = TaskExecutionContext.install(inner);
            try {
                assertThat(previousInner).isSameAs(outer);
                assertThat(TaskExecutionContext.current()).isSameAs(inner);
            } finally {
                TaskExecutionContext.restore(previousInner);
            }
            assertThat(TaskExecutionContext.current()).isSameAs(outer);
        } finally {
            TaskExecutionContext.restore(previousOuter);
        }
        assertThat(TaskExecutionContext.current()).isNull();
    }
}
