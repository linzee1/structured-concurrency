package io.github.huatalk.parallelinscope.spi;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.scope.BatchExecutionContext;
import io.github.huatalk.parallelinscope.scope.BatchExecutionOptions;
import io.github.huatalk.parallelinscope.scope.GlobalExecutionPolicy;
import io.github.huatalk.parallelinscope.scope.TaskContext;
import io.github.huatalk.parallelinscope.spi.TaskListener.TaskEvent;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class TaskListenerTest {
    @Test
    void eventExposesTimingMetadataAndFailure() {
        IllegalStateException failure = new IllegalStateException("failed");
        TaskContext context = taskContext();
        TaskEvent<String> event = TaskEvent.failed(context, failure, true);

        assertThat(event.taskContext()).isSameAs(context);
        assertThat(event.taskName()).isEqualTo("task");
        assertThat(event.submitTimeNanos()).isEqualTo(10);
        assertThat(event.startTimeNanos()).isEqualTo(30);
        assertThat(event.endTimeNanos()).isEqualTo(80);
        assertThat(event.enqueued()).isTrue();
        assertThat(event.successful()).isFalse();
        assertThat(event.result()).isNull();
        assertThat(event.exception()).isSameAs(failure);
        assertThat(event.waitTime()).isEqualTo(Duration.ofNanos(20));
        assertThat(event.executionTime()).isEqualTo(Duration.ofNanos(50));
        assertThat(event.totalTime()).isEqualTo(Duration.ofNanos(70));
    }

    @Test
    void successfulEventCanCarryNoException() {
        TaskContext context = taskContext();
        TaskEvent<String> event = TaskEvent.succeeded(context, "value", false);
        assertThat(event.enqueued()).isFalse();
        assertThat(event.successful()).isTrue();
        assertThat(event.result()).isEqualTo("value");
        assertThat(event.exception()).isNull();
    }

    @Test
    void successfulEventDistinguishesNullResultFromFailure() {
        TaskEvent<Void> event = TaskEvent.succeeded(taskContext(), null, false);
        assertThat(event.successful()).isTrue();
        assertThat(event.result()).isNull();
        assertThat(event.exception()).isNull();
    }

    private static TaskContext taskContext() {
        BatchExecutionContext batch = BatchExecutionContext.resolve(
                GlobalExecutionPolicy.builder().build(),
                BatchExecutionOptions.of("task").build(),
                1,
                null);
        return new TaskContext() {
            @Override
            public BatchExecutionContext batchContext() {
                return batch;
            }

            @Override
            public int taskIndex() {
                return 0;
            }

            @Override
            public long submitTimeNanos() {
                return 10;
            }

            @Override
            public long startTimeNanos() {
                return 30;
            }

            @Override
            public long endTimeNanos() {
                return 80;
            }

            @Override
            public long executionTimeNanos() {
                return 50;
            }

            @Override
            public long waitTimeNanos() {
                return 20;
            }

            @Override
            public long totalTimeNanos() {
                return 70;
            }
        };
    }
}
