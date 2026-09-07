package io.github.huatalk.parallelinscope.spi;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.spi.TaskListener.TaskEvent;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class TaskListenerTest {
    @Test
    void eventExposesTimingMetadataAndFailure() {
        IllegalStateException failure = new IllegalStateException("failed");
        TaskEvent<String> event = TaskEvent.failed("task", "unit-1", 2, 10, 30, 80, failure, true);

        assertThat(event.taskName()).isEqualTo("task");
        assertThat(event.unitId()).isEqualTo("unit-1");
        assertThat(event.taskIndex()).isEqualTo(2);
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
        TaskEvent<String> event = TaskEvent.succeeded("task", "unit-1", 0, 10, 30, 80, "value", false);
        assertThat(event.enqueued()).isFalse();
        assertThat(event.successful()).isTrue();
        assertThat(event.result()).isEqualTo("value");
        assertThat(event.exception()).isNull();
    }

    @Test
    void successfulEventDistinguishesNullResultFromFailure() {
        TaskEvent<Void> event = TaskEvent.succeeded("task", "unit-1", 0, 10, 30, 80, null, false);
        assertThat(event.successful()).isTrue();
        assertThat(event.result()).isNull();
        assertThat(event.exception()).isNull();
    }

    @Test
    void eventRejectsNegativeTaskIndex() {
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> TaskEvent.succeeded("task", "unit-1", -1, 10, 30, 80, "value", false))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
