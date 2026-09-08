package io.github.monadrome.parallelinscope.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.monadrome.parallelinscope.scope.MultiTaskContext;
import io.github.monadrome.parallelinscope.scope.MultiTaskOptions;
import io.github.monadrome.parallelinscope.scope.TaskCompletion;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ScopedCallableContextRestoreTest {
    @Test
    void listenerReceivesSuccessfulTaskTimingAndMetadata() throws Exception {
        MultiTaskContext context = context("listener");
        AtomicReference<TaskCompletion<?>> captured = new AtomicReference<>();
        TaskExecutionContext taskContext = task(context, 0);
        ScopedCallable<String> callable =
                new ScopedCallable<>(taskContext, () -> "value", Collections.singletonList(event -> {
                    assertThat(TaskExecutionContext.current()).isNull();
                    captured.set(event);
                }));

        assertThat(callable.call()).isEqualTo("value");
        TaskCompletion<?> event = captured.get();
        assertThat(event).isNotNull();
        assertThat(event.unitId()).isEqualTo(context.unitId());
        assertThat(event.taskIndex()).isEqualTo(0);
        assertThat(event.submitTimeNanos()).isEqualTo(taskContext.submitTimeNanos());
        assertThat(event.successful()).isTrue();
        assertThat(event.result()).isEqualTo("value");
        assertThat(event.taskName()).isEqualTo("listener");
        assertThat(event.failure()).isNull();
        assertThat(event.endTimeNanos()).isGreaterThanOrEqualTo(event.startTimeNanos());
        assertThat(callable.executionTime()).isGreaterThanOrEqualTo(0L);
        assertThat(callable.waitTime()).isGreaterThanOrEqualTo(0L);
        assertThat(callable.totalTime()).isGreaterThanOrEqualTo(callable.executionTime());
        assertThat(callable.cancellationToken()).isSameAs(context.cancellationToken());
        assertThat(callable.executorName()).isEqualTo("NA");
        assertThat(callable.toString()).contains("listener", "submitTime", "startTime", "endTime");
    }

    @Test
    void listenerReceivesFailureWhileOriginalFailureEscapes() {
        MultiTaskContext context = context("failed");
        AtomicReference<TaskCompletion<?>> captured = new AtomicReference<>();
        IllegalStateException failure = new IllegalStateException("boom");
        ScopedCallable<String> callable = new ScopedCallable<>(
                task(context, 0),
                () -> {
                    throw failure;
                },
                Collections.singletonList(event -> {
                    assertThat(TaskExecutionContext.current()).isNull();
                    captured.set(event);
                }));

        org.assertj.core.api.Assertions.assertThatThrownBy(callable::call).isSameAs(failure);
        assertThat(captured.get().failure()).isSameAs(failure);
        assertThat(captured.get().successful()).isFalse();
        assertThat(captured.get().result()).isNull();
    }

    @Test
    void nestedCallRestoresOuterCurrentTask() throws Exception {
        MultiTaskContext outer = context("same-name");
        MultiTaskContext inner = MultiTaskContext.resolve(
                MultiTaskOptions.of("same-name").timeout(Duration.ofSeconds(30)).build(), 1, outer);
        TaskExecutionContext outerTask = task(outer, 0);
        ScopedCallable<String> outerCallable = new ScopedCallable<>(
                outerTask,
                () -> {
                    assertThat(TaskExecutionContext.current()).isSameAs(outerTask);

                    TaskExecutionContext innerTask = task(inner, 0);
                    ScopedCallable<String> innerCallable = new ScopedCallable<>(
                            innerTask,
                            () -> {
                                assertThat(TaskExecutionContext.current()).isSameAs(innerTask);
                                return "inner";
                            },
                            Collections.singletonList(event ->
                                    assertThat(TaskExecutionContext.current()).isNull()));
                    assertThat(innerCallable.call()).isEqualTo("inner");

                    assertThat(TaskExecutionContext.current()).isSameAs(outerTask);
                    return "outer";
                },
                null);
        assertThat(outerCallable.call()).isEqualTo("outer");
        assertThat(TaskExecutionContext.current()).isNull();
    }

    private static MultiTaskContext context(String name) {
        return MultiTaskContext.resolve(
                MultiTaskOptions.of(name).timeout(Duration.ofSeconds(30)).build(), 1, null);
    }

    private static TaskExecutionContext task(MultiTaskContext context, int index) {
        return new TaskExecutionContext(
                context, index, com.google.common.base.Ticker.systemTicker().read());
    }
}
