package io.github.huatalk.parallelinscope.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huatalk.parallelinscope.scope.BatchExecutionContext;
import io.github.huatalk.parallelinscope.scope.BatchExecutionOptions;
import io.github.huatalk.parallelinscope.scope.GlobalExecutionPolicy;
import io.github.huatalk.parallelinscope.spi.TaskListener.TaskEvent;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ScopedCallableContextRestoreTest {
    @Test
    void listenerReceivesSuccessfulTaskTimingAndMetadata() throws Exception {
        BatchExecutionContext context = context("listener");
        AtomicReference<TaskEvent> captured = new AtomicReference<>();
        TaskExecutionContext taskContext = task(context, 0);
        ScopedCallable<String> callable =
                new ScopedCallable<>(taskContext, () -> "value", Collections.singletonList(event -> {
                    assertThat(TaskExecutionContext.current()).isNull();
                    captured.set(event);
                }));

        assertThat(callable.call()).isEqualTo("value");
        TaskEvent event = captured.get();
        assertThat(event).isNotNull();
        assertThat(event.taskContext()).isSameAs(taskContext);
        assertThat(event.successful()).isTrue();
        assertThat(event.result()).isEqualTo("value");
        assertThat(event.taskName()).isEqualTo("listener");
        assertThat(event.exception()).isNull();
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
        BatchExecutionContext context = context("failed");
        AtomicReference<TaskEvent> captured = new AtomicReference<>();
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
        assertThat(captured.get().exception()).isSameAs(failure);
        assertThat(captured.get().successful()).isFalse();
        assertThat(captured.get().result()).isNull();
    }

    @Test
    void nestedCallRestoresOuterCurrentTask() throws Exception {
        BatchExecutionContext outer = context("same-name");
        BatchExecutionContext inner = BatchExecutionContext.resolve(
                GlobalExecutionPolicy.builder().build(),
                BatchExecutionOptions.of("same-name").build(),
                1,
                outer);
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

    private static BatchExecutionContext context(String name) {
        return BatchExecutionContext.resolve(
                GlobalExecutionPolicy.builder().build(),
                BatchExecutionOptions.of(name).build(),
                1,
                null);
    }

    private static TaskExecutionContext task(BatchExecutionContext context, int index) {
        return new TaskExecutionContext(
                context, index, com.google.common.base.Ticker.systemTicker().read());
    }
}
