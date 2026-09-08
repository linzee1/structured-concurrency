package io.github.huatalk.parallelinscope.scope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huatalk.parallelinscope.context.TaskGraphObservationScope;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class MultiTaskContextTest {
    @Test
    void childDeadlineCannotOutliveParentDeadline() {
        MultiTaskContext parent = MultiTaskContext.resolve(
                MultiTaskOptions.of("outer").timeout(Duration.ofMillis(100)).build(), 1, null);
        MultiTaskContext child = MultiTaskContext.resolve(
                MultiTaskOptions.of("inner").timeout(Duration.ofSeconds(10)).build(), 1, parent);

        assertThat(child.deadlineNanos()).isLessThanOrEqualTo(parent.deadlineNanos());
        assertThat(child.cancellationToken()).isNotNull();
    }

    @Test
    void resolvesParallelismAndRuntimeMetadata() {
        MultiTaskOptions options = MultiTaskOptions.of("io")
                .parallelism(99)
                .taskType(TaskType.IO_BOUND)
                .rejectEnqueue(false)
                .timeout(Duration.ofSeconds(30))
                .build();
        ExecutorServiceStub executor = new ExecutorServiceStub();
        ExecutorIdentity identity = new ExecutorIdentity(executor);
        MultiTaskContext context = MultiTaskContext.resolve(options, 3, null, null, identity, "http");

        assertThat(context.effectiveParallelism()).isEqualTo(3);
        assertThat(context.executorIdentity()).isSameAs(identity);
        assertThat(context.parLabel()).isEqualTo("http");
        assertThat(context.taskType()).isEqualTo(TaskType.IO_BOUND);
        assertThat(context.rejectEnqueue()).isFalse();
        assertThat(context.remaining().isNegative()).isFalse();
    }

    @Test
    void rejectsNegativeTaskCount() {
        assertThatThrownBy(() -> MultiTaskContext.resolve(
                        MultiTaskOptions.of("x").timeout(Duration.ofSeconds(30)).build(), -1, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void inheritsParentObservationAndCreatesLinkedCancellationToken() {
        GlobalPar global = GlobalPar.builder().build();
        TaskGraphObservationScope observation = global.openTaskGraphObservation();
        try {
            MultiTaskContext parent = MultiTaskContext.resolve(
                    MultiTaskOptions.of("parent")
                            .timeout(Duration.ofSeconds(30))
                            .build(),
                    2,
                    null,
                    observation);
            MultiTaskContext child = MultiTaskContext.resolve(
                    MultiTaskOptions.of("child").timeout(Duration.ofSeconds(30)).build(), 1, parent);

            assertThat(parent.taskName()).isEqualTo("parent");
            assertThat(parent.taskCount()).isEqualTo(2);
            assertThat(parent.parent()).isNull();
            assertThat(child.parent()).isSameAs(parent);
            assertThat(child.taskGraphObservationScope()).isSameAs(observation);
            assertThat(child.cancellationToken()).isNotSameAs(parent.cancellationToken());
            assertThat(child.executorIdentity()).isNull();
            assertThat(child.parLabel()).isNull();
        } finally {
            observation.close();
            global.close();
        }
    }

    /** Minimal executor identity object; no tasks are submitted by this test. */
    private static final class ExecutorServiceStub extends java.util.concurrent.AbstractExecutorService {
        public void shutdown() {}

        public java.util.List<Runnable> shutdownNow() {
            return java.util.Collections.emptyList();
        }

        public boolean isShutdown() {
            return false;
        }

        public boolean isTerminated() {
            return false;
        }

        public boolean awaitTermination(long timeout, java.util.concurrent.TimeUnit unit) {
            return true;
        }

        public void execute(Runnable command) {
            throw new UnsupportedOperationException();
        }
    }
}
