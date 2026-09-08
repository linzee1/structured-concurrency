package io.github.huatalk.parallelinscope.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huatalk.parallelinscope.context.SubmissionScope;
import io.github.huatalk.parallelinscope.scope.BatchExecutionContext;
import io.github.huatalk.parallelinscope.scope.BatchExecutionOptions;
import io.github.huatalk.parallelinscope.scope.GlobalExecutionPolicy;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.util.concurrent.SynchronousQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SmartBlockingQueueTest {
    @AfterEach
    void clearContext() {
        SubmissionScope.restore(null);
    }

    @Test
    void validatesCapacityAndCreatesExpectedQueueKinds() {
        assertThatThrownBy(() -> new SmartBlockingQueue<Integer>(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SmartBlockingQueue<Integer>(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThat(SmartBlockingQueue.<Integer>create(0)).isInstanceOf(SynchronousQueue.class);
        assertThat(SmartBlockingQueue.<Integer>create(-1)).isInstanceOf(SynchronousQueue.class);
        assertThat(SmartBlockingQueue.<Integer>create(2)).isInstanceOf(SmartBlockingQueue.class);
    }

    @Test
    void delegatesCapacityAndOffersOutsideScope() {
        SmartBlockingQueue<Integer> queue = new SmartBlockingQueue<>(1);
        assertThat(queue.capacity()).isEqualTo(1);
        assertThat(queue.offer(1)).isTrue();
        assertThat(queue.offer(2)).isFalse();
        assertThat(queue.poll()).isEqualTo(1);
        queue.setCapacity(2);
        assertThat(queue.capacity()).isEqualTo(2);
    }

    @Test
    void cpuAndRejectEnqueueScopesRejectButIoScopeQueues() {
        SmartBlockingQueue<Integer> queue = new SmartBlockingQueue<>(2);
        assertOfferRejects(queue, context(TaskType.CPU_BOUND, false), 1);
        assertOfferRejects(queue, context(TaskType.IO_BOUND, true), 2);

        BatchExecutionContext previous = SubmissionScope.install(context(TaskType.IO_BOUND, false));
        try {
            assertThat(queue.offer(3)).isTrue();
            assertThat(queue.poll()).isEqualTo(3);
        } finally {
            SubmissionScope.restore(previous);
        }
    }

    private static void assertOfferRejects(
            SmartBlockingQueue<Integer> queue, BatchExecutionContext context, int element) {
        BatchExecutionContext previous = SubmissionScope.install(context);
        try {
            assertThat(queue.offer(element)).isFalse();
            assertThat(queue).isEmpty();
        } finally {
            SubmissionScope.restore(previous);
        }
    }

    private static BatchExecutionContext context(TaskType taskType, boolean rejectEnqueue) {
        return BatchExecutionContext.resolve(
                GlobalExecutionPolicy.builder().build(),
                BatchExecutionOptions.of("queue")
                        .taskType(taskType)
                        .rejectEnqueue(rejectEnqueue)
                        .build(),
                1,
                null);
    }
}
