package io.github.huatalk.parallelinscope.queue;

import com.google.common.util.concurrent.ForwardingBlockingQueue;
import io.github.huatalk.parallelinscope.context.SubmissionScope;
import io.github.huatalk.parallelinscope.scope.MultiTaskContext;
import io.github.huatalk.parallelinscope.scope.TaskType;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * Smart blocking queue that dynamically adjusts enqueue behavior based on task type.
 *
 * <p>For {@link TaskType#CPU_BOUND} tasks or when {@code rejectEnqueue} is set, {@link
 * #offer(Object)} returns {@code false}, forcing the {@code ThreadPoolExecutor}'s {@code
 * RejectedExecutionHandler} to trigger (typically CallerRunsPolicy). This prevents CPU-bound tasks
 * from queuing up and causing latency.
 *
 * @param <E> the type of elements held in this queue
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public class SmartBlockingQueue<E> extends ForwardingBlockingQueue<E> {

    private final VariableLinkedBlockingQueue<E> delegate;

    /**
     * Creates a queue with the supplied positive capacity.
     *
     * @param capacity the initial queue capacity
     */
    public SmartBlockingQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.delegate = new VariableLinkedBlockingQueue<>(capacity);
    }

    @Override
    protected BlockingQueue<E> delegate() {
        return delegate;
    }

    /**
     * Dynamically adjusts queue capacity.
     *
     * @param capacity the new positive capacity
     */
    public void setCapacity(int capacity) {
        delegate.setCapacity(capacity);
    }

    /**
     * Returns the current queue capacity.
     *
     * @return the positive queue capacity
     */
    public int capacity() {
        return delegate.capacity();
    }

    /**
     * CPU-bound tasks return false directly, triggering thread pool's RejectedExecutionHandler. Other
     * task types enqueue normally.
     */
    @Override
    public boolean offer(E o) {
        MultiTaskContext batch = SubmissionScope.currentBatch();
        if (batch != null) {
            if (batch.taskType() == TaskType.CPU_BOUND || batch.rejectEnqueue()) return false;
            return delegate.offer(o);
        }
        return delegate.offer(o);
    }

    /**
     * Creates a blocking queue: returns {@link SynchronousQueue} for capacity {@code <= 0}, otherwise
     * returns {@link SmartBlockingQueue}.
     *
     * @param <T> the queue element type
     * @param capacity the requested queue capacity
     * @return a synchronous or capacity-adjustable blocking queue
     */
    public static <T> BlockingQueue<T> create(int capacity) {
        if (capacity <= 0) {
            return new SynchronousQueue<>();
        }
        return new SmartBlockingQueue<>(capacity);
    }
}
