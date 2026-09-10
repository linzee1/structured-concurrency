/**
 * Capacity-aware and lifecycle-aware blocking queue implementations for parallel task executors.
 *
 * <p>{@link io.github.huatalk.parallelinscope.queue.DrainingBlockingQueue} is the lifecycle-aware
 * implementation: its one-way {@code OPEN → DRAINING → DRAINED} close permanently rejects
 * producers while allowing consumers and {@code drainTo} to remove every accepted element. Its
 * terminal signal and post-terminal mutation behavior are selected by {@link
 * io.github.huatalk.parallelinscope.queue.DrainingBlockingQueue.ShutdownPolicy}; the queue's API
 * documentation defines the method-level blocking, exception, traversal, and concurrency contracts.
 */
@ParametersAreNonnullByDefault
package io.github.huatalk.parallelinscope.queue;

import javax.annotation.ParametersAreNonnullByDefault;
