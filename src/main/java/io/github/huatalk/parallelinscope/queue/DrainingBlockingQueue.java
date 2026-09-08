package io.github.huatalk.parallelinscope.queue;

import com.google.common.util.concurrent.Monitor;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * A bounded FIFO blocking queue with a one-way draining close.
 *
 * <p>Lifecycle is a one-way three-state machine: {@code OPEN → DRAINING → DRAINED}. {@link
 * #close()} moves {@code OPEN} to {@code DRAINING}: producers are permanently rejected, while
 * consumers keep receiving real elements until the queue is empty. If the queue is empty when
 * {@code close()} takes effect, it publishes {@code DRAINED} in that same critical section.
 * Otherwise, the removal or drain of the final real element publishes {@code DRAINED} in the same
 * critical section as that element's ownership transfer. Therefore, when a call receives the last
 * real element, {@link #drained()} is already {@code true} when the call returns.
 *
 * <p>Every element for which a write method has successfully returned remains reachable through a
 * normal consumer operation or {@link #drainTo(Collection)} until one of those operations removes
 * it. Closing never discards or isolates accepted work. A configured poison is consequently a
 * <em>virtual terminal signal</em>, not an element: it is never returned before {@code DRAINED}
 * and never appears in snapshots, iterators, or streams. At {@code DRAINED}, value-returning
 * consumer methods return that poison when configured; otherwise special-value methods return
 * {@code null} and required-value methods throw {@link NoSuchElementException}.
 *
 * <p>The lifecycle rules are applied in this order: input validation first; real queued elements
 * take precedence over every terminal rule; closed producers are rejected; terminal consumers see
 * the poison or their normal empty result; and the configured mutation strategy applies only once
 * drained. Thus, after {@link #close()}, {@code add}/{@code put}/{@code offer} either throw
 * {@link IllegalStateException} or return {@code false}, without storing or waiting; consumers
 * still receive real elements while any remain; collection mutations ({@code clear}/{@code
 * remove}/{@code removeIf}/{@code removeAll}/{@code retainAll}) remain legal while draining and
 * are governed by {@link ShutdownPolicy}'s {@code mutations} strategy only after
 * {@code DRAINED}; and {@code drainTo} stays available in every state to remove remaining work.
 * Use {@link #shutdown()} to ask whether producers were rejected and {@link #drained()} to ask
 * whether the queue is empty and terminal; a closed but non-empty queue is in the {@link
 * #draining()} state.
 *
 * <p>Blocking methods react to lifecycle publication rather than interrupting user threads.
 * Closing wakes blocked producers and consumers so they re-evaluate their method-specific rule;
 * it never leaves a producer waiting for capacity after close. An external thread interruption
 * remains an {@link InterruptedException} and is not swallowed by a close result. {@link
 * #awaitDrained()} waits only for terminal-state publication, not for previously admitted calls to
 * finish returning to their callers.
 *
 * <p>Implementation follows {@link java.util.concurrent.LinkedBlockingQueue}'s two-lock structure
 * with Guava {@link Monitor monitors}: a producer monitor guards enqueue and capacity, a consumer
 * monitor guards dequeue and emptiness, and an {@link AtomicInteger} count publishes size across
 * both monitors. Lifecycle state is volatile so that close and drained publication release waiters
 * on either side through the monitors' rolling guard re-evaluation. Operations that need both
 * monitors always acquire producer then consumer. User code—element equality, collection
 * callbacks, predicates, and {@code toString()}—runs after queue monitors have been released.
 */
public class DrainingBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E>, AutoCloseable {

    private static final int TRAVERSAL_BATCH_SIZE = 64;
    private static final int MAX_SPLITERATOR_BATCH = 1 << 25;

    private enum Lifecycle {
        OPEN,
        DRAINING,
        DRAINED
    }

    /** Singly linked storage cell, including LBQ's self-link convention for detached heads. */
    private static final class Node<E> {
        @Nullable
        E item;

        @Nullable
        Node<E> next;

        Node(@Nullable E item) {
            this.item = item;
        }
    }

    private final int capacity;
    private final ShutdownPolicy<E> policy;

    /** Consumer monitor; guards consumer-side waits and the drained publication. */
    private final Monitor takeMonitor = new Monitor();

    /**
     * Releases a consumer for exactly the two legal blocking-exit conditions: a real element is
     * available, or terminal publication means that no real element can ever arrive again.
     */
    private final Monitor.Guard takeReady = new Monitor.Guard(takeMonitor) {
        @Override
        public boolean isSatisfied() {
            return count.get() > 0 || lifecycle == Lifecycle.DRAINED;
        }
    };

    /** Releases lifecycle waiters only after the terminal state is published. */
    private final Monitor.Guard drainedGuard = new Monitor.Guard(takeMonitor) {
        @Override
        public boolean isSatisfied() {
            return lifecycle == Lifecycle.DRAINED;
        }
    };

    /** Producer monitor; guards producer-side waits. */
    private final Monitor putMonitor = new Monitor();

    /**
     * Releases a producer when it may submit, or when shutdown supersedes capacity waiting so the
     * caller can promptly return its closed-write result.
     */
    private final Monitor.Guard putReady = new Monitor.Guard(putMonitor) {
        @Override
        public boolean isSatisfied() {
            return count.get() < capacity || lifecycle != Lifecycle.OPEN;
        }
    };

    /**
     * Published by close and the final removal; its volatile write makes terminal state observable
     * to status and lifecycle-wait APIs without a queue monitor.
     */
    private volatile Lifecycle lifecycle = Lifecycle.OPEN;

    /** Count is the cross-monitor state publication that lets either side evaluate its guard. */
    private final AtomicInteger count = new AtomicInteger();

    /** Head and tail of the linked queue; head.item is always null. */
    private Node<E> head = new Node<>(null);

    private Node<E> last = head;

    /**
     * Pre-admission gate for blocking producers. The later in-monitor lifecycle check is still
     * mandatory because close can race after this early check and before an enqueue commits.
     */
    private volatile boolean admissionClosed;

    /** Creates an effectively unbounded queue with the default empty terminal policy. */
    public DrainingBlockingQueue() {
        this(Integer.MAX_VALUE, Collections.emptyList(), ShutdownPolicy.empty());
    }

    /**
     * Creates a queue with the given positive capacity and the default empty terminal policy.
     *
     * @throws IllegalArgumentException if {@code capacity} is not positive
     */
    public DrainingBlockingQueue(int capacity) {
        this(capacity, Collections.emptyList(), ShutdownPolicy.empty());
    }

    /**
     * Creates a queue with the given positive capacity and a terminal poison signal. The signal is
     * returned only after all accepted elements are drained.
     *
     * @throws IllegalArgumentException if {@code capacity} is not positive
     * @throws NullPointerException if {@code poison} is null
     */
    public DrainingBlockingQueue(int capacity, E poison) {
        this(capacity, Collections.emptyList(), ShutdownPolicy.poison(poison));
    }

    /**
     * Creates a queue with the given positive capacity and terminal policy.
     *
     * @throws IllegalArgumentException if {@code capacity} is not positive
     * @throws NullPointerException if {@code policy} is null
     */
    public DrainingBlockingQueue(int capacity, ShutdownPolicy<E> policy) {
        this(capacity, Collections.emptyList(), policy);
    }

    /**
     * Creates an effectively unbounded queue containing the supplied FIFO initial elements and the
     * default empty terminal policy. Initial elements follow the same null and poison validation as
     * every write path.
     *
     * @throws NullPointerException if the collection or an element is null
     */
    public DrainingBlockingQueue(Collection<? extends E> initialElements) {
        this(Integer.MAX_VALUE, initialElements, ShutdownPolicy.empty());
    }

    /**
     * Creates a queue containing the supplied FIFO initial elements. Construction validates the
     * whole initial collection before the instance becomes visible: capacity overflow, null
     * elements, and poison-equivalent elements are rejected rather than partially accepted.
     *
     * @throws IllegalArgumentException if {@code capacity} is not positive, initial elements exceed
     *     it, or an initial element equals the configured poison
     * @throws NullPointerException if {@code initialElements}, {@code policy}, or an element is null
     */
    public DrainingBlockingQueue(int capacity, Collection<? extends E> initialElements, ShutdownPolicy<E> policy) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
        this.policy = Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(initialElements, "initialElements");
        int size = 0;
        for (E element : initialElements) {
            requireElement(element);
            if (size == capacity) {
                throw new IllegalArgumentException("initial elements exceed capacity");
            }
            last = last.next = new Node<>(element);
            size++;
        }
        count.set(size);
    }

    // region Helpers

    /**
     * Validates every write path before it observes queue state. A poison-equivalent value is
     * rejected by {@link Object#equals(Object)}, rather than identity, so a real element can never
     * be mistaken for the terminal signal. Consumers use the configured poison instance itself as
     * the signal; this intentionally favors deterministic rejection at write time.
     */
    private E requireElement(E element) {
        Objects.requireNonNull(element, "element");
        E poison = policy.poison();
        if (poison != null && poison.equals(element)) {
            throw new IllegalArgumentException("the poison object is reserved for shutdown signalling");
        }
        return element;
    }

    /** Creates the closed-producer exception with the stable contract message prefix. */
    private IllegalStateException closedWrite(String operation) {
        return new IllegalStateException("queue is closed: " + operation);
    }

    /** Creates the drained-consumer exception with the stable contract message prefix. */
    private NoSuchElementException closedRead(String operation) {
        return new NoSuchElementException("queue is drained: " + operation);
    }

    /** Returns the terminal result for special-value consumers: poison when configured, else null. */
    @Nullable
    private E drainedSpecialValue() {
        return policy.poison();
    }

    /** Returns poison or throws for a required-value consumer in the terminal state. */
    private E drainedRequiredValue(String operation) {
        E poison = policy.poison();
        if (poison != null) {
            return poison;
        }
        throw closedRead(operation);
    }

    private boolean open() {
        return lifecycle == Lifecycle.OPEN;
    }

    /**
     * Acquires both storage monitors in the sole global order, producer then consumer. Bulk
     * mutations, snapshots, and close use this order; no code may acquire them in reverse.
     */
    private void fullyLock() {
        putMonitor.enter();
        takeMonitor.enter();
    }

    /** Releases both storage monitors in reverse acquisition order. */
    private void fullyUnlock() {
        takeMonitor.leave();
        putMonitor.leave();
    }

    /**
     * Prompts the consumer monitor to re-evaluate satisfied guards after an enqueue or terminal
     * publication. This is the explicit wake-up path; callers do not wait for a later queue touch.
     */
    private void signalTakeReady() {
        takeMonitor.enter();
        takeMonitor.leave();
    }

    /**
     * Prompts the producer monitor to re-evaluate satisfied guards after capacity is released or
     * production is closed.
     */
    private void signalPutReady() {
        putMonitor.enter();
        putMonitor.leave();
    }

    /** First phase of a blocking producer: reject calls that began after close admission closed. */
    private boolean beginBlockingCall() {
        return !admissionClosed;
    }

    /**
     * Prevents future blocking calls from passing the pre-admission check; caller holds both
     * monitors. A call which passed earlier still rechecks lifecycle while holding its producer
     * monitor before it may enqueue.
     */
    private void closeAdmission() {
        admissionClosed = true;
    }

    /**
     * Transitions {@code DRAINING} to {@code DRAINED} when the queue is empty. This couples the
     * final real-element transfer to terminal publication. The caller must hold every monitor on
     * whose guard a waiter could be released.
     */
    private void publishDrainedIfEmptyLocked() {
        if (lifecycle == Lifecycle.DRAINING && count.get() == 0) {
            lifecycle = Lifecycle.DRAINED;
        }
    }

    /**
     * Applies the mutation policy only in {@code DRAINED}; draining mutations are deliberately
     * allowed because they are another valid way to finish draining stored work.
     */
    private void requireMutationAllowed(String operation) {
        if (drained() && policy.mutationsStrategy() == MutationsStrategy.THROW) {
            throw closedWrite(operation);
        }
    }

    /** Identifies the terminal no-op branch after policy validation has allowed a mutation to run. */
    private boolean drainedNoopMutation() {
        return drained() && policy.mutationsStrategy() == MutationsStrategy.NOOP;
    }

    // endregion

    // region Lifecycle

    /**
     * Returns whether production has been permanently closed. This is {@code true} in both
     * {@code DRAINING} and {@code DRAINED}; it does not mean that consumers have finished the
     * accepted work. Use {@link #drained()} for that stronger condition.
     */
    public boolean shutdown() {
        return lifecycle != Lifecycle.OPEN;
    }

    /**
     * Returns {@code true} while the queue is closed but not yet terminal. Consumers can still take
     * real elements in this state; {@link #shutdown()} is {@code true} and {@link #drained()} is
     * {@code false}. A concurrent consumer can make this result stale immediately by removing the
     * last element.
     */
    public boolean draining() {
        return lifecycle == Lifecycle.DRAINING;
    }

    /**
     * Returns {@code true} once the queue is closed and empty. This is the terminal state: no
     * element will ever be returned again, and consumer methods expose the configured terminal
     * signal (poison, {@code null}, or {@link NoSuchElementException}). It distinguishes a
     * permanently empty queue from an open queue that is only temporarily empty.
     */
    public boolean drained() {
        return lifecycle == Lifecycle.DRAINED;
    }

    /**
     * Blocks until the queue is {@link #drained() drained} (closed and empty), returning
     * immediately when the terminal state has already been reached. This observes only terminal
     * state publication; it does not wait for blocked callers released by the same transition to
     * finish returning to their callers. An external interruption takes precedence and throws
     * {@link InterruptedException}.
     */
    public void awaitDrained() throws InterruptedException {
        if (drained()) {
            return;
        }
        takeMonitor.enterWhen(drainedGuard);
        takeMonitor.leave();
    }

    /**
     * Waits at most the given timeout for the {@link #drained() drained} terminal state and
     * reports whether it was reached. Returns {@code true} immediately when already drained and
     * {@code false} only when the timeout elapses first. An external interruption takes precedence
     * and throws {@link InterruptedException}.
     */
    public boolean awaitDrained(long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(unit, "unit");
        if (drained()) {
            return true;
        }
        if (!takeMonitor.enterWhen(drainedGuard, timeout, unit)) {
            return false;
        }
        takeMonitor.leave();
        return true;
    }

    /**
     * Closes the queue to producers and begins the draining phase. Idempotent and non-blocking.
     *
     * <p>After this call, the {@code add}/{@code put}/{@code offer} family rejects new elements
     * (throwing {@link IllegalStateException} or returning {@code false}), while consumers
     * keep receiving real elements until the queue is empty, at which point the queue immediately
     * transitions to the {@link #drained() DRAINED} terminal state. If empty at the close
     * linearization point, this call publishes {@code DRAINED} itself. Waiting producers,
     * consumers, and lifecycle waiters are released promptly and re-evaluate their own rules
     * without needing the queue to be touched again. This method never interrupts user threads.
     */
    @Override
    public void close() {
        fullyLock();
        try {
            closeAdmission();
            if (lifecycle != Lifecycle.OPEN) {
                return;
            }
            lifecycle = Lifecycle.DRAINING;
            publishDrainedIfEmptyLocked();
        } finally {
            fullyUnlock();
        }
        signalTakeReady();
        signalPutReady();
    }

    // endregion

    // region Producer side

    /**
     * Inserts the element when capacity is immediately available, returning {@code false}
     * otherwise. Once {@link #close()} has been called this always returns {@code false} and never
     * stores an element. {@code null} and any value {@linkplain Object#equals(Object) equal} to a
     * configured poison are rejected before capacity or lifecycle is inspected.
     */
    @Override
    public boolean offer(E element) {
        requireElement(element);
        int oldCount = -1;
        putMonitor.enter();
        try {
            if (open() && count.get() < capacity) {
                last = last.next = new Node<>(element);
                oldCount = count.getAndIncrement();
            }
        } finally {
            putMonitor.leave();
        }
        if (oldCount == 0) {
            signalTakeReady();
        }
        return oldCount >= 0;
    }

    /**
     * Inserts the element, waiting until capacity is available. Once {@link #close()} has been
     * called this throws {@link IllegalStateException} immediately without waiting and without
     * storing the element. A call that was blocked before close is released, rechecks lifecycle,
     * and also throws rather than submitting. An external interruption takes precedence and throws
     * {@link InterruptedException}.
     */
    @Override
    public void put(E element) throws InterruptedException {
        requireElement(element);
        if (!beginBlockingCall()) {
            throw closedWrite("put");
        }
        int oldCount;
        putMonitor.enterWhen(putReady);
        try {
            if (!open()) {
                throw closedWrite("put");
            }
            last = last.next = new Node<>(element);
            oldCount = count.getAndIncrement();
        } finally {
            putMonitor.leave();
        }
        if (oldCount == 0) {
            signalTakeReady();
        }
    }

    /**
     * Inserts the element before the timeout expires, returning {@code false} on timeout. Once
     * {@link #close()} has been called this returns {@code false} immediately without waiting and
     * without storing the element. A call that was blocked before close is released, rechecks
     * lifecycle, and returns {@code false} without consuming the remaining timeout. An external
     * interruption takes precedence and throws {@link InterruptedException}.
     */
    @Override
    public boolean offer(E element, long timeout, TimeUnit unit) throws InterruptedException {
        requireElement(element);
        Objects.requireNonNull(unit, "unit");
        if (!beginBlockingCall()) {
            return false;
        }
        int oldCount;
        if (!putMonitor.enterWhen(putReady, timeout, unit)) {
            return false;
        }
        try {
            if (!open()) {
                return false;
            }
            last = last.next = new Node<>(element);
            oldCount = count.getAndIncrement();
        } finally {
            putMonitor.leave();
        }
        if (oldCount == 0) {
            signalTakeReady();
        }
        return true;
    }

    // endregion

    // region Consumer side

    /**
     * Removes and returns the head, or {@code null} when the queue is open and empty. After
     * {@link #close()} this keeps returning real elements while any remain; once drained it
     * returns the configured poison value, or {@code null} when no poison is configured. A
     * temporary empty {@code DRAINING} queue also returns {@code null}; use {@link #drained()} to
     * distinguish it from the terminal empty result.
     */
    @Override
    @Nullable
    public E poll() {
        E item = null;
        int oldCount = -1;
        takeMonitor.enter();
        try {
            if (count.get() > 0) {
                item = dequeue();
                oldCount = count.getAndDecrement();
                publishDrainedIfEmptyLocked();
            } else if (drained()) {
                return drainedSpecialValue();
            } else {
                return null;
            }
        } finally {
            takeMonitor.leave();
        }
        if (oldCount == capacity) {
            signalPutReady();
        }
        return item;
    }

    /**
     * Removes and returns the head, waiting until an element is available. After {@link #close()}
     * this keeps returning real elements while any remain; once drained it returns the configured
     * poison value or throws {@link NoSuchElementException} without waiting. A caller blocked
     * while open is released when close publishes {@code DRAINED}; an external interruption takes
     * precedence and throws {@link InterruptedException}.
     */
    @Override
    public E take() throws InterruptedException {
        E item;
        int oldCount = -1;
        takeMonitor.enterWhen(takeReady);
        try {
            if (count.get() > 0) {
                item = dequeue();
                oldCount = count.getAndDecrement();
                publishDrainedIfEmptyLocked();
            } else {
                return drainedRequiredValue("take");
            }
        } finally {
            takeMonitor.leave();
        }
        if (oldCount == capacity) {
            signalPutReady();
        }
        return item;
    }

    /**
     * Removes and returns the head before the timeout expires, or {@code null} on timeout. After
     * {@link #close()} this keeps returning real elements while any remain; once drained it
     * returns the configured poison value, or {@code null} when no poison is configured, without
     * waiting for the remainder of the timeout. While open, it waits only until an element arrives
     * or timeout expires; while draining, it waits only for a remaining element to be claimed by a
     * concurrent consumer or for terminal publication. An external interruption takes precedence
     * and throws {@link InterruptedException}.
     */
    @Override
    @Nullable
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(unit, "unit");
        E item;
        int oldCount = -1;
        if (!takeMonitor.enterWhen(takeReady, timeout, unit)) {
            return null;
        }
        try {
            if (count.get() > 0) {
                item = dequeue();
                oldCount = count.getAndDecrement();
                publishDrainedIfEmptyLocked();
            } else {
                return drainedSpecialValue();
            }
        } finally {
            takeMonitor.leave();
        }
        if (oldCount == capacity) {
            signalPutReady();
        }
        return item;
    }

    /**
     * Returns the head without removing it. Returns {@code null} when the queue is open and empty;
     * after {@link #close()} it keeps returning real elements while any remain, then the poison
     * value, or {@code null} without poison, once drained. The poison is not queued and this method
     * never removes it or a real element.
     */
    @Override
    @Nullable
    public E peek() {
        takeMonitor.enter();
        try {
            Node<E> first = head.next;
            if (first != null) {
                return first.item;
            }
            return drained() ? drainedSpecialValue() : null;
        } finally {
            takeMonitor.leave();
        }
    }

    // endregion

    // region Collection operations

    /**
     * Inserts the element, throwing {@link IllegalStateException} when the queue is full. After
     * {@link #close()} this throws {@link IllegalStateException} without storing anything. Like all
     * write paths, it rejects {@code null} and poison-equivalent elements before queue state is
     * inspected.
     */
    @Override
    public boolean add(E element) {
        requireElement(element);
        int oldCount = -1;
        putMonitor.enter();
        try {
            if (!open()) {
                throw closedWrite("add");
            }
            if (count.get() == capacity) {
                throw new IllegalStateException("Queue full");
            }
            last = last.next = new Node<>(element);
            oldCount = count.getAndIncrement();
        } finally {
            putMonitor.leave();
        }
        if (oldCount == 0) {
            signalTakeReady();
        }
        return true;
    }

    /**
     * Removes and returns the head, throwing {@link NoSuchElementException} when the queue is open
     * and empty. After {@link #close()} this keeps returning real elements while any remain; once
     * drained it returns the configured poison value or throws a {@link NoSuchElementException}
     * whose message begins {@code "queue is drained"}.
     */
    @Override
    public E remove() {
        E item;
        int oldCount = -1;
        takeMonitor.enter();
        try {
            if (count.get() > 0) {
                item = dequeue();
                oldCount = count.getAndDecrement();
                publishDrainedIfEmptyLocked();
            } else if (drained()) {
                return drainedRequiredValue("remove");
            } else {
                throw new NoSuchElementException();
            }
        } finally {
            takeMonitor.leave();
        }
        if (oldCount == capacity) {
            signalPutReady();
        }
        return item;
    }

    /**
     * Returns the head without removing it, throwing {@link NoSuchElementException} when the queue
     * is open and empty. After {@link #close()} this keeps returning real elements while any
     * remain; once drained it returns the configured poison value or throws a {@link
     * NoSuchElementException} whose message begins {@code "queue is drained"}.
     */
    @Override
    public E element() {
        takeMonitor.enter();
        try {
            Node<E> first = head.next;
            if (first != null) {
                return first.item;
            }
            if (drained()) {
                return drainedRequiredValue("element");
            }
            throw new NoSuchElementException();
        } finally {
            takeMonitor.leave();
        }
    }

    /**
     * Atomically inserts all elements as one batch, validating them before any queue state is
     * touched (user-supplied elements are never inspected inside a queue monitor). The batch is
     * all-or-nothing with respect to validation and remaining capacity: it returns {@code false}
     * for an empty source, or stores every supplied element, or stores none and throws {@link
     * IllegalStateException}. After {@link #close()} this throws {@link IllegalStateException}
     * without storing anything. Each source element, including poison-equivalent values, is
     * validated before lifecycle and capacity are inspected.
     */
    @Override
    public boolean addAll(Collection<? extends E> source) {
        Objects.requireNonNull(source, "source");
        List<E> additions = new ArrayList<>(source.size());
        // Element validation can invoke user equality code through the poison check; keep it out of
        // the producer monitor so an arbitrary collection cannot stall queue progress.
        for (E element : source) {
            additions.add(requireElement(element));
        }
        putMonitor.enter();
        try {
            if (!open()) {
                throw closedWrite("addAll");
            }
            if (additions.size() > capacity - count.get()) {
                throw new IllegalStateException("Queue full");
            }
            if (additions.isEmpty()) {
                return false;
            }
            for (E element : additions) {
                last = last.next = new Node<>(element);
            }
            count.getAndAdd(additions.size());
        } finally {
            putMonitor.leave();
        }
        signalTakeReady();
        return true;
    }

    /**
     * Discards every element. While the queue is draining this is legal and moves it straight to
     * the {@link #drained() DRAINED} terminal state; once drained it follows the policy's
     * {@code mutations} strategy (no-op or {@link IllegalStateException}). In the open state it
     * has normal {@link java.util.Collection#clear()} semantics and does not close production.
     */
    @Override
    public void clear() {
        fullyLock();
        try {
            requireMutationAllowed("clear");
            if (drainedNoopMutation()) {
                return;
            }
            if (count.get() > 0) {
                for (Node<E> node, trail = head; (node = trail.next) != null; trail = node) {
                    trail.next = trail;
                    node.item = null;
                }
                head = last;
                count.set(0);
                publishDrainedIfEmptyLocked();
            }
        } finally {
            fullyUnlock();
        }
        signalPutReady();
    }

    /**
     * Removes one element equal to the target, evaluating {@code target.equals} outside the queue
     * monitors. While the queue is draining this is legal; once drained it follows the policy's
     * {@code mutations} strategy (no-op returning {@code false}, or {@link
     * IllegalStateException}). It is a two-phase, weakly concurrent operation: a candidate removed
     * by another operation is skipped and the search continues.
     */
    @Override
    public boolean remove(@Nullable Object target) {
        if (target == null) {
            return false;
        }
        for (; ; ) {
            Node<E> candidate = findMatchingNode(target);
            if (candidate == null) {
                return false;
            }
            if (unlinkIfPresent(candidate)) {
                return true;
            }
        }
    }

    /**
     * Snapshots at most 64 live nodes at a time, then evaluates {@code target.equals} outside the
     * monitors so user code never runs in a critical section (contract §12.6). Bounds temporary
     * memory and lock hold time for a long unsuccessful search. The returned node may already be
     * unlinked by a concurrent consumer by the time it is used.
     */
    @Nullable
    private Node<E> findMatchingNode(Object target) {
        Node<E> cursor = null;
        @SuppressWarnings("unchecked")
        Node<E>[] nodes = (Node<E>[]) new Node<?>[TRAVERSAL_BATCH_SIZE];
        Object[] items = new Object[TRAVERSAL_BATCH_SIZE];
        int length;
        do {
            fullyLock();
            try {
                requireMutationAllowed("remove");
                if (drainedNoopMutation()) {
                    return null;
                }
                if (cursor == null) {
                    cursor = head.next;
                }
                for (length = 0; cursor != null && length < nodes.length; cursor = successor(cursor)) {
                    nodes[length] = cursor;
                    items[length] = cursor.item;
                    length++;
                }
            } finally {
                fullyUnlock();
            }

            for (int index = 0; index < length; index++) {
                if (target.equals(items[index])) {
                    return nodes[index];
                }
            }
            clearBatch(nodes, length);
            clearBatch(items, length);
        } while (length > 0 && cursor != null);
        return null;
    }

    /** Unlinks a snapshot node by identity if it is still part of the live queue. */
    private boolean unlinkIfPresent(Node<E> candidate) {
        boolean changed = false;
        fullyLock();
        try {
            requireMutationAllowed("remove");
            if (drainedNoopMutation()) {
                return false;
            }
            for (Node<E> trail = head, node = trail.next; node != null; trail = node, node = node.next) {
                if (node == candidate) {
                    unlink(trail, node);
                    publishDrainedIfEmptyLocked();
                    changed = true;
                    break;
                }
            }
        } finally {
            fullyUnlock();
        }
        if (changed) {
            signalPutReady();
        }
        return changed;
    }

    /**
     * Removes every element matching the predicate in batches of at most 64 nodes. Each batch is
     * snapshotted under both queue monitors, tested outside the monitors, and then revalidated and
     * unlinked under both monitors. This bounds lock hold time and keeps user code out of critical
     * sections. The operation is deliberately not globally atomic: if a later predicate call
     * throws, earlier committed batches remain removed. If it removes the final element while
     * draining, that same commit publishes {@code DRAINED}; a concurrent transition to {@code
     * DRAINED} then applies the configured mutation strategy on later batches.
     */
    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        Objects.requireNonNull(filter, "filter");
        return bulkRemove(filter, "removeIf");
    }

    private boolean bulkRemove(Predicate<? super E> filter, String operation) {
        boolean removed = false;
        Node<E> cursor = null;
        Node<E> ancestor = head;
        @SuppressWarnings("unchecked")
        Node<E>[] nodes = (Node<E>[]) new Node<?>[TRAVERSAL_BATCH_SIZE];
        int length;
        do {
            fullyLock();
            try {
                requireMutationAllowed(operation);
                if (drainedNoopMutation()) {
                    return removed;
                }
                if (cursor == null) {
                    cursor = head.next;
                }
                for (length = 0; cursor != null && length < nodes.length; cursor = successor(cursor)) {
                    nodes[length++] = cursor;
                }
            } finally {
                fullyUnlock();
            }

            long matched = 0L;
            for (int index = 0; index < length; index++) {
                E item = nodes[index].item;
                if (item != null && filter.test(item)) {
                    matched |= 1L << index;
                }
            }

            boolean changed = false;
            if (matched != 0L) {
                fullyLock();
                try {
                    requireMutationAllowed(operation);
                    if (drainedNoopMutation()) {
                        return removed;
                    }
                    for (int index = 0; index < length; index++) {
                        Node<E> node = nodes[index];
                        if ((matched & (1L << index)) != 0L && node.item != null) {
                            ancestor = findPredecessor(node, ancestor);
                            unlink(ancestor, node);
                            changed = true;
                        }
                    }
                    if (changed) {
                        publishDrainedIfEmptyLocked();
                    }
                } finally {
                    fullyUnlock();
                }
            }
            clearBatch(nodes, length);
            if (changed) {
                removed = true;
                signalPutReady();
            }
        } while (length > 0 && cursor != null);
        return removed;
    }

    private void clearBatch(Node<E>[] nodes, int length) {
        for (int index = 0; index < length; index++) {
            nodes[index] = null;
        }
    }

    private void clearBatch(Object[] values, int length) {
        for (int index = 0; index < length; index++) {
            values[index] = null;
        }
    }

    /** Returns the node after {@code node}, following LBQ's self-link convention. */
    @Nullable
    private Node<E> successor(Node<E> node) {
        Node<E> next = node.next;
        return next == node ? head.next : next;
    }

    /** Finds a live node's predecessor, reusing an earlier ancestor when possible. */
    private Node<E> findPredecessor(Node<E> node, Node<E> ancestor) {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(ancestor, "ancestor");
        if (ancestor.item == null) {
            ancestor = head;
        }
        for (Node<E> candidate; (candidate = ancestor.next) != node; ancestor = candidate) {
            // The node is known live and linked while both monitors are held.
        }
        return ancestor;
    }

    /**
     * Removes every element contained in the source collection; equivalent to {@code removeIf}
     * with {@code source::contains}, evaluated outside the queue monitors. It has the same batched,
     * non-transactional concurrency boundary as {@link #removeIf(Predicate)} and the same
     * draining/terminal mutation behavior.
     */
    @Override
    public boolean removeAll(Collection<?> source) {
        Objects.requireNonNull(source, "source");
        return bulkRemove(source::contains, "removeAll");
    }

    /**
     * Retains only the elements contained in the source collection. It has the same batched,
     * non-transactional concurrency boundary as {@link #removeIf(Predicate)} and the same
     * draining/terminal mutation behavior.
     */
    @Override
    public boolean retainAll(Collection<?> source) {
        Objects.requireNonNull(source, "source");
        return bulkRemove(value -> !source.contains(value), "retainAll");
    }

    /**
     * Removes every element into the target collection. Unlike every other mutation, this remains
     * available in every lifecycle state: after {@link #close()} it drains the remaining elements
     * and once drained it returns {@code 0}. It is ordinary removal, not a recovery channel: close
     * never isolates elements. The target collection receives each element after it was removed
     * from this queue and outside its monitors; target failures therefore do not roll back already
     * transferred ownership.
     */
    @Override
    public int drainTo(Collection<? super E> target) {
        return drainTo(target, Integer.MAX_VALUE);
    }

    /**
     * Removes up to {@code maxElements} elements into the target collection. A non-positive bound
     * returns {@code 0}; a target equal to this queue is rejected. If this call removes the final
     * real element while draining, it publishes {@code DRAINED} before transferring elements to
     * the target. See {@link #drainTo(Collection)} for the lifecycle and ownership behavior.
     */
    @Override
    public int drainTo(Collection<? super E> target, int maxElements) {
        Objects.requireNonNull(target, "target");
        if (target == this) {
            throw new IllegalArgumentException("cannot drain a queue into itself");
        }
        if (maxElements <= 0) {
            return 0;
        }
        List<E> drainedElements = new ArrayList<>();
        int amount;
        takeMonitor.enter();
        try {
            amount = Math.min(maxElements, count.get());
            for (int index = 0; index < amount; index++) {
                drainedElements.add(dequeue());
            }
            if (amount > 0) {
                count.getAndAdd(-amount);
                publishDrainedIfEmptyLocked();
            }
        } finally {
            takeMonitor.leave();
        }
        if (amount > 0) {
            signalPutReady();
        }
        for (E value : drainedElements) {
            target.add(value);
        }
        return drainedElements.size();
    }

    // endregion

    // region Node primitives

    /** Removes and returns the first element; caller must hold the consumer monitor. */
    private E dequeue() {
        Node<E> oldHead = head;
        Node<E> first = Objects.requireNonNull(oldHead.next, "queue is empty");
        oldHead.next = oldHead;
        head = first;
        E item = first.item;
        first.item = null;
        return item;
    }

    /** Prepends one node; caller must hold both monitors. */
    private void prepend(Node<E> node) {
        node.next = head.next;
        head.next = node;
        if (last == head) {
            last = node;
        }
    }

    /** Unlinks one known live node; caller must hold both monitors. Returns the removed item. */
    private E unlink(Node<E> trail, Node<E> node) {
        E item = node.item;
        node.item = null;
        trail.next = node.next;
        if (last == node) {
            last = trail;
        }
        count.getAndDecrement();
        return item;
    }

    /** Unlinks the tail node; caller must hold both monitors. */
    private E unlinkLast() {
        Node<E> trail = head;
        Node<E> node = Objects.requireNonNull(trail.next, "queue is empty");
        while (node.next != null) {
            trail = node;
            node = node.next;
        }
        return unlink(trail, node);
    }

    // endregion

    // region Queries and iteration

    /**
     * Returns the current number of real queued elements. The value remains honest while draining
     * and reaches zero only when storage is empty; poison is virtual and is never counted.
     */
    @Override
    public int size() {
        return count.get();
    }

    /**
     * Returns the number of elements that can be inserted without waiting, or {@code 0} once
     * {@link #close()} has been called (production is permanently closed). A positive result while
     * open is only an observation and does not reserve capacity for a later producer.
     */
    @Override
    public int remainingCapacity() {
        return open() ? capacity - count.get() : 0;
    }

    /**
     * Returns a FIFO reference snapshot of the real elements live while both queue monitors were
     * held. The returned array is independent of subsequent queue changes; it never contains the
     * virtual poison signal.
     */
    @Override
    public Object[] toArray() {
        fullyLock();
        try {
            Object[] elements = new Object[count.get()];
            int index = 0;
            for (Node<E> node = head.next; node != null; node = node.next) {
                elements[index++] = node.item;
            }
            return elements;
        } finally {
            fullyUnlock();
        }
    }

    /**
     * Returns a FIFO snapshot in the requested array type. The queue is only locked while element
     * references are copied; user-owned array operations happen after that snapshot is complete.
     * Like {@link #toArray()}, this exposes only real elements and never poison.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] destination) {
        Objects.requireNonNull(destination, "destination");
        Object[] snapshot = toArray();
        T[] result = destination.length >= snapshot.length
                ? destination
                : (T[]) java.lang.reflect.Array.newInstance(
                        destination.getClass().getComponentType(), snapshot.length);
        for (int index = 0; index < snapshot.length; index++) {
            result[index] = (T) snapshot[index];
        }
        if (result.length > snapshot.length) {
            result[snapshot.length] = null;
        }
        return result;
    }

    /**
     * Formats a point-in-time element-reference snapshot. Element {@code toString()} methods run
     * after the queue monitors have been released and therefore cannot delay queue operations.
     */
    @Override
    public String toString() {
        Object[] snapshot = toArray();
        if (snapshot.length == 0) {
            return "[]";
        }
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        for (int index = 0; index < snapshot.length; index++) {
            if (index > 0) {
                builder.append(", ");
            }
            Object element = snapshot[index];
            builder.append(element == this ? "(this Collection)" : element);
        }
        return builder.append(']').toString();
    }

    /**
     * Runs the action on elements observed during a weakly consistent traversal. Concurrent queue
     * changes can cause an element to be observed or skipped, but no element is delivered twice by
     * this traversal and poison is never delivered. At most 64 element references are copied while
     * holding the queue monitors; the action is always invoked after they are released.
     */
    @Override
    public void forEach(Consumer<? super E> action) {
        Objects.requireNonNull(action, "action");
        forEachFrom(action, null);
    }

    private void forEachFrom(Consumer<? super E> action, @Nullable Node<E> cursor) {
        Object[] elements = null;
        int length = 0;
        do {
            int amount = 0;
            fullyLock();
            try {
                if (elements == null) {
                    if (cursor == null) {
                        cursor = head.next;
                    }
                    for (Node<E> node = cursor; node != null; node = successor(node)) {
                        if (node.item != null && ++length == TRAVERSAL_BATCH_SIZE) {
                            break;
                        }
                    }
                    elements = new Object[length];
                }
                while (cursor != null && amount < length) {
                    E item = cursor.item;
                    cursor = successor(cursor);
                    if (item != null) {
                        elements[amount++] = item;
                    }
                }
            } finally {
                fullyUnlock();
            }
            for (int index = 0; index < amount; index++) {
                @SuppressWarnings("unchecked")
                E item = (E) elements[index];
                action.accept(item);
            }
        } while (length > 0 && cursor != null);
    }

    /**
     * Returns a weakly consistent, late-binding spliterator with the same characteristics as
     * {@link java.util.concurrent.LinkedBlockingQueue}. Elements concurrently added or removed may
     * be observed or skipped, but are never duplicated, and poison is never exposed. Streams
     * obtained from this queue inherit those traversal semantics.
     */
    @Override
    public Spliterator<E> spliterator() {
        return new QueueSpliterator();
    }

    private final class QueueSpliterator implements Spliterator<E> {
        @Nullable
        private Node<E> current;

        private int batch;
        private boolean exhausted;
        private long estimatedSize = size();

        @Override
        public long estimateSize() {
            return estimatedSize;
        }

        @Override
        public Spliterator<E> trySplit() {
            if (exhausted) {
                return null;
            }
            int splitSize = batch = Math.min(batch + 1, MAX_SPLITERATOR_BATCH);
            Object[] elements = new Object[splitSize];
            int amount = 0;
            fullyLock();
            try {
                Node<E> node = current == null ? head.next : current;
                while (node != null && amount < splitSize) {
                    E item = node.item;
                    node = successor(node);
                    if (item != null) {
                        elements[amount++] = item;
                    }
                }
                current = node;
                if (node == null) {
                    exhausted = true;
                    estimatedSize = 0L;
                } else if ((estimatedSize -= amount) < 0L) {
                    estimatedSize = 0L;
                }
            } finally {
                fullyUnlock();
            }
            return amount == 0 ? null : Spliterators.spliterator(elements, 0, amount, characteristics());
        }

        @Override
        public boolean tryAdvance(Consumer<? super E> action) {
            Objects.requireNonNull(action, "action");
            if (exhausted) {
                return false;
            }
            E item = null;
            fullyLock();
            try {
                Node<E> node = current == null ? head.next : current;
                while (node != null && item == null) {
                    item = node.item;
                    node = successor(node);
                }
                current = node;
                if (node == null) {
                    exhausted = true;
                }
            } finally {
                fullyUnlock();
            }
            if (item == null) {
                return false;
            }
            action.accept(item);
            return true;
        }

        @Override
        public void forEachRemaining(Consumer<? super E> action) {
            Objects.requireNonNull(action, "action");
            if (!exhausted) {
                exhausted = true;
                Node<E> cursor = current;
                current = null;
                forEachFrom(action, cursor);
            }
        }

        @Override
        public int characteristics() {
            return Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.CONCURRENT;
        }
    }

    /**
     * Returns a weakly consistent FIFO iterator over the live node chain, shaped after {@link
     * java.util.concurrent.LinkedBlockingQueue}'s iterator. Elements enqueued or dequeued after the
     * iterator is created may or may not be reflected. While the queue is draining the iterator
     * observes the remaining elements; once drained it observes none (the poison value is a
     * virtual signal, never an element). {@link Iterator#remove()} removes the last returned
     * element from the queue itself and follows the same draining/closed mutation rules as {@link
     * #remove(Object)}.
     */
    @Override
    public Iterator<E> iterator() {
        return new Itr();
    }

    /** Weakly consistent iterator over the live node chain; see {@link #iterator()}. */
    private final class Itr implements Iterator<E> {
        @Nullable
        private Node<E> next;

        @Nullable
        private E nextItem;

        @Nullable
        private Node<E> lastReturned;

        /** Lazily maintained predecessor for expected constant-time consecutive removals. */
        @Nullable
        private Node<E> ancestor;

        /** Captures the current first node while both storage monitors are occupied. */
        Itr() {
            fullyLock();
            try {
                next = head.next;
                if (next != null) {
                    nextItem = next.item;
                }
            } finally {
                fullyUnlock();
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public E next() {
            Node<E> node = next;
            if (node == null) {
                throw new NoSuchElementException();
            }
            E item = nextItem;
            lastReturned = node;
            fullyLock();
            try {
                E successorItem = null;
                for (node = node.next; node != null && (successorItem = node.item) == null; node = successor(node)) {
                    // Skip nodes concurrently removed before this traversal step.
                }
                next = node;
                nextItem = successorItem;
            } finally {
                fullyUnlock();
            }
            return item;
        }

        @Override
        public void forEachRemaining(Consumer<? super E> action) {
            Objects.requireNonNull(action, "action");
            Node<E> cursor = next;
            if (cursor == null) {
                return;
            }
            lastReturned = cursor;
            next = null;
            Object[] elements = null;
            int length = 1;
            int amount;
            do {
                fullyLock();
                try {
                    if (elements == null) {
                        cursor = cursor.next;
                        for (Node<E> node = cursor; node != null; node = successor(node)) {
                            if (node.item != null && ++length == TRAVERSAL_BATCH_SIZE) {
                                break;
                            }
                        }
                        elements = new Object[length];
                        elements[0] = nextItem;
                        nextItem = null;
                        amount = 1;
                    } else {
                        amount = 0;
                    }
                    while (cursor != null && amount < length) {
                        E item = cursor.item;
                        Node<E> node = cursor;
                        cursor = successor(cursor);
                        if (item != null) {
                            elements[amount++] = item;
                            lastReturned = node;
                        }
                    }
                } finally {
                    fullyUnlock();
                }
                for (int index = 0; index < amount; index++) {
                    @SuppressWarnings("unchecked")
                    E item = (E) elements[index];
                    action.accept(item);
                }
            } while (amount > 0 && cursor != null);
        }

        /** Removes the last returned node by identity when it remains linked to the live queue. */
        @Override
        public void remove() {
            Node<E> node = lastReturned;
            if (node == null) {
                throw new IllegalStateException();
            }
            lastReturned = null;
            boolean changed = false;
            fullyLock();
            try {
                requireMutationAllowed("iterator remove");
                if (drainedNoopMutation()) {
                    return;
                }
                if (node.item != null) {
                    if (ancestor == null) {
                        ancestor = head;
                    }
                    ancestor = findPredecessor(node, ancestor);
                    unlink(ancestor, node);
                    publishDrainedIfEmptyLocked();
                    changed = true;
                }
            } finally {
                fullyUnlock();
            }
            if (changed) {
                signalPutReady();
            }
        }
    }

    // endregion

    // region Shutdown policy

    /** Behavior of ordinary collection mutations after the queue has drained. */
    public enum MutationsStrategy {
        /** Terminal mutations report no change and leave the already empty queue untouched. */
        NOOP,

        /** Terminal mutations throw {@link IllegalStateException} to make a stale mutation visible. */
        THROW
    }

    /**
     * Immutable terminal-state behavior for the enclosing queue.
     *
     * <p>The dimensions are independent: {@code poison} controls what value-returning consumer
     * methods observe only after the queue is {@code DRAINED}; {@link MutationsStrategy} controls
     * {@code clear}, {@code remove}, {@code removeIf}, {@code removeAll}, and {@code retainAll} only
     * after that same terminal state. Neither setting changes the rule that consumers receive all real
     * queued elements while the queue is {@code DRAINING}. The poison is a virtual signal, never a
     * stored element, and queue write paths reject elements equal to it.
     */
    public static final class ShutdownPolicy<E> {

        @Nullable
        private final E poison;

        private final MutationsStrategy mutationsStrategy;

        private ShutdownPolicy(@Nullable E poison, MutationsStrategy mutationsStrategy) {
            this.poison = poison;
            this.mutationsStrategy = Objects.requireNonNull(mutationsStrategy, "mutationsStrategy");
        }

        /**
         * Returns the default: no poison, with terminal mutations as no-ops. At {@code DRAINED},
         * special-value reads return {@code null}, required-value reads throw, and ordinary mutations
         * report no change.
         */
        public static <E> ShutdownPolicy<E> empty() {
            return new ShutdownPolicy<>(null, MutationsStrategy.NOOP);
        }

        /**
         * Returns a policy that emits {@code poison} from every value-returning consumer method after
         * {@code DRAINED}, while terminal mutations remain no-ops. The poison must be non-null.
         */
        public static <E> ShutdownPolicy<E> poison(E poison) {
            return new ShutdownPolicy<>(Objects.requireNonNull(poison, "poison"), MutationsStrategy.NOOP);
        }

        /**
         * Returns a policy with no poison that rejects ordinary mutations after {@code DRAINED}.
         * Required-value consumers still throw {@link java.util.NoSuchElementException}, and
         * special-value consumers still return {@code null}.
         */
        public static <E> ShutdownPolicy<E> throwing() {
            return new ShutdownPolicy<>(null, MutationsStrategy.THROW);
        }

        /** Returns a builder for independently choosing the terminal signal and mutation behavior. */
        public static <E> Builder<E> builder() {
            return new Builder<>();
        }

        @Nullable
        E poison() {
            return poison;
        }

        MutationsStrategy mutationsStrategy() {
            return mutationsStrategy;
        }

        /**
         * Builder for independent poison and mutation choices. Unset choices use the same defaults as
         * {@link ShutdownPolicy#empty()}: no poison and {@link MutationsStrategy#NOOP}.
         */
        public static final class Builder<E> {
            @Nullable
            private E poison;

            private MutationsStrategy mutationsStrategy = MutationsStrategy.NOOP;

            /**
             * Configures the non-null virtual signal returned only after {@code DRAINED}. It does not
             * cause {@code DRAINING} consumers to stop receiving real queued elements.
             */
            public Builder<E> poison(E poison) {
                this.poison = Objects.requireNonNull(poison, "poison");
                return this;
            }

            /**
             * Configures the behavior of ordinary collection mutations only after {@code DRAINED};
             * mutations remain valid while {@code DRAINING} regardless of this choice.
             */
            public Builder<E> mutations(MutationsStrategy mutationsStrategy) {
                this.mutationsStrategy = Objects.requireNonNull(mutationsStrategy, "mutationsStrategy");
                return this;
            }

            /** Returns the immutable policy represented by the current independent choices. */
            public ShutdownPolicy<E> build() {
                return new ShutdownPolicy<>(poison, mutationsStrategy);
            }
        }
    }

    // endregion

}
