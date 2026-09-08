package io.github.huatalk.parallelinscope.queue;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises VariableLinkedBlockingQueue's capacity adjustment and two-lock signalling contracts.
 *
 * <p>The blocking tests run a waiter thread against a mutating main thread and first confirm the
 * waiter is genuinely parked (thread state {@code WAITING}) before acting; a mutation that drops a
 * signal, an await, or a lock release leaves the waiter permanently parked, which fails the test
 * deadline. Lock-leak probes follow the same shape: after a mutation that removes an {@code unlock}
 * the next cross-thread call on the same lock can never complete.
 */
class VariableLinkedBlockingQueueTest {

    private static final long TIMEOUT_SECONDS = 10;

    private final ExecutorService pool = Executors.newCachedThreadPool();

    @AfterEach
    void shutdownPool() {
        pool.shutdownNow();
    }

    // ==================== construction & capacity ====================

    @Test
    void constructor_nonPositiveCapacity_rejected() {
        assertThrows(IllegalArgumentException.class, () -> new VariableLinkedBlockingQueue<>(0));
        assertThrows(IllegalArgumentException.class, () -> new VariableLinkedBlockingQueue<>(-1));
    }

    @Test
    void constructor_withCollection_preservesOrderAndSize() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(Arrays.asList("a", "b", "c"));
        assertEquals(3, queue.size());
        assertEquals("a", queue.poll());
        assertEquals("b", queue.poll());
        assertEquals("c", queue.poll());
        assertNull(queue.poll());
    }

    @Test
    void constructor_withNullElement_rejected() {
        List<String> withNull = new ArrayList<>();
        withNull.add("a");
        withNull.add(null);
        assertThrows(NullPointerException.class, () -> new VariableLinkedBlockingQueue<>(withNull));
    }

    @Test
    void setCapacity_nonPositive_rejected() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(2);
        assertThrows(IllegalArgumentException.class, () -> queue.setCapacity(0));
        assertThrows(IllegalArgumentException.class, () -> queue.setCapacity(-3));
        assertEquals(2, queue.capacity());
    }

    /**
     * Shrinking below the current size keeps existing elements and makes remainingCapacity negative;
     * note the current implementation only rejects offers at exactly {@code size == capacity}, so a
     * shrunk queue keeps accepting elements until size catches up with the new capacity.
     */
    @Test
    void setCapacity_shrink_belowSize_keepsExistingElements() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(3);
        queue.offer("a");
        queue.offer("b");
        queue.setCapacity(1);
        assertEquals(1, queue.capacity());
        assertEquals(2, queue.size());
        assertEquals(-1, queue.remainingCapacity());
        assertTrue(queue.offer("c"));
        assertEquals(3, queue.size());
    }

    @Test
    void setCapacity_expand_acceptsFurtherOffers() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(1);
        queue.offer("a");
        assertFalse(queue.offer("b"));
        queue.setCapacity(2);
        assertTrue(queue.offer("b"));
        assertEquals(2, queue.size());
    }

    @Test
    void containsNullIsFalseAndPollPreservesFifoForMultipleElements() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(3);
        queue.offer("first");
        queue.offer("second");

        assertFalse(queue.contains(null));
        assertEquals("first", queue.poll());
        assertEquals("second", queue.poll());
    }

    // ==================== blocking put/take signalling ====================

    /**
     * A putter parked on a full queue must be released when a take frees a slot; the take path's
     * {@code signalNotFull} (signal and lock release) is what wakes it.
     */
    @Test
    void put_blockedOnFullQueue_completesAfterTake() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(1);
        queue.offer("occupied");

        BlockedCall<String> putter = BlockedCall.start(pool, () -> {
            queue.put("x");
            return "done";
        });
        putter.awaitParked();

        assertEquals("occupied", queue.take());
        assertEquals("done", putter.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals("x", queue.poll());
    }

    /**
     * A putter parked on a full queue must be released by capacity expansion; setCapacity's notFull
     * signal (and its conditional guard) is what wakes it.
     */
    @Test
    void put_blockedOnFullQueue_completesAfterCapacityExpansion() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(1);
        queue.offer("occupied");

        BlockedCall<String> putter = BlockedCall.start(pool, () -> {
            queue.put("x");
            return "done";
        });
        putter.awaitParked();

        queue.setCapacity(2);
        assertEquals("done", putter.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    /**
     * A taker parked on an empty queue must be released by the first offer; the offer's {@code
     * signalNotEmpty} (signal and lock release) is what wakes it.
     */
    @Test
    void take_blockedOnEmptyQueue_completesAfterOffer() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);

        BlockedCall<String> taker = BlockedCall.start(pool, () -> queue.take());
        taker.awaitParked();

        queue.offer("x");
        assertEquals("x", taker.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(0, queue.size());
    }

    /** A parked taker must also be woken by the timed offer variant (its signalNotEmpty path). */
    @Test
    void take_blockedOnEmptyQueue_completesAfterTimedOffer() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);

        BlockedCall<String> taker = BlockedCall.start(pool, () -> queue.take());
        taker.awaitParked();

        assertTrue(queue.offer("x", TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals("x", taker.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    /** A parked taker must also be woken by put() (its signalNotEmpty path). */
    @Test
    void take_blockedOnEmptyQueue_completesAfterPut() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);

        BlockedCall<String> taker = BlockedCall.start(pool, () -> queue.take());
        taker.awaitParked();

        queue.put("x");
        assertEquals("x", taker.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    /**
     * The second parked taker is released by the first take's {@code c > 1} notEmpty signal chain;
     * dropping that signal strands it even though the second offer's own {@code signalNotEmpty} (c ==
     * 0 only) cannot reach it.
     */
    @Test
    void secondBlockedTaker_completesAfterTwoOffers() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);

        BlockedCall<String> takerA = BlockedCall.start(pool, () -> queue.take());
        BlockedCall<String> takerB = BlockedCall.start(pool, () -> queue.take());
        takerA.awaitParked();
        takerB.awaitParked();

        queue.offer("a");
        queue.offer("b");
        assertEquals("a", takerA.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals("b", takerB.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    /**
     * Two putters parked on a full queue must both complete after clear() drains it: the first is
     * woken by clear's signalNotFull, the second by the first put's own {@code c + 1 < capacity}
     * notFull signal chain.
     */
    @Test
    void twoBlockedPutters_bothCompleteAfterClear() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(2);
        queue.offer("a");
        queue.offer("b");

        BlockedCall<String> putterA = BlockedCall.start(pool, () -> {
            queue.put("x");
            return "done";
        });
        BlockedCall<String> putterB = BlockedCall.start(pool, () -> {
            queue.put("y");
            return "done";
        });
        putterA.awaitParked();
        putterB.awaitParked();

        queue.clear();
        assertEquals("done", putterA.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals("done", putterB.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(2, queue.size());
    }

    /** Same signal chain as above, with drainTo freeing two slots at once. */
    @Test
    void twoBlockedPutters_bothCompleteAfterDrain() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(3);
        queue.offer("a");
        queue.offer("b");
        queue.offer("c");

        BlockedCall<String> putterA = BlockedCall.start(pool, () -> {
            queue.put("x");
            return "done";
        });
        BlockedCall<String> putterB = BlockedCall.start(pool, () -> {
            queue.put("y");
            return "done";
        });
        putterA.awaitParked();
        putterB.awaitParked();

        List<String> drained = new ArrayList<>();
        assertEquals(2, queue.drainTo(drained, 2));
        assertEquals(Arrays.asList("a", "b"), drained);
        assertEquals("done", putterA.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals("done", putterB.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    /** A timed putter must be woken when a take frees a slot (signalNotFull). */
    @Test
    void timedOffer_blockedOnFullQueue_completesAfterTake() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(1);
        queue.offer("occupied");

        BlockedCall<Boolean> putter =
                BlockedCall.start(pool, () -> queue.offer("x", TIMEOUT_SECONDS, TimeUnit.SECONDS));
        putter.awaitParked();

        queue.take();
        assertTrue(
                putter.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                "timed offer must complete after space appears");
        assertEquals("x", queue.poll(), "timed offer must have enqueued its element");
    }

    /** A putter parked on a full queue must be woken by poll() (its signalNotFull path). */
    @Test
    void put_blockedOnFullQueue_completesAfterPoll() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(1);
        queue.offer("occupied");

        BlockedCall<String> putter = BlockedCall.start(pool, () -> {
            queue.put("x");
            return "done";
        });
        putter.awaitParked();

        assertEquals("occupied", queue.poll());
        assertEquals("done", putter.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    /** A putter parked on a full queue must be woken by the timed poll (its signalNotFull path). */
    @Test
    void put_blockedOnFullQueue_completesAfterTimedPoll() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(1);
        queue.offer("occupied");

        BlockedCall<String> putter = BlockedCall.start(pool, () -> {
            queue.put("x");
            return "done";
        });
        putter.awaitParked();

        assertEquals("occupied", queue.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals("done", putter.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    /**
     * A putter parked on a full queue must be woken when remove frees a slot (unlink's {@code count
     * == capacity} signalNotFull).
     */
    @Test
    void put_blockedOnFullQueue_completesAfterRemove() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(1);
        queue.offer("occupied");

        BlockedCall<String> putter = BlockedCall.start(pool, () -> {
            queue.put("x");
            return "done";
        });
        putter.awaitParked();

        assertTrue(queue.remove("occupied"));
        assertEquals("done", putter.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    // ==================== lock-leak probes ====================
    // A mutation removing an unlock/fullyUnlock leaks the lock; a second cross-thread call
    // on the same lock then never completes, so asserting its completion kills the mutation.

    @Test
    void leak_fromCollectionConstructor_blocksOtherOffer() throws Exception {
        // The probe must run against the same queue instance whose constructor ran.
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(Arrays.asList("a"));
        assertOtherThreadOfferCompletes(queue);
    }

    @Test
    void leak_fromTimedOffer_blocksOtherOffer() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a", TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertOtherThreadOfferCompletes(queue);
    }

    @Test
    void leak_fromPlainOffer_blocksOtherOffer() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        assertOtherThreadOfferCompletes(queue);
    }

    @Test
    void leak_fromPut_blocksOtherOffer() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.put("a");
        assertOtherThreadOfferCompletes(queue);
    }

    @Test
    void leak_fromPeek_blocksOtherTake() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.peek();
        assertOtherThreadTakeCompletes(queue);
    }

    @Test
    void leak_fromPoll_blocksOtherTake() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertOtherThreadTakeCompletes(queue);
    }

    @Test
    void leak_fromPlainPoll_blocksOtherTake() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.poll();
        assertOtherThreadTakeCompletes(queue);
    }

    @Test
    void leak_fromTake_blocksOtherTake() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.take();
        assertOtherThreadTakeCompletes(queue);
    }

    @Test
    void leak_fromDrainTo_blocksOtherTake() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.drainTo(new ArrayList<>());
        assertOtherThreadTakeCompletes(queue);
    }

    @Test
    void leak_fromRemove_blocksOtherOffer() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.remove("a");
        assertOtherThreadOfferCompletes(queue);
    }

    @Test
    void leak_fromContains_blocksOtherOffer() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.contains("a");
        assertOtherThreadOfferCompletes(queue);
    }

    @Test
    void leak_fromToArray_blocksOtherOffer() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.toArray();
        assertOtherThreadOfferCompletes(queue);
    }

    @Test
    void leak_fromTypedToArray_blocksOtherOffer() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.toArray(new String[1]);
        assertOtherThreadOfferCompletes(queue);
    }

    @Test
    void leak_fromIteratorConstructor_blocksOtherOffer() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        Iterator<String> it = queue.iterator();
        it.hasNext();
        assertOtherThreadOfferCompletes(queue);
    }

    @Test
    void leak_fromIteratorNext_blocksOtherOffer() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        Iterator<String> it = queue.iterator();
        it.next();
        assertOtherThreadOfferCompletes(queue);
    }

    @Test
    void leak_fromIteratorRemove_blocksOtherOffer() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        Iterator<String> it = queue.iterator();
        it.next();
        it.remove();
        assertOtherThreadOfferCompletes(queue);
    }

    // ==================== non-blocking & timed operations ====================

    @Test
    void offer_nonBlocking_full_returnsFalse() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(1);
        assertTrue(queue.offer("a"));
        assertFalse(queue.offer("b"));
    }

    @Test
    void offer_null_rejected() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(1);
        assertThrows(NullPointerException.class, () -> queue.offer(null));
        assertThrows(NullPointerException.class, () -> queue.put(null));
        assertThrows(NullPointerException.class, () -> queue.offer(null, 1, TimeUnit.SECONDS));
    }

    @Test
    void timedOffer_onFullQueue_expiresWithFalse() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(1);
        queue.offer("a");
        assertFalse(queue.offer("b", 50, TimeUnit.MILLISECONDS));
    }

    @Test
    void timedOffer_zeroTimeout_returnsFalseImmediately() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(1);
        queue.offer("a");
        assertFalse(queue.offer("b", 0, TimeUnit.MILLISECONDS));
    }

    @Test
    void timedPoll_zeroTimeout_returnsNullImmediately() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(1);
        assertNull(queue.poll(0, TimeUnit.MILLISECONDS));
    }

    @Test
    void poll_empty_returnsNull() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(1);
        assertNull(queue.poll());
    }

    @Test
    void timedPoll_emptyQueue_expiresWithNull() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(1);
        assertNull(queue.poll(50, TimeUnit.MILLISECONDS));
    }

    @Test
    void poll_afterOffer_returnsFifoOrder() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.offer("b");
        assertEquals("a", queue.poll());
        assertEquals("b", queue.poll());
        assertNull(queue.poll());
    }

    @Test
    void timedPoll_blockedOnEmptyQueue_returnsAfterOffer() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(1);

        BlockedCall<String> poller = BlockedCall.start(pool, () -> queue.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        poller.awaitParked();

        queue.offer("x");
        assertEquals("x", poller.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void peek_and_remainingCapacity_roundTrip() throws Exception {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(2);
        assertNull(queue.peek());
        assertEquals(2, queue.remainingCapacity());
        queue.offer("a");
        assertEquals("a", queue.peek());
        assertEquals("a", queue.peek(), "peek must not consume");
        assertEquals(1, queue.remainingCapacity());
        assertEquals("a", queue.take());
        assertEquals(2, queue.remainingCapacity());
    }

    // ==================== removal, contains, clear, drainTo ====================

    @Test
    void remove_object_present_removesFirstMatch() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.offer("b");
        queue.offer("a");
        assertTrue(queue.remove("a"));
        assertEquals(Arrays.asList("b", "a"), toList(queue));
    }

    @Test
    void remove_object_absentOrNull_returnsFalse() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(2);
        queue.offer("a");
        assertFalse(queue.remove("b"));
        assertFalse(queue.remove(null));
    }

    @Test
    void remove_lastElement_keepsTailConsistent() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.offer("b");
        queue.offer("c");
        assertTrue(queue.remove("c"));
        assertEquals(2, queue.size());
        queue.offer("d");
        assertEquals(Arrays.asList("a", "b", "d"), toList(queue));
    }

    @Test
    void contains_object_roundTrip() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(2);
        assertFalse(queue.contains("a"));
        queue.offer("a");
        assertTrue(queue.contains("a"));
        assertFalse(queue.contains("b"));
    }

    @Test
    void toArray_plain_and_typed_roundTrip() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.offer("b");
        assertArrayEquals(new Object[] {"a", "b"}, queue.toArray());
        assertArrayEquals(new String[] {"a", "b"}, queue.toArray(new String[0]));
        assertArrayEquals(new String[] {"a", "b", null}, queue.toArray(new String[3]));
    }

    @Test
    void clear_emptiesQueueAndRestoresCapacity() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(2);
        queue.offer("a");
        queue.offer("b");
        queue.clear();
        assertEquals(0, queue.size());
        assertEquals(2, queue.remainingCapacity());
        // Nodes themselves must be unlinked, not just the count reset.
        assertTrue(toList(queue).isEmpty());
        assertTrue(queue.offer("c"));
    }

    @Test
    void drainTo_selfOrNull_rejected() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(2);
        queue.offer("a");
        assertThrows(IllegalArgumentException.class, () -> queue.drainTo(queue));
        assertThrows(NullPointerException.class, () -> queue.drainTo(null));
    }

    @Test
    void drainTo_maxElements_zeroOrNegative_returnsZero() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(2);
        queue.offer("a");
        List<String> out = new ArrayList<>();
        assertEquals(0, queue.drainTo(out, 0));
        assertEquals(0, queue.drainTo(out, -5));
        assertTrue(out.isEmpty());
    }

    @Test
    void drainTo_movesElementsAndRestoresCapacity() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(3);
        queue.offer("a");
        queue.offer("b");
        queue.offer("c");
        List<String> out = new ArrayList<>();
        assertEquals(3, queue.drainTo(out));
        assertEquals(Arrays.asList("a", "b", "c"), out);
        assertEquals(0, queue.size());
        assertEquals(3, queue.remainingCapacity());
    }

    // ==================== iterator ====================

    @Test
    void iterator_traversal_inFifoOrder() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.offer("b");
        queue.offer("c");
        assertEquals(Arrays.asList("a", "b", "c"), toList(queue));
    }

    @Test
    void iterator_remove_removesFromQueue() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.offer("b");
        queue.offer("c");
        Iterator<String> it = queue.iterator();
        assertEquals("a", it.next());
        it.remove();
        assertEquals(Arrays.asList("b", "c"), toList(queue));
        assertEquals(2, queue.size());
    }

    @Test
    void iterator_removeWithoutNext_throws() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(2);
        queue.offer("a");
        Iterator<String> it = queue.iterator();
        assertThrows(IllegalStateException.class, it::remove);
    }

    @Test
    void iterator_nextPastEnd_throws() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(2);
        queue.offer("a");
        Iterator<String> it = queue.iterator();
        assertEquals("a", it.next());
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, it::next);
    }

    /**
     * Weakly-consistent iterator: elements are snapshotted at traversal time, so an element removed
     * from the queue mid-traversal is still returned once from the iterator snapshot, and the
     * traversal continues past it (nextNode skips unlinked nodes).
     */
    @Test
    void iterator_returnsSnapshotOfRemovedElementThenContinues() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.offer("b");
        queue.offer("c");
        Iterator<String> it = queue.iterator();
        assertEquals("a", it.next());
        queue.remove("b");
        assertEquals("b", it.next(), "snapshotted element is returned once");
        assertEquals("c", it.next());
        assertFalse(it.hasNext());
    }

    /**
     * Weakly-consistent iterator: after the queue is cleared the iterator still returns its current
     * element snapshot, then ends without throwing (nextNode falls back to the head and then to
     * null).
     */
    @Test
    void iterator_survivesExternalClear() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer("a");
        queue.offer("b");
        Iterator<String> it = queue.iterator();
        assertEquals("a", it.next());
        queue.clear();
        assertEquals("b", it.next());
        assertFalse(it.hasNext());
    }

    // ==================== helpers ====================

    /** Asserts that a cross-thread offer completes, i.e. the putLock is not leaked. */
    private void assertOtherThreadOfferCompletes(VariableLinkedBlockingQueue<String> queue) throws Exception {
        BlockedCall<Boolean> caller = BlockedCall.start(pool, () -> queue.offer("probe"));
        assertTrue(
                caller.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                "cross-thread offer must complete unless the putLock was leaked");
    }

    /**
     * Asserts that a cross-thread take completes, i.e. the takeLock is not leaked. Draining first
     * empties the queue (a leaked takeLock makes even this drain block); the probe taker then parks
     * and is woken by an offer from the main thread.
     */
    private void assertOtherThreadTakeCompletes(VariableLinkedBlockingQueue<String> queue) throws Exception {
        queue.drainTo(new ArrayList<>());
        BlockedCall<String> caller = BlockedCall.start(pool, () -> queue.take());
        caller.awaitParked();
        queue.offer("probe");
        assertEquals(
                "probe",
                caller.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                "cross-thread take must complete unless the takeLock was leaked");
    }

    private static List<String> toList(VariableLinkedBlockingQueue<String> queue) {
        List<String> result = new ArrayList<>();
        queue.forEach(result::add);
        return result;
    }

    /**
     * Runs a call on the pool and exposes both the worker thread (for parking detection) and the
     * future (for the terminal result).
     */
    private static final class BlockedCall<T> {
        private final AtomicReference<Thread> thread = new AtomicReference<>();
        private Future<T> future;

        static <T> BlockedCall<T> start(ExecutorService pool, CheckedCall<T> call) {
            BlockedCall<T> blocked = new BlockedCall<>();
            blocked.future = pool.submit(() -> {
                blocked.thread.set(Thread.currentThread());
                return call.run();
            });
            return blocked;
        }

        Future<T> future() {
            return future;
        }

        /**
         * Waits until the worker is genuinely parked in a lock/condition wait. The future must still be
         * in flight: a completed callable leaves the pool thread WAITING on its idle work queue, which
         * is not the blocking state we probe for.
         */
        void awaitParked() {
            await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS).until(() -> {
                Thread t = thread.get();
                return t != null
                        && !future.isDone()
                        && (t.getState() == Thread.State.WAITING || t.getState() == Thread.State.TIMED_WAITING);
            });
        }

        interface CheckedCall<T> {
            T run() throws Exception;
        }
    }
}
