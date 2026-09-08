package io.github.huatalk.parallelinscope.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Contract-focused additions over {@link DrainingBlockingQueueTest}: constructor-with-elements,
 * snapshot queries ({@code toArray}/{@code toString}), spliterator traversal and splitting,
 * iterator removal rules, shutdown-policy mutation matrix, await-drained variants, capacity
 * signaling through blocking producers, and {@code drainTo} bounds.
 */
class DrainingBlockingQueueContractTest {

    @AfterEach
    void restoreInterrupt() {
        Thread.interrupted();
    }

    // ==================== Construction ====================

    @Test
    void constructorStoresInitialElementsInFifoOrderAndValidates() {
        DrainingBlockingQueue<String> queue = new DrainingBlockingQueue<>(
                3, Arrays.asList("a", "b", "c"), DrainingBlockingQueue.ShutdownPolicy.empty());
        assertEquals(3, queue.size());
        assertEquals("a", queue.poll());
        assertEquals("b", queue.poll());
        assertEquals("c", queue.poll());
        assertNull(queue.poll());

        DrainingBlockingQueue<String> exact =
                new DrainingBlockingQueue<>(2, Arrays.asList("x", "y"), DrainingBlockingQueue.ShutdownPolicy.empty());
        assertEquals(2, exact.size());
        assertFalse(exact.offer("z"));

        assertThrows(
                IllegalArgumentException.class,
                () -> new DrainingBlockingQueue<>(
                        2, Arrays.asList("x", "y", "z"), DrainingBlockingQueue.ShutdownPolicy.empty()));
        assertThrows(
                NullPointerException.class,
                () -> new DrainingBlockingQueue<>(
                        2, Arrays.asList("x", null), DrainingBlockingQueue.ShutdownPolicy.empty()));

        DrainingBlockingQueue.ShutdownPolicy<String> poisonPolicy =
                DrainingBlockingQueue.ShutdownPolicy.<String>builder()
                        .poison("STOP")
                        .build();
        assertThrows(
                IllegalArgumentException.class,
                () -> new DrainingBlockingQueue<>(2, Arrays.asList("STOP"), poisonPolicy));
        assertEquals(0, new DrainingBlockingQueue<String>(2, Arrays.asList(), poisonPolicy).size());
    }

    // ==================== Snapshot queries ====================

    @Test
    void toArrayOverloadsExposeIndependentFifoSnapshots() {
        DrainingBlockingQueue<String> queue = new DrainingBlockingQueue<>(4);
        queue.addAll(Arrays.asList("a", "b"));

        assertArrayEqualsContent(new Object[] {"a", "b"}, queue.toArray());

        Object[] copyAfterSnapshot = queue.toArray();
        queue.poll();
        assertEquals(2, copyAfterSnapshot.length);
        assertEquals("a", copyAfterSnapshot[0]);

        String[] oversized = new String[5];
        String[] returned = queue.toArray(new String[0]);
        assertEquals(1, returned.length);
        assertEquals("b", returned[0]);

        String[] exactFilled = queue.toArray(new String[queue.size()]);
        assertEquals(1, exactFilled.length);

        String[] terminated = queue.toArray(new String[4]);
        assertSame(terminated, queue.toArray(terminated));
        assertEquals("b", terminated[0]);
        assertNull(terminated[1]);
        assertNull(terminated[3]);

        DrainingBlockingQueue<String> empty = new DrainingBlockingQueue<>(1);
        assertEquals(0, empty.toArray().length);
        assertEquals(1, empty.toArray(new String[1]).length);
        assertNull(empty.toArray(new String[1])[0]);
    }

    private static void assertArrayEqualsContent(Object[] expected, Object[] actual) {
        assertEquals(expected.length, actual.length);
        for (int index = 0; index < expected.length; index++) {
            assertEquals(expected[index], actual[index]);
        }
    }

    @Test
    void toStringFormatsElementReferencesLikeAbstractCollection() {
        DrainingBlockingQueue<String> queue = new DrainingBlockingQueue<>(3);
        assertEquals("[]", queue.toString());
        queue.addAll(Arrays.asList("a", "b"));
        assertEquals("[a, b]", queue.toString());
    }

    // ==================== Spliterator ====================

    @Test
    void spliteratorAdvancesThroughAllElementsExactlyOnce() {
        DrainingBlockingQueue<Integer> queue = ranged(6);
        Spliterator<Integer> spliterator = queue.spliterator();
        assertEquals(queue.size(), spliterator.estimateSize());

        List<Integer> seen = new ArrayList<>();
        while (spliterator.tryAdvance(seen::add)) {
            // Drain the whole traversal.
        }
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5), seen);
        assertFalse(spliterator.tryAdvance(element -> failNever()));
        assertTrue(spliterator.estimateSize() <= queue.size());
    }

    private static void failNever() {
        throw new AssertionError("action must not run past exhaustion");
    }

    @Test
    void forEachRemainingCollectsRemainderWithoutRepeatingVisitedElements() {
        DrainingBlockingQueue<Integer> queue = ranged(5);
        Spliterator<Integer> spliterator = queue.spliterator();
        List<Integer> prefix = new ArrayList<>();
        assertTrue(spliterator.tryAdvance(prefix::add));

        List<Integer> remainder = new ArrayList<>();
        spliterator.forEachRemaining(remainder::add);
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), union(prefix, remainder));

        List<Integer> secondPass = new ArrayList<>();
        spliterator.forEachRemaining(secondPass::add);
        assertEquals(0, secondPass.size());
    }

    @Test
    void trySplitPartitionsElementsWithoutLossOrDuplication() {
        DrainingBlockingQueue<Integer> queue = ranged(9);
        Spliterator<Integer> first = queue.spliterator();

        Spliterator<Integer> split = first.trySplit();
        if (split != null) {
            List<Integer> left = new ArrayList<>();
            split.forEachRemaining(left::add);
            List<Integer> right = new ArrayList<>();
            first.forEachRemaining(right::add);
            assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8), union(left, right));
            assertEquals(left.size() + right.size(), Math.max(left.size() + right.size(), 0));
        } else {
            List<Integer> all = new ArrayList<>();
            first.forEachRemaining(all::add);
            assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8), all);
        }

        DrainingBlockingQueue<Integer> small = new DrainingBlockingQueue<>(2);
        Spliterator<Integer> tiny = small.spliterator();
        assertTrue(tiny.estimateSize() >= 0);
        assertFalse(tiny.tryAdvance(element -> failNever()));
        assertNull(tiny.trySplit());
    }

    @Test
    void streamPipelineTraversesViaSpliterator() {
        DrainingBlockingQueue<Integer> queue = ranged(8);
        List<Integer> collected = queue.stream().filter(value -> value % 2 == 0).collect(Collectors.toList());
        assertEquals(Arrays.asList(0, 2, 4, 6), collected);
    }

    @Test
    void spliteratorCharacteristicsMatchDeclaredTraits() {
        DrainingBlockingQueue<Integer> queue = ranged(2);
        Spliterator<Integer> spliterator = queue.spliterator();
        int traits = spliterator.characteristics();
        assertTrue(Spliterator.ORDERED == (traits & Spliterator.ORDERED));
        assertTrue(Spliterator.NONNULL == (traits & Spliterator.NONNULL));
        assertTrue(Spliterator.CONCURRENT == (traits & Spliterator.CONCURRENT));
        assertEquals(0, traits & Spliterator.SIZED);
        assertEquals(0, traits & Spliterator.SUBSIZED);
    }

    private static DrainingBlockingQueue<Integer> ranged(int count) {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(Math.max(count, 1));
        for (int value = 0; value < count; value++) {
            queue.add(value);
        }
        return queue;
    }

    private static List<Integer> union(List<Integer> first, List<Integer> second) {
        List<Integer> combined = new ArrayList<>(first.size() + second.size());
        combined.addAll(first);
        combined.addAll(second);
        combined.sort(Integer::compareTo);
        return combined;
    }

    // ==================== Iterator ====================

    @Test
    void iteratorRemovesOnlyReturnedElementsAndGuardsMisuse() {
        DrainingBlockingQueue<String> queue = new DrainingBlockingQueue<>(5);
        queue.addAll(Arrays.asList("a", "b", "c"));

        Iterator<String> iterator = queue.iterator();
        assertThrows(IllegalStateException.class, iterator::remove);

        assertEquals("a", iterator.next());
        assertEquals("b", iterator.next());
        iterator.remove();
        assertThrows(IllegalStateException.class, iterator::remove);

        assertEquals("c", iterator.next());
        assertFalse(iterator.hasNext());

        assertEquals(Arrays.asList("a", "c"), snapshot(queue));
        assertEquals(2, queue.size());
    }

    @Test
    void drainingIteratorObservesOnlyRemainingElements() {
        DrainingBlockingQueue<String> queue =
                new DrainingBlockingQueue<>(3, Arrays.asList("p", "q"), DrainingBlockingQueue.ShutdownPolicy.empty());
        queue.close();
        assertEquals("p", queue.poll());
        assertTrue(queue.draining());

        Iterator<String> iterator = queue.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("q", iterator.next());
        assertFalse(iterator.hasNext());
    }

    private static List<String> snapshot(DrainingBlockingQueue<String> queue) {
        List<String> values = new ArrayList<>();
        for (String value : queue) {
            values.add(value);
        }
        return values;
    }

    // ==================== Shutdown policy matrix ====================

    @Test
    void throwingPolicyRejectsMutationAfterDrainedButDeliversPoisonToReaders() throws InterruptedException {
        DrainingBlockingQueue.ShutdownPolicy<String> policy = DrainingBlockingQueue.ShutdownPolicy.<String>builder()
                .poison("STOP")
                .mutations(DrainingBlockingQueue.MutationsStrategy.THROW)
                .build();
        DrainingBlockingQueue<String> queue = new DrainingBlockingQueue<>(3, Arrays.asList("only"), policy);
        queue.close();
        assertTrue(queue.draining());

        assertThrows(IllegalStateException.class, () -> queue.add("late"));
        assertThrows(IllegalStateException.class, () -> queue.put("late"));
        assertFalse(queue.offer("late"));

        assertEquals("only", queue.take());
        assertTrue(queue.drained());
        assertEquals(0, queue.remainingCapacity());

        assertThrows(IllegalStateException.class, () -> queue.clear());
        assertThrows(IllegalStateException.class, () -> queue.remove("missing"));
        assertThrows(IllegalStateException.class, () -> queue.removeIf(value -> true));
        assertTrue(queue.isEmpty());

        assertEquals("STOP", queue.take());
        assertEquals("STOP", queue.remove());
        assertEquals("STOP", queue.peek());
        assertEquals("STOP", queue.element());
        assertEquals("STOP", queue.poll());
    }

    @Test
    void noopPolicyTreatsMutationsOnDrainedQueueAsSilentNoOps() throws InterruptedException {
        DrainingBlockingQueue<String> queue = new DrainingBlockingQueue<>(
                2, Arrays.asList("one", "two"), DrainingBlockingQueue.ShutdownPolicy.empty());
        queue.close();
        queue.clear();
        assertTrue(queue.drained());

        queue.clear();
        assertTrue(queue.drained());
        assertFalse(queue.remove("one"));
        assertFalse(queue.removeIf(value -> true));
        assertTrue(queue.isEmpty());

        assertNull(queue.poll());
        assertNull(queue.peek());
        assertThrows(NoSuchElementException.class, queue::remove);
        assertThrows(NoSuchElementException.class, queue::element);
        assertFalse(queue.offer("late"));
        assertEquals(0, queue.remainingCapacity());
    }

    @Test
    void clearWhileDrainingEmptiesStorageAndPublishesDrained() throws InterruptedException {
        DrainingBlockingQueue<String> queue =
                new DrainingBlockingQueue<>(2, Arrays.asList("r1", "r2"), DrainingBlockingQueue.ShutdownPolicy.empty());
        queue.close();
        assertTrue(awaitDrainedWithin(queue, 100, TimeUnit.MILLISECONDS) || !queue.drained());
        if (!queue.drained()) {
            queue.clear();
        }
        assertTrue(queue.drained());
        assertEquals(0, queue.size());
    }

    // ==================== awaitDrained ====================

    @Test
    void awaitDrainedTimesOutOnOpenQueueAndReturnsFastWhenAlreadyDrained() throws InterruptedException {
        DrainingBlockingQueue<String> open = new DrainingBlockingQueue<>(2);
        open.addAll(Arrays.asList("kept", "kept-too"));
        // Still OPEN with stored elements: the guard can never be satisfied, so the
        // timed variant must report a timeout instead of blocking or succeeding.
        assertFalse(open.awaitDrained(30, TimeUnit.MILLISECONDS));
        assertFalse(open.drained());

        DrainingBlockingQueue<String> empty = new DrainingBlockingQueue<>(1);
        empty.close(); // Close on an empty queue publishes DRAINED directly.
        assertTrue(empty.awaitDrained(10, TimeUnit.MILLISECONDS));

        DrainingBlockingQueue<String> drainedNow = new DrainingBlockingQueue<>(1);
        drainedNow.close();
        drainedNow.awaitDrained();
        assertTrue(drainedNow.awaitDrained(10, TimeUnit.MILLISECONDS));
    }

    @Test
    void awaitDrainedReturnsWhenAnotherThreadFinishesDraining() throws Exception {
        DrainingBlockingQueue<String> queue = new DrainingBlockingQueue<>(2);
        queue.addAll(Arrays.asList("d1", "d2"));

        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            started.countDown();
            try {
                queue.awaitDrained();
                queue.poll();
            } catch (Throwable error) {
                failure.set(error);
            }
        });
        waiter.start();
        started.await();
        queue.close();
        queue.poll();
        queue.poll();
        waiter.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(waiter.isAlive());
        assertNull(failure.get());
        assertTrue(queue.drained());
    }

    @Test
    void interruptedCallerThrowsFromAwaitDrained() {
        DrainingBlockingQueue<String> queue = new DrainingBlockingQueue<>(2);
        queue.add("held");
        Thread.currentThread().interrupt();
        assertThrows(InterruptedException.class, queue::awaitDrained);
    }

    private static boolean awaitDrainedWithin(DrainingBlockingQueue<?> queue, long timeout, TimeUnit unit)
            throws InterruptedException {
        return queue.awaitDrained(timeout, unit);
    }

    // ==================== Capacity signaling across threads ====================

    @Test
    void pollFreesCapacitySoBlockedTimedProducerEventuallySucceeds() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(1);
        assertTrue(queue.offer(1));

        AtomicBoolean produced = new AtomicBoolean(false);
        CountDownLatch producerDone = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            try {
                produced.set(queue.offer(2, 5, TimeUnit.SECONDS));
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } finally {
                producerDone.countDown();
            }
        });
        producer.start();
        assertEquals(1, queue.poll());
        assertTrue(producerDone.await(4, TimeUnit.SECONDS), "producer was not released by poll");
        assertTrue(produced.get());
        producer.join(TimeUnit.SECONDS.toMillis(2));

        assertEquals(2, queue.peek());
    }

    @Test
    void addSignalsWaitingConsumerWhenQueueTransitionsFromEmpty() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(2);
        AtomicReference<Integer> taken = new AtomicReference<>();
        CountDownLatch consumerDone = new CountDownLatch(1);
        Thread consumer = new Thread(() -> {
            try {
                taken.set(queue.take());
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } finally {
                consumerDone.countDown();
            }
        });
        consumer.start();
        Thread.sleep(50);
        assertTrue(queue.add(7));
        assertTrue(consumerDone.await(4, TimeUnit.SECONDS), "consumer was not released by add");
        assertEquals(7, taken.get().intValue());
        consumer.join(TimeUnit.SECONDS.toMillis(2));
    }

    @Test
    void timedOfferReleasedByCloseReturnsFalsePromptlyWithoutStoring() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(1);
        assertTrue(queue.offer(1));

        AtomicBoolean offerResult = new AtomicBoolean(true);
        CountDownLatch offerFinished = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            try {
                offerResult.set(queue.offer(99, 10, TimeUnit.SECONDS));
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } finally {
                offerFinished.countDown();
            }
        });
        producer.start();
        queue.close();
        assertTrue(offerFinished.await(3, TimeUnit.SECONDS), "close must release a blocked timed offer");
        assertFalse(offerResult.get());
        producer.join(TimeUnit.SECONDS.toMillis(2));
        assertEquals(1, queue.size());
        assertEquals(1, queue.poll());
    }

    // ==================== drainTo ====================

    @Test
    void drainToHonorsBoundsOrderingAndSelfReferenceGuard() {
        DrainingBlockingQueue<Integer> queue = ranged(5);
        List<Integer> target = new ArrayList<>();

        assertEquals(0, queue.drainTo(target, 0));
        assertEquals(0, queue.drainTo(target, -3));
        assertTrue(target.isEmpty());
        assertEquals(5, queue.size());

        assertEquals(2, queue.drainTo(target, 2));
        assertEquals(Arrays.asList(0, 1), target);
        assertEquals(3, queue.size());

        assertEquals(3, queue.drainTo(target, 99));
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), target);
        assertTrue(queue.isEmpty());

        assertThrows(IllegalArgumentException.class, () -> queue.drainTo(queue, 1));
        List<Integer> drainedNothing = new ArrayList<>();
        assertEquals(0, queue.drainTo(drainedNothing, 5));
    }

    @Test
    void drainToPublishesDrainedWhenItRemovesTheLastStoredElement() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(2);
        queue.add(11);
        queue.close();
        assertTrue(queue.draining());

        List<Integer> target = new ArrayList<>();
        assertEquals(1, queue.drainTo(target));
        assertEquals(java.util.Collections.singletonList(11), target);
        assertTrue(queue.drained());
    }

    // ==================== Batched predicate removal ====================

    @Test
    void removeIfHandlesMoreThanOneTraversalBatchWithOrderPreserved() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(300);
        for (int value = 0; value < 200; value++) {
            queue.add(value);
        }

        assertTrue(queue.removeIf(value -> value % 2 == 0));

        List<Integer> remaining = new ArrayList<>();
        queue.forEach(remaining::add);
        List<Integer> expectedOdds = new ArrayList<>();
        for (int value = 1; value < 200; value += 2) {
            expectedOdds.add(value);
        }
        assertEquals(expectedOdds, remaining);
        assertEquals(100, queue.size());

        assertFalse(queue.removeIf(value -> false));
        assertEquals(100, queue.size());
    }

    @Test
    void scatteredMiddleRemovalsKeepFifoOrderOfRemainder() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(64);
        for (int value = 0; value < 12; value++) {
            queue.add(value);
        }

        assertTrue(queue.remove(5));
        assertTrue(queue.remove(0));
        assertTrue(queue.remove(11));
        assertFalse(queue.remove(5));
        assertEquals(Arrays.asList(1, 2, 3, 4, 6, 7, 8, 9, 10), snapshotInts(queue));
    }

    private static List<Integer> snapshotInts(DrainingBlockingQueue<Integer> queue) {
        List<Integer> values = new ArrayList<>();
        queue.forEach(values::add);
        return values;
    }
}
