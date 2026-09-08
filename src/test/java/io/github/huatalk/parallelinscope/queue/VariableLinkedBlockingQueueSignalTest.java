package io.github.huatalk.parallelinscope.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Signal-path additions over {@link VariableLinkedBlockingQueueTest}: blocked producers and
 * consumers must be released by the complementary operation (take/put/clear/setCapacity), and
 * array snapshots must follow {@link java.util.concurrent.LinkedBlockingQueue} semantics.
 */
class VariableLinkedBlockingQueueSignalTest {

    @Test
    void takeOnEmptyQueueIsReleasedByConcurrentPut() throws Exception {
        VariableLinkedBlockingQueue<Integer> queue = new VariableLinkedBlockingQueue<>(1);
        AtomicReference<Integer> taken = new AtomicReference<>();
        CountDownLatch finished = new CountDownLatch(1);
        Thread consumer = new Thread(() -> {
            try {
                taken.set(queue.take());
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } finally {
                finished.countDown();
            }
        });
        consumer.start();
        Thread.sleep(50);
        queue.put(31);
        assertTrue(finished.await(4, TimeUnit.SECONDS), "take was not released by put");
        assertEquals(31, taken.get().intValue());
    }

    @Test
    void timedOfferBlockedAtCapacityIsReleasedByTake() throws Exception {
        VariableLinkedBlockingQueue<Integer> queue = new VariableLinkedBlockingQueue<>(2);
        assertTrue(queue.offer(1));
        assertTrue(queue.offer(2));

        AtomicBoolean produced = new AtomicBoolean(false);
        CountDownLatch finished = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            try {
                produced.set(queue.offer(3, 5, TimeUnit.SECONDS));
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } finally {
                finished.countDown();
            }
        });
        producer.start();
        Thread.sleep(50);
        assertEquals(2, queue.size());
        assertEquals(1, queue.poll()); // Frees one slot; releases the blocked producer.
        assertTrue(finished.await(4, TimeUnit.SECONDS), "timed offer was not released by poll");
        assertTrue(produced.get());
        // Queue is FIFO: [2] plus the late-produced [3].
        assertEquals(2, queue.size());
        assertEquals(2, queue.poll());
        assertEquals(3, queue.poll());
    }

    @Test
    void clearOnFullQueueReleasesBlockedProducer() throws Exception {
        VariableLinkedBlockingQueue<Integer> queue = new VariableLinkedBlockingQueue<>(1);
        assertTrue(queue.offer(9));

        AtomicBoolean stored = new AtomicBoolean(false);
        CountDownLatch finished = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            try {
                queue.put(10);
                stored.set(true);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } finally {
                finished.countDown();
            }
        });
        producer.start();
        Thread.sleep(50);
        queue.clear();
        assertTrue(queue.isEmpty());
        assertTrue(finished.await(4, TimeUnit.SECONDS), "put was not released by clear");
        producer.join(TimeUnit.SECONDS.toMillis(2));
        assertTrue(stored.get());
        assertFalse(queue.isEmpty());
    }

    @Test
    void capacityGrowthReleasesBlockedProducer() throws Exception {
        VariableLinkedBlockingQueue<Integer> queue = new VariableLinkedBlockingQueue<>(1);
        assertTrue(queue.offer(1));

        AtomicBoolean stored = new AtomicBoolean(false);
        CountDownLatch finished = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            try {
                queue.put(2);
                stored.set(true);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } finally {
                finished.countDown();
            }
        });
        producer.start();
        Thread.sleep(50);
        queue.setCapacity(3);
        assertEquals(3, queue.capacity());
        assertTrue(finished.await(4, TimeUnit.SECONDS), "capacity growth did not release waiting put");
        producer.join(TimeUnit.SECONDS.toMillis(2));
        assertTrue(stored.get());
        assertEquals(2, queue.size());
    }

    @Test
    void capacityAndElementValidationStayGuarded() {
        assertThrows(IllegalArgumentException.class, () -> new VariableLinkedBlockingQueue<Object>(0));
        assertThrows(IllegalArgumentException.class, () -> new VariableLinkedBlockingQueue<Object>(-2));
        VariableLinkedBlockingQueue<Integer> queue = new VariableLinkedBlockingQueue<>(1);
        assertThrows(IllegalArgumentException.class, () -> queue.setCapacity(0));
        assertThrows(IllegalArgumentException.class, () -> queue.setCapacity(-1));
        assertThrows(NullPointerException.class, () -> queue.offer(null));
        assertThrows(NullPointerException.class, () -> queue.put(null));
        assertEquals(0, queue.drainTo(new ArrayList<>(), 0));
        assertEquals(0, queue.drainTo(new ArrayList<>(), -7));
        assertThrows(IllegalArgumentException.class, () -> queue.drainTo(queue));
    }

    @Test
    void drainToTransfersUpToTheBoundInFifoOrder() {
        VariableLinkedBlockingQueue<Integer> queue = new VariableLinkedBlockingQueue<>(5);
        queue.addAll(Arrays.asList(1, 2, 3, 4));
        List<Integer> target = new ArrayList<>();
        assertEquals(2, queue.drainTo(target, 2));
        assertEquals(Arrays.asList(1, 2), target);
        assertEquals(2, queue.size());
        assertEquals(2, queue.drainTo(target));
        assertEquals(Arrays.asList(1, 2, 3, 4), target);
        assertTrue(queue.isEmpty());
    }

    @Test
    void toArraySnapshotsAreIndependentWithTerminatorRules() {
        VariableLinkedBlockingQueue<String> queue = new VariableLinkedBlockingQueue<>(4);
        queue.addAll(Arrays.asList("a", "b"));

        String[] exact = queue.toArray(new String[2]);
        assertEquals("a", exact[0]);
        assertEquals("b", exact[1]);

        String[] oversized = queue.toArray(new String[4]);
        assertEquals("a", oversized[0]);
        assertEquals("b", oversized[1]);
        assertNull(oversized[2]);

        Object[] generic = queue.toArray();
        assertEquals(2, generic.length);

        queue.clear();
        assertEquals(0, queue.toArray().length);
        IteratorDrivenAssertions.assertExhausted(queue.iterator());
    }

    /** Tiny helper keeping iterator assertions off the main test body. */
    private static final class IteratorDrivenAssertions {
        private static void assertExhausted(java.util.Iterator<?> iterator) {
            assertFalse(iterator.hasNext());
        }
    }
}
