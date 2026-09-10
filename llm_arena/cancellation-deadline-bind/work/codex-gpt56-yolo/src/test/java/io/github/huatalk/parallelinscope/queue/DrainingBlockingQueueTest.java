package io.github.huatalk.parallelinscope.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DrainingBlockingQueueTest {

    @AfterEach
    void restoreInterrupt() {
        Thread.interrupted();
    }

    @Test
    void shutdownPolicyFactoriesAndBuilderReturnConfiguredInstances() {
        DrainingBlockingQueue.ShutdownPolicy<String> empty = DrainingBlockingQueue.ShutdownPolicy.empty();
        assertNull(empty.poison());
        assertEquals(DrainingBlockingQueue.MutationsStrategy.NOOP, empty.mutationsStrategy());
        DrainingBlockingQueue.ShutdownPolicy<String> throwing = DrainingBlockingQueue.ShutdownPolicy.throwing();
        assertEquals(DrainingBlockingQueue.MutationsStrategy.THROW, throwing.mutationsStrategy());
        DrainingBlockingQueue.ShutdownPolicy.Builder<String> builder = DrainingBlockingQueue.ShutdownPolicy.builder();
        assertSame(builder, builder.poison("stop"));
        assertSame(builder, builder.mutations(DrainingBlockingQueue.MutationsStrategy.THROW));
        DrainingBlockingQueue.ShutdownPolicy<String> built = builder.build();
        assertEquals("stop", built.poison());
        assertEquals(DrainingBlockingQueue.MutationsStrategy.THROW, built.mutationsStrategy());
        assertThrows(NullPointerException.class, () -> DrainingBlockingQueue.ShutdownPolicy.poison(null));
    }

    @Test
    void offerReturnsFalseWhenFullAndTrueWhenSpaceAvailable() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(2);
        assertTrue(queue.offer(1));
        assertTrue(queue.offer(2));
        assertFalse(queue.offer(3));
        assertEquals(1, queue.poll());
        assertTrue(queue.offer(3));
        assertEquals(2, queue.peek());
    }

    @Test
    void addReturnsTrueOnSuccessAndThrowsWhenFull() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(1);
        assertTrue(queue.add(1));
        assertThrows(IllegalStateException.class, () -> queue.add(2));
    }

    @Test
    void addAllReturnsFalseForEmptySourceAndTrueForNonEmpty() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(5);
        assertFalse(queue.addAll(Collections.emptyList()));
        assertTrue(queue.addAll(Arrays.asList(1, 2)));
        assertEquals(2, queue.size());
    }

    @Test
    void removeReturnsTrueWhenMatchFoundAndFalseWhenAbsent() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(5);
        queue.addAll(Arrays.asList(1, 2, 3));
        assertTrue(queue.remove(Integer.valueOf(2)));
        assertFalse(queue.remove(Integer.valueOf(2)));
        assertFalse(queue.remove(Integer.valueOf(99)));
        assertEquals(2, queue.size());
    }

    @Test
    void removeFindsAMatchBeyondTheFirstTraversalBatch() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(130);
        for (int value = 0; value < 130; value++) {
            queue.add(value);
        }

        assertTrue(queue.remove(Integer.valueOf(129)));
        assertEquals(129, queue.size());
        assertFalse(queue.contains(129));
        assertEquals(0, queue.peek());
    }

    @Test
    void collectionRemovalsReleaseBlockedProducer() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(2);
        queue.addAll(Arrays.asList(1, 2));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                queue.put(3);
            } catch (Throwable error) {
                failure.set(error);
            }
        });
        producer.start();
        waitUntilBlocked(producer);

        assertTrue(queue.remove(Integer.valueOf(1)));
        producer.join(1000);
        assertFalse(producer.isAlive());
        assertNull(failure.get());
        assertEquals(Arrays.asList(2, 3), new ArrayList<>(queue));
    }

    @Test
    void bulkAndIteratorRemovalsReleaseBlockedProducers() throws Exception {
        DrainingBlockingQueue<Integer> bulkQueue = new DrainingBlockingQueue<>(2);
        bulkQueue.addAll(Arrays.asList(1, 2));
        AtomicReference<Throwable> bulkFailure = new AtomicReference<>();
        Thread bulkProducer = new Thread(() -> {
            try {
                bulkQueue.put(3);
            } catch (Throwable error) {
                bulkFailure.set(error);
            }
        });
        bulkProducer.start();
        waitUntilBlocked(bulkProducer);
        assertTrue(bulkQueue.removeIf(value -> value == 1));
        bulkProducer.join(1000);
        assertFalse(bulkProducer.isAlive());
        assertNull(bulkFailure.get());

        DrainingBlockingQueue<Integer> iteratorQueue = new DrainingBlockingQueue<>(2);
        iteratorQueue.addAll(Arrays.asList(1, 2));
        AtomicReference<Throwable> iteratorFailure = new AtomicReference<>();
        Thread iteratorProducer = new Thread(() -> {
            try {
                iteratorQueue.put(3);
            } catch (Throwable error) {
                iteratorFailure.set(error);
            }
        });
        iteratorProducer.start();
        waitUntilBlocked(iteratorProducer);
        Iterator<Integer> iterator = iteratorQueue.iterator();
        assertEquals(1, iterator.next());
        iterator.remove();
        iteratorProducer.join(1000);
        assertFalse(iteratorProducer.isAlive());
        assertNull(iteratorFailure.get());
    }

    @Test
    void removeIfReturnsFalseWhenNoMatchAndTrueWhenMatched() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(5);
        queue.addAll(Arrays.asList(1, 2, 3));
        assertFalse(queue.removeIf(value -> value > 10));
        assertTrue(queue.removeIf(value -> value % 2 == 1));
        assertEquals(1, queue.size());
        assertEquals(2, queue.peek());
    }

    @Test
    void bulkRemoveProcessesMoreThanOneBatch() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(130);
        ArrayList<Integer> values = new ArrayList<>();
        for (int value = 0; value < 130; value++) {
            values.add(value);
        }
        queue.addAll(values);

        assertTrue(queue.removeIf(value -> value % 2 == 0));
        assertEquals(65, queue.size());
        for (int value = 1; value < 130; value += 2) {
            assertEquals(value, queue.poll());
        }
    }

    @Test
    void bulkRemoveKeepsEarlierCommittedBatchesWhenPredicateThrows() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(128);
        ArrayList<Integer> values = new ArrayList<>();
        for (int value = 0; value < 128; value++) {
            values.add(value);
        }
        queue.addAll(values);
        AtomicInteger evaluations = new AtomicInteger();

        assertThrows(
                IllegalArgumentException.class,
                () -> queue.removeIf(value -> {
                    if (evaluations.getAndIncrement() == 64) {
                        throw new IllegalArgumentException("second batch");
                    }
                    return true;
                }));

        assertEquals(64, queue.size());
        assertEquals(64, queue.poll());
    }

    @Test
    void retainAllReturnsTrueWhenElementsRemovedAndFalseWhenUnchanged() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(5);
        queue.addAll(Arrays.asList(1, 2, 3));
        assertFalse(queue.retainAll(Arrays.asList(1, 2, 3)));
        assertTrue(queue.retainAll(Collections.singletonList(1)));
        assertEquals(1, queue.size());
    }

    @Test
    void removeAllReturnsTrueWhenElementsRemovedAndFalseWhenUnchanged() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(5);
        queue.addAll(Arrays.asList(1, 2, 3));
        assertFalse(queue.removeAll(Arrays.asList(9, 10)));
        assertTrue(queue.removeAll(Arrays.asList(1, 3)));
        assertEquals(1, queue.size());
    }

    @Test
    void elementReturnsHeadWhenNonEmptyAndThrowsWhenEmpty() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(3);
        assertThrows(NoSuchElementException.class, queue::element);
        queue.add(1);
        queue.add(2);
        assertEquals(1, queue.element());
        assertEquals(1, queue.remove());
    }

    @Test
    void elementOnDrainedQueueReturnsPoisonOrThrows() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>();
        queue.close();
        assertThrows(NoSuchElementException.class, queue::element);

        Object poison = new Object();
        DrainingBlockingQueue<Object> poisoned = new DrainingBlockingQueue<>(1, poison);
        poisoned.close();
        assertSame(poison, poisoned.element());
    }

    @Test
    void removeIfDoesNotHoldTheLockWhileEvaluatingThePredicate() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(5);
        queue.addAll(Arrays.asList(1, 2, 3));
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread remover = new Thread(() -> queue.removeIf(value -> {
            inside.countDown();
            try {
                release.await();
            } catch (InterruptedException error) {
                throw new AssertionError(error);
            }
            return false;
        }));
        remover.start();
        assertTrue(inside.await(2, TimeUnit.SECONDS));
        AtomicReference<Integer> polled = new AtomicReference<>();
        Thread poller = new Thread(() -> polled.set(queue.poll()));
        poller.start();
        poller.join(1000);
        assertFalse(poller.isAlive(), "poll() must not wait for the predicate");
        assertEquals(1, polled.get());
        release.countDown();
        remover.join(2000);
        assertFalse(remover.isAlive());
        assertEquals(2, queue.size());
    }

    @Test
    void removeDoesNotHoldTheLockWhileEvaluatingEquals() throws Exception {
        DrainingBlockingQueue<Object> queue = new DrainingBlockingQueue<>(5);
        Object first = new Object();
        queue.add(first);
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Object target = new Object() {
            @Override
            public boolean equals(Object other) {
                if (other == first) {
                    inside.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException error) {
                        throw new AssertionError(error);
                    }
                }
                return false;
            }
        };
        Thread remover = new Thread(() -> queue.remove(target));
        remover.start();
        assertTrue(inside.await(2, TimeUnit.SECONDS));
        AtomicReference<Object> polled = new AtomicReference<>();
        Thread poller = new Thread(() -> polled.set(queue.poll()));
        poller.start();
        poller.join(1000);
        assertFalse(poller.isAlive(), "poll() must not wait for equals()");
        assertSame(first, polled.get());
        release.countDown();
        remover.join(2000);
        assertFalse(remover.isAlive());
        assertTrue(queue.isEmpty());
    }

    @Test
    void toStringDoesNotHoldQueueMonitorsWhileFormattingElements() throws Exception {
        DrainingBlockingQueue<Object> queue = new DrainingBlockingQueue<>(1);
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Object slow = new Object() {
            @Override
            public String toString() {
                inside.countDown();
                try {
                    release.await();
                } catch (InterruptedException error) {
                    throw new AssertionError(error);
                }
                return "slow";
            }
        };
        queue.add(slow);
        AtomicReference<String> rendered = new AtomicReference<>();
        Thread formatter = new Thread(() -> rendered.set(queue.toString()));
        formatter.start();
        assertTrue(inside.await(2, TimeUnit.SECONDS));

        AtomicReference<Object> polled = new AtomicReference<>();
        Thread consumer = new Thread(() -> polled.set(queue.poll()));
        consumer.start();
        consumer.join(1000);
        assertFalse(consumer.isAlive(), "poll() must not wait for element.toString()");
        assertSame(slow, polled.get());

        release.countDown();
        formatter.join(2000);
        assertFalse(formatter.isAlive());
        assertEquals("[slow]", rendered.get());
    }

    @Test
    void drainToBoundaryConditionsReturnCorrectCounts() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(3);
        queue.add(1);
        ArrayList<Integer> target = new ArrayList<>();
        assertEquals(0, queue.drainTo(target, 0));
        assertEquals(0, queue.drainTo(target, -1));
        assertEquals(0, target.size());
        assertEquals(1, queue.drainTo(target, 1));
        assertIterableEquals(Collections.singletonList(1), target);
    }

    @Test
    void mutationsExecuteDuringDrainingBeforeDrained() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(5);
        queue.addAll(Arrays.asList(1, 2, 3));
        queue.close();
        assertTrue(queue.draining());
        assertFalse(queue.drained());
        assertTrue(queue.removeIf(value -> true));
        assertTrue(queue.drained());
        assertFalse(queue.removeIf(value -> true));
    }

    @Test
    void removeOnDrainedQueueReturnsPoisonOrThrows() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>();
        queue.close();
        assertThrows(NoSuchElementException.class, queue::remove);

        Object poison = new Object();
        DrainingBlockingQueue<Object> poisoned = new DrainingBlockingQueue<>(1, poison);
        poisoned.close();
        assertSame(poison, poisoned.remove());
    }

    @Test
    void offerTimeoutReturnsFalseWhenMonitorTimesOut() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(1);
        queue.add(1);
        assertFalse(queue.offer(2, 1, TimeUnit.MILLISECONDS));
    }

    @Test
    void isShutdownIsFalseWhenOpenAndTrueAfterClose() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(2);
        assertFalse(queue.shutdown());
        queue.close();
        assertTrue(queue.shutdown());
    }

    @Test
    void offerTimeoutReturnsFalseWhenClosedAndDoesNotBlock() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(1);
        queue.add(1);
        queue.close();
        assertFalse(queue.offer(2, 50, TimeUnit.MILLISECONDS));
    }

    @Test
    void removeReturnsTrueForMatchAndFalseForMismatchAndNull() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(5);
        queue.add(1);
        assertTrue(queue.remove(Integer.valueOf(1)));
        assertFalse(queue.remove(Integer.valueOf(1)));
        assertFalse(queue.remove(null));
    }

    @Test
    void removeOnDrainedNoopQueueReturnsFalseWithoutThrowing() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(3);
        queue.add(1);
        queue.close();
        assertTrue(queue.remove(Integer.valueOf(1)));
        assertTrue(queue.drained());
        assertFalse(queue.remove(Integer.valueOf(1)));
        assertFalse(queue.remove(null));
    }

    @Test
    void pollReturnsNullWhenOpenEmptyAndPoisonWhenDrainedEmpty() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(2);
        assertNull(queue.poll());
        queue.close();
        assertNull(queue.poll());
    }

    @Test
    void constructorValidationCoversCapacityAndPolicyBoundaries() {
        assertThrows(IllegalArgumentException.class, () -> new DrainingBlockingQueue<Integer>(0));
        assertThrows(IllegalArgumentException.class, () -> new DrainingBlockingQueue<Integer>(-1));
        assertThrows(
                IllegalArgumentException.class,
                () -> new DrainingBlockingQueue<>(
                        1, Arrays.asList(1, 2), DrainingBlockingQueue.ShutdownPolicy.empty()));
        assertThrows(
                NullPointerException.class,
                () -> new DrainingBlockingQueue<Integer>(1, (DrainingBlockingQueue.ShutdownPolicy<Integer>) null));
        assertThrows(NullPointerException.class, () -> new DrainingBlockingQueue<Integer>(1, (Integer) null));
        assertThrows(NullPointerException.class, () -> new DrainingBlockingQueue<Integer>(1).offer(null));
        assertThrows(NullPointerException.class, () -> new DrainingBlockingQueue<Integer>(1).offer(1, 1, null));
        assertThrows(NullPointerException.class, () -> new DrainingBlockingQueue<Integer>(1).poll(1, null));
    }

    @Test
    void openQueueUsesBoundedFifoSemantics() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(2);
        assertTrue(queue.offer(1));
        assertTrue(queue.add(2));
        assertFalse(queue.offer(3));
        assertEquals(1, queue.take());
        assertTrue(queue.offer(3, 1, TimeUnit.SECONDS));
        assertEquals(Arrays.asList(2, 3), Arrays.asList(queue.poll(), queue.poll(1, TimeUnit.SECONDS)));
        assertNull(queue.peek());
    }

    @Test
    void closeStartsDrainingAndPreservesExistingElements() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(3);
        queue.addAll(Arrays.asList(1, 2));
        queue.close();

        assertTrue(queue.shutdown());
        assertTrue(queue.draining());
        assertFalse(queue.drained());
        assertEquals(2, queue.size());
        assertFalse(queue.isEmpty());
        assertEquals(1, queue.take());
        assertEquals(2, queue.poll());
        assertTrue(queue.drained());
        assertFalse(queue.draining());
        assertTrue(queue.awaitDrained(1, TimeUnit.SECONDS));
    }

    @Test
    void closeEmptyQueuePublishesDrainedImmediately() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>();
        queue.close();
        assertTrue(queue.shutdown());
        assertFalse(queue.draining());
        assertTrue(queue.drained());
        queue.awaitDrained();
        assertTrue(queue.awaitDrained(1, TimeUnit.MILLISECONDS));
    }

    @Test
    void producersAreRejectedAfterClose() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(1);
        queue.add(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                queue.put(2);
            } catch (Throwable error) {
                failure.set(error);
            }
        });
        producer.start();
        waitUntilBlocked(producer);
        queue.close();
        producer.join(1000);

        assertFalse(producer.isAlive());
        assertTrue(failure.get() instanceof IllegalStateException);
        assertFalse(queue.offer(2));
        assertFalse(queue.offer(2, 1, TimeUnit.DAYS));
        assertThrows(IllegalStateException.class, () -> queue.add(2));
        assertFalse(queue.offer(2));
    }

    @Test
    void producerWaitingOnFullQueueCanProceedAfterConsumerRemovesElement() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(1);
        queue.add(1);
        AtomicReference<Boolean> inserted = new AtomicReference<>(false);
        Thread producer = new Thread(() -> {
            try {
                inserted.set(queue.offer(2, 1, TimeUnit.SECONDS));
            } catch (InterruptedException error) {
                throw new AssertionError(error);
            }
        });
        producer.start();
        waitUntilBlocked(producer);
        assertEquals(1, queue.poll());
        producer.join(1000);
        assertFalse(producer.isAlive());
        assertTrue(inserted.get());
        assertEquals(2, queue.poll());
    }

    @Test
    void consumerWaitingOnEmptyQueueCanProceedAfterProducerAddsElement() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(1);
        AtomicReference<Integer> result = new AtomicReference<>();
        Thread consumer = new Thread(() -> {
            try {
                result.set(queue.poll(1, TimeUnit.SECONDS));
            } catch (InterruptedException error) {
                throw new AssertionError(error);
            }
        });
        consumer.start();
        waitUntilBlocked(consumer);
        queue.add(1);
        consumer.join(1000);
        assertFalse(consumer.isAlive());
        assertEquals(1, result.get());
    }

    @Test
    void clearWhileDrainingSignalsWaitingProducerAndPublishesDrained() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(1);
        queue.add(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                queue.put(2);
            } catch (Throwable error) {
                failure.set(error);
            }
        });
        producer.start();
        waitUntilBlocked(producer);
        queue.clear();
        producer.join(1000);
        assertFalse(producer.isAlive());
        assertNull(failure.get());
        assertEquals(2, queue.poll());
        queue.close();
        assertTrue(queue.drained());
    }

    @Test
    void blockedConsumersReceiveStoredElementsThenTerminalSignal() throws Exception {
        Object poison = new Object();
        DrainingBlockingQueue<Object> queue = new DrainingBlockingQueue<>(1, poison);
        AtomicReference<Object> result = new AtomicReference<>();
        Thread consumer = new Thread(() -> {
            try {
                result.set(queue.take());
            } catch (InterruptedException error) {
                throw new AssertionError(error);
            }
        });
        consumer.start();
        waitUntilBlocked(consumer);
        queue.close();
        consumer.join(1000);
        assertFalse(consumer.isAlive());
        assertSame(poison, result.get());
    }

    @Test
    void poisonOnlyAppearsAfterTheLastRealElement() throws Exception {
        String poison = new String("stop");
        DrainingBlockingQueue<String> queue = new DrainingBlockingQueue<>(2, poison);
        queue.add("one");
        queue.close();
        assertEquals("one", queue.poll());
        assertSame(poison, queue.poll());
        assertSame(poison, queue.peek());
        assertSame(poison, queue.take());
    }

    @Test
    void noPoisonUsesMethodFamilyTerminalResults() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>();
        queue.close();
        assertNull(queue.poll());
        assertNull(queue.poll(1, TimeUnit.MILLISECONDS));
        assertNull(queue.peek());
        assertThrows(NoSuchElementException.class, queue::take);
        assertThrows(NoSuchElementException.class, queue::remove);
        assertThrows(NoSuchElementException.class, queue::element);
    }

    @Test
    void poisonEqualityIsReservedAcrossAllWrites() {
        String poison = new String("stop");
        DrainingBlockingQueue<String> queue = new DrainingBlockingQueue<>(3, poison);
        assertThrows(IllegalArgumentException.class, () -> queue.offer(new String("stop")));
        assertThrows(IllegalArgumentException.class, () -> queue.addAll(Arrays.asList("ok", new String("stop"))));
        assertTrue(queue.isEmpty());
    }

    @Test
    void mutationsDrainNormallyWhileDrainingAndAreConfiguredAfterDrained() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(3);
        queue.addAll(Arrays.asList(1, 2, 3));
        queue.close();
        assertTrue(queue.removeIf(value -> value % 2 == 1));
        assertEquals(1, queue.size());
        assertTrue(queue.remove(Integer.valueOf(2)));
        assertTrue(queue.drained());
        assertFalse(queue.removeIf(value -> true));
        assertFalse(queue.remove(2));

        DrainingBlockingQueue.ShutdownPolicy<Integer> policy = DrainingBlockingQueue.ShutdownPolicy.<Integer>builder()
                .mutations(DrainingBlockingQueue.MutationsStrategy.THROW)
                .build();
        DrainingBlockingQueue<Integer> throwing = new DrainingBlockingQueue<>(2, policy);
        throwing.close();
        assertThrows(IllegalStateException.class, throwing::clear);
        assertThrows(IllegalStateException.class, () -> throwing.remove(1));
        assertThrows(IllegalStateException.class, () -> throwing.removeIf(value -> true));
        assertThrows(IllegalStateException.class, () -> throwing.removeAll(Collections.singletonList(1)));
        assertThrows(IllegalStateException.class, () -> throwing.retainAll(Collections.singletonList(1)));
    }

    @Test
    void drainToTransfersFifoAndPublishesDrained() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(3);
        queue.addAll(Arrays.asList(1, 2, 3));
        queue.close();
        ArrayList<Integer> target = new ArrayList<>();
        assertEquals(2, queue.drainTo(target, 2));
        assertIterableEquals(Arrays.asList(1, 2), target);
        assertEquals(1, queue.size());
        assertEquals(1, queue.drainTo(target));
        assertIterableEquals(Arrays.asList(1, 2, 3), target);
        assertTrue(queue.drained());
    }

    @Test
    void drainToTargetFailureDoesNotHoldQueueLock() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(2);
        queue.add(1);
        queue.close();
        CollectionThatFails target = new CollectionThatFails();
        assertThrows(IllegalStateException.class, () -> queue.drainTo(target));
        assertTrue(queue.isEmpty());
        assertTrue(queue.drained());
    }

    @Test
    void timedAwaitReportsTimeoutAndCompletion() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(1);
        assertFalse(queue.awaitDrained(1, TimeUnit.MILLISECONDS));
        queue.close();
        assertTrue(queue.awaitDrained(1, TimeUnit.MILLISECONDS));
    }

    @Test
    void interruptionWinsOverCloseForBlockedMethods() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(1);
        queue.add(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                queue.put(2);
            } catch (Throwable error) {
                failure.set(error);
            }
        });
        producer.start();
        waitUntilBlocked(producer);
        producer.interrupt();
        producer.join(1000);
        queue.close();
        assertTrue(failure.get() instanceof InterruptedException);
    }

    @Test
    void addAllAtomicValidationAndQueryMethodsRemainHonest() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(3);
        assertFalse(queue.addAll(Collections.emptyList()));
        assertThrows(IllegalStateException.class, () -> queue.addAll(Arrays.asList(1, 2, 3, 4)));
        assertTrue(queue.addAll(Arrays.asList(1, 2)));
        assertEquals(1, queue.remainingCapacity());
        assertEquals(2, queue.size());
        assertFalse(queue.isEmpty());
        assertTrue(queue.contains(1));
        assertIterableEquals(Arrays.asList(1, 2), queue);
        assertEquals(1, queue.peek());
        queue.add(3);
        assertEquals(1, queue.peek());
        queue.close();
        assertEquals(0, queue.remainingCapacity());
        assertEquals(3, queue.size());
        assertThrows(IllegalStateException.class, () -> queue.addAll(Collections.singletonList(4)));
    }

    @Test
    void bulkMutationsReturnWhetherTheyChangedTheQueue() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(5);
        queue.addAll(Arrays.asList(1, 2, 3, 4));
        assertTrue(queue.remove(Integer.valueOf(1)));
        assertFalse(queue.remove(Integer.valueOf(9)));
        assertTrue(queue.removeAll(Arrays.asList(2, 3)));
        assertFalse(queue.removeAll(Collections.singletonList(9)));
        assertFalse(queue.retainAll(Collections.singletonList(4)));
        assertEquals(4, queue.poll());
        assertTrue(queue.isEmpty());
    }

    @Test
    void iteratorsAreWeaklyConsistentAndDoNotExposePoison() throws Exception {
        DrainingBlockingQueue<String> queue = new DrainingBlockingQueue<>(2, new String("stop"));
        queue.add("one");
        Iterator<String> live = queue.iterator();
        assertTrue(live.hasNext());
        queue.add("two");
        assertEquals("one", live.next());
        assertEquals("two", live.next());
        queue.close();
        assertEquals("one", queue.take());
        assertEquals("two", queue.take());
        assertFalse(queue.iterator().hasNext());
    }

    @Test
    void spliteratorIsLateBindingAndWeaklyConsistent() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(2);
        Spliterator<Integer> traversal = queue.spliterator();
        queue.add(1);
        AtomicReference<Integer> observed = new AtomicReference<>();

        assertTrue(traversal.tryAdvance(observed::set));
        assertEquals(1, observed.get());
        assertTrue((traversal.characteristics() & Spliterator.ORDERED) != 0);
        assertTrue((traversal.characteristics() & Spliterator.NONNULL) != 0);
        assertTrue((traversal.characteristics() & Spliterator.CONCURRENT) != 0);
    }

    @Test
    void forEachTraversesMultipleBatchesAndLeavesIteratorOnLastElement() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(130);
        ArrayList<Integer> values = new ArrayList<>();
        for (int value = 0; value < 130; value++) {
            values.add(value);
        }
        queue.addAll(values);

        ArrayList<Integer> observed = new ArrayList<>();
        queue.forEach(observed::add);
        assertEquals(values, observed);

        Iterator<Integer> iterator = queue.iterator();
        assertEquals(0, iterator.next());
        ArrayList<Integer> remaining = new ArrayList<>();
        iterator.forEachRemaining(remaining::add);
        assertEquals(values.subList(1, values.size()), remaining);
        iterator.remove();
        assertEquals(129, queue.size());
        assertEquals(0, queue.peek());
        assertEquals(128, queue.toArray()[128]);
    }

    @Test
    void iteratorRemoveRemovesFromLiveQueue() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(5);
        queue.addAll(Arrays.asList(1, 2, 3));
        Iterator<Integer> it = queue.iterator();
        assertThrows(IllegalStateException.class, it::remove);
        assertEquals(1, it.next());
        it.remove();
        assertEquals(2, queue.size());
        assertEquals(Arrays.asList(2, 3), new ArrayList<>(queue));
        assertThrows(IllegalStateException.class, it::remove);
        assertEquals(2, it.next());
        assertEquals(3, it.next());
        assertFalse(it.hasNext());
        assertEquals(2, queue.size());
    }

    @Test
    void iteratorRemoveDuringDrainingPublishesDrainedOnLastElement() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(5);
        queue.addAll(Arrays.asList(1, 2));
        queue.close();
        Iterator<Integer> it = queue.iterator();
        assertEquals(1, it.next());
        it.remove();
        assertFalse(queue.drained());
        assertEquals(2, it.next());
        it.remove();
        assertTrue(queue.drained());
    }

    @Test
    void drainToRejectsInvalidTargetsAndZeroCounts() {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>();
        assertThrows(NullPointerException.class, () -> queue.drainTo(null));
        assertThrows(IllegalArgumentException.class, () -> queue.drainTo(queue));
        assertEquals(0, queue.drainTo(new ArrayList<>(), 0));
    }

    @Test
    void capacityTwoDequeueFromFullSignalsProducer() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(2);
        queue.add(1);
        queue.add(2);
        AtomicReference<Boolean> inserted = new AtomicReference<>(false);
        Thread producer = new Thread(() -> {
            try {
                inserted.set(queue.offer(3, 2, TimeUnit.SECONDS));
            } catch (InterruptedException error) {
                throw new AssertionError(error);
            }
        });
        producer.start();
        waitUntilBlocked(producer);
        assertEquals(1, queue.poll());
        producer.join(2000);
        assertFalse(producer.isAlive());
        assertTrue(inserted.get());
    }

    @Test
    void capacityTwoEnqueueToEmptySignalsConsumer() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(2);
        AtomicReference<Integer> result = new AtomicReference<>();
        Thread consumer = new Thread(() -> {
            try {
                result.set(queue.poll(2, TimeUnit.SECONDS));
            } catch (InterruptedException error) {
                throw new AssertionError(error);
            }
        });
        consumer.start();
        waitUntilBlocked(consumer);
        assertTrue(queue.offer(1));
        consumer.join(2000);
        assertFalse(consumer.isAlive());
        assertEquals(1, result.get());
    }

    @Test
    void timedAwaitDrainedReturnsTrueWhenAlreadyDrained() throws Exception {
        DrainingBlockingQueue<Integer> queue = new DrainingBlockingQueue<>(1);
        queue.close();
        assertTrue(queue.awaitDrained(1, TimeUnit.NANOSECONDS));
        assertTrue(queue.awaitDrained(0, TimeUnit.SECONDS));
    }

    private static void waitUntilBlocked(Thread thread) throws InterruptedException {
        for (int attempt = 0; attempt < 100; attempt++) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                return;
            }
            Thread.sleep(5);
        }
        fail("thread did not block: " + thread.getState());
    }

    private static final class CollectionThatFails extends ArrayList<Integer> {
        @Override
        public boolean add(Integer value) {
            throw new IllegalStateException("rejected");
        }
    }
}
