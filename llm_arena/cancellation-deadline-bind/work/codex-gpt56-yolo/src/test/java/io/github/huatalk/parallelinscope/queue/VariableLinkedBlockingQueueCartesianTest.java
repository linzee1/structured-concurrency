package io.github.huatalk.parallelinscope.queue;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Exercises producer/consumer waiters by counterpart, interrupt, and timeout release modes. */
public class VariableLinkedBlockingQueueCartesianTest {

    private enum Waiter {
        PRODUCER,
        CONSUMER
    }

    private enum Release {
        COUNTERPART,
        INTERRUPT,
        TIMEOUT
    }

    private enum ProducerRelease {
        TAKE,
        REMOVE,
        CLEAR,
        DRAIN,
        CAPACITY_GROW
    }

    /** Builds the complete waiter-kind by release-mode Cartesian surface. */
    private static Stream<Arguments> waiterCases() {
        return Stream.of(Waiter.values())
                .flatMap(waiter -> Stream.of(Release.values())
                        .map(release -> Arguments.of(Named.of(caseId(waiter, release), waiter), release)));
    }

    /** Verifies wake-up, interruption, timeout, and queue-state invariants for each case. */
    @ParameterizedTest(name = "{0};release={1}")
    @MethodSource("waiterCases")
    public void blockedWaiterResolvesWithoutLostSignal(Waiter waiter, Release release) throws Exception {
        VariableLinkedBlockingQueue<Integer> queue = new VariableLinkedBlockingQueue<>(1);
        if (waiter == Waiter.PRODUCER) {
            queue.put(0);
        }
        CountDownLatch invoked = new CountDownLatch(1);
        AtomicReference<Object> result = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread thread = new Thread(
                () -> runWaiter(queue, waiter, release, invoked, result, interrupted),
                "queue-cartesian-" + waiter + '-' + release);

        thread.start();
        assertThat(invoked.await(5, TimeUnit.SECONDS)).isTrue();
        if (release != Release.TIMEOUT) {
            awaitParked(thread);
        }

        if (release == Release.COUNTERPART) {
            if (waiter == Waiter.PRODUCER) {
                assertThat(queue.take()).isZero();
            } else {
                queue.put(1);
            }
        } else if (release == Release.INTERRUPT) {
            thread.interrupt();
        }

        thread.join(TimeUnit.SECONDS.toMillis(5));
        assertThat(thread.isAlive()).isFalse();
        assertOutcome(queue, waiter, release, result, interrupted);
    }

    /** Builds every queue mutation that should release a producer waiting on a full queue. */
    private static Stream<Arguments> producerReleaseCases() {
        return Stream.of(ProducerRelease.values())
                .map(release ->
                        Arguments.of(Named.of("surface=producer-condition-release;mutation=" + release, release)));
    }

    /** Verifies every capacity-creating mutation signals a producer without losing its element. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("producerReleaseCases")
    public void fullQueueMutationReleasesBlockedProducer(ProducerRelease release) throws Exception {
        VariableLinkedBlockingQueue<Integer> queue = new VariableLinkedBlockingQueue<>(1);
        queue.put(0);
        CountDownLatch invoked = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean();
        Thread producer = new Thread(
                () -> {
                    invoked.countDown();
                    try {
                        queue.put(1);
                        completed.set(true);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                "queue-producer-release-" + release);

        producer.start();
        assertThat(invoked.await(5, TimeUnit.SECONDS)).isTrue();
        awaitParked(producer);
        createCapacity(queue, release);

        producer.join(TimeUnit.SECONDS.toMillis(5));
        assertThat(producer.isAlive()).isFalse();
        assertThat(completed).isTrue();
        if (release == ProducerRelease.CAPACITY_GROW) {
            assertThat(queue).containsExactly(0, 1);
        } else {
            assertThat(queue).containsExactly(1);
        }
    }

    /** Runs one blocking or timed queue operation and captures its outcome. */
    private static void runWaiter(
            VariableLinkedBlockingQueue<Integer> queue,
            Waiter waiter,
            Release release,
            CountDownLatch invoked,
            AtomicReference<Object> result,
            AtomicBoolean interrupted) {
        invoked.countDown();
        try {
            if (waiter == Waiter.PRODUCER) {
                if (release == Release.TIMEOUT) {
                    result.set(queue.offer(1, 100, TimeUnit.MILLISECONDS));
                } else {
                    queue.put(1);
                    result.set(Boolean.TRUE);
                }
            } else if (release == Release.TIMEOUT) {
                result.set(queue.poll(100, TimeUnit.MILLISECONDS));
            } else {
                result.set(queue.take());
            }
        } catch (InterruptedException e) {
            interrupted.set(true);
        }
    }

    /** Applies one mutation that creates room in a previously full queue. */
    private static void createCapacity(VariableLinkedBlockingQueue<Integer> queue, ProducerRelease release)
            throws InterruptedException {
        if (release == ProducerRelease.TAKE) {
            assertThat(queue.take()).isZero();
        } else if (release == ProducerRelease.REMOVE) {
            assertThat(queue.remove(0)).isTrue();
        } else if (release == ProducerRelease.CLEAR) {
            queue.clear();
        } else if (release == ProducerRelease.DRAIN) {
            List<Integer> drained = new ArrayList<>();
            assertThat(queue.drainTo(drained)).isOne();
            assertThat(drained).containsExactly(0);
        } else {
            queue.setCapacity(2);
        }
    }

    /** Waits until the operation has actually reached a blocking condition. */
    private static void awaitParked(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != Thread.State.WAITING
                && thread.getState() != Thread.State.TIMED_WAITING
                && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertThat(thread.getState()).isIn(Thread.State.WAITING, Thread.State.TIMED_WAITING);
    }

    /** Asserts the result and queue mutation allowed by the selected release mode. */
    private static void assertOutcome(
            VariableLinkedBlockingQueue<Integer> queue,
            Waiter waiter,
            Release release,
            AtomicReference<Object> result,
            AtomicBoolean interrupted) {
        if (release == Release.INTERRUPT) {
            assertThat(interrupted).isTrue();
            assertThat(queue.size()).isEqualTo(waiter == Waiter.PRODUCER ? 1 : 0);
        } else if (release == Release.TIMEOUT) {
            assertThat(interrupted).isFalse();
            assertThat(result.get()).isEqualTo(waiter == Waiter.PRODUCER ? Boolean.FALSE : null);
            assertThat(queue.size()).isEqualTo(waiter == Waiter.PRODUCER ? 1 : 0);
        } else {
            assertThat(interrupted).isFalse();
            assertThat(result.get()).isEqualTo(waiter == Waiter.PRODUCER ? Boolean.TRUE : 1);
            assertThat(queue.size()).isEqualTo(waiter == Waiter.PRODUCER ? 1 : 0);
        }
    }

    /** Returns a stable generated-case identity. */
    private static String caseId(Waiter waiter, Release release) {
        return "surface=condition-waiter;waiter=" + waiter + ";release=" + release;
    }
}
