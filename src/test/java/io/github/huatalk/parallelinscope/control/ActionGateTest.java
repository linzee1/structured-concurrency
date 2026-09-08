package io.github.huatalk.parallelinscope.control;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ActionGateTest {

    @Test
    void countBoundaryControlsWhenActionIsDue() {
        ActionGate gate = ActionGate.every(3);

        assertFalse(gate.due());
        assertFalse(gate.due());
        assertTrue(gate.due());
    }

    @Test
    void combinedModeRequiresInvocationAndTimeBoundaries() {
        ActionGate gate = ActionGate.whenBoth(3, Duration.ofHours(1));

        assertFalse(gate.due());
        assertFalse(gate.due());
        assertFalse(gate.due());
    }

    @Test
    void combinedModeKeepsInvocationBoundarySatisfiedWhileWaitingForTime() {
        ActionGate gate = ActionGate.whenBoth(3, Duration.ofMillis(20));

        assertFalse(gate.due());
        assertFalse(gate.due());
        assertFalse(gate.due());

        await().atMost(Duration.ofSeconds(1)).until(gate::due);
    }

    @Test
    void timeBoundaryEventuallyOpens() {
        ActionGate gate = ActionGate.every(Duration.ofMillis(20));

        await().atMost(Duration.ofSeconds(1)).until(gate::due);
    }

    @Test
    void boundActionRunsOnlyWhenDue() {
        AtomicInteger actions = new AtomicInteger();
        ActionGate gate = ActionGate.every(2, actions::incrementAndGet);

        assertFalse(gate.runIfDue());
        assertTrue(gate.runIfDue());
        assertEquals(1, actions.get());
    }

    @Test
    void suppliedActionRunsOnlyWhenDue() {
        AtomicInteger actions = new AtomicInteger();
        ActionGate gate = ActionGate.every(2);

        assertFalse(gate.runIfDue(actions::incrementAndGet));
        assertTrue(gate.runIfDue(actions::incrementAndGet));
        assertEquals(1, actions.get());
    }

    @Test
    void actionFailurePropagatesAndKeepsBoundaryConsumed() {
        ActionGate gate = ActionGate.every(2);

        assertFalse(gate.runIfDue(() -> {
            throw new AssertionError("not due");
        }));
        assertThrows(
                IllegalStateException.class,
                () -> gate.runIfDue(() -> {
                    throw new IllegalStateException("action failed");
                }));
        assertFalse(gate.due());
    }

    @Test
    void boundActionIsRequiredForNoArgumentRun() {
        ActionGate gate = ActionGate.every(1);

        assertThrows(IllegalStateException.class, gate::runIfDue);
    }

    @Test
    void invalidBoundariesAndActionsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> ActionGate.every(0));
        assertThrows(IllegalArgumentException.class, () -> ActionGate.every(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> ActionGate.whenBoth(-1, Duration.ofSeconds(1)));
        assertThrows(NullPointerException.class, () -> ActionGate.every((Duration) null));
        assertThrows(NullPointerException.class, () -> ActionGate.every(1, null));
        assertThrows(NullPointerException.class, () -> ActionGate.whenBoth(1, Duration.ofMillis(1), null));
    }

    @Test
    void suppliedActionMustNotBeNull() {
        assertThrows(NullPointerException.class, () -> ActionGate.every(1).runIfDue(null));
    }

    @Test
    void timeAndCombinedGatesCanRunBoundActions() {
        AtomicInteger actions = new AtomicInteger();
        ActionGate timed = ActionGate.every(Duration.ofMillis(1), actions::incrementAndGet);
        await().atMost(Duration.ofSeconds(1)).until(timed::runIfDue);

        ActionGate combined = ActionGate.whenBoth(1, Duration.ofMillis(1), actions::incrementAndGet);
        await().atMost(Duration.ofSeconds(1)).until(combined::runIfDue);
        assertEquals(2, actions.get());
    }

    @Test
    void concurrentInvocationsConsumeOneCountBoundary() throws Exception {
        int invocationCount = 64;
        ActionGate gate = ActionGate.every(invocationCount);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> results = new ArrayList<>();

        try {
            for (int i = 0; i < invocationCount; i++) {
                results.add(executor.submit(() -> {
                    start.await();
                    return gate.due();
                }));
            }
            start.countDown();

            int openings = 0;
            for (Future<Boolean> result : results) {
                if (result.get()) {
                    openings++;
                }
            }
            assertEquals(1, openings);
        } finally {
            executor.shutdownNow();
        }
    }
}
