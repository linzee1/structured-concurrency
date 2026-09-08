package io.github.huatalk.parallelinscope.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.ListenableFuture;
import io.github.huatalk.parallelinscope.spi.ExecutionPhase;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Exercises the full lifecycle-timing by interrupt-policy Cartesian surface. */
public class ExecutionPhaseCartesianTest {

    private enum Timing {
        BEFORE_RUN,
        RUNNING,
        TERMINAL
    }

    /** Builds all lifecycle timing and interruption-policy combinations. */
    private static Stream<Arguments> lifecycleCases() {
        return Stream.of(Timing.values())
                .flatMap(timing -> Stream.of(false, true)
                        .map(interrupt -> Arguments.of(Named.of(caseId(timing, interrupt), timing), interrupt)));
    }

    /** Verifies Future, phase, body-entry, completion, and interrupt planes for each combination. */
    @ParameterizedTest(name = "{0};mayInterrupt={1}")
    @MethodSource("lifecycleCases")
    public void cancellationLifecycleMatchesTimingAndInterruptPolicy(Timing timing, boolean mayInterrupt)
            throws Exception {
        AtomicReference<Runnable> submitted = new AtomicReference<>();
        LinkedBlockingQueue<ListenableFuture<Integer>> completions = new LinkedBlockingQueue<>();
        List<ExecutionPhase> phases = new CopyOnWriteArrayList<>();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        ListenableCompletionService<Integer> service =
                new ListenableCompletionService<>(submitted::set, completions, phases::add);
        ListenableFuture<Integer> future = service.submit(() -> {
            entered.countDown();
            try {
                awaitRelease(release, interrupted);
                return 7;
            } finally {
                exited.countDown();
            }
        });

        Thread runner = null;
        try {
            if (timing == Timing.BEFORE_RUN) {
                assertThat(future.cancel(mayInterrupt)).isTrue();
                submitted.get().run();
            } else if (timing == Timing.RUNNING) {
                runner = new Thread(submitted.get(), "phase-cartesian-runner");
                runner.start();
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(future.cancel(mayInterrupt)).isTrue();
            } else {
                release.countDown();
                submitted.get().run();
                assertThat(future.cancel(mayInterrupt)).isFalse();
            }

            assertThat(completions.poll(5, TimeUnit.SECONDS)).isSameAs(future);
            assertLifecycle(timing, mayInterrupt, future, phases, entered, interrupted);
        } finally {
            release.countDown();
            if (runner != null) {
                runner.join(TimeUnit.SECONDS.toMillis(5));
                assertThat(runner.isAlive()).isFalse();
            }
        }

        if (timing == Timing.RUNNING) {
            assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(phases)
                    .containsExactly(
                            ExecutionPhase.RUNNING, ExecutionPhase.CANCEL_REQUESTED_RUNNING, ExecutionPhase.TERMINAL);
        }
    }

    /** Waits until released while retaining evidence that interruption was requested. */
    private static void awaitRelease(CountDownLatch release, AtomicBoolean interrupted) {
        while (true) {
            try {
                release.await();
                return;
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        }
    }

    /** Asserts the observation planes that are stable before fixture cleanup. */
    private static void assertLifecycle(
            Timing timing,
            boolean mayInterrupt,
            ListenableFuture<Integer> future,
            List<ExecutionPhase> phases,
            CountDownLatch entered,
            AtomicBoolean interrupted)
            throws Exception {
        if (timing == Timing.BEFORE_RUN) {
            assertThat(future.isCancelled()).isTrue();
            assertThat(entered.getCount()).isOne();
            assertThat(interrupted).isFalse();
            assertThat(phases).containsExactly(ExecutionPhase.CANCELED_BEFORE_RUN);
        } else if (timing == Timing.RUNNING) {
            assertThat(future.isCancelled()).isTrue();
            if (mayInterrupt) {
                awaitBoolean(interrupted);
            } else {
                assertThat(interrupted).isFalse();
            }
            assertThat(phases).containsExactly(ExecutionPhase.RUNNING, ExecutionPhase.CANCEL_REQUESTED_RUNNING);
        } else {
            assertThat(future.get(5, TimeUnit.SECONDS)).isEqualTo(7);
            assertThat(interrupted).isFalse();
            assertThat(phases).containsExactly(ExecutionPhase.RUNNING, ExecutionPhase.TERMINAL);
        }
    }

    /** Waits for an interrupt observation without using elapsed time as the oracle. */
    private static void awaitBoolean(AtomicBoolean value) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!value.get() && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertThat(value).isTrue();
    }

    /** Returns a stable generated-case identity. */
    private static String caseId(Timing timing, boolean interrupt) {
        return "surface=future-lifecycle;timing=" + timing + ";mayInterrupt=" + interrupt;
    }
}
