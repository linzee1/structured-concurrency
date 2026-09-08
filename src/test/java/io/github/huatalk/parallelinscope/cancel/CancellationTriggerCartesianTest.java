package io.github.huatalk.parallelinscope.cancel;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Exercises cancellation triggers across pending, cooperative, and non-cooperative workloads. */
public class CancellationTriggerCartesianTest {

    private enum Trigger {
        TIMEOUT,
        MANUAL_INTERRUPT
    }

    private enum Workload {
        PENDING,
        RUNNING_IO_GATE,
        RUNNING_CPU_COOPERATIVE,
        RUNNING_MIXED,
        RUNNING_IGNORES_INTERRUPT
    }

    /** Builds the full supported trigger by workload Cartesian surface. */
    private static Stream<Arguments> triggerCases() {
        return Stream.of(Trigger.values())
                .flatMap(trigger -> Stream.of(Workload.values())
                        .map(workload -> Arguments.of(Named.of(caseId(trigger, workload), trigger), workload)));
    }

    /** Verifies token, Future, interrupt, liveness, and submitter-cancellation planes. */
    @ParameterizedTest(name = "{0};workload={1}")
    @MethodSource("triggerCases")
    public void triggerCancelsEveryWorkloadWithItsExpectedLivenessBoundary(Trigger trigger, Workload workload)
            throws Exception {
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        WorkFixture fixture = WorkFixture.create(workload);
        SettableFuture<Void> submitter = SettableFuture.create();
        CancellationToken token = trigger == Trigger.TIMEOUT
                ? new CancellationToken(null, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(75))
                : CancellationToken.create();

        try {
            fixture.awaitEntry();
            token.bind(Collections.singletonList(fixture.future), submitter, timer);
            if (trigger == Trigger.MANUAL_INTERRUPT) {
                token.cancel(true);
            }

            CancellationToken.State expected = trigger == Trigger.TIMEOUT
                    ? CancellationToken.State.TIMEOUT_CANCELED
                    : CancellationToken.State.MUTUAL_CANCELED;
            awaitState(token, expected);
            awaitCancelled(fixture.future);
            awaitCancelled(submitter);

            if (workload == Workload.PENDING) {
                assertThat(fixture.interrupted).isFalse();
            } else {
                awaitTrue(fixture.interrupted);
                if (workload != Workload.RUNNING_IGNORES_INTERRUPT) {
                    assertThat(fixture.exited.await(5, TimeUnit.SECONDS)).isTrue();
                } else {
                    assertThat(fixture.exited.getCount()).isOne();
                }
            }
        } finally {
            fixture.close();
            timer.shutdownNow();
        }
    }

    /** Specifies that manual non-interrupting cancellation must not interrupt entered work. */
    @Test
    public void manualNonInterruptingCancellationDoesNotInterruptRunningWork() throws Exception {
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        WorkFixture fixture = WorkFixture.create(Workload.RUNNING_IGNORES_INTERRUPT);
        CancellationToken token = CancellationToken.create();

        try {
            fixture.awaitEntry();
            token.bind(Collections.singletonList(fixture.future), Futures.immediateVoidFuture(), timer);
            token.cancel(false);

            awaitState(token, CancellationToken.State.MUTUAL_CANCELED);
            awaitCancelled(fixture.future);
            assertThat(fixture.interrupted)
                    .as("cancel(false) must preserve the non-interrupting contract")
                    .isFalse();
        } finally {
            fixture.close();
            timer.shutdownNow();
        }
    }

    /** Waits for one exact token state produced by direct-executor callbacks. */
    private static void awaitState(CancellationToken token, CancellationToken.State expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (token.state() != expected && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertThat(token.state()).isEqualTo(expected);
    }

    /** Waits for Future cancellation; the token commits its state before cancelling bound work. */
    private static void awaitCancelled(ListenableFuture<?> future) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!future.isCancelled() && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertThat(future).isCancelled();
    }

    /** Waits for a controlled task to observe interruption. */
    private static void awaitTrue(AtomicBoolean value) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!value.get() && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertThat(value).isTrue();
    }

    /** Returns a stable generated-case identity. */
    private static String caseId(Trigger trigger, Workload workload) {
        return "surface=cancellation-trigger;trigger=" + trigger + ";workload=" + workload;
    }

    /** Owns one controlled workload and its independent cleanup channel. */
    private static final class WorkFixture implements AutoCloseable {
        private final Workload workload;
        private final ExecutorService rawExecutor;
        private final ListeningExecutorService executor;
        private final CountDownLatch entered;
        private final CountDownLatch release;
        private final CountDownLatch exited;
        private final AtomicBoolean interrupted;
        private final ListenableFuture<Integer> future;

        /** Creates a fixture around an already-constructed workload Future. */
        private WorkFixture(
                Workload workload,
                ExecutorService rawExecutor,
                ListeningExecutorService executor,
                CountDownLatch entered,
                CountDownLatch release,
                CountDownLatch exited,
                AtomicBoolean interrupted,
                ListenableFuture<Integer> future) {
            this.workload = workload;
            this.rawExecutor = rawExecutor;
            this.executor = executor;
            this.entered = entered;
            this.release = release;
            this.exited = exited;
            this.interrupted = interrupted;
            this.future = future;
        }

        /** Creates pending or entered work with deterministic entry and release controls. */
        private static WorkFixture create(Workload workload) {
            if (workload == Workload.PENDING) {
                return new WorkFixture(
                        workload,
                        null,
                        null,
                        new CountDownLatch(0),
                        new CountDownLatch(0),
                        new CountDownLatch(0),
                        new AtomicBoolean(),
                        SettableFuture.create());
            }
            ExecutorService raw = Executors.newSingleThreadExecutor();
            ListeningExecutorService listening = MoreExecutors.listeningDecorator(raw);
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch exited = new CountDownLatch(1);
            AtomicBoolean interrupted = new AtomicBoolean();
            ListenableFuture<Integer> future =
                    listening.submit(() -> runWorkload(workload, entered, release, exited, interrupted));
            return new WorkFixture(workload, raw, listening, entered, release, exited, interrupted, future);
        }

        /** Runs the selected workload until release or cooperative interruption. */
        private static int runWorkload(
                Workload workload,
                CountDownLatch entered,
                CountDownLatch release,
                CountDownLatch exited,
                AtomicBoolean interrupted) {
            try {
                if (workload == Workload.RUNNING_MIXED) {
                    long checksum = 0L;
                    for (int i = 0; i < 100_000; i++) {
                        checksum += i;
                    }
                    assertThat(checksum).isPositive();
                }
                entered.countDown();
                if (workload == Workload.RUNNING_CPU_COOPERATIVE) {
                    while (release.getCount() > 0 && !Thread.currentThread().isInterrupted()) {
                        Thread.yield();
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        interrupted.set(true);
                    }
                    return 1;
                }
                while (true) {
                    try {
                        release.await();
                        return 1;
                    } catch (InterruptedException e) {
                        interrupted.set(true);
                        if (workload != Workload.RUNNING_IGNORES_INTERRUPT) {
                            return 1;
                        }
                    }
                }
            } finally {
                exited.countDown();
            }
        }

        /** Waits until running work has entered its controlled body. */
        private void awaitEntry() throws InterruptedException {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        }

        /** Releases non-cooperative work and shuts down its executor. */
        @Override
        public void close() throws Exception {
            release.countDown();
            if (executor != null) {
                executor.shutdownNow();
                assertThat(rawExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }
}
