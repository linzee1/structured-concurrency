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

/** Exercises parent cancellation timing across pending and running child work. */
public class CancellationPropagationCartesianTest {

    private enum ParentTiming {
        BEFORE_CHILD_BIND,
        AFTER_CHILD_BIND
    }

    private enum ChildWork {
        PENDING,
        RUNNING
    }

    /** Builds the full parent-timing by child-work Cartesian surface. */
    private static Stream<Arguments> propagationCases() {
        return Stream.of(ParentTiming.values())
                .flatMap(timing -> Stream.of(ChildWork.values())
                        .map(work -> Arguments.of(Named.of(caseId(timing, work), timing), work)));
    }

    /** Verifies parent-to-child cancellation effects independently from state classification. */
    @ParameterizedTest(name = "{0};childWork={1}")
    @MethodSource("propagationCases")
    public void parentCancellationReachesPendingAndRunningChildWork(ParentTiming timing, ChildWork childWork)
            throws Exception {
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        ChildFixture fixture = ChildFixture.create(childWork);
        CancellationToken parent = CancellationToken.create();
        CancellationToken child = new CancellationToken(parent);

        try {
            fixture.awaitEntry();
            if (timing == ParentTiming.BEFORE_CHILD_BIND) {
                parent.cancel(true);
            }
            child.bind(Collections.singletonList(fixture.future), Futures.immediateVoidFuture(), timer);
            if (timing == ParentTiming.AFTER_CHILD_BIND) {
                parent.cancel(true);
            }

            awaitCancelled(fixture.future);
            assertThat(child.state().shouldInterruptCurrentThread()).isTrue();
            if (childWork == ChildWork.RUNNING) {
                awaitTrue(fixture.interrupted);
            } else {
                assertThat(fixture.interrupted).isFalse();
            }
        } finally {
            fixture.close();
            timer.shutdownNow();
        }
    }

    /** Specifies consistent propagation classification when the parent was already cancelled. */
    @Test
    public void alreadyCancelledParentClassifiesChildAsPropagatingCancellation() {
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        try {
            CancellationToken parent = CancellationToken.create();
            parent.cancel(true);
            CancellationToken child = new CancellationToken(parent);
            SettableFuture<Integer> childFuture = SettableFuture.create();

            child.bind(Collections.singletonList(childFuture), Futures.immediateVoidFuture(), timer);

            assertThat(childFuture).isCancelled();
            assertThat(child.state())
                    .as("parent cancellation should have one state regardless of bind timing")
                    .isEqualTo(CancellationToken.State.PROPAGATED_CANCELED);
        } finally {
            timer.shutdownNow();
        }
    }

    /** Waits for Future cancellation without relying on a fixed sleep. */
    private static void awaitCancelled(ListenableFuture<?> future) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!future.isCancelled() && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertThat(future).isCancelled();
    }

    /** Waits for a running child to observe interruption. */
    private static void awaitTrue(AtomicBoolean value) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!value.get() && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertThat(value).isTrue();
    }

    /** Returns a stable generated-case identity. */
    private static String caseId(ParentTiming timing, ChildWork work) {
        return "surface=parent-propagation;timing=" + timing + ";childWork=" + work;
    }

    /** Owns one pending or running child Future and its cleanup controls. */
    private static final class ChildFixture implements AutoCloseable {
        private final ExecutorService executor;
        private final CountDownLatch entered;
        private final CountDownLatch release;
        private final AtomicBoolean interrupted;
        private final ListenableFuture<Integer> future;

        /** Creates a fixture around one child Future. */
        private ChildFixture(
                ExecutorService executor,
                CountDownLatch entered,
                CountDownLatch release,
                AtomicBoolean interrupted,
                ListenableFuture<Integer> future) {
            this.executor = executor;
            this.entered = entered;
            this.release = release;
            this.interrupted = interrupted;
            this.future = future;
        }

        /** Creates pending work or running work parked at a controlled gate. */
        private static ChildFixture create(ChildWork work) {
            if (work == ChildWork.PENDING) {
                return new ChildFixture(
                        null,
                        new CountDownLatch(0),
                        new CountDownLatch(0),
                        new AtomicBoolean(),
                        SettableFuture.create());
            }
            ExecutorService raw = Executors.newSingleThreadExecutor();
            ListeningExecutorService listening = MoreExecutors.listeningDecorator(raw);
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicBoolean interrupted = new AtomicBoolean();
            ListenableFuture<Integer> future = listening.submit(() -> {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    interrupted.set(true);
                }
                return 1;
            });
            return new ChildFixture(raw, entered, release, interrupted, future);
        }

        /** Waits until running child work enters its body. */
        private void awaitEntry() throws InterruptedException {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        }

        /** Releases work and shuts down the optional child executor. */
        @Override
        public void close() throws Exception {
            release.countDown();
            if (executor != null) {
                executor.shutdownNow();
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }
}
