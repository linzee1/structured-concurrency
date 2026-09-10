package io.github.huatalk.parallelinscope.cancel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.ListenableFutureTask;
import io.github.huatalk.parallelinscope.queue.SmartBlockingQueue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Exercises purge pressure, cancellation ratio, and enablement as a full boundary matrix. */
public class HeuristicPurgerCartesianTest {

    private enum Boundary {
        BELOW,
        EXACT,
        ABOVE
    }

    /** Builds all 3 x 3 x 2 purge-threshold combinations. */
    private static Stream<Arguments> thresholdCases() {
        return Stream.of(Boundary.values())
                .flatMap(pressure -> Stream.of(Boundary.values())
                        .flatMap(ratio -> Stream.of(false, true)
                                .map(enabled -> Arguments.of(
                                        Named.of(caseId(pressure, ratio, enabled), pressure), ratio, enabled))));
    }

    /** Verifies purge scheduling only when both boundaries and enablement permit it. */
    @ParameterizedTest(name = "{0};ratio={1};enabled={2}")
    @MethodSource("thresholdCases")
    public void purgeRequiresBothThresholdsAndEnablement(Boundary pressure, Boundary ratio, boolean enabled)
            throws Exception {
        CountingThreadPoolExecutor executor = new CountingThreadPoolExecutor(new SmartBlockingQueue<>(10));
        HeuristicPurger purger =
                new HeuristicPurger(new AtomicBoolean(enabled), new AtomicDouble(0.80), new AtomicDouble(0.20));
        Runnable observer = purger.cancellationObserverFor(executor);
        int queueSize = valueFor(pressure, 7, 8, 9);
        int cancellations = valueFor(ratio, 1, 2, 3);

        try {
            List<ListenableFutureTask<Void>> tasks = enqueue(executor, queueSize);
            cancel(tasks, cancellations, observer);
            boolean expectedPurge = enabled && pressure != Boundary.BELOW && ratio != Boundary.BELOW;
            if (expectedPurge) {
                await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                    assertThat(executor.purgeCount).hasValue(1);
                    assertThat(executor.getQueue()).hasSize(queueSize - cancellations);
                });
            } else {
                await().during(100, TimeUnit.MILLISECONDS)
                        .atMost(1, TimeUnit.SECONDS)
                        .untilAsserted(() -> assertThat(executor.purgeCount).hasValue(0));
                assertThat(executor.getQueue()).hasSize(queueSize);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /** Adds dormant Future tasks directly to the controlled smart queue. */
    private static List<ListenableFutureTask<Void>> enqueue(CountingThreadPoolExecutor executor, int count)
            throws InterruptedException {
        List<ListenableFutureTask<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ListenableFutureTask<Void> task = ListenableFutureTask.create(() -> null);
            executor.getQueue().put(task);
            tasks.add(task);
        }
        return tasks;
    }

    /** Cancels an exact prefix and emits one advisory signal per successful cancellation. */
    private static void cancel(List<ListenableFutureTask<Void>> tasks, int count, Runnable observer) {
        for (int i = 0; i < count; i++) {
            assertThat(tasks.get(i).cancel(false)).isTrue();
            observer.run();
        }
    }

    /** Selects the integer fixture value for one boundary category. */
    private static int valueFor(Boundary boundary, int below, int exact, int above) {
        if (boundary == Boundary.BELOW) {
            return below;
        }
        return boundary == Boundary.EXACT ? exact : above;
    }

    /** Returns a stable generated-case identity. */
    private static String caseId(Boundary pressure, Boundary ratio, boolean enabled) {
        return "surface=purge-threshold;pressure=" + pressure + ";ratio=" + ratio + ";enabled=" + enabled;
    }

    /** Counts purge calls while retaining normal ThreadPoolExecutor removal behavior. */
    private static final class CountingThreadPoolExecutor extends ThreadPoolExecutor {
        private final AtomicInteger purgeCount = new AtomicInteger();

        /** Creates a dormant executor whose queue is populated directly by the test. */
        private CountingThreadPoolExecutor(BlockingQueue<Runnable> queue) {
            super(1, 1, 0L, TimeUnit.MILLISECONDS, queue);
        }

        /** Records and performs one queue purge. */
        @Override
        public void purge() {
            purgeCount.incrementAndGet();
            super.purge();
        }
    }
}
