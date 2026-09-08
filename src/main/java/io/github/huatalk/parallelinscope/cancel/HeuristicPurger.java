package io.github.huatalk.parallelinscope.cancel;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.github.huatalk.parallelinscope.queue.SmartBlockingQueue;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Coalesces cleanup of canceled tasks retained by bounded {@link BlockingQueue} instances.
 *
 * <p>Queue pressure and canceled-task ratio are concurrent snapshots used as advisory signals;
 * neither value is an exact queue accounting guarantee.
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public final class HeuristicPurger {

    private static final Logger LOGGER = Logger.getLogger(HeuristicPurger.class.getName());
    private static final long COALESCING_DELAY_MILLIS = 50L;
    private static final long CANCELLATION_ESTIMATE_EXPIRY_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final Runnable NOOP = () -> {};

    private enum MaintenanceState {
        IDLE,
        BUSY
    }

    private static final class CancellationMarker {
        private final long generation;
        private final long sequence;
        private final long timestampNanos;

        /** Captures the latest idle cancellation evaluated in one reset generation. */
        private CancellationMarker(long generation, long sequence, long timestampNanos) {
            this.generation = generation;
            this.sequence = sequence;
            this.timestampNanos = timestampNanos;
        }
    }

    private final AtomicBoolean enabled;
    private final AtomicDouble queuePressureThreshold;
    private final AtomicDouble canceledTaskRatioThreshold;
    private final LongSupplier nanoTime;
    private final long estimateExpiryNanos;
    private final AtomicLong resetGeneration = new AtomicLong();
    private final ConcurrentHashMap<ThreadPoolExecutor, PoolState> states = new ConcurrentHashMap<>();
    private final ScheduledExecutorService maintenanceExecutor;

    /**
     * Creates a purger backed by atomically adjustable thresholds.
     *
     * @param queuePressureThreshold minimum queue-size-to-capacity ratio
     * @param canceledTaskRatioThreshold minimum estimated canceled-task ratio
     */
    public HeuristicPurger(AtomicDouble queuePressureThreshold, AtomicDouble canceledTaskRatioThreshold) {
        this(new AtomicBoolean(true), queuePressureThreshold, canceledTaskRatioThreshold);
    }

    /**
     * Creates a purger backed by atomically adjustable enablement and thresholds.
     *
     * @param enabled whether automatic purge is enabled
     * @param queuePressureThreshold minimum queue-size-to-capacity ratio
     * @param canceledTaskRatioThreshold minimum estimated canceled-task ratio
     */
    public HeuristicPurger(
            AtomicBoolean enabled, AtomicDouble queuePressureThreshold, AtomicDouble canceledTaskRatioThreshold) {
        this(
                enabled,
                queuePressureThreshold,
                canceledTaskRatioThreshold,
                System::nanoTime,
                CANCELLATION_ESTIMATE_EXPIRY_NANOS);
    }

    /** Creates a purger with an injectable monotonic clock for expiry tests. */
    HeuristicPurger(
            AtomicBoolean enabled,
            AtomicDouble queuePressureThreshold,
            AtomicDouble canceledTaskRatioThreshold,
            LongSupplier nanoTime,
            long estimateExpiryNanos) {
        this.enabled = Objects.requireNonNull(enabled);
        this.queuePressureThreshold = Objects.requireNonNull(queuePressureThreshold);
        this.canceledTaskRatioThreshold = Objects.requireNonNull(canceledTaskRatioThreshold);
        this.nanoTime = Objects.requireNonNull(nanoTime);
        if (estimateExpiryNanos <= 0) {
            throw new IllegalArgumentException("estimateExpiryNanos must be positive");
        }
        this.estimateExpiryNanos = estimateExpiryNanos;
        this.maintenanceExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("ThreadPoolPurger-%d")
                .build());
    }

    /**
     * Discards cancellation estimates issued before this reset generation.
     *
     * <p>Already running maintenance is not interrupted. The generation check instead prevents stale
     * cancellation observations from scheduling a later purge after the reset.
     */
    public void clearPendingCancellations() {
        resetGeneration.incrementAndGet();
        states.values().forEach(PoolState::settleCurrentGeneration);
    }

    /**
     * Stops this purger's scheduler and releases its pool-level state. This method never shuts down
     * or otherwise mutates an observed application executor.
     */
    public void close() {
        maintenanceExecutor.shutdownNow();
        states.clear();
    }

    /**
     * Returns the queued-cancellation callback bound to the submitted task's executor. Queues
     * without a finite positive capacity — {@link java.util.concurrent.SynchronousQueue} and
     * unbounded queues such as {@code new LinkedBlockingQueue()} — receive a static no-op callback.
     *
     * <p>The returned callback must be invoked only when a submitted task is canceled before it
     * starts. It is safe to invoke more than once: accounting is heuristic and maintenance is
     * coalesced. The executor is keyed by object identity, not by its display name.
     *
     * @param executor actual supplied executor used to run the task
     * @return executor-bound cancellation callback
     */
    public Runnable cancellationObserverFor(ThreadPoolExecutor executor) {
        BlockingQueue<Runnable> queue = executor.getQueue();
        if (!hasFiniteCapacity(queue)) {
            return NOOP;
        }
        PoolState state = states.computeIfAbsent(executor, ignored -> new PoolState(executor, queue));
        return state::onTaskCancelled;
    }

    /** Reports whether a queue has a finite positive capacity worth monitoring. */
    private static boolean hasFiniteCapacity(BlockingQueue<?> queue) {
        int capacity = capacityOf(queue);
        return capacity > 0 && capacity < Integer.MAX_VALUE;
    }

    /** Reads one queue's capacity without requiring a concrete queue type. */
    private static int capacityOf(BlockingQueue<?> queue) {
        if (queue instanceof SmartBlockingQueue) {
            return ((SmartBlockingQueue<?>) queue).capacity();
        }
        return queue.size() + queue.remainingCapacity();
    }

    private final class PoolState {

        private final ThreadPoolExecutor executor;
        private final BlockingQueue<?> queue;
        private final AtomicLong issuedSequence = new AtomicLong();
        private final AtomicLong settledThrough = new AtomicLong();
        private final AtomicReference<MaintenanceState> maintenanceState = new AtomicReference<>(MaintenanceState.IDLE);
        private final AtomicReference<CancellationMarker> lastCancellation =
                new AtomicReference<>(new CancellationMarker(0L, 0L, 0L));
        private final AtomicReference<String> lastLoggedDecision = new AtomicReference<>();
        private final String executorId;

        /** Creates cancellation accounting state for one actual executor. */
        private PoolState(ThreadPoolExecutor executor, BlockingQueue<?> queue) {
            this.executor = executor;
            this.queue = queue;
            this.executorId =
                    executor.getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(executor));
        }

        /** Records one possible queued cancellation and evaluates maintenance only while idle. */
        private void onTaskCancelled() {
            if (!enabled.get()) {
                return;
            }
            long generation = resetGeneration.get();
            long sequence = issuedSequence.incrementAndGet();
            if (!enabled.get() || generation != resetGeneration.get()) {
                settleThrough(sequence);
                return;
            }
            if (maintenanceState.get() != MaintenanceState.IDLE) {
                return;
            }

            recordIdleCancellation(generation, sequence, nanoTime.getAsLong());
            if (!enabled.get() || generation != resetGeneration.get()) {
                settleThrough(sequence);
                return;
            }
            if (maintenanceState.get() != MaintenanceState.IDLE) {
                return;
            }
            evaluateAndSubmit(COALESCING_DELAY_MILLIS);
        }

        /** Expires only the old sequence boundary observed by one atomic marker update. */
        private void recordIdleCancellation(long generation, long sequence, long now) {
            CancellationMarker previous = lastCancellation.get();
            while (generation > previous.generation
                    || (generation == previous.generation && sequence > previous.sequence)) {
                CancellationMarker next = new CancellationMarker(generation, sequence, now);
                if (lastCancellation.compareAndSet(previous, next)) {
                    if (generation == previous.generation
                            && previous.timestampNanos != 0L
                            && now - previous.timestampNanos > estimateExpiryNanos) {
                        settleThrough(previous.sequence);
                    }
                    return;
                }
                previous = lastCancellation.get();
            }
        }

        /** Evaluates both advisory thresholds and submits one fixed-delay maintenance task. */
        private void evaluateAndSubmit(long delayMillis) {
            long estimatedCancelled = estimatedCancelled();
            if (estimatedCancelled <= 0L || !meetsThresholds(estimatedCancelled, true)) {
                return;
            }
            submitMaintenance(delayMillis, estimatedCancelled);
        }

        /** Claims the idle state and submits one maintenance task after a fixed delay. */
        private void submitMaintenance(long delayMillis, long estimatedCancelled) {
            if (maintenanceState.compareAndSet(MaintenanceState.IDLE, MaintenanceState.BUSY)) {
                logCurrentDecision("submitted", estimatedCancelled);
                try {
                    maintenanceExecutor.schedule(this::runMaintenance, delayMillis, TimeUnit.MILLISECONDS);
                } catch (RuntimeException e) {
                    maintenanceState.compareAndSet(MaintenanceState.BUSY, MaintenanceState.IDLE);
                    logCurrentDecision("failed-submit", estimatedCancelled());
                    LOGGER.log(Level.WARNING, "Unable to schedule canceled-task purge", e);
                }
            }
        }

        /**
         * Rechecks the latest snapshots and calls {@link ThreadPoolExecutor#purge()} when eligible.
         * One submitted task always runs exactly once, so the busy state needs no entry check.
         */
        private void runMaintenance() {
            long claimThrough = issuedSequence.get();
            try {
                long estimatedCancelled = estimatedCancelled();
                if (!enabled.get() || estimatedCancelled <= 0L || !meetsThresholds(estimatedCancelled, true)) {
                    return;
                }
                int beforeSize = queue.size();
                long started = System.nanoTime();
                try {
                    executor.purge();
                    settleThrough(claimThrough);
                    logPurge(estimatedCancelled, beforeSize, queue.size(), System.nanoTime() - started);
                } catch (RuntimeException e) {
                    logCurrentDecision("failed", estimatedCancelled());
                    LOGGER.log(Level.WARNING, "Unable to purge canceled tasks", e);
                }
            } finally {
                maintenanceState.set(MaintenanceState.IDLE);
                if (enabled.get() && issuedSequence.get() > claimThrough) {
                    evaluateAndSubmit(COALESCING_DELAY_MILLIS);
                }
            }
        }

        /** Returns the unsettled cancellation estimate without resetting concurrent signals. */
        private long estimatedCancelled() {
            return Math.max(0L, issuedSequence.get() - settledThrough.get());
        }

        /** Advances the settlement high-water mark without moving it backwards. */
        private void settleThrough(long sequence) {
            long settled = settledThrough.get();
            while (sequence > settled && !settledThrough.compareAndSet(settled, sequence)) {
                settled = settledThrough.get();
            }
        }

        /** Settles all signals visible to a disable/reset operation. */
        private void settleCurrentGeneration() {
            settleThrough(issuedSequence.get());
            lastLoggedDecision.set(null);
            long generation = resetGeneration.get();
            CancellationMarker marker = lastCancellation.get();
            while (marker.generation < generation
                    && !lastCancellation.compareAndSet(marker, new CancellationMarker(generation, 0L, 0L))) {
                marker = lastCancellation.get();
            }
            logCurrentDecision("disabled", 0L);
        }

        /** Evaluates advisory queue pressure and garbage ratio snapshots. */
        private boolean meetsThresholds(long canceled, boolean logSkip) {
            int queueSize = queue.size();
            if (queueSize == 0) {
                return false;
            }
            int capacity = capacityOf(queue);
            double queuePressure = (double) queueSize / capacity;
            double canceledRatio = (double) Math.min(canceled, queueSize) / capacity;
            double pressureThreshold = queuePressureThreshold.get();
            double ratioThreshold = canceledTaskRatioThreshold.get();
            if (queuePressure < pressureThreshold) {
                if (logSkip) {
                    logDecisionOnce(
                            "skip-pressure",
                            canceled,
                            queueSize,
                            capacity,
                            queuePressure,
                            pressureThreshold,
                            canceledRatio,
                            ratioThreshold);
                }
                return false;
            }
            if (canceledRatio < ratioThreshold) {
                if (logSkip) {
                    logDecisionOnce(
                            "skip-ratio",
                            canceled,
                            queueSize,
                            capacity,
                            queuePressure,
                            pressureThreshold,
                            canceledRatio,
                            ratioThreshold);
                }
                return false;
            }
            return true;
        }

        /** Emits a threshold decision only when its action changes during a signal burst. */
        private void logDecisionOnce(
                String action,
                long canceled,
                int queueSize,
                int capacity,
                double pressure,
                double pressureThreshold,
                double ratio,
                double ratioThreshold) {
            String previous = lastLoggedDecision.getAndSet(action);
            if (!action.equals(previous)) {
                logDecision(action, canceled, queueSize, capacity, pressure, pressureThreshold, ratio, ratioThreshold);
            }
        }

        /** Emits the current advisory queue snapshot at FINEST level. */
        private void logCurrentDecision(String action, long canceled) {
            if (!LOGGER.isLoggable(Level.FINEST)) {
                return;
            }
            int queueSize = queue.size();
            int capacity = capacityOf(queue);
            double pressure = (double) queueSize / capacity;
            double ratio = (double) Math.min(canceled, queueSize) / capacity;
            logDecision(
                    action,
                    canceled,
                    queueSize,
                    capacity,
                    pressure,
                    queuePressureThreshold.get(),
                    ratio,
                    canceledTaskRatioThreshold.get());
        }

        /** Writes one structured threshold decision without claiming exact queue accounting. */
        private void logDecision(
                String action,
                long canceled,
                int queueSize,
                int capacity,
                double pressure,
                double pressureThreshold,
                double ratio,
                double ratioThreshold) {
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.log(
                        Level.FINEST,
                        "purge action={0} executor={1} queueSize={2} capacity={3} "
                                + "pressure={4} pressureThreshold={5} estimatedCancelled={6} "
                                + "canceledRatio={7} canceledRatioThreshold={8}",
                        new Object[] {
                            action,
                            executorId,
                            queueSize,
                            capacity,
                            pressure,
                            pressureThreshold,
                            canceled,
                            ratio,
                            ratioThreshold
                        });
            }
        }

        /** Writes approximate queue change and elapsed purge time at FINEST level. */
        private void logPurge(long claimed, int beforeSize, int afterSize, long durationNanos) {
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.log(
                        Level.FINEST,
                        "purge action=purged executor={0} estimatedCancelled={1} beforeSize={2} "
                                + "afterSize={3} approximateSizeChange={4} durationNanos={5}",
                        new Object[] {executorId, claimed, beforeSize, afterSize, beforeSize - afterSize, durationNanos
                        });
            }
        }
    }
}
