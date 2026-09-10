package io.github.huatalk.parallelinscope.scope;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.github.huatalk.parallelinscope.cancel.HeuristicPurger;
import io.github.huatalk.parallelinscope.context.TaskGraphObservationContext;
import io.github.huatalk.parallelinscope.spi.ExecutionPhase;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Immutable application execution topology containing logical {@link Par} entries.
 *
 * <p>Registration is a composition-root operation: after {@link Builder#build()}, the names,
 * policies, and executor bindings cannot change. This is an application-scoped resource, normally
 * created at the composition root and closed during application or container shutdown. It owns its
 * timer, submission, and maintenance services; registered executors are borrowed and are never
 * shut down by this object.
 *
 * <p>{@link #close()} immediately rejects all new {@link Par#map(List, Function, BatchExecutionOptions)}
 * calls. Batches admitted before closing retain their submission, timeout, and cancellation
 * processing while the framework-owned services drain; {@code close()} itself does not wait for
 * those batches to finish.
 */
public final class GlobalPar implements AutoCloseable {
    private static final AtomicReference<GlobalPar> INSTALLED = new AtomicReference<>();
    private final Map<String, Par> pars;
    private final Map<String, ExecutorRuntime> runtimes;
    private final Map<ExecutorIdentity, ExecutorRuntime> runtimesByIdentity;
    private final String defaultName;
    private final GlobalExecutionPolicy executionPolicy;
    private final Map<String, GlobalExecutionPolicy> policyOverrides;
    private final GlobalParDeadlockPolicy deadlockPolicy;
    private final GlobalParPurgePolicy purgePolicy;
    private final HeuristicPurger purger;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicInteger activeAdmissions = new AtomicInteger();
    private final AtomicInteger activeBatches = new AtomicInteger();
    private final AtomicBoolean servicesShutdown = new AtomicBoolean();
    private final ScheduledExecutorService timerService;
    private final ExecutorService timeoutActionPool;
    private final ListeningExecutorService submitterPool;

    private GlobalPar(Builder builder) {
        this.executionPolicy = builder.executionPolicy;
        this.policyOverrides = Collections.unmodifiableMap(new LinkedHashMap<>(builder.policyOverrides));
        this.deadlockPolicy = builder.deadlockPolicy;
        this.purgePolicy = builder.purgePolicy;
        this.purger = new HeuristicPurger(
                new AtomicBoolean(purgePolicy.enabled()),
                new AtomicDouble(purgePolicy.queuePressureThreshold()),
                new AtomicDouble(purgePolicy.canceledTaskRatioThreshold()));
        AtomicInteger threadId = new AtomicInteger();
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "GlobalPar-runtime-" + threadId.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this.timerService = Executors.newSingleThreadScheduledExecutor(factory);
        this.timeoutActionPool = Executors.newCachedThreadPool(factory);
        this.submitterPool = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool(factory));
        this.defaultName = builder.defaultName;
        Map<String, Par> builtPars = new LinkedHashMap<>();
        Map<String, ExecutorRuntime> builtRuntimes = new LinkedHashMap<>();
        Map<ExecutorIdentity, ExecutorRuntime> identityRuntimes = new LinkedHashMap<>();
        for (Map.Entry<String, ExecutorService> entry : builder.executors.entrySet()) {
            ExecutorIdentity identity = new ExecutorIdentity(entry.getValue());
            ExecutorRuntime runtime = identityRuntimes.get(identity);
            if (runtime == null) {
                runtime = new ExecutorRuntime(entry.getValue());
                identityRuntimes.put(identity, runtime);
            }
            bindPurgeObserver(runtime);
            builtRuntimes.put(entry.getKey(), runtime);
            builtPars.put(entry.getKey(), Par.forGlobal(this, entry.getKey(), runtime));
        }
        this.runtimes = Collections.unmodifiableMap(builtRuntimes);
        this.runtimesByIdentity = Collections.unmodifiableMap(identityRuntimes);
        this.pars = Collections.unmodifiableMap(builtPars);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Installs the optional process-wide convenience instance exactly once.
     *
     * <p>This does not transfer ownership of supplied executors. Applications should normally pass
     * individual {@code Par} instances to their components rather than use {@link #global()} as a
     * service locator.
     */
    public static void installGlobal(GlobalPar globalPar) {
        Objects.requireNonNull(globalPar, "globalPar cannot be null");
        if (!INSTALLED.compareAndSet(null, globalPar)) {
            throw new IllegalStateException("GlobalPar is already installed");
        }
    }

    public static GlobalPar global() {
        GlobalPar value = INSTALLED.get();
        if (value == null) throw new IllegalStateException("GlobalPar has not been installed");
        return value;
    }

    public Par defaultPar() {
        if (defaultName == null) throw new IllegalStateException("GlobalPar has no default Par");
        return par(defaultName);
    }

    public Par par(String name) {
        Par value = pars.get(name);
        if (value == null) throw new IllegalArgumentException("No Par registered with name '" + name + "'");
        return value;
    }

    public Optional<Par> find(String name) {
        return Optional.ofNullable(pars.get(name));
    }

    public GlobalExecutionPolicy executionPolicy() {
        return executionPolicy;
    }

    public GlobalExecutionPolicy executionPolicyFor(String name) {
        GlobalExecutionPolicy override = policyOverrides.get(name);
        return override == null ? executionPolicy : override;
    }

    public GlobalParDeadlockPolicy deadlockPolicy() {
        return deadlockPolicy;
    }

    public GlobalParPurgePolicy purgePolicy() {
        return purgePolicy;
    }

    public Map<String, Par> pars() {
        return pars;
    }

    /**
     * Creates a configuration-only builder for one fixed heterogeneous task group. The builder is
     * one-shot; freeze and submit with {@link ParallelTaskGroup.Builder#buildAndSubmitAll()}.
     *
     * @throws IllegalStateException if this GlobalPar has begun shutdown
     */
    public ParallelTaskGroup.Builder taskGroupBuilder(TaskGroupOptions options) {
        Objects.requireNonNull(options, "options cannot be null");
        return whileOpen(() -> new ParallelTaskGroup.Builder(this, options));
    }

    /** Package-private diagnostic topology for scope tests and internal maintenance. */
    Map<String, ExecutorRuntime> runtimes() {
        return runtimes;
    }

    /** Package-private identity index; runtime binding is not a public application API. */
    Map<ExecutorIdentity, ExecutorRuntime> runtimesByIdentity() {
        return runtimesByIdentity;
    }

    HeuristicPurger purger() {
        return purger;
    }

    ScheduledExecutorService timerService() {
        return timerService;
    }

    ListeningExecutorService submitterPool() {
        return submitterPool;
    }

    /**
     * Opens a request-scoped task-graph observation owned by this topology.
     *
     * <p>The caller must close the returned context. Only nested batches belonging to this same
     * {@code GlobalPar} join it; crossing to another topology deliberately starts no shared graph.
     *
     * @throws IllegalStateException if this GlobalPar has begun shutdown
     */
    public TaskGraphObservationContext openTaskGraphObservation() {
        return whileOpen(() -> new TaskGraphObservationContext(this));
    }

    /**
     * Returns whether shutdown has begun.
     *
     * <p>A {@code true} result means new batch submissions and observation scopes are rejected.
     */
    public boolean closed() {
        return closed.get();
    }

    /**
     * Rejects new work and begins releasing framework-owned resources.
     *
     * <p>This method is idempotent and never shuts down a registered executor. It coordinates with
     * a {@link Par#map(List, Function, BatchExecutionOptions)} call already setting up a batch, so that
     * call either completes setup and returns its result or is rejected before any task is
     * submitted. Services drain batches admitted before shutdown; this method does not wait for
     * their task bodies to finish.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            purger.close();
            shutdownServicesWhenAdmissionsComplete();
        }
    }

    /** Runs one synchronous batch setup while this topology remains open. */
    <T> T whileOpen(Supplier<T> action) {
        Objects.requireNonNull(action, "action cannot be null");
        enterAdmission();
        try {
            return action.get();
        } finally {
            if (activeAdmissions.decrementAndGet() == 0) {
                shutdownServicesWhenAdmissionsComplete();
            }
        }
    }

    private void enterAdmission() {
        while (true) {
            if (closed.get()) throw new IllegalStateException("GlobalPar is closed");
            activeAdmissions.incrementAndGet();
            if (!closed.get()) return;
            if (activeAdmissions.decrementAndGet() == 0) {
                shutdownServicesWhenAdmissionsComplete();
            }
        }
    }

    private void shutdownServicesWhenAdmissionsComplete() {
        if (closed.get()
                && activeAdmissions.get() == 0
                && activeBatches.get() == 0
                && servicesShutdown.compareAndSet(false, true)) {
            timerService.shutdown();
            timeoutActionPool.shutdown();
            submitterPool.shutdown();
        }
    }

    /** Keeps a submitted batch's futures from being dropped until every one reaches a terminal state. */
    void retainUntilComplete(List<? extends ListenableFuture<?>> results) {
        if (results.isEmpty()) return;
        activeBatches.incrementAndGet();
        Futures.successfulAsList(results)
                .addListener(
                        () -> {
                            activeBatches.decrementAndGet();
                            shutdownServicesWhenAdmissionsComplete();
                        },
                        MoreExecutors.directExecutor());
    }

    /** Scheduler adapter that keeps deadline detection separate from timeout actions. */
    ScheduledExecutorService timeoutScheduler() {
        return new DispatchingScheduledExecutorService(timerService, timeoutActionPool);
    }

    private static final class DispatchingScheduledExecutorService extends AbstractExecutorService
            implements ScheduledExecutorService {
        private final ScheduledExecutorService scheduler;
        private final ExecutorService actions;

        private DispatchingScheduledExecutorService(ScheduledExecutorService scheduler, ExecutorService actions) {
            this.scheduler = scheduler;
            this.actions = actions;
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return scheduler.schedule(() -> actions.execute(command), delay, unit);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("timeout scheduler only supports Runnable deadlines");
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            throw new UnsupportedOperationException("timeout scheduler does not support periodic tasks");
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("timeout scheduler does not support periodic tasks");
        }

        @Override
        public void execute(Runnable command) {
            if (scheduler.isShutdown()) throw new RejectedExecutionException("Timer scheduler is shut down");
            actions.execute(command);
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException("timeout scheduler lifecycle is owned by GlobalPar");
        }

        @Override
        public java.util.List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException("timeout scheduler lifecycle is owned by GlobalPar");
        }

        @Override
        public boolean isShutdown() {
            return scheduler.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return scheduler.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return scheduler.awaitTermination(timeout, unit);
        }
    }

    private void bindPurgeObserver(ExecutorRuntime runtime) {
        if (!(runtime.suppliedExecutor() instanceof ThreadPoolExecutor)) return;
        Runnable observer = purger.cancellationObserverFor((ThreadPoolExecutor) runtime.suppliedExecutor());
        runtime.setPhaseObserver(phase -> {
            if (phase == ExecutionPhase.CANCELLED_BEFORE_RUN) observer.run();
        });
    }

    public static final class Builder {
        private final Map<String, ExecutorService> executors = new LinkedHashMap<>();
        private GlobalExecutionPolicy executionPolicy =
                GlobalExecutionPolicy.builder().build();
        private final Map<String, GlobalExecutionPolicy> policyOverrides = new LinkedHashMap<>();
        private GlobalParDeadlockPolicy deadlockPolicy =
                GlobalParDeadlockPolicy.builder().build();
        private GlobalParPurgePolicy purgePolicy =
                GlobalParPurgePolicy.builder().build();
        private String defaultName;

        public Builder executionPolicy(GlobalExecutionPolicy policy) {
            this.executionPolicy = Objects.requireNonNull(policy);
            return this;
        }

        public Builder parPolicyOverride(String name, GlobalExecutionPolicy policy) {
            if (name == null || name.isEmpty()) throw new IllegalArgumentException("name must not be empty");
            if (policyOverrides.containsKey(name)) {
                throw new IllegalArgumentException("Duplicate policy override for Par '" + name + "'");
            }
            policyOverrides.put(name, Objects.requireNonNull(policy));
            return this;
        }

        public Builder deadlockPolicy(GlobalParDeadlockPolicy policy) {
            this.deadlockPolicy = Objects.requireNonNull(policy);
            return this;
        }

        public Builder purgePolicy(GlobalParPurgePolicy policy) {
            this.purgePolicy = Objects.requireNonNull(policy);
            return this;
        }

        /**
         * Registers a logical entry with one exact executor object.
         *
         * <p>The name is only a build-time lookup and diagnostic label. Executor sharing is instead
         * detected by object identity, so two names may intentionally use the same physical pool.
         */
        public Builder register(String name, ExecutorService executor) {
            if (name == null || name.isEmpty()) throw new IllegalArgumentException("name must not be empty");
            if (executors.containsKey(name)) throw new IllegalArgumentException("Duplicate Par name '" + name + "'");
            executors.put(name, Objects.requireNonNull(executor));
            return this;
        }

        public Builder defaultPar(String name) {
            if (name == null || name.isEmpty()) throw new IllegalArgumentException("default name must not be empty");
            if (defaultName != null) throw new IllegalStateException("default Par already configured");
            defaultName = name;
            return this;
        }

        public GlobalPar build() {
            if (defaultName != null && !executors.containsKey(defaultName)) {
                throw new IllegalArgumentException("default Par is not registered: " + defaultName);
            }
            for (String name : policyOverrides.keySet()) {
                if (!executors.containsKey(name)) {
                    throw new IllegalArgumentException("policy override is not registered: " + name);
                }
            }
            return new GlobalPar(this);
        }
    }
}
