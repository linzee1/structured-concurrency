package io.github.huatalk.parallelinscope.scope;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.github.huatalk.parallelinscope.spi.ExecutionPhase;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

/**
 * Runtime capability record for one supplied executor. Internal to the {@code GlobalPar} package.
 *
 * <p>The supplied executor is the resource identity used for queue inspection, purge, and blocking
 * risk. The submission executor is either that same object or a Guava listening adapter used only
 * to obtain {@code ListenableFuture}s; the adapter must never be mistaken for the physical pool.
 */
final class ExecutorRuntime {
    private final ExecutorService suppliedExecutor;
    private final ListeningExecutorService submissionExecutor;
    private final boolean adapter;
    private final ExecutorIdentity identity;
    private final BlockingRisk blockingRisk;
    private volatile Consumer<? super ExecutionPhase> phaseObserver = phase -> {};

    ExecutorRuntime(ExecutorService suppliedExecutor) {
        this(suppliedExecutor, detectRisk(suppliedExecutor));
    }

    ExecutorRuntime(ExecutorService suppliedExecutor, BlockingRisk blockingRisk) {
        this.suppliedExecutor = Objects.requireNonNull(suppliedExecutor);
        this.identity = new ExecutorIdentity(suppliedExecutor);
        this.blockingRisk = Objects.requireNonNull(blockingRisk);
        if (suppliedExecutor instanceof ListeningExecutorService) {
            this.submissionExecutor = (ListeningExecutorService) suppliedExecutor;
            this.adapter = false;
        } else {
            this.submissionExecutor = MoreExecutors.listeningDecorator(suppliedExecutor);
            this.adapter = true;
        }
    }

    ExecutorService suppliedExecutor() {
        return suppliedExecutor;
    }

    ListeningExecutorService submissionExecutor() {
        return submissionExecutor;
    }

    boolean submissionExecutorIsAdapter() {
        return adapter;
    }

    ExecutorIdentity identity() {
        return identity;
    }

    BlockingRisk blockingRisk() {
        return blockingRisk;
    }

    Consumer<? super ExecutionPhase> phaseObserver() {
        return phaseObserver;
    }

    void setPhaseObserver(Consumer<? super ExecutionPhase> observer) {
        this.phaseObserver = Objects.requireNonNull(observer);
    }

    private static BlockingRisk detectRisk(ExecutorService executor) {
        if (executor instanceof ThreadPoolExecutor) {
            return BlockingRisk.BOUNDED_PLATFORM_POOL;
        }
        return BlockingRisk.UNKNOWN;
    }
}
