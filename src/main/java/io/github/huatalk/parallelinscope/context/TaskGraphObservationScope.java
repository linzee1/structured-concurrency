package io.github.huatalk.parallelinscope.context;

import com.alibaba.ttl.TransmittableThreadLocal;
import io.github.huatalk.parallelinscope.context.graph.TaskEdge;
import io.github.huatalk.parallelinscope.context.graph.TaskGraphData;
import io.github.huatalk.parallelinscope.scope.GlobalPar;
import io.github.huatalk.parallelinscope.spi.DeadlockDetectionListener;
import io.github.huatalk.parallelinscope.spi.DeadlockDetectionListener.DeadlockDetectionEvent;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Explicit request-level task-graph observation scope owned by one {@link GlobalPar}.
 *
 * <p>The scope is a request-level global object: it is held in a {@link TransmittableThreadLocal}
 * with identity copy semantics, so every worker thread within the request observes the same
 * instance and shares its {@link TaskGraphData}. Nested scopes stack on the opening thread, and
 * {@link #close()} is idempotent so it can be used with try-with-resources. Closing the scope runs
 * the deadlock detection pass over the recorded graph and notifies the owner's
 * {@code GlobalParDeadlockPolicy} listeners.
 *
 * <p>Lifecycle:
 *
 * <ul>
 *   <li>Request start: {@link GlobalPar#openTaskGraphObservation()} creates the scope and a fresh
 *       {@link TaskGraphData}
 *   <li>During request: the GlobalPar execution path records batch-instance relationships via
 *       {@link #logTaskPair}
 *   <li>Request end: {@link #close()} checks for cycles and notifies listeners
 * </ul>
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
public final class TaskGraphObservationScope implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(TaskGraphObservationScope.class.getName());

    /** Identity-propagating TTL: the default copy returns the same scope reference to workers. */
    private static final TransmittableThreadLocal<TaskGraphObservationScope> CURRENT =
            new TransmittableThreadLocal<TaskGraphObservationScope>() {};

    private final GlobalPar owner;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final @Nullable TaskGraphObservationScope previousScope;
    private final TaskGraphData data;

    public TaskGraphObservationScope(GlobalPar owner) {
        this.owner = Objects.requireNonNull(owner, "owner cannot be null");
        this.previousScope = CURRENT.get();
        CURRENT.set(this);
        this.data = new TaskGraphData();
    }

    /** Returns the observation scope active on the calling thread, if any. */
    public static @Nullable TaskGraphObservationScope current() {
        TaskGraphObservationScope scope = CURRENT.get();
        return scope != null && !scope.closed() ? scope : null;
    }

    /**
     * Gets the current scope's graph data.
     *
     * @return the current request data, or {@code null} outside an observation scope
     */
    public static @Nullable TaskGraphData data() {
        TaskGraphObservationScope scope = current();
        return scope == null ? null : scope.data;
    }

    /** Installs an observation scope on the current worker thread. */
    public static void install(TaskGraphObservationScope scope) {
        CURRENT.set(Objects.requireNonNull(scope, "scope cannot be null"));
    }

    /** Restores a scope captured before entering a scoped worker task. */
    public static void restore(@Nullable TaskGraphObservationScope scope) {
        if (scope == null) CURRENT.remove();
        else CURRENT.set(scope);
    }

    /** Records a new-model edge using unique batch identities and display labels. */
    public static void logTaskPair(
            @Nullable String parentId,
            @Nullable String parentLabel,
            String childId,
            @Nullable String childLabel,
            TaskEdge edge) {
        TaskGraphData data = data();
        if (data == null) return;
        data.logTaskPair(parentId, parentLabel, childId, childLabel, edge);
    }

    /**
     * Checks if any task cycle exists.
     *
     * @return {@code true} when the current request graph contains a task cycle
     */
    public static boolean hasTaskCycle() {
        TaskGraphData data = data();
        return data != null && data.taskCycle();
    }

    /**
     * Checks if any task self-loop exists.
     *
     * @return {@code true} when the current request graph contains a task self-loop
     */
    public static boolean hasSelfLoop() {
        TaskGraphData data = data();
        return data != null && data.selfLoop();
    }

    /**
     * Returns whether the current request graph contains an executor cycle.
     *
     * @return {@code true} when an executor cycle exists
     */
    public static boolean hasExecutorCycle() {
        TaskGraphData data = data();
        return data != null && data.executorCycle();
    }

    /**
     * Returns whether the current request graph contains an executor self-loop.
     *
     * @return {@code true} when an executor self-loop exists
     */
    public static boolean hasExecutorSelfLoop() {
        TaskGraphData data = data();
        return data != null && data.executorSelfLoop();
    }

    public GlobalPar owner() {
        return owner;
    }

    public boolean closed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                runDeadlockDetection();
            } finally {
                if (CURRENT.get() == this) {
                    if (previousScope == null) CURRENT.remove();
                    else CURRENT.set(previousScope);
                }
            }
        }
    }

    /** Runs the GlobalPar deadlock policy over this scope's graph and restores the outer scope. */
    private void runDeadlockDetection() {
        try {
            if (!owner.deadlockPolicy().enabled()) {
                return;
            }
            DeadlockDetectionEvent event = buildDetectionEvent(data);
            if (event != null && event.hasAnyIssue()) {
                logger.log(Level.WARNING, "[[title=TaskGraph,function=deadlockDetection]]" + event);
                for (DeadlockDetectionListener listener : owner.deadlockPolicy().listeners()) {
                    try {
                        listener.onDetection(event);
                    } catch (Exception e) {
                        logger.log(
                                Level.WARNING,
                                "DeadlockDetectionListener callback failed: "
                                        + listener.getClass().getName(),
                                e);
                    }
                }
            }
        } catch (Exception e) {
            logger.log(
                    Level.WARNING,
                    "[[title=TaskGraph,function=finishObservation]]"
                            + "Failed to run GlobalPar potential-deadlock detection",
                    e);
        }
    }

    private static @Nullable DeadlockDetectionEvent buildDetectionEvent(TaskGraphData data) {
        boolean hasTaskCycle = data.taskCycle();
        boolean hasSelfLoop = data.selfLoop();
        boolean hasExecutorCycle = data.executorCycle();
        boolean hasExecutorSelfLoop = data.executorSelfLoop();

        if (!hasTaskCycle && !hasSelfLoop && !hasExecutorCycle && !hasExecutorSelfLoop) {
            return null;
        }

        String taskEdges = data.graph().edges().stream()
                .map(p -> {
                    List<TaskEdge> edges =
                            data.graph().edgeValueOrDefault(p.source(), p.target(), Collections.emptyList());
                    return data.displayNode(p.source()) + " -> " + data.displayNode(p.target()) + " " + edges;
                })
                .collect(Collectors.joining(", "));
        String executorEdges = data.executorGraph().edges().stream()
                .map(p -> {
                    List<TaskEdge> edges =
                            data.executorGraph().edgeValueOrDefault(p.source(), p.target(), Collections.emptyList());
                    return p.source() + " -> " + p.target() + " " + edges;
                })
                .collect(Collectors.joining(", "));

        return new DeadlockDetectionEvent(
                hasTaskCycle, hasSelfLoop,
                hasExecutorCycle, hasExecutorSelfLoop,
                taskEdges, executorEdges);
    }
}
