package io.github.huatalk.parallelinscope.context.graph;

import com.google.common.graph.ElementOrder;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.Graphs;
import com.google.common.graph.ImmutableValueGraph;
import com.google.common.graph.ValueGraph;
import com.google.common.graph.ValueGraphBuilder;
import io.github.huatalk.parallelinscope.scope.ExecutorIdentity;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedTransferQueue;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Task dependency graph data for potential deadlock detection.
 *
 * <p>Holds a directed {@link ValueGraph} of task dependencies shared by all threads within a
 * single request. Records parent-child task relationships as edges with {@link TaskEdge} metadata
 * (parallelism, task type, executor name, task count, timeout).
 *
 * <p>Instances are created and owned by {@code TaskGraphObservationContext}; at request end the
 * context builds directed graphs at both task level and executor level, checking for cycles
 * (potential deadlocks) and self-loops.
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
@SuppressWarnings("UnstableApiUsage")
public final class TaskGraphData {

    final BlockingQueue<TaskEdgeEntry> subTaskList = new LinkedTransferQueue<>();
    private final Map<String, String> nodeLabels = new ConcurrentHashMap<>();

    private volatile ValueGraph<String, List<TaskEdge>> graph;
    private volatile Boolean taskCycle;
    private volatile Boolean selfLoop;

    /** Creates an empty request-scoped graph. */
    public TaskGraphData() {}

    /**
     * Returns the task dependency graph.
     *
     * @return the task dependency graph
     */
    public ValueGraph<String, List<TaskEdge>> graph() {
        if (graph == null) {
            synchronized (this) {
                if (graph == null) {
                    graph = generateGraph();
                }
            }
        }
        return graph;
    }

    /**
     * Checks whether the task graph contains a cycle.
     *
     * @return {@code true} when a task cycle exists
     */
    public boolean taskCycle() {
        if (taskCycle == null) {
            taskCycle = checkTaskCycle();
        }
        return taskCycle;
    }

    /**
     * Checks whether the task graph contains a self-loop.
     *
     * @return {@code true} when a task self-loop exists
     */
    public boolean selfLoop() {
        if (selfLoop == null) {
            selfLoop = checkSelfLoop();
        }
        return selfLoop;
    }

    /**
     * Returns the executor graph built from deadlock-risk snapshots on task edges.
     *
     * @return executor dependency graph
     */
    public ValueGraph<String, List<TaskEdge>> executorGraph() {
        return generateExecutorGraph();
    }

    /**
     * Returns whether the captured executor graph contains a cycle.
     *
     * @return {@code true} when an executor cycle exists
     */
    public boolean executorCycle() {
        ValueGraph<ExecutorIdentity, List<TaskEdge>> g = generateExecutorIdentityGraph();
        if (g != null && !g.nodes().isEmpty()) {
            return Graphs.hasCycle(g.asGraph());
        }
        ValueGraph<String, List<TaskEdge>> legacy = executorGraph();
        return legacy != null && Graphs.hasCycle(legacy.asGraph());
    }

    /**
     * Returns whether the captured executor graph contains a self-loop.
     *
     * @return {@code true} when an executor self-loop exists
     */
    public boolean executorSelfLoop() {
        ValueGraph<ExecutorIdentity, List<TaskEdge>> identityGraph = generateExecutorIdentityGraph();
        if (identityGraph != null && !identityGraph.nodes().isEmpty()) {
            return identityGraph.edges().stream().anyMatch(p -> Objects.equals(p.nodeU(), p.nodeV()));
        }
        return executorGraph().edges().stream().anyMatch(p -> Objects.equals(p.nodeU(), p.nodeV()));
    }

    /** Records one parent-to-child edge. Batch IDs, not reusable task names, keep nodes distinct. */
    public void logTaskPair(
            @Nullable String parentId,
            @Nullable String parentLabel,
            String childId,
            @Nullable String childLabel,
            TaskEdge edge) {
        String source = parentId == null ? "root" : parentId;
        nodeLabels.put(source, parentLabel == null ? "NA" : parentLabel);
        nodeLabels.put(childId, childLabel == null ? "NA" : childLabel);
        subTaskList.add(new TaskEdgeEntry(EndpointPair.ordered(source, childId), edge));
    }

    ValueGraph<String, List<TaskEdge>> generateGraph() {
        Map<EndpointPair<String>, List<TaskEdge>> edgeMap = new LinkedHashMap<>();
        for (TaskEdgeEntry entry : subTaskList) {
            edgeMap.computeIfAbsent(entry.edge(), k -> new ArrayList<>()).add(entry.value());
        }
        ImmutableValueGraph.Builder<String, List<TaskEdge>> graphBuilder =
                ValueGraphBuilder.directed().allowsSelfLoops(true).immutable();
        for (Map.Entry<EndpointPair<String>, List<TaskEdge>> entry : edgeMap.entrySet()) {
            graphBuilder.putEdgeValue(
                    entry.getKey().source(), entry.getKey().target(), Collections.unmodifiableList(entry.getValue()));
        }
        return graphBuilder.build();
    }

    /** Returns the node prefixed with its display label, or the bare node when unlabelled. */
    public String displayNode(String node) {
        String label = nodeLabels.get(node);
        return label == null ? node : label + "[" + node + "]";
    }

    boolean checkTaskCycle() {
        ValueGraph<String, List<TaskEdge>> g = graph();
        return g != null && Graphs.hasCycle(g.asGraph());
    }

    boolean checkSelfLoop() {
        return graph().edges().stream().anyMatch(p -> Objects.equals(p.nodeU(), p.nodeV()));
    }

    ValueGraph<String, List<TaskEdge>> generateExecutorGraph() {
        Map<EndpointPair<String>, List<TaskEdge>> executorEdges = new LinkedHashMap<>();

        for (EndpointPair<String> taskEdgePair : graph().edges()) {
            List<TaskEdge> edges = Objects.requireNonNull(
                    graph().edgeValueOrDefault(taskEdgePair.source(), taskEdgePair.target(), Collections.emptyList()));
            for (TaskEdge taskEdge : edges) {
                String sourceExecutor = taskEdge.sourceExecutorName();
                String targetExecutor = taskEdge.executorName();
                if (!taskEdge.executorDeadlockProne()) {
                    continue;
                }
                EndpointPair<String> executorPair = EndpointPair.ordered(sourceExecutor, targetExecutor);
                executorEdges
                        .computeIfAbsent(executorPair, k -> new ArrayList<>())
                        .add(taskEdge);
            }
        }

        ImmutableValueGraph.Builder<String, List<TaskEdge>> graphBuilder = ValueGraphBuilder.directed()
                .allowsSelfLoops(true)
                .incidentEdgeOrder(ElementOrder.stable())
                .immutable();
        for (Map.Entry<EndpointPair<String>, List<TaskEdge>> entry : executorEdges.entrySet()) {
            graphBuilder.putEdgeValue(
                    entry.getKey().source(), entry.getKey().target(), Collections.unmodifiableList(entry.getValue()));
        }
        return graphBuilder.build();
    }

    private ValueGraph<ExecutorIdentity, List<TaskEdge>> generateExecutorIdentityGraph() {
        Map<EndpointPair<ExecutorIdentity>, List<TaskEdge>> executorEdges = new LinkedHashMap<>();
        for (TaskEdgeEntry entry : subTaskList) {
            TaskEdge edge = entry.value();
            if (!edge.executorDeadlockProne()
                    || edge.executorIdentity() == null
                    || edge.sourceExecutorIdentity() == null) {
                continue;
            }
            EndpointPair<ExecutorIdentity> pair =
                    EndpointPair.ordered(edge.sourceExecutorIdentity(), edge.executorIdentity());
            executorEdges.computeIfAbsent(pair, k -> new ArrayList<>()).add(edge);
        }
        ImmutableValueGraph.Builder<ExecutorIdentity, List<TaskEdge>> builder = ValueGraphBuilder.directed()
                .allowsSelfLoops(true)
                .incidentEdgeOrder(ElementOrder.stable())
                .immutable();
        for (Map.Entry<EndpointPair<ExecutorIdentity>, List<TaskEdge>> entry : executorEdges.entrySet()) {
            builder.putEdgeValue(
                    entry.getKey().source(), entry.getKey().target(), Collections.unmodifiableList(entry.getValue()));
        }
        return builder.build();
    }
}
