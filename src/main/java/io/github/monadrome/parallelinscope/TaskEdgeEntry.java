package io.github.monadrome.parallelinscope;

import com.google.common.graph.EndpointPair;

/**
 * Bundles an {@link EndpointPair} edge with its associated {@link TaskEdge} metadata. Used in the
 * concurrent queue within {@link TaskGraphData}.
 *
 * @author Eric Lin (linqinghua4 at gmail dot com)
 */
final class TaskEdgeEntry {

    private final EndpointPair<String> edge;
    private final TaskEdge value;

    public TaskEdgeEntry(EndpointPair<String> edge, TaskEdge value) {
        this.edge = edge;
        this.value = value;
    }

    EndpointPair<String> edge() {
        return edge;
    }

    TaskEdge value() {
        return value;
    }
}
