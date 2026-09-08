package io.github.huatalk.parallelinscope.spi;

import io.github.huatalk.parallelinscope.scope.TaskGroupResult;
import java.util.Objects;

/** Receives one immutable event when a parallel task group reaches terminal convergence. */
@FunctionalInterface
public interface TaskGroupListener {
    void onTaskGroupComplete(TaskGroupEvent event);

    /** Immutable group completion event. */
    final class TaskGroupEvent {
        private final TaskGroupResult result;

        public TaskGroupEvent(TaskGroupResult result) {
            this.result = Objects.requireNonNull(result, "result cannot be null");
        }

        public TaskGroupResult result() {
            return result;
        }
    }
}
