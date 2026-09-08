package io.github.huatalk.parallelinscope.scope;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.github.huatalk.parallelinscope.cancel.CancellationToken;
import io.github.huatalk.parallelinscope.context.SubmissionScope;
import io.github.huatalk.parallelinscope.context.TaskGraphObservationContext;
import io.github.huatalk.parallelinscope.context.graph.TaskEdge;
import io.github.huatalk.parallelinscope.internal.ExecutionPhaseHintFuture;
import io.github.huatalk.parallelinscope.internal.SubmissionException;
import io.github.huatalk.parallelinscope.internal.TaskExecutionContext;
import io.github.huatalk.parallelinscope.spi.TaskGroupListener;
import io.github.huatalk.parallelinscope.spi.TaskGroupListener.TaskGroupEvent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/** A fixed, heterogeneous set of named tasks submitted at one explicit build boundary. */
public final class ParallelTaskGroup implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(ParallelTaskGroup.class.getName());

    private final String groupId = UUID.randomUUID().toString();
    private final String groupName;
    private final long startTimeNanos;
    private final long deadlineNanos;
    private final List<TaskGroupListener> listeners;
    private final Map<String, MemberState> memberStates;
    private final Map<String, ListenableFuture<?>> members;
    private final SettableFuture<TaskGroupResult> completion = SettableFuture.create();
    private final CancellationToken groupToken;

    private int terminalCount;
    private @Nullable TaskGroupCompletionReason completionReason;
    private @Nullable String failedMemberName;
    private @Nullable ScheduledFuture<?> deadlineTimer;
    private boolean closed;

    private ParallelTaskGroup(
            String groupName,
            long startTimeNanos,
            long deadlineNanos,
            List<TaskGroupListener> listeners,
            CancellationToken groupToken,
            Map<String, MemberState> memberStates) {
        this.groupName = groupName;
        this.startTimeNanos = startTimeNanos;
        this.deadlineNanos = deadlineNanos;
        this.listeners = listeners;
        this.groupToken = groupToken;
        this.memberStates = new LinkedHashMap<>(memberStates);
        Map<String, ListenableFuture<?>> publicMembers = new LinkedHashMap<>();
        for (MemberState member : memberStates.values()) publicMembers.put(member.name, member.future);
        this.members = Collections.unmodifiableMap(publicMembers);
    }

    public String groupId() {
        return groupId;
    }

    public String groupName() {
        return groupName;
    }

    public ListenableFuture<TaskGroupResult> completionFuture() {
        return completion;
    }

    public Optional<ListenableFuture<?>> findMember(String memberName) {
        return Optional.ofNullable(members.get(memberName));
    }

    public Map<String, ListenableFuture<?>> members() {
        return members;
    }

    /** Cancels every unfinished member without blocking for user code to stop. */
    public void cancel() {
        cancelGroup(TaskGroupCompletionReason.CANCELED, TaskOutcome.GROUP_CANCELED);
    }

    @Override
    public void close() {
        if (!completion.isDone()) cancel();
    }

    private void start(GlobalPar global) {
        if (memberStates.isEmpty()) {
            completeEmpty();
            return;
        }
        groupToken.addCompletionListener(
                () -> {
                    if (groupToken.state() == CancellationToken.State.PROPAGATING_CANCELED) {
                        cancelGroup(TaskGroupCompletionReason.CANCELED, TaskOutcome.GROUP_CANCELED);
                    }
                },
                directExecutor());
        long delay = Math.max(0L, deadlineNanos - System.nanoTime());
        deadlineTimer = global.timeoutScheduler()
                .schedule(
                        () -> cancelGroup(TaskGroupCompletionReason.TIMEOUT, TaskOutcome.TIMEOUT),
                        delay,
                        TimeUnit.NANOSECONDS);
        for (MemberState member : memberStates.values()) {
            long memberDelay = Math.max(0L, member.context.batchContext().deadlineNanos() - System.nanoTime());
            member.deadlineTimer =
                    global.timeoutScheduler().schedule(() -> timeoutMember(member), memberDelay, TimeUnit.NANOSECONDS);
            member.future.addListener(() -> memberCompleted(member), directExecutor());
        }
        // Handle "already expired before submission" — the zero-delay timer above may not have run
        // yet, so fire the same path synchronously before the caller proceeds to submitPrepared().
        if (delay == 0L) cancelGroup(TaskGroupCompletionReason.TIMEOUT, TaskOutcome.TIMEOUT);
    }

    private void submitPrepared() {
        for (MemberState member : memberStates.values()) {
            if (!member.future.isDone()) member.submit();
        }
    }

    private void timeoutMember(MemberState member) {
        synchronized (this) {
            if (member.future.isDone() || member.reason != null) return;
            member.reason = TaskOutcome.TIMEOUT;
        }
        cancelGroup(TaskGroupCompletionReason.TIMEOUT, TaskOutcome.TIMEOUT);
    }

    private void memberCompleted(MemberState member) {
        TaskOutcome observedReason;
        Throwable observedFailure = null;
        List<MemberState> toCancel = Collections.emptyList();
        synchronized (this) {
            if (member.counted) return;
            member.counted = true;
            if (member.deadlineTimer != null) member.deadlineTimer.cancel(false);
            if (member.reason == null) {
                if (member.future.isCancelled()) {
                    member.reason = TaskOutcome.MEMBER_CANCELED;
                } else {
                    try {
                        member.future.get();
                        member.reason = TaskOutcome.SUCCESS;
                    } catch (ExecutionException failure) {
                        observedFailure = failure.getCause();
                        member.failure = observedFailure;
                        member.reason = observedFailure instanceof SubmissionException
                                ? TaskOutcome.SUBMISSION_FAILURE
                                : TaskOutcome.USER_FAILURE;
                    } catch (CancellationException impossible) {
                        member.reason = TaskOutcome.MEMBER_CANCELED;
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        member.failure = interrupted;
                        member.reason = TaskOutcome.USER_FAILURE;
                    }
                }
            }
            observedReason = member.reason;
            terminalCount++;
            if ((observedReason == TaskOutcome.USER_FAILURE
                            || observedReason == TaskOutcome.SUBMISSION_FAILURE)
                    && completionReason == null) {
                completionReason = TaskGroupCompletionReason.FAILED;
                failedMemberName = member.name;
                toCancel = markUnfinished(TaskOutcome.FAIL_FAST);
            } else if (observedReason == TaskOutcome.TIMEOUT && completionReason == null) {
                completionReason = TaskGroupCompletionReason.TIMEOUT;
                toCancel = markUnfinished(TaskOutcome.TIMEOUT);
            }
        }
        cancelMembers(toCancel);
        convergeIfTerminal();
    }

    private void cancelGroup(TaskGroupCompletionReason reason, TaskOutcome memberReason) {
        List<MemberState> toCancel;
        synchronized (this) {
            if (completionReason != null) return;
            completionReason = reason;
            toCancel = markUnfinished(memberReason);
        }
        groupToken.cancel();
        cancelMembers(toCancel);
        convergeIfTerminal();
    }

    private List<MemberState> markUnfinished(TaskOutcome reason) {
        List<MemberState> result = new ArrayList<>();
        for (MemberState member : memberStates.values()) {
            if (!member.future.isDone()) {
                if (member.reason == null) member.reason = reason;
                result.add(member);
            }
        }
        return result;
    }

    private static void cancelMembers(List<MemberState> members) {
        for (MemberState member : members) {
            member.context.batchContext().cancellationToken().cancel();
            member.future.cancel(true);
        }
    }

    private void convergeIfTerminal() {
        TaskGroupResult result;
        synchronized (this) {
            if (closed || terminalCount != memberStates.size()) return;
            if (completionReason == null) {
                completionReason = memberStates.values().stream()
                                .allMatch(member -> member.reason == TaskOutcome.SUCCESS)
                        ? TaskGroupCompletionReason.SUCCESS
                        : TaskGroupCompletionReason.CANCELED;
            }
            closed = true;
            if (deadlineTimer != null) deadlineTimer.cancel(false);
            result = snapshot();
        }
        completion.set(result);
        notifyListeners(result);
    }

    private void completeEmpty() {
        TaskGroupResult result;
        synchronized (this) {
            completionReason = TaskGroupCompletionReason.SUCCESS;
            closed = true;
            result = snapshot();
        }
        completion.set(result);
        notifyListeners(result);
    }

    private TaskGroupResult snapshot() {
        Map<String, TaskGroupMemberResult> snapshots = new LinkedHashMap<>();
        for (MemberState member : memberStates.values()) {
            snapshots.put(
                    member.name, new TaskGroupMemberResult(member.name, member.reason, member.failure, member.context));
        }
        return new TaskGroupResult(
                groupId,
                groupName,
                startTimeNanos,
                System.nanoTime(),
                deadlineNanos,
                completionReason,
                failedMemberName,
                snapshots);
    }

    private void notifyListeners(TaskGroupResult result) {
        TaskGroupEvent event = new TaskGroupEvent(result);
        for (TaskGroupListener listener : listeners) {
            try {
                listener.onTaskGroupComplete(event);
            } catch (Throwable failure) {
                LOGGER.log(Level.WARNING, "TaskGroupListener callback failed", failure);
            }
        }
    }

    /** One-shot, non-thread-safe task-group builder. */
    public static final class Builder {
        private final GlobalPar global;
        private final TaskGroupOptions options;
        private final @Nullable BatchExecutionContext structuralParent;
        private final @Nullable TaskGraphObservationContext observation;
        private final LinkedHashMap<String, Definition<?>> definitions = new LinkedHashMap<>();
        private boolean consumed;

        Builder(GlobalPar global, TaskGroupOptions options) {
            this.global = Objects.requireNonNull(global, "global cannot be null");
            this.options = Objects.requireNonNull(options, "options cannot be null");
            TaskExecutionContext currentTask = TaskExecutionContext.current();
            this.structuralParent = currentTask == null ? null : currentTask.batchContext();
            TaskGraphObservationContext currentObservation = TaskGraphObservationContext.current();
            this.observation = structuralParent != null
                            && structuralParent.taskGraphObservationContext() != null
                            && structuralParent.taskGraphObservationContext().owner() == global
                    ? structuralParent.taskGraphObservationContext()
                    : structuralParent == null && currentObservation != null && currentObservation.owner() == global
                            ? currentObservation
                            : null;
        }

        public <T> TaskHandle<T> addTask(
                String memberName, Par par, Callable<T> callable, BatchExecutionOptions taskOptions) {
            ensureConfiguring();
            Objects.requireNonNull(memberName, "memberName cannot be null");
            if (memberName.trim().isEmpty()) throw new IllegalArgumentException("memberName cannot be empty");
            Objects.requireNonNull(par, "par cannot be null");
            Objects.requireNonNull(callable, "callable cannot be null");
            Objects.requireNonNull(taskOptions, "options cannot be null");
            if (par.globalPar() != global) throw new IllegalArgumentException("Par belongs to another GlobalPar");
            if (definitions.containsKey(memberName)) {
                throw new IllegalArgumentException("Duplicate memberName '" + memberName + "'");
            }
            TaskHandle<T> handle = new TaskHandle<>(memberName);
            definitions.put(memberName, new Definition<>(memberName, par, callable, taskOptions, handle));
            return handle;
        }

        public ParallelTaskGroup buildAndSubmitAll() {
            ensureConfiguring();
            consumed = true;
            ParallelTaskGroup group = global.whileOpen(this::buildWhileOpen);
            group.start(global);
            group.submitPrepared();
            return group;
        }

        private ParallelTaskGroup buildWhileOpen() {
            long start = System.nanoTime();
            long groupDeadline =
                    deadline(start, options.timeout(), global.executionPolicy().defaultTimeoutMillis());
            if (structuralParent != null) {
                groupDeadline = Math.min(groupDeadline, structuralParent.deadlineNanos());
            }
            CancellationToken groupToken =
                    new CancellationToken(structuralParent == null ? null : structuralParent.cancellationToken());
            Map<String, MemberState> states = new LinkedHashMap<>();
            TaskGraphObservationContext previousObservation = TaskGraphObservationContext.current();
            try {
                if (observation != null && !observation.closed()) {
                    TaskGraphObservationContext.install(observation);
                } else {
                    TaskGraphObservationContext.restore(null);
                }
                for (Definition<?> definition : definitions.values()) {
                    BatchExecutionContext batch = BatchExecutionContext.resolve(
                            global.executionPolicyFor(definition.par.displayName()),
                            definition.options,
                            1,
                            structuralParent,
                            groupToken,
                            groupDeadline,
                            start,
                            observation,
                            definition.par.executorIdentity(),
                            definition.par.displayName());
                    TaskExecutionContext taskContext = new TaskExecutionContext(batch, 0, start);
                    ExecutionPhaseHintFuture<Object> future =
                            definition.par.prepareGroupTask(castCallable(definition.callable), batch, taskContext);
                    MemberState state = new MemberState(
                            definition.name,
                            taskContext,
                            future,
                            definition.par.submissionExecutor(),
                            batch.taskType() == TaskType.CPU_BOUND);
                    states.put(definition.name, state);
                }
                for (Definition<?> definition : definitions.values()) {
                    MemberState state = states.get(definition.name);
                    bindUnknown(definition.handle, state.future);
                    logForking(
                            state.context.batchContext(),
                            definition.par.runtime().blockingRisk());
                }
            } catch (Throwable failure) {
                for (MemberState state : states.values()) state.future.cancel(true);
                throw failure;
            } finally {
                TaskGraphObservationContext.restore(previousObservation);
            }
            ParallelTaskGroup group = new ParallelTaskGroup(
                    options.groupName(), start, groupDeadline, options.listeners(), groupToken, states);
            global.retainUntilComplete(new ArrayList<>(group.members.values()));
            return group;
        }

        private void ensureConfiguring() {
            if (consumed) throw new IllegalStateException("Task group builder is already consumed");
        }
    }

    /** Type-safe reference to a member future, bound by {@link Builder#buildAndSubmitAll()}. */
    public static final class TaskHandle<T> {
        private final String memberName;
        private @Nullable ListenableFuture<T> future;

        private TaskHandle(String memberName) {
            this.memberName = memberName;
        }

        public String memberName() {
            return memberName;
        }

        public synchronized ListenableFuture<T> future() {
            if (future == null) throw new IllegalStateException("Task group has not been built");
            return future;
        }
    }

    private static final class Definition<T> {
        private final String name;
        private final Par par;
        private final Callable<T> callable;
        private final BatchExecutionOptions options;
        private final TaskHandle<T> handle;

        private Definition(
                String name, Par par, Callable<T> callable, BatchExecutionOptions options, TaskHandle<T> handle) {
            this.name = name;
            this.par = par;
            this.callable = callable;
            this.options = options;
            this.handle = handle;
        }
    }

    private static final class MemberState {
        private final String name;
        private final TaskExecutionContext context;
        private final ExecutionPhaseHintFuture<Object> future;
        private final Executor executor;
        private final boolean cpuBound;
        private @Nullable TaskOutcome reason;
        private @Nullable Throwable failure;
        private @Nullable ScheduledFuture<?> deadlineTimer;
        private boolean counted;

        private MemberState(
                String name,
                TaskExecutionContext context,
                ExecutionPhaseHintFuture<Object> future,
                Executor executor,
                boolean cpuBound) {
            this.name = name;
            this.context = context;
            this.future = future;
            this.executor = executor;
            this.cpuBound = cpuBound;
        }

        /** Submits once with the member's batch scope installed; CPU-bound work runs inline on rejection. */
        private void submit() {
            BatchExecutionContext previous = SubmissionScope.install(context.batchContext());
            try {
                future.submitPrepared(executor, cpuBound);
            } finally {
                SubmissionScope.restore(previous);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Callable<Object> castCallable(Callable<?> callable) {
        return (Callable<Object>) callable;
    }

    @SuppressWarnings("unchecked")
    private static <T> void bind(TaskHandle<T> handle, ListenableFuture<?> future) {
        synchronized (handle) {
            handle.future = (ListenableFuture<T>) future;
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void bindUnknown(TaskHandle<?> handle, ListenableFuture<Object> future) {
        bind((TaskHandle) handle, future);
    }

    private static long deadline(long start, @Nullable Duration timeout, long defaultMillis) {
        long nanos;
        try {
            nanos = timeout == null ? Math.multiplyExact(defaultMillis, 1_000_000L) : timeout.toNanos();
        } catch (ArithmeticException overflow) {
            nanos = Long.MAX_VALUE;
        }
        return nanos > Long.MAX_VALUE - start ? Long.MAX_VALUE : start + nanos;
    }

    private static void logForking(BatchExecutionContext context, BlockingRisk blockingRisk) {
        BatchExecutionContext parent = context.parent();
        if (parent == null) return;
        TaskEdge edge = new TaskEdge(
                1,
                context.taskType(),
                context.executorIdentity(),
                parent.executorIdentity(),
                context.parLabel(),
                parent.parLabel(),
                1,
                context.remaining().toMillis(),
                blockingRisk == BlockingRisk.BOUNDED_PLATFORM_POOL);
        TaskGraphObservationContext.logTaskPair(
                parent.batchId(), parent.taskName(), context.batchId(), context.taskName(), edge);
    }
}
