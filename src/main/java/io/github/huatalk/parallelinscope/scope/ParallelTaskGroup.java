package io.github.huatalk.parallelinscope.scope;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.common.util.concurrent.Futures;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * A fixed, heterogeneous set of named tasks submitted at one explicit build boundary.
 *
 * <p>Cancellation is fully structured: a member failure, a direct member cancellation, the group
 * deadline, or any single member deadline cancels every unfinished member. All outcomes are
 * attributed by reading {@link CancellationToken} states after the fact — never by capturing who
 * initiated a cancel — so attribution stays correct under races.
 */
public final class ParallelTaskGroup implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(ParallelTaskGroup.class.getName());

    /** Null-object submission canceller: group members carry no submission pipeline to stop. */
    private static final ListenableFuture<Void> NO_SUBMISSION = Futures.immediateVoidFuture();

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
        groupToken.cancel();
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
        // Member observers first, so cancellation performed by the binds below is always counted.
        for (MemberState member : memberStates.values()) {
            member.future.addListener(() -> memberCompleted(member), directExecutor());
        }
        // Group level: group deadline, unified fail-fast (any failure or member cancellation), and
        // all-success detection, in one bind over the member futures.
        groupToken.bind(memberFutures(), NO_SUBMISSION, global.timeoutScheduler());
        // Member level: each member's own deadline. A member timeout escalates to the group token
        // while the group bind is still pending, so the group converges on TIMEOUT, not FAILED.
        for (MemberState member : memberStates.values()) {
            CancellationToken memberToken = member.context.batchContext().cancellationToken();
            memberToken.addStateListener(state -> {
                if (state == CancellationToken.State.TIMEOUT_CANCELED) {
                    groupToken.timeoutCancel();
                }
            });
            memberToken.bind(Collections.singletonList(member.future), NO_SUBMISSION, global.timeoutScheduler());
        }
    }

    private List<ListenableFuture<Object>> memberFutures() {
        List<ListenableFuture<Object>> futures = new ArrayList<>();
        for (MemberState member : memberStates.values()) {
            futures.add(member.future);
        }
        return futures;
    }

    private void submitPrepared() {
        for (MemberState member : memberStates.values()) {
            if (!member.future.isDone()) member.submit();
        }
    }

    private void memberCompleted(MemberState member) {
        TaskOutcome observedReason;
        synchronized (this) {
            if (member.counted) return;
            member.counted = true;
            if (member.future.isCancelled()) {
                member.reason = classifyCancelled(member);
            } else {
                try {
                    member.future.get();
                    member.reason = TaskOutcome.SUCCESS;
                } catch (ExecutionException failure) {
                    member.failure = failure.getCause();
                    member.reason = member.failure instanceof SubmissionException
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
            observedReason = member.reason;
            terminalCount++;
            if ((observedReason == TaskOutcome.USER_FAILURE || observedReason == TaskOutcome.SUBMISSION_FAILURE)
                    && failedMemberName == null) {
                failedMemberName = member.name;
            }
        }
        if (observedReason == TaskOutcome.MEMBER_CANCELED) {
            // A directly canceled member cascades to the whole group; the group token is canceled
            // first so members cancelled through their tokens read a terminal group state.
            groupToken.cancel();
            for (MemberState other : memberStates.values()) {
                if (!other.future.isDone()) {
                    other.context.batchContext().cancellationToken().cancel();
                }
            }
        }
        convergeIfTerminal();
    }

    /**
     * Classifies a cancelled member by reading token states only. The member token records its own
     * deadline; the group token is otherwise the single authority, because it commits its state
     * before cancelling member futures. A group token still RUNNING means no framework path
     * cancelled the member: the user cancelled it directly.
     */
    private TaskOutcome classifyCancelled(MemberState member) {
        if (member.context.batchContext().cancellationToken().state() == CancellationToken.State.TIMEOUT_CANCELED) {
            return TaskOutcome.TIMEOUT;
        }
        switch (groupToken.state()) {
            case TIMEOUT_CANCELED:
                return TaskOutcome.TIMEOUT;
            case FAIL_FAST_CANCELED:
                return TaskOutcome.FAIL_FAST;
            case MUTUAL_CANCELED:
            case PROPAGATING_CANCELED:
                return TaskOutcome.GROUP_CANCELED;
            case SUCCESS:
            case RUNNING:
            default:
                return TaskOutcome.MEMBER_CANCELED;
        }
    }

    private void convergeIfTerminal() {
        TaskGroupResult result;
        synchronized (this) {
            if (closed || terminalCount != memberStates.size()) return;
            if (completionReason == null) {
                completionReason = deriveCompletionReason();
            }
            closed = true;
            result = snapshot();
        }
        completion.set(result);
        notifyListeners(result);
    }

    /**
     * Derives the group reason from the group token state. A canceled group token with no failed
     * member means the trigger was a direct member cancellation, which is a plain cancel.
     */
    private TaskGroupCompletionReason deriveCompletionReason() {
        switch (groupToken.state()) {
            case TIMEOUT_CANCELED:
                return TaskGroupCompletionReason.TIMEOUT;
            case FAIL_FAST_CANCELED:
                return failedMemberName != null ? TaskGroupCompletionReason.FAILED : TaskGroupCompletionReason.CANCELED;
            case MUTUAL_CANCELED:
            case PROPAGATING_CANCELED:
                return TaskGroupCompletionReason.CANCELED;
            case SUCCESS:
            case RUNNING:
            default:
                boolean allSuccess =
                        memberStates.values().stream().allMatch(member -> member.reason == TaskOutcome.SUCCESS);
                return allSuccess ? TaskGroupCompletionReason.SUCCESS : TaskGroupCompletionReason.CANCELED;
        }
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
            CancellationToken groupToken = new CancellationToken(
                    structuralParent == null ? null : structuralParent.cancellationToken(), groupDeadline);
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
