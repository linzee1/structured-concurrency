package io.github.huatalk.parallelinscope.scope;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.github.huatalk.parallelinscope.cancel.CancellationToken;
import io.github.huatalk.parallelinscope.context.TaskGraphObservationContext;
import io.github.huatalk.parallelinscope.context.graph.TaskEdge;
import io.github.huatalk.parallelinscope.internal.ExecutionPhaseHintFuture;
import io.github.huatalk.parallelinscope.internal.SubmissionException;
import io.github.huatalk.parallelinscope.internal.TaskExecutionContext;
import io.github.huatalk.parallelinscope.internal.TaskSubmissions;
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
 * A fixed, heterogeneous set of named tasks submitted at one explicit boundary.
 *
 * <p>A group is described by a reusable {@link TaskGroupSpec} and submitted via {@link
 * #submit(GlobalPar, TaskGroupSpec)}, which builds, starts, and submits all members in one call.
 * Member futures are looked up by name ({@link #members()}, {@link #findMember(String)}) or through
 * the typed {@link TaskRef} tokens registered while configuring the spec ({@link #future(TaskRef)}).
 *
 * <p>Cancellation is fully structured: a member failure, a direct member cancellation, the group
 * deadline, or any single member deadline cancels every unfinished member. All outcomes are
 * attributed by reading {@link CancellationToken} states after the fact — never by capturing who
 * initiated a cancel — so attribution stays correct under races.
 */
public final class TaskGroup implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(TaskGroup.class.getName());

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
    private @Nullable TaskOutcome outcome;
    private @Nullable String failedMemberName;
    private boolean closed;

    private TaskGroup(
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

    /**
     * Resolves the future of the member the token was created for in this group.
     *
     * @throws IllegalArgumentException if no member carries the token's name, or if the token's
     *     raw result type is not assignable from the type the member was registered with (a token
     *     claiming a supertype of the registered type is accepted)
     */
    @SuppressWarnings("unchecked")
    public <T> ListenableFuture<T> future(TaskRef<T> ref) {
        Objects.requireNonNull(ref, "ref cannot be null");
        MemberState member = memberStates.get(ref.memberName());
        if (member == null) {
            throw new IllegalArgumentException("No member named '" + ref.memberName() + "'");
        }
        if (!ref.resultType().getRawType().isAssignableFrom(member.resultType.getRawType())) {
            throw new IllegalArgumentException("Member '" + ref.memberName() + "' was registered with result type "
                    + member.resultType + " but the ref claims " + ref.resultType());
        }
        return (ListenableFuture<T>) member.future;
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
        // Member level: bind only members whose own deadline is strictly tighter than the group's.
        // A member timeout escalates to the group token while the group bind is still pending, so
        // the group converges on TIMEOUT, not FAILED. A member that inherits the group deadline
        // resolves to exactly the same deadlineNanos and skips this step: downward propagation is
        // wired by the CancellationToken constructor listener (group token -> member token
        // PROPAGATED_CANCELED), and member future cancellation is covered by the group bind above,
        // so a member bind would only arm a redundant timer for the same instant. Note that a
        // skipped member token never binds, so it stays RUNNING forever (it never observes SUCCESS);
        // attribution reads the group token instead (see classifyCancelled).
        for (MemberState member : memberStates.values()) {
            CancellationToken memberToken = member.context.batchContext().cancellationToken();
            if (memberToken.deadlineNanos() >= groupToken.deadlineNanos()) {
                continue;
            }
            memberToken.addStateListener(state -> {
                if (state == CancellationToken.State.TIMEOUT) {
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
     * cancelled the member: the user cancelled it directly. A propagated cancellation keeps the
     * originating reason via {@link CancellationToken#originState()}, so an ancestor timeout is
     * still reported as {@link TaskOutcome#TIMEOUT}.
     */
    private TaskOutcome classifyCancelled(MemberState member) {
        if (member.context.batchContext().cancellationToken().state() == CancellationToken.State.TIMEOUT) {
            return TaskOutcome.TIMEOUT;
        }
        switch (groupToken.state()) {
            case TIMEOUT:
                return TaskOutcome.TIMEOUT;
            case FAIL_FAST:
                return TaskOutcome.FAIL_FAST;
            case PROPAGATED_CANCELED:
                // An ancestor timeout stays a timeout; any other propagated cause is a plain
                // group cancellation from this group's viewpoint.
                return groupToken.originState() == CancellationToken.State.TIMEOUT
                        ? TaskOutcome.TIMEOUT
                        : TaskOutcome.GROUP_CANCELED;
            case CANCELED:
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
            if (outcome == null) {
                outcome = deriveOutcome();
            }
            closed = true;
            result = snapshot();
        }
        completion.set(result);
        notifyListeners(result);
    }

    /**
     * Derives the group outcome from the group token state. On fail-fast, the group reports the
     * failed member's own outcome; a fail-fast with no failed member means the trigger was a
     * direct member cancellation, so the group reports {@link TaskOutcome#MEMBER_CANCELED}.
     */
    private TaskOutcome deriveOutcome() {
        switch (groupToken.state()) {
            case TIMEOUT:
                return TaskOutcome.TIMEOUT;
            case FAIL_FAST:
                return failedMemberName != null
                        ? memberStates.get(failedMemberName).reason
                        : TaskOutcome.MEMBER_CANCELED;
            case PROPAGATED_CANCELED:
                return groupToken.originState() == CancellationToken.State.TIMEOUT
                        ? TaskOutcome.TIMEOUT
                        : TaskOutcome.GROUP_CANCELED;
            case CANCELED:
                return TaskOutcome.GROUP_CANCELED;
            case SUCCESS:
            case RUNNING:
            default:
                boolean allSuccess =
                        memberStates.values().stream().allMatch(member -> member.reason == TaskOutcome.SUCCESS);
                return allSuccess ? TaskOutcome.SUCCESS : TaskOutcome.MEMBER_CANCELED;
        }
    }

    private void completeEmpty() {
        TaskGroupResult result;
        synchronized (this) {
            outcome = TaskOutcome.SUCCESS;
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
                outcome,
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

    /**
     * Builds a group from the spec and submits all of its members at one boundary.
     *
     * <p>The structural parent, graph observation, and group deadline are resolved from the calling
     * thread at submit time, so a {@link TaskGroupSpec} may be reused across submissions. A group
     * options timeout of {@link MultiTaskOptions.Builder#inheritTimeout()} requires an enclosing
     * scoped task; without one this method throws {@link IllegalArgumentException}.
     *
     * @throws IllegalArgumentException if a member references an unregistered executor name, or if
     *     the group inherits a deadline that does not exist
     * @throws IllegalStateException if the given GlobalPar has begun shutdown
     */
    public static TaskGroup submit(GlobalPar env, TaskGroupSpec spec) {
        Objects.requireNonNull(env, "env cannot be null");
        Objects.requireNonNull(spec, "spec cannot be null");
        TaskGroup group = env.whileOpen(() -> buildWhileOpen(env, spec));
        group.start(env);
        group.submitPrepared();
        return group;
    }

    private static TaskGroup buildWhileOpen(GlobalPar env, TaskGroupSpec spec) {
        MultiTaskOptions options = spec.groupOptions();
        TaskExecutionContext currentTask = TaskExecutionContext.current();
        MultiTaskContext structuralParent = currentTask == null ? null : currentTask.batchContext();
        TaskGraphObservationContext currentObservation = TaskGraphObservationContext.current();
        TaskGraphObservationContext observation = structuralParent != null
                        && structuralParent.taskGraphObservationContext() != null
                        && structuralParent.taskGraphObservationContext().owner() == env
                ? structuralParent.taskGraphObservationContext()
                : structuralParent == null && currentObservation != null && currentObservation.owner() == env
                        ? currentObservation
                        : null;
        long start = System.nanoTime();
        long groupDeadline;
        Optional<Duration> groupTimeout = options.timeout();
        if (groupTimeout.isPresent()) {
            groupDeadline = deadline(start, groupTimeout.get());
            if (structuralParent != null) {
                groupDeadline = Math.min(groupDeadline, structuralParent.deadlineNanos());
            }
        } else {
            if (structuralParent == null) {
                throw new IllegalArgumentException("no enclosing deadline to inherit; call timeout(Duration)");
            }
            groupDeadline = structuralParent.deadlineNanos();
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
            List<Par> memberPars = new ArrayList<>();
            for (TaskGroupSpec.MemberSpec<?> member : spec.members()) {
                Par par = env.par(member.executorName());
                memberPars.add(par);
                MultiTaskContext batch = MultiTaskContext.resolve(
                        member.options(),
                        1,
                        structuralParent,
                        groupToken,
                        groupDeadline,
                        start,
                        observation,
                        par.executorIdentity(),
                        par.displayName());
                TaskExecutionContext taskContext = new TaskExecutionContext(batch, 0, start);
                ExecutionPhaseHintFuture<Object> future =
                        par.prepareGroupTask(castCallable(member.callable()), batch, taskContext);
                states.put(
                        member.memberName(),
                        new MemberState(
                                member.memberName(),
                                taskContext,
                                future,
                                par.submissionExecutor(),
                                batch.taskType() == TaskType.CPU_BOUND,
                                member.ref().resultType()));
            }
            int index = 0;
            for (MemberState state : states.values()) {
                logForking(
                        state.context.batchContext(),
                        memberPars.get(index++).runtime().blockingRisk());
            }
        } catch (Throwable failure) {
            for (MemberState state : states.values()) state.future.cancel(true);
            throw failure;
        } finally {
            TaskGraphObservationContext.restore(previousObservation);
        }
        TaskGroup group = new TaskGroup(options.name(), start, groupDeadline, options.listeners(), groupToken, states);
        env.retainUntilComplete(new ArrayList<>(group.members.values()));
        return group;
    }

    private static final class MemberState {
        private final String name;
        private final TaskExecutionContext context;
        private final ExecutionPhaseHintFuture<Object> future;
        private final Executor executor;
        private final boolean cpuBound;
        private final TypeToken<?> resultType;
        private @Nullable TaskOutcome reason;
        private @Nullable Throwable failure;
        private boolean counted;

        private MemberState(
                String name,
                TaskExecutionContext context,
                ExecutionPhaseHintFuture<Object> future,
                Executor executor,
                boolean cpuBound,
                TypeToken<?> resultType) {
            this.name = name;
            this.context = context;
            this.future = future;
            this.executor = executor;
            this.cpuBound = cpuBound;
            this.resultType = resultType;
        }

        /** Submits once with the member's batch scope installed; CPU-bound work runs inline on rejection. */
        private void submit() {
            TaskSubmissions.submitScoped(future, context.batchContext(), executor, cpuBound);
        }
    }

    @SuppressWarnings("unchecked")
    private static Callable<Object> castCallable(Callable<?> callable) {
        return (Callable<Object>) callable;
    }

    private static long deadline(long start, Duration timeout) {
        long nanos;
        try {
            nanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            nanos = Long.MAX_VALUE;
        }
        return nanos > Long.MAX_VALUE - start ? Long.MAX_VALUE : start + nanos;
    }

    private static void logForking(MultiTaskContext context, BlockingRisk blockingRisk) {
        MultiTaskContext parent = context.parent();
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
