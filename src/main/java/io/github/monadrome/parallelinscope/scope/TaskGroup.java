package io.github.monadrome.parallelinscope.scope;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.github.monadrome.parallelinscope.cancel.CancellationToken;
import io.github.monadrome.parallelinscope.context.TaskGraphObservationScope;
import io.github.monadrome.parallelinscope.context.graph.TaskEdge;
import io.github.monadrome.parallelinscope.internal.ExecutionPhaseHintFuture;
import io.github.monadrome.parallelinscope.internal.SubmissionException;
import io.github.monadrome.parallelinscope.internal.TaskExecutionContext;
import io.github.monadrome.parallelinscope.internal.TaskSubmissions;
import io.github.monadrome.parallelinscope.internal.TokenOutcomes;
import io.github.monadrome.parallelinscope.spi.TaskGroupListener;
import io.github.monadrome.parallelinscope.spi.TaskGroupListener.TaskGroupEvent;
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
 * <p>A group is described by a reusable {@link TaskGroupDefinition} and submitted via {@link
 * #submit(GlobalPar, TaskGroupDefinition)}, which builds, starts, and submits all members in one call.
 * Member futures are looked up by name ({@link #members()}, {@link #findMember(String)}) or through
 * the typed {@link TaskRef} tokens registered while configuring the definition ({@link #future(TaskRef)}).
 *
 * <p>A definition may declare one terminal combine: a real scoped task that depends on every member.
 * Its token, execution context, and TTL snapshot are prepared at submit like a member's, but it is
 * submitted to its own {@code Par} only after all members succeed, and the group completes only when
 * its future is terminal. Its future resolves through {@link #future(TaskRef)} like a member's, yet
 * it is not part of {@link #members()}.
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
    private final @Nullable MemberState terminal;
    private final Map<String, ListenableFuture<?>> members;
    private final SettableFuture<TaskGroupResult> completion = SettableFuture.create();
    private final CancellationToken groupToken;

    private int terminalCount;
    private @Nullable TaskOutcome outcome;
    private @Nullable String failedMemberName;
    private boolean terminalSubmitted;
    private boolean closed;

    private TaskGroup(
            String groupName,
            long startTimeNanos,
            long deadlineNanos,
            List<TaskGroupListener> listeners,
            CancellationToken groupToken,
            Map<String, MemberState> memberStates,
            @Nullable MemberState terminal) {
        this.groupName = groupName;
        this.startTimeNanos = startTimeNanos;
        this.deadlineNanos = deadlineNanos;
        this.listeners = listeners;
        this.groupToken = groupToken;
        this.memberStates = new LinkedHashMap<>(memberStates);
        this.terminal = terminal;
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
     * Resolves the future of the member — or the terminal combine — the token was created for in
     * this group.
     *
     * @throws IllegalArgumentException if no member or combine carries the token's name, or if the
     *     token's raw result type is not assignable from the type it was registered with (a token
     *     claiming a supertype of the registered type is accepted)
     */
    @SuppressWarnings("unchecked")
    public <T> ListenableFuture<T> future(TaskRef<T> ref) {
        Objects.requireNonNull(ref, "ref cannot be null");
        MemberState member = memberStates.get(ref.memberName());
        if (member == null && terminal != null && terminal.name.equals(ref.memberName())) {
            member = terminal;
        }
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
        if (memberStates.isEmpty() && terminal == null) {
            completeEmpty();
            return;
        }
        // Member observers first, so cancellation performed by the binds below is always counted.
        for (MemberState member : memberStates.values()) {
            member.future.addListener(() -> memberCompleted(member), directExecutor());
        }
        if (terminal != null) {
            terminal.future.addListener(() -> memberCompleted(terminal), directExecutor());
        }
        // Group level: group deadline, unified fail-fast (any failure or member cancellation), and
        // all-success detection, in one bind over the member futures. The terminal future joins the
        // bind while still pending, so the group deadline and fail-fast reach the unsubmitted
        // combine, and the group token cannot observe SUCCESS before the combine completes.
        groupToken.bind(observedFutures(), NO_SUBMISSION, global.timeoutScheduler());
        // Member level: bind only members whose own deadline is strictly tighter than the group's.
        // A member timeout escalates to the group token while the group bind is still pending, so
        // the group converges on TIMEOUT, not FAILED. A member that inherits the group deadline
        // resolves to exactly the same deadlineNanos and skips this step: downward propagation is
        // wired by the CancellationToken constructor listener (group token -> member token
        // PROPAGATED_CANCELED), and member future cancellation is covered by the group bind above,
        // so a member bind would only arm a redundant timer for the same instant. Note that a
        // skipped member token never binds, so it stays RUNNING forever (it never observes SUCCESS);
        // attribution reads the group token instead (see classifyCancelled). The combine follows
        // the same rule: its own tighter deadline escalates to the group as TIMEOUT.
        for (MemberState member : membersAndTerminal()) {
            CancellationToken memberToken = member.context.multiTaskContext().cancellationToken();
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

    private List<ListenableFuture<Object>> observedFutures() {
        List<ListenableFuture<Object>> futures = memberFutures();
        if (terminal != null) {
            futures.add(terminal.future);
        }
        return futures;
    }

    private List<MemberState> membersAndTerminal() {
        List<MemberState> all = new ArrayList<>(memberStates.values());
        if (terminal != null) {
            all.add(terminal);
        }
        return all;
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
        // An empty group satisfies the join condition immediately, so the combine submits inside
        // the submit flow, on the submitting thread, exactly where members would have been.
        if (terminal != null && memberStates.isEmpty()) {
            submitTerminalOnce();
        }
    }

    /**
     * Submits the prepared combine to its own executor exactly once, outside the group lock. The
     * submission runs on the convergence callback thread (or the submit thread for an empty
     * group); the user function itself runs only on the combine executor's worker. A combine that
     * lost to cancellation is never submitted, and a cancellation racing the submission still
     * cannot enter user code because the future's phase claim guards the call.
     */
    private void submitTerminalOnce() {
        MemberState combine = terminal;
        if (combine == null) {
            return;
        }
        synchronized (this) {
            if (terminalSubmitted) return;
            terminalSubmitted = true;
        }
        if (!combine.future.isDone()) {
            combine.submit();
        }
    }

    private void memberCompleted(MemberState member) {
        TaskOutcome observedReason;
        boolean joinSatisfied;
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
                    member.reason = classifyFailure(member, member.failure);
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
            // The combine is not part of memberStates, so this counts members only: the join
            // condition is every member counted and successful.
            joinSatisfied = terminal != null
                    && !terminalSubmitted
                    && memberStates.values().stream()
                            .allMatch(state -> state.counted && state.reason == TaskOutcome.SUCCESS);
        }
        if (observedReason == TaskOutcome.MEMBER_CANCELED) {
            // A directly canceled member cascades to the whole group; the group token is canceled
            // first so members cancelled through their tokens read a terminal group state.
            groupToken.cancel();
            for (MemberState other : membersAndTerminal()) {
                if (!other.future.isDone()) {
                    other.context.multiTaskContext().cancellationToken().cancel();
                }
            }
        }
        if (observedReason == TaskOutcome.USER_FAILURE || observedReason == TaskOutcome.SUBMISSION_FAILURE) {
            // The combine is always the last task to complete, so its failure must commit
            // FAIL_FAST synchronously: convergence below must not read a still-RUNNING group
            // token and misattribute the terminal business failure as a cancellation. Members
            // keep the established attribution rule — a lone member failure may still converge
            // on a RUNNING token and read MEMBER_CANCELED.
            if (member == terminal) {
                groupToken.failFastCancel();
            }
        }
        if (joinSatisfied) {
            submitTerminalOnce();
        }
        convergeIfTerminal();
    }

    /**
     * Classifies an exceptionally completed member. A failure that merely signals observed
     * cancellation — a checkpoint threw a {@link CancellationException}, or the worker thread was
     * interrupted — can win the race against the cascade cancel on the member future; it is
     * attributed through the tokens like a cancellation instead of being recorded as a user
     * failure. A spontaneous {@code CancellationException} from user code with no committed
     * framework cancellation still reads {@link TaskOutcome#USER_FAILURE}.
     */
    private TaskOutcome classifyFailure(MemberState member, Throwable failure) {
        if (failure instanceof SubmissionException) {
            return TaskOutcome.SUBMISSION_FAILURE;
        }
        if (TokenOutcomes.causedByCancellation(failure)) {
            return classifyCancelled(member, TaskOutcome.USER_FAILURE);
        }
        return TaskOutcome.USER_FAILURE;
    }

    /**
     * Classifies a cancelled member by reading token states only. The member token records its own
     * deadline; the group token is otherwise the single authority, because it commits its state
     * before cancelling member futures. A group token still RUNNING means no framework path
     * cancelled the member: the user cancelled it directly.
     */
    private TaskOutcome classifyCancelled(MemberState member) {
        return classifyCancelled(member, TaskOutcome.MEMBER_CANCELED);
    }

    private TaskOutcome classifyCancelled(MemberState member, TaskOutcome whenUncommitted) {
        if (member.context.multiTaskContext().cancellationToken().state() == CancellationToken.State.TIMEOUT) {
            return TaskOutcome.TIMEOUT;
        }
        return TokenOutcomes.forCanceled(groupToken, whenUncommitted);
    }

    private void convergeIfTerminal() {
        TaskGroupResult result;
        synchronized (this) {
            if (closed || terminalCount != memberStates.size() + (terminal == null ? 0 : 1)) return;
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
     * failed task's own outcome; a fail-fast with no failed member means the trigger was a
     * direct member cancellation, so the group reports {@link TaskOutcome#MEMBER_CANCELED}. A
     * token still RUNNING or SUCCESS means no framework cancellation path committed: the group
     * succeeded only if every member did.
     */
    private TaskOutcome deriveOutcome() {
        switch (groupToken.state()) {
            case FAIL_FAST:
                if (failedMemberName != null) {
                    MemberState failed = memberStates.get(failedMemberName);
                    // The failed name may belong to the terminal combine, which is not a member.
                    return (failed != null ? failed : terminal).reason;
                }
                return TaskOutcome.MEMBER_CANCELED;
            case SUCCESS:
            case RUNNING:
                boolean allSuccess =
                        memberStates.values().stream().allMatch(member -> member.reason == TaskOutcome.SUCCESS)
                                && (terminal == null || terminal.reason == TaskOutcome.SUCCESS);
                return allSuccess ? TaskOutcome.SUCCESS : TaskOutcome.MEMBER_CANCELED;
            default:
                return TokenOutcomes.forCanceled(groupToken, TaskOutcome.MEMBER_CANCELED);
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
        Map<String, TaskCompletion<?>> snapshots = new LinkedHashMap<>();
        for (MemberState member : memberStates.values()) {
            snapshots.put(member.name, memberSnapshot(member));
        }
        return new TaskGroupResult(
                groupId,
                groupName,
                startTimeNanos,
                System.nanoTime(),
                deadlineNanos,
                outcome,
                failedMemberName,
                snapshots,
                terminal == null ? null : memberSnapshot(terminal));
    }

    private static TaskCompletion<?> memberSnapshot(MemberState member) {
        return TaskCompletion.memberSnapshot(
                member.name,
                member.context.multiTaskContext().unitId(),
                member.reason,
                member.failure,
                member.context.submitTimeNanos(),
                member.context.startTimeNanos(),
                member.context.endTimeNanos());
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
     * Builds a group from the definition and submits all of its members at one boundary.
     *
     * <p>The structural parent, graph observation, and group deadline are resolved from the calling
     * thread at submit time, so a {@link TaskGroupDefinition} may be reused across submissions. A group
     * options timeout of {@link MultiTaskOptions.Builder#inheritTimeout()} requires an enclosing
     * scoped task; without one this method throws {@link IllegalArgumentException}.
     *
     * @throws IllegalArgumentException if a member references an unregistered executor name, or if
     *     the group inherits a deadline that does not exist
     * @throws IllegalStateException if the given GlobalPar has begun shutdown
     */
    public static TaskGroup submit(GlobalPar env, TaskGroupDefinition definition) {
        Objects.requireNonNull(env, "env cannot be null");
        Objects.requireNonNull(definition, "definition cannot be null");
        TaskGroup group = env.whileOpen(() -> buildWhileOpen(env, definition));
        group.start(env);
        group.submitPrepared();
        return group;
    }

    private static TaskGroup buildWhileOpen(GlobalPar env, TaskGroupDefinition definition) {
        MultiTaskOptions options = definition.groupOptions();
        TaskExecutionContext currentTask = TaskExecutionContext.current();
        MultiTaskContext structuralParent = currentTask == null ? null : currentTask.multiTaskContext();
        TaskGraphObservationScope currentObservation = TaskGraphObservationScope.current();
        TaskGraphObservationScope observation = structuralParent != null
                        && structuralParent.taskGraphObservationScope() != null
                        && structuralParent.taskGraphObservationScope().owner() == env
                ? structuralParent.taskGraphObservationScope()
                : structuralParent == null && currentObservation != null && currentObservation.owner() == env
                        ? currentObservation
                        : null;
        long start = System.nanoTime();
        Optional<Duration> groupTimeout = options.timeout();
        if (!groupTimeout.isPresent() && structuralParent == null) {
            throw new IllegalArgumentException("no enclosing deadline to inherit; call timeout(Duration)");
        }
        long groupDeadline = MultiTaskContext.resolveDeadlineNanos(
                groupTimeout, structuralParent == null ? Long.MAX_VALUE : structuralParent.deadlineNanos(), start);
        CancellationToken groupToken = new CancellationToken(
                structuralParent == null ? null : structuralParent.cancellationToken(), groupDeadline);
        Map<String, MemberState> states = new LinkedHashMap<>();
        MemberState terminal = null;
        TaskGraphObservationScope previousObservation = TaskGraphObservationScope.current();
        try {
            if (observation != null && !observation.closed()) {
                TaskGraphObservationScope.install(observation);
            } else {
                TaskGraphObservationScope.restore(null);
            }
            List<Par> memberPars = new ArrayList<>();
            for (TaskGroupDefinition.TaskDefinition<?> member : definition.tasks()) {
                Par par = env.par(member.parName());
                memberPars.add(par);
                MultiTaskContext unit = MultiTaskContext.resolve(
                        member.options(),
                        1,
                        structuralParent,
                        groupToken,
                        groupDeadline,
                        start,
                        observation,
                        par.executorIdentity(),
                        par.name().value());
                TaskExecutionContext taskContext = new TaskExecutionContext(unit, 0, start);
                ExecutionPhaseHintFuture<Object> future =
                        par.prepareGroupTask(castCallable(member.callable()), unit, taskContext);
                states.put(
                        member.memberName(),
                        new MemberState(
                                member.memberName(),
                                taskContext,
                                future,
                                par.submissionExecutor(),
                                unit.taskType() == TaskType.CPU_BOUND,
                                member.ref().resultType()));
            }
            int index = 0;
            for (MemberState state : states.values()) {
                logForking(
                        state.context.multiTaskContext(),
                        memberPars.get(index++).runtime().blockingRisk());
            }
            TaskGroupDefinition.CombineDefinition<?> combineDefinition = definition.combine();
            if (combineDefinition != null) {
                // The combine is prepared exactly like a member — token, context, TTL snapshot,
                // structural parent — so its context capture happens on the submitting thread at
                // submit time; only the executor submission is deferred to the join. The values
                // view captures the frozen member states created above. cpuBound is fixed false:
                // at join time there is no caller thread to borrow, so a rejected combine must
                // fail as SUBMISSION_FAILURE instead of running inline on the convergence
                // callback thread.
                Par par = env.par(combineDefinition.parName());
                MultiTaskContext unit = MultiTaskContext.resolve(
                        combineDefinition.options(),
                        1,
                        structuralParent,
                        groupToken,
                        groupDeadline,
                        start,
                        observation,
                        par.executorIdentity(),
                        par.name().value());
                TaskExecutionContext taskContext = new TaskExecutionContext(unit, 0, start);
                CompletedTaskValues values = new CompletedTaskValues(states, combineDefinition.memberName());
                CombineFunction<?> function = combineDefinition.function();
                ExecutionPhaseHintFuture<Object> future =
                        par.prepareGroupTask(() -> function.apply(values), unit, taskContext);
                terminal = new MemberState(
                        combineDefinition.memberName(),
                        taskContext,
                        future,
                        par.submissionExecutor(),
                        false,
                        combineDefinition.ref().resultType());
                logForking(unit, par.runtime().blockingRisk());
            }
        } catch (Throwable failure) {
            for (MemberState state : states.values()) state.future.cancel(true);
            if (terminal != null) terminal.future.cancel(true);
            throw failure;
        } finally {
            TaskGraphObservationScope.restore(previousObservation);
        }
        TaskGroup group =
                new TaskGroup(options.name(), start, groupDeadline, options.listeners(), groupToken, states, terminal);
        List<ListenableFuture<?>> retained = new ArrayList<>(group.members.values());
        if (terminal != null) {
            retained.add(terminal.future);
        }
        env.retainUntilComplete(retained);
        return group;
    }

    /** Package-visible for {@link CompletedTaskValues}, which reads member futures and types. */
    static final class MemberState {
        final String name;
        final ExecutionPhaseHintFuture<Object> future;
        final TypeToken<?> resultType;
        private final TaskExecutionContext context;
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
            TaskSubmissions.submitScoped(future, context.multiTaskContext(), executor, cpuBound);
        }
    }

    @SuppressWarnings("unchecked")
    private static Callable<Object> castCallable(Callable<?> callable) {
        return (Callable<Object>) callable;
    }

    private static void logForking(MultiTaskContext context, BlockingRisk blockingRisk) {
        MultiTaskContext parent = context.structuralParent();
        if (parent == null) return;
        TaskEdge edge = new TaskEdge(
                1,
                context.taskType(),
                context.executorIdentity(),
                parent.executorIdentity(),
                context.executorLabel(),
                parent.executorLabel(),
                1,
                context.remaining(),
                blockingRisk == BlockingRisk.BOUNDED_PLATFORM_POOL);
        TaskGraphObservationScope.logTaskPair(parent.unitId(), parent.name(), context.unitId(), context.name(), edge);
    }
}
