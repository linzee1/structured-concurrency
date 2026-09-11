import com.google.common.util.concurrent.MoreExecutors;
import io.github.monadrome.parallelinscope.BatchOptions;
import io.github.monadrome.parallelinscope.GlobalPar;
import io.github.monadrome.parallelinscope.ParName;
import io.github.monadrome.parallelinscope.TaskBatchResult;
import io.github.monadrome.parallelinscope.TaskGroup;
import io.github.monadrome.parallelinscope.TaskGroupDefinition;
import io.github.monadrome.parallelinscope.TaskGroupOptions;
import io.github.monadrome.parallelinscope.TaskGroupResult;
import io.github.monadrome.parallelinscope.TaskKey;
import io.github.monadrome.parallelinscope.TaskOptions;
import io.github.monadrome.parallelinscope.TaskOutcome;
import io.github.monadrome.parallelinscope.TaskType;
import io.github.monadrome.parallelinscope.queue.VariableLinkedBlockingQueue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reproduces the five defects reported in reports/defect-analysis-2026-09-10.md using only the public API.
 *
 * <p>Each check prints raw observations; the process exits non-zero when any claimed defect fails to
 * reproduce, so a green run means "all five are real on this revision", not "the library is healthy".
 */
public final class DefectRepro {

    private static final List<String> RESULTS = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        System.out.println("revision: " + revision());
        check1DrainToThrowingTarget();
        check2ShrinkLosesNotFullSignal();
        check3SubmitHandoffRace();
        check4ExpiredDeadlineRunsUserCode();
        check5GroupOutcomeOrderDependence();
        controlHealthyGroupWithCombine();

        System.out.println();
        System.out.println("=== SUMMARY (REPRODUCED = the reported defect was observed) ===");
        boolean allReproduced = true;
        for (String result : RESULTS) {
            System.out.println(result);
            allReproduced &= result.contains(": REPRODUCED") || result.startsWith("control");
        }
        System.out.println(allReproduced
                ? "exit 0: every reported defect reproduced"
                : "exit 1: at least one reported defect did NOT reproduce");
        System.exit(allReproduced ? 0 : 1);
    }

    private static String revision() {
        try {
            Process process = new ProcessBuilder("git", "rev-parse", "--short", "HEAD")
                    .redirectErrorStream(true)
                    .start();
            java.io.BufferedReader reader =
                    new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream()));
            String line = reader.readLine();
            process.waitFor(10, TimeUnit.SECONDS);
            return line == null ? "unknown" : line.trim();
        } catch (Exception failure) {
            return "unknown";
        }
    }

    // ==================== 1. drainTo with a throwing target ====================

    private static void check1DrainToThrowingTarget() {
        System.out.println();
        System.out.println("=== CHECK 1: VariableLinkedBlockingQueue.drainTo is not exception-safe ===");
        VariableLinkedBlockingQueue<Integer> queue = new VariableLinkedBlockingQueue<>(5);
        queue.offer(1);
        queue.offer(2);
        Collection<Integer> throwing = new ArrayList<Integer>() {
            @Override
            public boolean add(Integer element) {
                throw new IllegalStateException("target refuses " + element);
            }
        };

        String raised;
        try {
            queue.drainTo(throwing);
            raised = "none";
        } catch (Throwable failure) {
            raised = failure.toString();
        }
        System.out.println("after drainTo : raised=" + raised
                + " size=" + queue.size() + " isEmpty=" + queue.isEmpty() + "  (chain holds 1 element)");
        System.out.println("poll#1        : " + describe(queue::poll));
        System.out.println("poll#2        : " + describe(queue::poll));
        System.out.println("final         : size=" + queue.size() + " isEmpty=" + queue.isEmpty());

        boolean reproduced = queue.size() != 0 && !queue.isEmpty();
        record("1 drainTo-throwing-target", reproduced,
                "size stays " + queue.size() + " and the second poll throws NPE after one element is lost");
    }

    // ==================== 2. shrink loses the notFull signal ====================

    private static void check2ShrinkLosesNotFullSignal() throws Exception {
        System.out.println();
        System.out.println("=== CHECK 2: producer stays parked after setCapacity shrink + clear/drainTo ===");
        boolean anyParked = false;
        for (String variant : Arrays.asList("clear", "drainTo")) {
            VariableLinkedBlockingQueue<Integer> queue = new VariableLinkedBlockingQueue<>(2);
            queue.offer(1);
            queue.offer(2);
            queue.setCapacity(1); // count (2) now exceeds capacity (1)

            CountDownLatch started = new CountDownLatch(1);
            AtomicBoolean wrote = new AtomicBoolean();
            Thread producer = new Thread(() -> {
                started.countDown();
                try {
                    queue.put(3);
                    wrote.set(true);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            });
            producer.setDaemon(true);
            producer.start();
            started.await(2, TimeUnit.SECONDS);
            Thread.sleep(200); // let the producer reach notFull.await()

            if (variant.equals("clear")) {
                queue.clear();
            } else {
                queue.drainTo(new ArrayList<>());
            }
            producer.join(1000);

            boolean parked = producer.isAlive();
            anyParked |= parked;
            System.out.println(variant + "        : queueSize=" + queue.size()
                    + " remainingCapacity=" + queue.remainingCapacity()
                    + " producerStillParked=" + parked
                    + " producerWrote=" + wrote.get());
        }
        record("2 shrink-lost-wakeup", anyParked,
                "producer remains parked although the queue is empty and has capacity again");
    }

    // ==================== 3. submit-cancel vs executor handoff ====================

    private static void check3SubmitHandoffRace() throws Exception {
        System.out.println();
        System.out.println("=== CHECK 3: batch element reported SUBMISSION_FAILURE after its callable ran ===");
        int rounds = 10;
        int contradicted = 0;
        int canceledBeforeRun = 0;
        int boundAsSuccess = 0;
        for (int round = 0; round < rounds; round++) {
            String outcome = submitHandoffRound(round == 0);
            if (outcome.startsWith("callable-ran-while-abandoned")) {
                contradicted++;
            } else if (outcome.equals("canceled-before-run")) {
                canceledBeforeRun++;
            } else {
                boundAsSuccess++;
            }
        }
        System.out.println("over " + rounds + " rounds: " + contradicted
                + " contradiction (callable ran, caller sees SUBMISSION_FAILURE), "
                + canceledBeforeRun + " canceled-before-run, " + boundAsSuccess + " bound-as-SUCCESS");
        record("3 submitter-handoff-race", contradicted > 0,
                "the abandon/bind race produced a caller-visible SUBMISSION_FAILURE for a task that ran its callable in "
                        + contradicted + "/" + rounds + " rounds");
    }

    /**
     * One round of the handoff window: the executor blocks inside {@code execute} until the cancel
     * lands. {@code cancel(true)} interrupts the submitter thread, so from there the submitter loop
     * (which would run the task and bind the placeholder) races the cancel listener (which abandons
     * the in-flight placeholder). Returns how the round resolved.
     */
    private static String submitHandoffRound(boolean verbose) throws Exception {
        CountDownLatch secondExecuteEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger executions = new AtomicInteger();
        AtomicBoolean elementOneRan = new AtomicBoolean();
        ExecutorService blocking = new AbstractExecutorService() {
            private volatile boolean shutdown;

            @Override
            public void shutdown() {
                shutdown = true;
            }

            @Override
            public List<Runnable> shutdownNow() {
                shutdown = true;
                return Collections.emptyList();
            }

            @Override
            public boolean isShutdown() {
                return shutdown;
            }

            @Override
            public boolean isTerminated() {
                return shutdown;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) {
                return true;
            }

            @Override
            public void execute(Runnable command) {
                int execution = executions.incrementAndGet();
                if (execution >= 2) {
                    secondExecuteEntered.countDown();
                    try {
                        release.await(2, TimeUnit.SECONDS);
                    } catch (InterruptedException interrupted) {
                        // cancel(true) interrupts the submitter thread; run the handoff anyway
                        Thread.currentThread().interrupt();
                    }
                }
                command.run();
            }
        };
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("worker"), blocking)
                .build();
        try {
            TaskBatchResult<Integer> batch = global.par(ParName.of("worker"))
                    .map(
                            Arrays.asList(1, 2, 3),
                            value -> {
                                if (value == 2) {
                                    elementOneRan.set(true);
                                }
                                return value;
                            },
                            BatchOptions.timeout("batch", Duration.ofSeconds(30))
                                    .parallelism(1)
                                    .taskType(TaskType.IO_BOUND));

            boolean entered = secondExecuteEntered.await(5, TimeUnit.SECONDS);
            boolean cancelled = batch.submitCanceller().cancel(true);
            release.countDown();

            TaskOutcome elementOneOutcome = batch.results().get(1).outcome();
            boolean contradicted = elementOneRan.get() && elementOneOutcome == TaskOutcome.SUBMISSION_FAILURE;
            if (verbose) {
                System.out.println("reachedHandoff=" + entered + " cancelReturned=" + cancelled
                        + " userCallableRan=" + elementOneRan.get()
                        + " element1Outcome=" + elementOneOutcome);
                for (int index = 0; index < batch.results().size(); index++) {
                    final int elementIndex = index;
                    System.out.println("  element" + index + " outcome="
                            + batch.results().get(index).outcome() + " value=" + describe(() -> {
                                try {
                                    return batch.results().get(elementIndex).get(1, TimeUnit.SECONDS);
                                } catch (Exception failure) {
                                    return failure.toString();
                                }
                            }));
                }
            }
            String resolution;
            if (contradicted) {
                resolution = "callable-ran-while-abandoned";
            } else if (elementOneOutcome == TaskOutcome.SUBMISSION_FAILURE) {
                resolution = "canceled-before-run";
            } else {
                resolution = "bound-as-" + elementOneOutcome;
            }
            return resolution;
        } finally {
            global.close();
            blocking.shutdownNow();
        }
    }

    // ==================== 4. already-expired deadline still runs user code ====================

    private static void check4ExpiredDeadlineRunsUserCode() throws Exception {
        System.out.println();
        System.out.println("=== CHECK 4: group deadline already expired at submit, member callable still runs ===");
        int rounds = 20;
        int ran = 0;
        List<String> outcomes = new ArrayList<>();
        String firstRound = "";
        for (int round = 0; round < rounds; round++) {
            ExecutorService direct = MoreExecutors.newDirectExecutorService();
            GlobalPar global = GlobalPar.builder()
                    .register(ParName.of("worker"), direct)
                    .build();
            try {
                AtomicBoolean memberRan = new AtomicBoolean();
                TaskGroupDefinition.Builder definition =
                        TaskGroupDefinition.builder(TaskGroupOptions.timeout("group", Duration.ofNanos(1)));
                definition.task(
                        new TaskKey<Integer>("member") {},
                        ParName.of("worker"),
                        () -> {
                            memberRan.set(true);
                            return 1;
                        },
                        TaskOptions.inheritTimeout());

                TaskGroupResult result = TaskGroup.submit(global, definition.build())
                        .completionFuture()
                        .get(3, TimeUnit.SECONDS);
                if (memberRan.get()) {
                    ran++;
                }
                outcomes.add(String.valueOf(result.outcome()));
                if (round == 0) {
                    firstRound = "groupOutcome=" + result.outcome()
                            + " memberOutcome=" + result.members().get("member").outcome()
                            + " memberCallableRan=" + memberRan.get();
                }
            } finally {
                global.close();
                direct.shutdownNow();
            }
        }
        System.out.println("firstRound    : " + firstRound);
        System.out.println("over " + rounds + " rounds: memberCallableRan in " + ran + " rounds, group outcomes="
                + outcomes.stream().distinct().sorted().collect(java.util.stream.Collectors.toList()));
        record("4 expired-deadline-runs-code", ran > 0,
                "callable entered user code in " + ran + "/" + rounds
                        + " rounds although the deadline had already expired before submission");
    }

    // ==================== 5. group outcome depends on completion order ====================

    private static void check5GroupOutcomeOrderDependence() throws Exception {
        System.out.println();
        System.out.println("=== CHECK 5: group outcome for a failing member depends on completion order ===");
        String single = failingGroupShape("single");
        String first = failingGroupShape("first");
        String last = failingGroupShape("last");
        System.out.println("single failing member   : " + single);
        System.out.println("failure completes first : " + first);
        System.out.println("failure completes last  : " + last);

        boolean reproduced = first.contains("outcome=USER_FAILURE")
                && last.contains("outcome=MEMBER_CANCELED")
                && last.contains("failedTask=boom-late");
        record("5 group-outcome-order-dependence", reproduced,
                "the same failure reads MEMBER_CANCELED when it completes last and USER_FAILURE when it completes first");
    }

    private static String failingGroupShape(String shape) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("worker"), executor)
                .build();
        try {
            TaskGroupDefinition.Builder definition =
                    TaskGroupDefinition.builder(TaskGroupOptions.timeout("group", Duration.ofSeconds(5)));
            if (shape.equals("single")) {
                definition.task(
                        new TaskKey<Integer>("boom") {},
                        ParName.of("worker"),
                        () -> {
                            throw new IllegalStateException("boom");
                        },
                        TaskOptions.timeout(Duration.ofSeconds(5)));
            } else if (shape.equals("first")) {
                definition.task(
                        new TaskKey<Integer>("boom") {},
                        ParName.of("worker"),
                        () -> {
                            throw new IllegalStateException("boom");
                        },
                        TaskOptions.timeout(Duration.ofSeconds(5)));
                definition.task(
                        new TaskKey<Integer>("slow") {},
                        ParName.of("worker"),
                        () -> {
                            Thread.sleep(400);
                            return 1;
                        },
                        TaskOptions.timeout(Duration.ofSeconds(5)));
            } else {
                definition.task(
                        new TaskKey<Integer>("fast") {},
                        ParName.of("worker"),
                        () -> 1,
                        TaskOptions.timeout(Duration.ofSeconds(5)));
                definition.task(
                        new TaskKey<Integer>("boom-late") {},
                        ParName.of("worker"),
                        () -> {
                            Thread.sleep(200);
                            throw new IllegalStateException("boom");
                        },
                        TaskOptions.timeout(Duration.ofSeconds(5)));
            }

            TaskGroupResult result = TaskGroup.submit(global, definition.build())
                    .completionFuture()
                    .get(4, TimeUnit.SECONDS);
            String member = shape.equals("single")
                    ? result.members().get("boom").outcome().toString()
                    : shape.equals("first")
                            ? result.members().get("boom").outcome().toString()
                            : result.members().get("boom-late").outcome().toString();
            return "outcome=" + result.outcome() + " failedTask=" + result.failedTaskName() + " member=" + member;
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    // ==================== control ====================

    private static void controlHealthyGroupWithCombine() throws Exception {
        System.out.println();
        System.out.println("=== CONTROL: healthy group with a terminal combine still succeeds ===");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        GlobalPar global = GlobalPar.builder()
                .register(ParName.of("worker"), executor)
                .build();
        try {
            TaskGroupDefinition.Builder definition =
                    TaskGroupDefinition.builder(TaskGroupOptions.timeout("group", Duration.ofSeconds(5)));
            TaskKey<Integer> left = definition.task(
                    new TaskKey<Integer>("left") {},
                    ParName.of("worker"),
                    () -> 20,
                    TaskOptions.timeout(Duration.ofSeconds(5)));
            TaskKey<Integer> right = definition.task(
                    new TaskKey<Integer>("right") {},
                    ParName.of("worker"),
                    () -> 22,
                    TaskOptions.timeout(Duration.ofSeconds(5)));
            TaskGroupDefinition built = definition.buildWithCombiner(
                    new TaskKey<Integer>("sum") {},
                    ParName.of("worker"),
                    values -> values.value(left) + values.value(right));

            TaskGroupResult result = TaskGroup.submit(global, built)
                    .completionFuture()
                    .get(4, TimeUnit.SECONDS);
            System.out.println("outcome=" + result.outcome() + " combine=" + result.terminal().outcome());
            record("control healthy-group-with-combine", result.outcome() == TaskOutcome.SUCCESS,
                    "harness reports healthy paths as healthy");
        } finally {
            global.close();
            executor.shutdownNow();
        }
    }

    // ==================== helpers ====================

    private static String describe(java.util.function.Supplier<Object> action) {
        try {
            return String.valueOf(action.get());
        } catch (Throwable failure) {
            return failure.getClass().getSimpleName() + ": " + failure.getMessage();
        }
    }

    private static void record(String name, boolean reproduced, String detail) {
        String tag;
        if (name.startsWith("control")) {
            tag = reproduced ? "OK" : "BROKEN";
        } else {
            tag = reproduced ? "REPRODUCED" : "NOT REPRODUCED";
        }
        RESULTS.add(name + ": " + tag + " — " + detail);
    }

    private DefectRepro() {}
}
