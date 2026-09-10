# ADR 0001: Layered Cartesian Concurrency Testing

- Status: Accepted
- Date: 2026-07-26
- Decision scope: Concurrency test architecture
- Supersedes: None

## Context

The library coordinates task submission, executor admission, Future state,
cancellation propagation, timeout handling, context relay, sliding-window
progress, blocking queues, and cancelled-task cleanup. These mechanisms overlap
in time but do not share one authoritative state.

For example:

- `ExecutionPhase.RUNNING` means that a Future wrapper claimed `run()`; it does
  not prove that the task body received a CPU time slice.
- A cancelled Future does not prove that running work stopped.
- A cancellation signal does not prove that the corresponding Future is still
  physically present in an executor queue.
- A completion event may advance the submission window even when the completed
  task failed, while cancellation may stop that window.
- `TaskType` describes scheduling policy, not whether a task actually performs
  CPU-bound, IO-bound, mixed, or long-running work.

Tests organized only as isolated examples made these distinctions implicit.
Adding more examples without a feature model would increase test count without
showing which dimensions are covered, which combinations are meaningful, or
which state each assertion actually observes.

The test architecture therefore needs to support combinations of independent
dimensions while avoiding impossible or redundant global products. It also
needs deterministic control over concurrency boundaries and must leave
production defects unfixed during diagnostic testing.

## Decision

Concurrency tests SHALL use a layered feature model and construct cases from
constrained Cartesian products.

Small surfaces whose axes are independent SHALL use their full Cartesian
product. Cross-layer surfaces SHALL apply explicit compatibility predicates
before generating cases. Tests SHALL keep observation planes separate and
assert only state that the fixture directly controls or observes.

### Observation planes

| Plane | Observable values | Does not prove |
|---|---|---|
| Admission | not submitted, initial submission, placeholder, later submission, rejected, direct fallback | worker execution |
| Executor | queued identity, queue membership, worker claim, executing thread, terminal removal | Future or token state |
| Future | pending, success, failure, cancelled before run, cancellation requested while running, terminal | task-body side effects |
| Cancellation | trigger, token state, propagation direction, interrupt request, interrupt observation, checkpoint response | worker termination |
| User code | not entered, entered, gated, side effect, exception, return, ignores interruption | queue cleanup |
| Cleanup | completion publication, window advancement or stop, purge result, estimate retention or expiry | cancellation cause |

An assertion about one plane SHALL NOT be used as indirect proof of another.
When a contract spans planes, the test SHALL collect separate evidence for each
plane.

### Feature layers

#### L1. Workload behavior

| Value | Controlled construction | Primary risk |
|---|---|---|
| `IMMEDIATE` | return immediately | work finishes before late binding |
| `FAIL_IMMEDIATE` | throw immediately | fail-fast races cancellation wiring |
| `IO_GATE` | signal entry, then wait on a latch | running interruption and timeout |
| `CPU_BOUNDED` | bounded arithmetic or cooperative polling | scheduling and caller-runs behavior |
| `MIXED` | bounded CPU stage followed by a gate | cancellation between workload phases |
| `LONG_COOPERATIVE` | loop with interruption or checkpoint checks | propagation and prompt exit |
| `LONG_IGNORES_INTERRUPT` | record interruption but wait for an independent release | cancelled Future while capacity remains occupied |

Workload behavior and `TaskType` SHALL be independent axes. Naming a task
`CPU_BOUND` is not sufficient evidence that its body is CPU-bound.

#### L2. Executor topology

- worker model: direct, single worker, fixed workers, or elastic workers;
- queue model: synchronous, bounded smart queue, or unbounded queue;
- saturation: empty, workers occupied, queue partially full, queue full, or
  executor shut down;
- rejection: propagate rejection or perform CPU direct fallback;
- submitter: inline test executor, named single worker, framework submitter, or
  interrupted/rejected submitter.

#### L3. Admission phase

| Value | Deterministic setup |
|---|---|
| `INITIAL_SUBMITTED` | index is below `min(taskCount, parallelism)` |
| `INITIAL_QUEUED` | all workers are occupied before initial submission |
| `PLACEHOLDER` | index is outside the initial window while earlier work is gated |
| `LATER_SUBMITTED` | release exactly one completion event |
| `LATER_QUEUED` | advance the window while a worker remains occupied |
| `INITIAL_REJECTED` | reject during the caller's initial loop |
| `LATER_REJECTED` | reject from the submitter after a completion event |
| `DIRECT_FALLBACK` | combine rejection with `CPU_BOUND` |
| `ABANDONED_PLACEHOLDER` | cancel a placeholder or submission loop before admission; direct placeholder cancellation is `CANCELLED`, submitter interruption records its cause |

#### L4. Task outcome

The modeled outcomes are success, failure, cancellation before run,
cancellation requested while running, timeout, and non-terminal work after its
Future is cancelled.

#### L5. Cancellation trigger

The modeled triggers are:

- direct Future `cancel(false)` and `cancel(true)`;
- manual token cancellation;
- timeout;
- fail-fast sibling failure;
- parent cancellation before or after child binding;
- submission-loop cancellation or interruption;
- executor shutdown and rejection.

Every case SHALL name the trigger and propagation direction. Child
cancellation SHALL NOT be treated as evidence of parent cancellation.

#### L6. Timeout boundary

Timeout tests distinguish:

- timeout before task-body entry;
- timeout during interruptible running work;
- timeout during non-cooperative running work;
- timeout while later tasks remain placeholders;
- timeout racing success or failure;
- parent timeout before or after child binding;
- invalid timeout configuration, which is an option-validation case rather
  than a runtime timeout case.

#### L7. Propagation topology

The modeled directions are no propagation, parent to child, outer `Par` call
to nested `Par` call, failure to sibling, batch to submitter, and direct Future
only.

#### L8. Queue condition race

Queue-local tests combine:

- operation: `put`, timed `offer`, `take`, or timed `poll`;
- precondition: empty, full, capacity increase or decrease, removal, clear, or
  drain;
- release: counterpart operation, capacity change, interruption, or timeout;
- ordering: waiter parked before release or release racing waiter registration.

The invariants are no lost signal, no capacity overshoot, FIFO order, correct
count, correct interruption contract, no mutation after a timed-out operation,
and eventual waiter completion after a sufficient state change.

Queue condition races SHALL be tested directly on
`VariableLinkedBlockingQueue`. They SHALL NOT be multiplied through every
`Par` scenario, because doing so would obscure the condition responsible for a
failure.

#### L9. Thread scheduling state

Tests use these deterministic substitutes for scheduler state:

- a captured runnable that the test has not invoked models a submitted task
  that has not received a useful time slice;
- an entry latch proves that the task body started;
- an entry latch plus release latch proves that the body is parked at a
  controlled gate;
- an interrupt flag plus exit latch distinguishes interruption from worker
  termination;
- a cancelled Future plus an unreleased exit latch proves that work remains
  active after cancellation.

`Thread.State` MAY establish that a waiter reached a blocking precondition, but
it SHALL NOT be the sole correctness oracle because JVM thread states are
transient.

#### L10. Purger and cleanup

Purger tests combine enablement, queue pressure, cancellation estimate,
estimate age, burst shape, physical queue contents, purge result, and runtime
reconfiguration.

Queue pressure and cancellation ratio boundary tests SHALL include values
below, exactly at, and above their thresholds. The cancellation estimate is
advisory and SHALL NOT be asserted as exact queue membership.

### Local Cartesian surfaces

The following surfaces use full products:

1. Future lifecycle: `{before-run, running, terminal}` x
   `{cancel(false), cancel(true)}`.
2. Rejection: `{initial, later}` x `{CPU_BOUND, IO_BOUND}`.
3. Sliding window: `{success, failure, cancelled}` x
   `{live placeholder, pre-cancelled placeholder}`.
4. Cancellation: `{timeout, manual interrupt}` x
   `{pending, IO gate, CPU cooperative, mixed, ignores interruption}`.
5. Parent propagation: `{before child bind, after child bind}` x
   `{pending child, running child}`.
6. Queue waiter: `{producer, consumer}` x
   `{counterpart, interruption, timeout}`, plus capacity-creating queue
   mutations for blocked producers.
7. Purger thresholds: `{below, exact, above pressure}` x
   `{below, exact, above cancellation ratio}` x `{disabled, enabled}`.

### Compatibility predicates

Cross-layer generators SHALL enforce these constraints:

1. A queued case requires occupied workers and a queue that accepts the
   runnable.
2. Placeholder and later-submission cases require more tasks than the initial
   parallelism and a live submitter.
3. Direct fallback requires rejection and `CPU_BOUND`.
4. IO rejection requires an executor that actually rejects; IO workload alone
   is insufficient.
5. Cancellation-before-run requires cancellation to win the wrapper's `run()`
   race; physical queue membership is observed independently.
6. Interrupt assertions require an interrupting trigger and a body that has
   reached an observable interruptible or cooperative point.
7. Non-cooperative tasks require an independent test-only release path.
8. Timeout cases require completed late binding unless the late-bind window is
   the subject of the test.
9. Parent propagation requires a real parent token or inherited context.
10. Purge cases require the same Future object to be physically queued;
    placeholders are excluded.
11. OS scheduling outcomes that cannot be forced portably SHALL use controlled
    executor-boundary substitutes.

### Case generation and identity

Each local surface owns a small JUnit `@MethodSource` or `@TestFactory`
generator. The suite SHALL NOT introduce a general-purpose concurrency test
DSL unless concrete duplication makes it necessary.

Generated names SHALL expose their axes, for example:

```text
surface=rejection;admission=LATER;taskType=CPU_BOUND
```

Case objects contain axis values and expected invariants only. Invalid
combinations are excluded by named compatibility predicates rather than
executed as meaningless tests.

### Deterministic fixture rules

1. Every blocking task has explicit entry and release controls.
2. Every executor and thread is shut down in `finally` or `@AfterEach`.
3. Await operations have safety timeouts, while correctness depends on state
   transitions rather than elapsed time.
4. `sleep` SHALL NOT create a race. A bounded wait MAY observe a real production
   timer after the fixture proves its precondition.
5. Non-cooperative tasks always have a release channel independent of
   interruption.
6. Race tests use a controlled simultaneous start and assert the allowed outcome
   set plus invariants, not one preferred scheduler winner.
7. Failure messages and generated names identify the complete case.

### Product defect handling

When a new test exposes a production defect during diagnostic work:

1. production code remains unchanged;
2. the smallest expected-contract reproduction is retained;
3. if the known defect would keep the suite red, the reproduction is disabled
   with a precise reason;
4. the observed behavior, expected behavior, impact, and source evidence are
   recorded in the deep test report.

The current report is
[`../tmp/deep-concurrency-test-report.md`](../tmp/deep-concurrency-test-report.md).

## Alternatives Considered

### Continue adding isolated example tests

Rejected because examples do not make coverage dimensions, exclusions, or
cross-plane assumptions visible. Similar examples also tend to duplicate
fixtures without proving new behavior.

### Generate one unconstrained global Cartesian product

Rejected because most cross-layer combinations are impossible, redundant, or
too slow. A large green count would not imply meaningful coverage.

### Use pairwise generation for every surface

Rejected as the default because small high-risk surfaces are cheap enough to
cover fully. Pairwise selection remains acceptable for large cross-layer
surfaces after compatibility filtering.

### Depend primarily on randomized stress tests

Rejected as the contract suite because failures would be difficult to
reproduce and scheduler outcomes would become implicit inputs. Repeated stress
runs may supplement deterministic cases.

### Build a reusable concurrency testing DSL

Rejected because the current surfaces are expressible with JUnit generators,
latches, barriers, and standard executors. A DSL would add abstraction before
stable duplication exists.

### Use sleeps and timing margins

Rejected because timing is weak evidence for queue membership, task entry,
interrupt delivery, or condition registration. Explicit state controls provide
stronger and faster tests.

## Consequences

### Positive

- Coverage is expressed as named dimensions rather than raw test count.
- Boundary and policy differences are visible in generated case identities.
- Failures identify the observation plane and combination that diverged.
- Deterministic fixtures reduce scheduler-dependent flakes.
- Compatibility predicates prevent meaningless case multiplication.
- Product defects can be reproduced without silently changing production
  behavior.

### Negative

- Generators and fixtures require more up-front modeling than one-off tests.
- Full products can grow quickly, so every new axis requires an applicability
  and runtime review.
- Some negative asynchronous assertions, such as proving that a purge was not
  scheduled, still require a bounded observation interval.
- JVM scheduling itself cannot be modeled exhaustively or forced portably.

### Neutral constraints

- The decision changes test architecture, not public production APIs.
- Existing JUnit and Java concurrency primitives are sufficient; this decision
  adds no dependency.
- Test counts and current verification results are intentionally kept out of
  this ADR because they change over time.

## Implementation Mapping

The accepted decision is currently represented by:

- `ExecutionPhaseCartesianTest`
- `ConcurrentLimitExecutorCartesianTest`
- `SlidingWindowCartesianTest`
- `CancellationTriggerCartesianTest`
- `CancellationPropagationCartesianTest`
- `VariableLinkedBlockingQueueCartesianTest`
- `HeuristicPurgerCartesianTest`

Current execution results, known defects, and environmental limits belong in
the linked deep test report rather than in this decision record.

## Reconsider When

Revisit this ADR when one of these conditions becomes true:

- local generators duplicate enough behavior to justify a shared test utility;
- the number of valid cases makes full local products too slow for normal CI;
- a deterministic scheduler or model checker is adopted;
- production lifecycle contracts change the observation planes or compatibility
  predicates.

If the decision changes materially, add a new ADR and mark this record
`Superseded` rather than rewriting its historical context.
