# ADR 0003: Event-Coalesced Purge Maintenance

- Status: Accepted
- Date: 2026-07-26
- Decision scope: Cancelled-task purge maintenance protocol
- Supersedes: None

## Context

Cancelled Futures can remain as references in a `ThreadPoolExecutor` work queue
until the executor consumes them or `purge()` removes them. The library can
observe a queue-relevant cancellation only when `ExecutionPhaseHintFuture`
reports `CANCELLED_BEFORE_RUN`; cancellation exceptions, batch reports, and
placeholder Futures do not prove queue membership. `purge()` must remain the
cleanup operation, and it cannot stop a task that is already running.

The cancellation callback is on a latency-sensitive path. It must not scan a
queue for every cancellation, and concurrent cancellations must not erase one
another or submit unbounded maintenance work. Queue size and capacity are
concurrent advisory snapshots, not an accounting transaction.

## Decision

Automatic cleanup is implemented by `HeuristicPurger` for directly registered
executors whose queue is a `SmartBlockingQueue`. The callback is bound to the
actual executor at task submission; no global Future-to-executor registry is
used.

Each executor owns an independent state machine:

```text
IDLE -> SUBMITTED -> RUNNING -> IDLE
```

The state carries a monotonic `issuedSequence` and a monotonic
`settledThrough` high-water mark. A cancellation increments `issuedSequence`
once. If the state is `SUBMITTED` or `RUNNING`, the callback then returns as a
logical NOOP: it does not read the queue, evaluate thresholds, log, or submit
another task. This preserves the signal for the maintenance-task handshake.

When idle, the callback evaluates two advisory thresholds using the same
snapshot:

```text
P = Q / K
R = min(G, Q) / K
```

where `Q` is observed queue size, `K` is current queue capacity, and `G` is
the unsettled cancellation estimate. Both conditions must hold:

```text
P >= capacityPressureThreshold
R >= canceledRatioThreshold
```

The first qualifying signal CASes `IDLE` to `SUBMITTED` and schedules one
maintenance task after a fixed 50ms coalescing delay. The delay is fixed rather
than sliding, so continuous cancellation cannot starve purge. The maintenance
task changes `SUBMITTED` to `RUNNING`, captures an `issuedSequence` claim
boundary, rechecks enablement and both thresholds, and calls
`ThreadPoolExecutor.purge()` when they still qualify. On success it advances
`settledThrough` only through the captured boundary. Signals issued during the
scan therefore remain for a later round.

After every maintenance attempt the state returns to `IDLE` and performs one
handshake evaluation. If new demand qualifies, exactly one next task is
submitted. A purge exception keeps the demand unsettled, logs a warning, and
allows one delayed retry for the same unchanged sequence. A later cancellation
advances the sequence and permits another bounded retry. A scheduling rejection
returns the state to `IDLE` without changing Future cancellation semantics.

Cancellation estimate expiry is lazy. An atomic marker containing reset
generation, observed idle sequence, and timestamp lets a later idle callback
settle only the marker's previously observed sequence after the expiry window;
the implementation never resets a shared counter to zero. Disabling increments
the reset generation and settles all currently visible sequences. Re-enabling
starts a new generation, so signals from the disabled generation cannot qualify
later maintenance.

Automatic purge is disabled by default. `ParConfig` exposes atomic runtime
getters/setters for enablement and both thresholds. Trace-level diagnostics use
JUL `Level.FINEST`; callback fast paths do not log.

## Alternatives Considered

- **Strict zero-write callback while active:** rejected because it loses the
  cancellation sequence needed to schedule a follow-up purge after the scan.
- **Sliding quiet-period debounce:** rejected because continuous cancellation
  can postpone maintenance indefinitely.
- **Resetting a cancellation counter to zero:** rejected because concurrent
  increments can be overwritten; settlement must advance monotonically.
- **Periodic full scans:** rejected because they create background work when no
  cancellation occurs and still do not provide bounded response to bursts.
- **Queue-private batch deletion:** rejected because the cleanup contract is to
  call `ThreadPoolExecutor.purge()`.
- **BatchReport or Fat/Lean cancellation exception counts:** rejected because
  they include cancellation causes and placeholders that never entered the
  executor queue.

## Consequences

The cancellation path is constant-time in the common case and active callbacks
are isolated from queue contention. At most one purge task per executor is
submitted at a time, and cancellation after the scan is not discarded. The
two configurable thresholds let deployments choose more aggressive or more
conservative cleanup without adding a third public strength parameter.

The estimate remains advisory and may over- or under-estimate queue garbage
when workers dequeue tasks or producers change capacity concurrently. The
underlying `ThreadPoolExecutor.purge()` and the queue iterator's deletion cost
still determine maintenance latency. This design removes stale references; it
does not reclaim running tasks or govern permanently blocked workers.
