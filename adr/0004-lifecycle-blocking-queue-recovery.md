# ADR 0004: Lifecycle Blocking Queue Recovery Contract

- Status: Superseded
- Date: 2026-08-22
- Decision scope: `ClosableBlockingQueue` shutdown and recovery semantics
- Supersedes: None
- Superseded by: [Draining close contract](../design/draining-queue-contract.md)

> This ADR recorded the abrupt-close recovery model (`OPEN -> CLOSING -> CLOSED` with
> `remainingList()`). That model was removed and replaced by the draining-close model
> (`OPEN -> DRAINING -> DRAINED`, no recovery channel) recorded in the draining close contract.
> Kept for history; do not treat it as the current shutdown contract.

## Context

The queue must release blocked producers and consumers during shutdown while preserving elements
that were already linked at the shutdown linearization point. Recovery must be deterministic,
bounded, and safe when multiple consumers drain it concurrently. The implementation also targets
Java 8 bytecode while exposing the backed reverse-view behavior available on newer JDKs.

## Decision

Use a one-way `OPEN -> CLOSING -> CLOSED` lifecycle, detach queued elements into a recovery list at
shutdown, and keep `drainTo(Collection)` available after shutdown to claim recovery elements in FIFO
order. `remainingList()` remains the post-termination inspection API. Poison signaling is virtual and
identity-based; it is never stored in the queue or recovery list.

## Alternatives Considered

- Reject every collection operation after shutdown and expose recovery only through
  `remainingList()`. Rejected because standard `drainTo` is the natural transfer API and recovery
  callers may need bounded incremental ownership.
- Store poison objects as queue nodes. Rejected because one node cannot release an unbounded number
  of blocked consumers and would corrupt capacity/iteration semantics.
- Keep a thread registry to interrupt blocked callers. Rejected because lifecycle guards and monitor
  admission provide release without interrupting user threads.

## Consequences

Shutdown recovery has an explicit ownership-transfer operation and concurrent drains cannot duplicate
elements. `remainingList()` is empty after successful drains, while target collection failures do
not restore already claimed elements. The queue has more lifecycle-specific code and callers must
distinguish recovery transfer (`drainTo`) from closed consumer operations (`take`/`poll`).

This document is the single source for `LifecycleQueueV2`. It replaces the former separate design
draft and JDK-difference exploration.

## 1. Requirement Sources

### User goals

1. Provide a bounded `BlockingQueue` with explicit lifecycle shutdown, blocked-call release, and
   recovery of elements that were queued at shutdown.
2. Match standard-library collection behavior whenever it does not conflict with the lifecycle
   contract. In particular, use the Java 8 `LinkedBlockingQueue` weakly consistent FIFO iterator
   model rather than a stable iterator snapshot.
3. Support two constructor-selected shutdown behaviors: exception rejection and poison-object
   signalling. Producer and collection mutations never succeed after shutdown, except that standard
   `BlockingQueue.drainTo` remains available for recovery transfer.
4. Make the reverse view behave as a normal backed mutable `List`. On Java 21+, the returned `List`
   is a real `SequencedCollection`, including its endpoint defaults and `reversed()` behavior.
5. Keep implementation, tests, and this document synchronized.

### Existing project boundary

The project is built with Java 8 bytecode. `java.util.SequencedCollection` cannot appear in the
production class signature without changing the artifact baseline or adding a multi-release JAR.
This is an inherited release boundary, not a lifecycle requirement. `LifecycleQueueV2` therefore
publishes the same endpoint methods and returns `List<E>` from `reversed()`. On Java 21+, that list
is itself a `SequencedCollection`. Making the queue object itself implement the Java 21 interface is
a separate release-format decision.

### Implementation choices

The following are engineering choices, not new product requirements:

- two Guava `Monitor`s protect producer and consumer progress, with global order
  `putMonitor -> takeMonitor`;
- user code (equality, predicates, target collections, source collections, and Service listeners)
  never executes while either Monitor is held;
- shutdown detaches the linked chain in O(1); `remainingList()` materializes that chain lazily after
  termination;
- reverse `addAll` copies its source outside the Monitors and commits the complete batch only if the
  full batch fits.

### Construction

Constructors preserve the ordinary collection conventions:

- no-argument and capacity/name constructors create an empty queue using `ShutdownBehavior.THROW`;
- `LifecycleQueueV2(Collection<? extends E>)` copies initial elements in encounter order and uses an
  effectively unbounded capacity;
- named bounded constructors may combine capacity, initial elements, shutdown behavior, and poison;
- null elements, null collections, initial contents beyond capacity, and a null poison in `POISON`
  mode are rejected during construction.

The poison object is identity-reserved. The same instance cannot be placed in initial contents or
through later writes. Equal but distinct elements remain legal; consumers should compare poison by
identity.

## 2. Lifecycle Contract

`OPEN -> CLOSING -> CLOSED` is one-way. Guava `Service` states are an integration surface, not the
queue's correctness predicate.

| State | Reads | Producers/collection mutations | Element-removing consumers |
|---|---|---|---|
| `OPEN` | Standard bounded FIFO queue behavior | Accepted subject to capacity | Wait for data or timeout |
| `CLOSING` / `CLOSED` | Non-removing reads observe an empty live queue | Throw `QueueShutdownException`; `drainTo` transfers recovery elements | THROW: exception; POISON: configured poison object |

Shutdown holds both Monitors, closes admission, detaches the live chain, installs a fresh empty
sentinel, resets `count`, and publishes `CLOSING`. A blocking call admitted before that point is
released by its Guard and rechecks the lifecycle while holding its Monitor. Producers throw;
consumers apply their configured shutdown behavior. Shutdown never uses `Thread.interrupt()`.

`POISON` is virtual rather than a queue node. `take`, timed and untimed `poll`, `remove`,
`removeFirst`, and `removeLast` return the same configured object after shutdown. It is not counted,
iterated, visible through `peek`, or included in `remainingList`. This releases any number of
consumers without manufacturing one stored poison node per consumer.

Elements linked before the shutdown linearization point belong to the recovery list. An element still
held by a blocked producer was never linked and remains owned by that producer.

`remainingList()` is available only after `TERMINATED`. It returns one mutable
`CopyOnWriteArrayList` instance, in FIFO order, and changes to it never affect the queue.

## 3. Open-State Queue Compatibility

While `OPEN`, null elements are rejected, normal queue order is FIFO, and `put`, timed `offer`,
`take`, timed `poll`, `offer`, `poll`, `peek`, `size`, and `remainingCapacity` follow the bounded
`LinkedBlockingQueue` model.

### Forward iteration

`iterator()` ports the Java 8 `LinkedBlockingQueue.Itr` algorithm:

- it is weakly consistent, not a snapshot;
- it holds the next item so `hasNext()` remains meaningful after concurrent removal;
- it follows self-links created by dequeues, skips internally removed nodes, and removes by node
  identity;
- `clear()` uses the same item-clearing and self-link pattern as `LinkedBlockingQueue`.

A forward iterator created before shutdown may still return an already buffered or old-chain element.
That is weak-consistency visibility only; it never transfers recovery-list ownership. Its `remove()`
is a mutation and therefore throws after shutdown.

`forEach`, `stream`, `toArray`, and the default `spliterator()` use the inherited collection
traversal. They are late-binding through the weak iterator and invoke user callbacks outside the
Monitors. The default spliterator does not claim `LinkedBlockingQueue`'s specialized
`CONCURRENT|ORDERED|NONNULL` characteristics; callers needing those exact characteristics should not
use V2's inherited spliterator as a substitute.

### Bulk and drain operations

`remove(Object)`, `removeIf`, `removeAll`, `retainAll`, `clear`, iterator removal, producer operations,
and every reverse-view mutation throw after shutdown under both behaviors. `drainTo` is the sole
mutation exception: it remains available in every lifecycle state and transfers a FIFO prefix from
the detached recovery list after shutdown. The element-removing consumer methods listed above are
the only poison-capable operations.

The recovery list remains publicly mutable. Concurrent recovery-list writes may interleave with a
shutdown-state drain; each drain claims elements through atomic list removals, so an element cannot
also remain available to a later drain. Calls to `drainTo` serialize with each other to preserve FIFO
claim order.

`drainTo` first detaches the selected FIFO batch from the live queue or shutdown recovery list and
calls the target's `add` once per item after releasing the Monitor. If a target callback throws, the
detached batch is not restored to the queue or recovery list. This differs from the current
`LinkedBlockingQueue` implementation but is permitted by the `BlockingQueue.drainTo` exception
contract and prevents arbitrary target code from blocking shutdown.

## 4. Sequenced Reverse View

`reversed()` is a write-through reverse-order `List<E>` over the live queue. On Java 21+, `List`
extends `SequencedCollection`, so the returned view supports standard `getFirst`, `getLast`,
`addFirst`, `addLast`, `removeFirst`, `removeLast`, and double-reversal defaults.

The view supports standard mutable List operations:

- `get`, `set`, `add(index, element)`, `addAll(index, source)`, `remove(index)`, and `clear` map to
   the corresponding reverse positions in the queue;
- source encounter order is preserved at every insertion index;
- `ListIterator.next`, `previous`, `add`, `set`, and `remove` obey normal cursor and last-returned
  rules;
- external structural queue changes make existing reverse iterators fail-fast with
  `ConcurrentModificationException`;
- a reverse iterator mutation after shutdown throws `QueueShutdownException`.

The view never exposes `remainingList()`: after shutdown it reads as an empty live view, while any
mutation is rejected.

## 5. Acceptance Checks

1. Close releases blocked producers and consumers without interrupting them and without permitting a
   later commit.
2. THROW mode rejects all closed element-removing consumers with `QueueShutdownException`.
3. POISON mode returns the configured identity from blocked and new consumer retrievals, while
   producer, bulk, iterator, and reverse-view mutations still throw.
4. The recovery list contains exactly the FIFO elements linked before shutdown and never the poison.
5. Collection constructors preserve encounter order and validate nulls and capacity atomically.
6. Forward iterator behavior matches Java 8 `LinkedBlockingQueue` for self-links, clear, and
   identity removal.
7. Reverse view positional operations and `ListIterator` state transitions match a mutable backed
   standard `List`; structural external changes are fail-fast.
8. Source/target callbacks and user predicates do not hold either Monitor while shutdown proceeds.
9. Targeted V2 tests, the full Maven suite, compile, Javadoc, and whitespace checks pass.
