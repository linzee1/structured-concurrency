# A False Positive in an AI Concurrency Review

## Background

An AI review flagged the cancellation and future aggregation code as unsafe. The warning sounded plausible because the code combines `catchingAsync`, `withTimeout`, `allAsList`, and explicit cancellation. The issue was not a compiler error; it was a misunderstanding of Guava future state semantics.

## Where the false positive came from

The review treated an exception handled by `catchingAsync` as proof that the operation had recovered successfully. It also treated a cancelled future as equivalent to an ordinary failed future. In Guava, cancellation is a distinct terminal state and can propagate through aggregation independently of ordinary exceptions.

The review also assumed that a timeout callback immediately stopped every task. The implementation uses cooperative cancellation: blocked I/O can respond to interruption, while CPU-bound work responds at its next checkpoint.

## How to correct the review

1. Read the actual future state transitions instead of inferring behavior from method names.
2. Separate exception recovery from cancellation propagation.
3. Trace the late-binding point where all task futures receive the same cancellation token.
4. Check whether a task is blocked, queued, or CPU-bound before claiming that interruption is sufficient.
5. Reproduce the state transition with a focused test using settable futures.

## Why concurrency code is easy to misread

Concurrency APIs compress several independent dimensions into a small expression: completion, failure, cancellation, timeout, executor choice, and callback scheduling. A review that follows only the happy-path value flow can miss the control flow carried by cancellation states.

## Practical review checklist

- What are the terminal states of each future?
- Which executor runs each callback?
- Is cancellation propagated to siblings and children?
- Does timeout mean “stop submitting”, “interrupt blocked work”, or both?
- Are CPU-bound loops checking a cooperative token?
- Is the assertion observing a public contract or an implementation detail?

## Takeaway

AI assistance is useful for generating review questions, but concurrency claims still need source-level state tracing and deterministic tests. A plausible warning is a hypothesis until the future state machine and a focused reproduction confirm it.
