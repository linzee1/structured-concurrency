# AsyncBatchResult Report Testing Design

## Goals

The report API must describe completed, failed, cancelled, and timed-out work without depending on a lucky thread schedule. Tests should verify observable state and the public contract, not implementation timing.

## Behaviors under test

- Results preserve the input order where the API promises ordering.
- Successful, failed, cancelled, and timed-out tasks are reported separately.
- A task failure triggers fail-fast cancellation of the remaining batch.
- `report()` remains stable after all futures have completed.
- Empty and partially completed batches have deterministic counts.

## Test layers

### Unit tests

Construct immediate, settable, and cancelled futures directly. This keeps state assertions deterministic and avoids starting an unnecessary executor for every case. Assert exact counts and states instead of checking that a string merely contains a fragment.

### Demo integration tests

Use the demo test classes to exercise a real `Par.map` call, batch cancellation, and the report formatting together. Integration tests should validate the public workflow while unit tests own the combinatorial state matrix.

## Avoid timing-based tests

Do not arrange threads with arbitrary `sleep` calls or require a fixed number of tasks to finish before cancellation. Use latches, settable futures, and explicit completion signals. A test that swallows task exceptions just to obtain a convenient count is testing the wrong contract.

## Maintenance rules

When report fields change, update the state matrix and the integration scenario together. Keep assertions specific enough to catch a regression but independent of executor thread names and incidental scheduling order.
