# English Documentation

This page is the English entry point for the user guide, API references, design notes, tests, and case studies.

## User Documentation

| Document | Role |
|---|---|
| [Full user guide](user-guide.md) | **Primary guide**: configuration, public APIs, runtime behavior, and advanced features |
| [v0.2 migration guide](migration-v0.2.md) | Breaking changes from the `0.1.x` API |
| [Demo project](https://github.com/HuaTalk/parallel-in-scope/blob/main/demo/README.en.md) | Runnable examples and build commands |
| [Demo documentation map](https://github.com/HuaTalk/parallel-in-scope/blob/main/demo/docs/en/README.md) | English entry point for the example catalog |

## API and Contracts

| Document | Role |
|---|---|
| [Cooperative cancellation](reference/cooperative-cancellation.md) | Checkpoints, exceptions, and cancellation propagation |
| [Nullability annotations](reference/nullability-annotations.md) | Public API and internal annotation rules |

## Implementation and Design

| Document | Role |
|---|---|
| [Design philosophy](design/philosophy.md) | Design decisions and boundaries |
| [Idea Graveyard](design/idea-graveyard.md) | Intentionally unsupported features |

## Tests and Cases

| Document | Role |
|---|---|
| [AsyncBatchResult test design](testing/async-batch-result-report-test-design.md) | Stable report contracts and concurrency assertions |
| [AI concurrency review case study](case-studies/ai-concurrency-review-false-positive.md) | A false-positive review and how to verify it |

## Documentation Roles

- The primary guide describes supported user workflows.
- API/contract references define current contracts.
- Demo articles explain problems and examples; they do not override the API contract.
- Internal documents may evolve with the implementation.
