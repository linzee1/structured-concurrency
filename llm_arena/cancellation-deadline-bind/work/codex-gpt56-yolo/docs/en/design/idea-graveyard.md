# Idea Graveyard

> Historical examples in this note may use the pre-v0.2 API. Current application wiring uses `GlobalPar.builder()` and `BatchExecutionOptions`; see the [migration guide](../migration-v0.2.md).

This page records features we seriously considered but ultimately decided not to implement, together with the reasons for rejecting them.

## Configurable failure policies

The library uses fail-fast cancellation as its batch contract. Adding `IGNORE`, `COLLECT`, and other policies would multiply result states and make cancellation ownership unclear. Applications that need partial success can catch errors inside the task function and return an explicit domain result.

## Fluent task orchestration

Methods such as `thenApply`, `thenCompose`, and `exceptionally` would turn the library into another future-composition framework. `parallel-in-scope` owns bounded batch execution; orchestration belongs to Guava or `CompletableFuture` at the application boundary.

## Built-in retry

Retries need a policy for backoff, idempotency, error classification, and attempt budgets. Those decisions depend on the remote system, so a generic retry engine would be misleading. Use a dedicated resilience library or implement retry in the task function.

## Intra-batch priority

Priority scheduling can starve work and conflicts with the predictable sliding-window model. If priority is required, submit separate batches or use an executor policy designed for that workload.

## Spring Boot starter

The core library is framework-neutral and targets Java 8. A starter would add Spring Boot 2.x/3.x compatibility and auto-configuration maintenance for a small amount of wiring. Explicit `GlobalPar.builder()` setup keeps the dependency boundary clear.

## Per-task timeout

The current timeout is a batch-level contract. Per-task deadlines would make result states and cancellation races substantially harder to explain. If individual deadlines are required, enforce them in the task function or an application-level client.

## `CompletableFuture` as the public result type

The implementation relies on Guava future primitives for listener executors, cancellation, and completion aggregation. Returning `CompletableFuture` would create two future semantics and hide the executor choices that matter for this library. An adapter can be kept at the application boundary.

## Streaming and reactive input

The API deliberately models a finite batch. Streaming, backpressure, and reactive cancellation require a different lifecycle model and are better served by Reactor or a similar framework.

## Heterogeneous task composition

An `invokeAll`-style API for unrelated callable types cannot provide a useful type-safe aggregate result without a large overload matrix. Use typed batches and compose their results explicitly.

## Adaptive concurrency

Automatically changing parallelism makes latency and resource behavior difficult to predict. The caller should own the concurrency limit and adjust it using application metrics and an explicit policy.

## What will not be added

The project will stay focused on structured batch execution, cooperative cancellation, bounded scheduling, context propagation, and executor deadlock diagnostics. Features outside that boundary should be integrated explicitly rather than hidden in the core API.
