# Why durable-executor?

## The problem: the durability gap

A method does real, multi-step, often non-idempotent work — create an order, reserve stock, charge a
card, dispatch fulfilment. The process dies in the middle: an OOM kill, a deploy, a node eviction, a
crash. What happens to the work that was in flight?

With an ordinary method, it's simply gone — or worse, half-done: the card charged but the order never
marked paid. Nothing retries it, because nothing recorded that it was running. That window — between "the
method started" and "the method finished" — is the **durability gap**, and it's invisible until a crash
lands inside it under load.

`durable-executor` closes that gap with a single annotation:

```java
@Durable
public void processOrder(String orderId, BigDecimal amount) { ... }
```

The invocation (target + arguments) is persisted **before** the method runs and removed only once it
succeeds. If the process dies mid-method, the record survives, and on the next startup the method is
re-invoked with its original arguments. Work that exhausts its retries goes to a dead letter queue rather
than retrying forever.

## What you get

Durable execution as a property of a method, not a rearchitecture:

- **One annotation.** No new programming model, no separate server, no message schemas — annotate the
  method you already have.
- **In-process.** The only infrastructure is a durable directory (a mounted volume). No broker, no
  workflow cluster, no extra service to operate.
- **Honest guarantees you choose.** At-least-once by default; at-most-once for non-idempotent methods via
  `TRANSACTIONAL` close mode + the DLQ; across replicas via the coordination modes. See
  [architecture-decisions.md](architecture-decisions.md).
- **Batteries included:** exponential-backoff retries, a dead-letter queue with an HTTP management
  endpoint (list / requeue / delete / retention), generic-typed argument recovery, startup validation
  that fails fast on a non-durable store or an async return type, and pluggable multi-instance
  coordination (filesystem leases out of the box, or your own database/Redis backend).

## Why not just…?

**…a jobs table + a scheduler?** That's essentially what this *is* — productised. Rolling your own means
hand-building the serialize-before-run, the scheduler that re-picks-up, retry/backoff, a DLQ, the
crashed-vs-still-running distinction, and multi-replica coordination — and getting the crash-window edge
cases right. Reach for DIY only if your needs are trivial or genuinely unusual.

**…a workflow engine (Temporal, Cadence, AWS Step Functions, Camunda)?** Those are strictly more powerful:
durable timers, signals, child workflows, versioning, fan-out/fan-in. If you need real **orchestration** —
long-running processes with waits, human approval steps, complex DAGs — use one. The cost is a separate
server or managed service to operate, and a workflow/activity programming model to adopt. `durable-executor`
is the lightweight middle ground: when you just need "this method must complete even across a crash"
without standing up an engine or rewriting into workflows.

**…a message queue (SQS, Kafka, RabbitMQ)?** Queues give durable hand-off, retries, and a DLQ — but they
require restructuring into producers and consumers, operating a broker, designing message schemas and
consumer idempotency, and they make the call asynchronous whether you wanted that or not. Use a queue when
you genuinely want **async decoupling, pub/sub, or cross-service fan-out**. Use `durable-executor` when the
work is a local method that should keep its synchronous shape and simply survive a crash.

**…in-process retry (Spring Retry, Resilience4j)?** Those retry within the *same* process — a crash, OOM,
or redeploy loses the in-flight attempt entirely. `durable-executor` persists across process death and
resumes on the next startup. They're complementary: retry transient in-process failures, and use
`@Durable` to survive losing the process.

**…a database transaction?** A transaction atomically commits or rolls back database state, but it cannot
resume a multi-step process with **non-transactional side effects** (an HTTP call, a file write, a second
service) after a crash — and it rolls back rather than completing. `durable-executor` resumes the whole
method. Use both: transactions for atomic state, `@Durable` for surviving the crash.

## When *not* to reach for it

- You need full **orchestration** — durable timers, signals, human-in-the-loop, large DAGs. Use a workflow
  engine.
- Your work is already an **idempotent consumer behind a durable queue** — you may already be covered.
- You can't provide **durable storage** (everything is ephemeral). It can't manufacture durability it
  doesn't have — and it will warn you at startup rather than pretend.
- You need **exactly-once distributed *effects* with no operator reconciliation**. The library gives
  at-most-once plus a DLQ; closing the last gap needs a downstream-honoured fencing token (see
  [coordination.md](coordination.md)).

## In one line

If you have a JVM method that does important, multi-step work and you've ever worried "what if we crash
right *here* under load" — that's the gap this library closes, with an annotation instead of an
architecture.

See [architecture-decisions.md](architecture-decisions.md) to choose how to deploy it.
