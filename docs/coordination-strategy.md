# Implementing a CoordinationStrategy (Redis / database / etc.)

`shared-store` coordinates replicas through lease files on the shared volume. If you'd rather
coordinate through an **independent store** — a database row lock, Redis, etcd/ZooKeeper — implement a
`CoordinationStrategy` and register it as a bean. The record store stays on disk; only the
*who-owns-what* decision moves to your backend.

```java
@Bean
CoordinationStrategy coordinationStrategy(/* your client */) {
    return new RedisCoordination(...);
}
```

Defining the bean is enough: the autoconfiguration's default is `@ConditionalOnMissingBean(CoordinationStrategy.class)`,
so yours replaces it, and the aspect and recovery use it everywhere. (The `durable.coordination` property
then no longer applies — your bean decides.)

## Extend `AbstractLeaseCoordination` — don't implement the interface from scratch

The interface has eleven methods, but the **correctness-critical** ones are not yours to write. The
monotonic self-fence (`stillOwns`) and owner-checked renewal (`heartbeat`) are identical for every
backend and are exactly the parts that, done naively, reintroduce double execution under a stall. So
extend `AbstractLeaseCoordination` and implement only **five raw primitives**:

| Primitive | Contract | Redis sketch | SQL sketch |
|-----------|----------|--------------|------------|
| `writeOwner(id)` | Unconditionally set owner = me, refresh expiry | `SET lease:{id} {me} PX {leaseMs}` | `UPSERT (id, me, now()+lease)` |
| `readOwner(id)` | Current owner, or null if unowned/expired | `GET lease:{id}` | `SELECT owner WHERE id=? AND expires_at>now()` |
| `removeOwner(id)` | Delete **only if** owner = me | `DEL` guarded by a Lua `GET`-compare | `DELETE WHERE id=? AND owner=?` |
| `tryClaim(id)` | Atomically take it; true iff I now own it | `SET lease:{id} {me} NX PX {leaseMs}` (or take-if-expired via Lua) | `INSERT ... ON CONFLICT DO UPDATE ... WHERE expired RETURNING owner` |
| `isHeldByLiveOwner(id)` | A non-expired entry exists (any owner) | `EXISTS lease:{id}` | `SELECT 1 WHERE id=? AND expires_at>now()` |

The base then gives you `acquire`, `claimForRecovery`, `markRunning`/`markStopped`, `release`, `stillOwns`,
`isActive`, `heartbeat`, and `sweep` for free. `isMultiInstance()` returns true (so the heartbeat runs
and a reclaimed `TRANSACTIONAL` record is routed to the DLQ), and `heartbeatInterval()` is one third of
the lease duration.

Notes:
- **A TTL store doesn't need the visibility-lag (Δ) margin.** The file backend waits `lease + Δ + skew`
  before takeover to absorb NFS stale reads; a strongly consistent store (Redis, a DB) has no Δ, so
  `tryClaim`/`isHeldByLiveOwner` can key off the raw TTL. Size the TTL like the file lease duration.
- **`tryClaim` is the only operation that must be atomic** (compare-and-set). It is the authoritative
  recovery race-winner; `isActive` is only a best-effort pre-filter, so it need not be atomic.
- **`sweepBackend` can be a no-op** — entries with a TTL expire on their own; there is nothing to clean up.
- If you implement the bare `CoordinationStrategy` interface directly (rather than extending the base),
  a multi-instance strategy must return a non-zero `heartbeatInterval()` — the recovery loop schedules
  the heartbeat at that period, so zero would busy-loop. `AbstractLeaseCoordination` handles this for you.

## Worked example

[`InMemoryCasCoordination`](../durable-executor-spring/src/test/java/com/lafeir/durableexecutor/InMemoryCasCoordination.java)
in the test sources is a complete TTL + CAS backend (a `ConcurrentHashMap` standing in for Redis/DB). It
is the shape your Redis or JDBC class will take — copy its five primitives and swap the map for your
client. `CoordinationStrategyTest` exercises it, including electing exactly one claimant under
concurrent recovery.

## What you keep

The library's guarantees carry over to any backend that honours the primitive contracts:

- **At-most-once for `TRANSACTIONAL` across instances** — from the base's self-fence (a stalled owner
  bows out) plus recovery's DLQ routing (a reclaimed record is not re-run). A stronger backend changes
  *how* ownership is decided, not this outcome.
- With a backend that issues a monotonic **fencing token** the downstream resource honours, you could go
  further (exactly-once *effect* without operator reconciliation) — but that is your backend's and your
  downstream's contract to add; the SPI does not require or provide it.

See [coordination.md](coordination.md) for the design and [shared-store-operations.md](shared-store-operations.md)
for the DLQ-triage operational model (which applies to any multi-instance backend).
