# Architecture decisions

Deploying durable-executor involves three independent choices. They interact, but each answers a
different question. This guide helps you pick; the linked docs go deeper on each.

| Axis | Options | Decide on |
|------|---------|-----------|
| [1. Close mode](#1-close-mode-idempotent-vs-transactional) | `IDEMPOTENT` · `TRANSACTIONAL` | Is the method's side effect safe to repeat? |
| [2. Store topology](#2-store-topology-per-pod-volume-vs-shared-store) | per-pod volume · shared store · single-writer + external lock | Must a *dead* replica's work be picked up by another? |
| [3. Coordination backend](#3-coordination-backend-filesystem-vs-separate-infrastructure) | filesystem leases · separate infra (DB/Redis/…) | Do you need true mutual exclusion / fencing, or is best-effort enough? |

A useful default: **per-pod volume + `IDEMPOTENT`** is the simplest thing that self-heals. Reach for the
others only when a specific need below applies.

---

## 1. Close mode: `IDEMPOTENT` vs `TRANSACTIONAL`

Controls what happens in the ambiguous window when a record is recovered — re-run it, or set it aside?

**`IDEMPOTENT` → at-least-once.** A recovered/reclaimed record is **re-run**. Choose it when the method
is naturally repeatable — an upsert by key, a no-op-if-already-done check, a set-to-a-value. This is the
low-friction option: work auto-recovers and nothing lands in the DLQ for ambiguity.

**`TRANSACTIONAL` (default) → at-most-once.** The library prefers a DLQ entry over a double execution: it
will not *automatically* re-run a non-idempotent method whose outcome is ambiguous. Choose it when a
duplicate side effect (a second charge, a second shipment) is worse than a delayed, operator-reconciled
one.

The guarantee depends on the store topology (axis 2), because "ambiguous" means different things:

- **Single-writer recovery** (per-pod volume, or single-instance behind a lock): a restart unambiguously
  means the previous run crashed, so a `TRANSACTIONAL` record is **re-run** to complete it — at-least-once
  for a mid-execution crash; only the crash-*after-success* window is routed to the DLQ.
- **Multi-instance recovery** (shared store): a record reclaimed from an expired lease could mean the owner
  crashed *or* merely stalled — undecidable — so a `TRANSACTIONAL` record is **routed to the DLQ**, never
  auto-re-run. This is the at-most-once-across-instances guarantee, and it holds regardless of filesystem
  timing.

> Rule of thumb: make the work idempotent and use `IDEMPOTENT` whenever you can — it converts triage into
> auto-recovery. Use `TRANSACTIONAL` for the genuinely non-idempotent steps. (A two-layer `@Durable`
> pattern — idempotent dispatcher, transactional inner step — keeps the at-most-once boundary small.)

See the README's [Close mode](../README.md#close-mode) section.

---

## 2. Store topology: per-pod volume vs shared store

Where the record files live decides whether a replica can recover *another* replica's work.

**Per-pod volume (`ReadWriteOnce`) + `single-instance`.** Each replica owns a private store; on restart it
recovers its own in-flight records. Deterministic, zero coordination overhead, no DLQ-triage tax. The
catch: a crashed pod's work is recovered **only when that pod restarts** — fine for a `StatefulSet` whose
pods keep a stable identity and reattach the same volume, but if a pod never comes back (or its volume is
lost) its work is stranded. No cross-replica takeover.

**Shared store (`ReadWriteMany`) + `shared-store`.** All replicas share one store directory, so **any**
replica can recover a crashed one's work — faster recovery, and resilient to a pod that never returns. The
cost: you need a volume that meets the [storage contract](coordination.md#the-supported-storage-contract),
the lease coordination machinery, and — for `TRANSACTIONAL` — the DLQ-triage model
([operations](shared-store-operations.md)). `TRANSACTIONAL` + shared-store is **not self-healing**: expect
DLQ entries proportional to your crash/stall rate.

**Single writer behind an external lock** (leader election, a DB advisory lock). Run `single-instance` but
ensure only one replica is ever active against the store. Strict single-writer semantics without the
shared-store ambiguity — at the cost of only one active worker at a time.

Decide by: **does a dead replica's work need to be picked up by a live one before that replica restarts?**
No → per-pod volume. Yes → shared store (or external-lock failover). Can't get a contract-meeting RWX
volume → don't use shared store; use per-pod or an external lock.

See the README's [Coordination mode](../README.md#coordination-mode) and
[coordination.md](coordination.md).

---

## 3. Coordination backend: filesystem vs separate infrastructure

*Only relevant once you've chosen a multi-instance topology.* Coordination is a pluggable
`CoordinationStrategy`; the record store stays on disk either way — only the *who-owns-what* decision moves.

**Filesystem leases (`FileLeaseCoordination`, the built-in `shared-store`).** Coordination rides on the
shared volume as `{id}.lease` sidecar files — **zero extra infrastructure**. Correct under the storage
contract (atomic rename, a bounded visibility lag Δ, read-after-write, bounded clock skew), and it delivers
at-most-once for `TRANSACTIONAL` independent of Δ. Trade-offs: you must know/tune Δ for your filesystem
(on NFS, the attribute-cache bound); claims are **best-effort**, not true mutual exclusion (two claimants
can both "win" — safe because the loser's outcome still routes by close mode); and a filesystem can't mint
a fencing token.

**Separate infrastructure (a `CoordinationStrategy` over a DB / Redis / etcd).** Implement the SPI against
a store you already run. Gains: **true compare-and-set** (exactly one recovery claimant), strong
consistency (no Δ to tune), and the option to issue a monotonic **fencing token** a downstream resource
honours — the path to exactly-once *effect* without operator reconciliation. Costs: an extra dependency and
operational surface, and you write (and test) the backend. The library hands you most of it — extend
`AbstractLeaseCoordination` and implement five primitives; the self-fence and owner-checked renewal are
provided. See [coordination-strategy.md](coordination-strategy.md).

Decide by: filesystem leases are the right default for shared-store — no new infra, and at-most-once holds.
Move coordination to separate infra when you want **true mutual exclusion**, want to **avoid tuning Δ**,
already operate a suitable store, or need **fencing tokens** for exactly-once effect.

---

## Putting it together

Common, coherent configurations:

| Configuration | Guarantee | Recovery of a dead replica's work | Operational cost |
|---------------|-----------|-----------------------------------|------------------|
| Per-pod volume + `IDEMPOTENT` | at-least-once | on that pod's restart | lowest — self-heals |
| Per-pod volume + `TRANSACTIONAL` | at-most-once for the post-success window; mid-crash re-runs | on that pod's restart | low |
| Shared store (FS) + `IDEMPOTENT` | at-least-once, cross-replica | any live replica, promptly | lease overhead; no triage |
| Shared store (FS) + `TRANSACTIONAL` | at-most-once across replicas | reclaimed → **DLQ** for adjudication | DLQ triage ∝ crash rate |
| External coordination + `TRANSACTIONAL` | at-most-once (exactly-once *effect* with a honoured fencing token) | any live replica, single claimant | runs + maintains the backend |
| `single-instance` behind an external lock | strict single-writer | failover to the new leader | leader-election infra |

Start at the top and move down only when a column you need isn't satisfied.

## Further reading

- [coordination.md](coordination.md) — the coordination design: the storage contract, the self-fencing lease, what at-most-once does and does not cover.
- [shared-store-operations.md](shared-store-operations.md) — operating shared-store: the DLQ-triage model and how to reduce it.
- [coordination-strategy.md](coordination-strategy.md) — implementing a `CoordinationStrategy` over Redis / a database / etc.
