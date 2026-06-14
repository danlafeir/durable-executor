# Multi-instance coordination — design notes

How `coordination: shared-store` lets multiple replicas share one store directory without re-running
each other's live work, what guarantee that actually buys, and where the design stops. This is the
reasoning behind the code; the user-facing knobs are in the [README](../README.md#coordination-mode).

Tracking issues: [#3](https://github.com/danlafeir/durable-executor/issues/3) (the limitation),
[#11](https://github.com/danlafeir/durable-executor/issues/11) (the broader design).

## The problem

A pending `{id}.msgpack` file means one of two things — an execution that **crashed** and needs
recovery, or one that is **running right now** — and the file alone cannot tell them apart. With a
single writer this is resolved in memory (`single-instance`: a live set the recovery scan consults).
With several replicas on a `ReadWriteMany` / NFS volume there is no shared memory, so the signal has
to live on the filesystem.

## The reframe: which "exactly-once"?

"Exactly-once across replicas" splits into two claims that are not the same:

- **Exactly-once *ownership*** — at most one replica ever treats a given record as its own to run and
  commit. Achievable on a shared filesystem **under a timing contract** (below).
- **Exactly-once *effect*** — the side effect inside the method body happens at most once. **Not**
  achievable by coordination metadata alone: the side effect is opaque code the library cannot see
  into, undo, or gate. It needs either a checkpoint the user calls right before the effect, or a
  fencing token the downstream resource honors.

So the honest target for the filesystem-only design is **at-most-once ownership** (with `TRANSACTIONAL`
close) / **at-least-once** (with `IDEMPOTENT`), not unconditional exactly-once effect. The library
already lets the method author choose which side of that line to fail toward, via `closeMode`.

## Generic, or coupled to the storage?

Generic — coupled only to a *contract*, not to a vendor API.

- **The mechanism is storage-agnostic.** The lease protocol (write owner + heartbeat, owner-checked
  renewal, waited takeover, monotonic self-fence, `CloseMode`-driven close) is pure logic. One
  implementation runs on EFS, Filestore, Azure Files, a clustered POSIX FS — all the same.
- **The only storage-specific input is a number, not a code path.** The takeover margin is
  `L + Δ + skew`. Δ (visibility lag) is an empirical property of the filesystem; it enters as the
  `visibility-lag` config value, not as a branch. You don't write EFS code; you set Δ for your FS.
- **Genericity is bounded by the contract, and you can't configure past a missing guarantee.** The
  implementation is sound only on storage that satisfies the [contract](#the-supported-storage-contract).
  Storage that can't bound Δ (a truly eventually-consistent object store behind a FUSE mount; NFS with
  caching that has no convergence deadline) cannot be made at-most-once by any value of Δ.

The one axis where real coupling would otherwise creep in is *non-filesystem* coordination (a DB
advisory lock, Redis, etcd/ZooKeeper, conditional writes). That is deliberately **held in reserve** as
a `CoordinationStrategy` SPI — see [Held in reserve](#held-in-reserve-the-spi-seam). It is not built;
one filesystem implementation does not justify the abstraction.

## The supported-storage contract

`shared-store` is sound when the underlying filesystem provides all of:

1. **Atomic rename** (`ATOMIC_MOVE`) for the open → commit-marker transition and for lease writes. The
   library already depends on this for crash-safety; here it also makes a lease write all-or-nothing.
2. **Bounded visibility lag (Δ)** — a real upper bound on how long after one instance writes a lease
   another may still read the previous value. This is what `visibility-lag` must be set to. On a
   strongly-consistent FS Δ ≈ 0; on NFS it is the attribute-cache bound (`acregmax`/`acdirmax`), which
   can be tens of seconds.
3. **Read-after-write on the lease file** — an instance reads back at least its own most recent write.
   `claim()` is write-then-read-back; it resolves a concurrent claim to a single winner only if the
   read reflects the last write. Without this property, two claimants can each read their own stale
   write and both believe they won, so `claim()` would have to wait Δ before reading back.
4. **Bounded clock skew** — lease expiry is computed from the file mtime stamped by a possibly
   differently-clocked owner, so `clock-skew` is added to the takeover margin. Keep instances on NTP.

If your storage does not meet (1)–(4), run `single-instance` behind an external lock instead.

## The mechanism

1. **Lease file.** Each in-flight record is stamped `{id}.lease` containing the owner id; its mtime is
   the heartbeat timestamp. A dedicated heartbeat thread renews live leases at ~`L/3`.
2. **Owner-checked renewal.** Renewal re-stamps a lease *only if it still names us*. A blind renewal
   would let a stalled instance, on resuming, stomp its owner id back over a lease another instance had
   already taken over — re-creating double ownership. (Reachable without a GC pause: sequential
   per-file NFS renewal of a long live-set can run past a lease's expiry.)
3. **Waited takeover.** A reclaiming instance treats a lease as free only once it has been expired for
   the **full** `L + Δ + skew` past the last heartbeat — not merely `L`. The extra margin guarantees a
   still-live owner whose renewal simply hasn't propagated yet is not prematurely taken over.
4. **Monotonic self-fence.** The load-bearing piece. Each instance records the monotonic time
   (`System.nanoTime`) of every successful lease write. `stillOwn(id)` is true only if the lease file
   still names us **and** less than `L` has elapsed, by our own monotonic clock, since we last renewed.
   Before closing, the aspect checks `stillOwn`; if false it **mutates the record in no way** and bows
   out. The monotonic half matters because a stalled instance cannot trust a wall-clock comparison or a
   possibly-stale file read to notice it lost the lease — only the elapsed-since-renew on its own
   monotonic clock is reliable. Safety here comes from the **owner self-fencing on its own clock**, not
   from any instance inferring another's death from the outside.
5. **`CloseMode`-driven close.** For a non-fenced close, `TRANSACTIONAL` renames to a commit marker
   (crash-after-success → DLQ, no retry) and `IDEMPOTENT` deletes directly (crash-after-success → one
   retry). A *fenced* close is different from crash-after-success and routes nowhere: the new owner is
   authoritative, so committing, deleting, or dead-lettering would corrupt its state.
6. **DLQ safety valve on takeover.** When recovery reclaims a record (its lease expired — on a shared
   filesystem an undecidable signal: the previous owner crashed, or merely stalled and is still
   running), the `closeMode` decides the action. `IDEMPOTENT` → re-invoke (safe to repeat).
   `TRANSACTIONAL` → **route to the DLQ, do not re-invoke** — the library will not automatically re-run
   a non-idempotent method when it cannot prove the previous owner didn't already run it. An operator,
   who can check whether the side effect actually landed, adjudicates the DLQ entry. This is what makes
   the *taker* side safe; the self-fence (4) makes the *stalled owner* side safe. Together they bound
   the effect to at-most-once across instances without a user-written checkpoint. Single-instance is
   exempt — a restart there unambiguously means the previous run ended, so `TRANSACTIONAL` re-runs.

## What this delivers — and what it does not

**Delivers (under the contract):**

- Recovery is **single-claimant** — `claim()` resolves concurrent recovery attempts to one winner.
- A stale owner **cannot corrupt the live owner's record** — it neither steals the commit rename nor
  pushes an actively-running record into the DLQ.
- The library **never automatically double-executes a `TRANSACTIONAL` method across instances.** The
  combination of the self-fence (stalled owner touches nothing) and the DLQ safety valve (taker
  routes ambiguous non-idempotent records to the DLQ instead of re-running) bounds the *effect* to
  at-most-once. The residual ambiguity — did the previous owner's effect land 0 or 1 times? — is not
  resolved automatically; it is surfaced as a DLQ entry for an operator to adjudicate.

**The cost (does not deliver for free):**

- **`TRANSACTIONAL` + shared-store is not self-healing.** A reclaimed non-idempotent record is **not**
  auto-recovered — it lands in the DLQ and waits for an operator to verify and requeue. Expect DLQ
  entries at volume proportional to your crash/stall rate. This is the deliberate trade — prefer
  operator-mediated reconciliation over automatic duplication — and is the same preference `CloseMode`
  already encodes, extended across instances. See [shared-store-operations.md](shared-store-operations.md).
- **No fully-automatic at-most-once *effect*.** Closing the residual (0-or-1) gap without an operator
  would need either a user checkpoint or a downstream-honored fencing token (next section).

## Fully-automatic at-most-once effect — alternatives not taken

The DLQ safety valve achieves no-double-execution today with no new public API, at the price of
operator triage. Removing the operator from the loop would need one of:

- **A user checkpoint** — e.g. `DurableContext.fence()` called immediately before the non-idempotent
  side effect, aborting *before* it if the lease can't be proven fresh. Only the author knows where
  the effect is, so only they can place it. Net-new public API; **not taken** — the DLQ valve covers
  the goal without burdening user code.
- **A fencing token** — a monotonically increasing token handed out with the lease and checked by the
  downstream resource, which rejects a stale writer. Exactly-once effect *iff* the downstream honors
  it. A plain filesystem cannot mint a strong token cheaply; a DB/Redis/ZK backend can — which is why
  this pairs naturally with the SPI below.

## The SPI seam (built)

Coordination is a `CoordinationStrategy` SPI; the record store (`DurableStore`) is pure file I/O. The
file lease (`FileLeaseCoordination`) and the in-memory single-instance liveness
(`SingleInstanceCoordination`) are two implementations, and non-filesystem coordination (a database
lock, Redis, etcd/ZooKeeper) plugs in as a user-supplied `@Bean CoordinationStrategy` that overrides the
default. The recovery/close/aspect logic above the seam is unchanged regardless of backend.

The correctness-critical, backend-independent parts — the monotonic self-fence and owner-checked
renewal — live in `AbstractLeaseCoordination`, so a backend implements only raw ownership primitives
(`writeOwner`/`readOwner`/`removeOwner`/`tryClaim`/`isHeldByLiveOwner`) and cannot reintroduce the
stall/stomp races. The guarantee then scales to the primitive plugged in: plain FS → at-most-once
ownership under the timing contract; a DB/ZK backend with conditional writes + a downstream-honored
fencing token → exactly-once ownership and effect (the token is the backend's and downstream's contract
to add — the SPI neither requires nor provides it).

See [coordination-strategy.md](coordination-strategy.md) for how to implement a backend.

## Validation status

The at-most-once claim for `TRANSACTIONAL` is backed by both unit tests and an end-to-end chaos run.
Discriminating unit tests cover each mechanism (stomp regression, waited-takeover margin, monotonic
self-fence, fenced-close-touches-nothing, takeover→DLQ routing).

The end-to-end run (`spring-durable-executor-sample`, shared-store variant: 3 replicas sharing one
store, `coordination: shared-store`, a non-idempotent charge per execution) survived continuous load
with random pod kills: **0 double charges across 197 orders / 15 kills**, with reclaimed records
correctly routed to the DLQ. Because a reclaimed `TRANSACTIONAL` record is *always* DLQ'd regardless of
Δ, this validates the at-most-once safety property independent of filesystem timing.

Two dimensions remain **not** covered by that run, and the README scopes its claim accordingly:

- **Δ / NFS stale reads** — the run used a single-node `hostPath` (strongly consistent). The at-most-once
  property is Δ-independent by construction, but the *DLQ-volume* behavior under real NFS attribute-cache
  lag is unmeasured; that needs an actual `ReadWriteMany`/NFS volume.
- **The self-fence under stalls** — pod *kills* exercise the takeover→DLQ valve (crash); a *stalled*
  owner that resumes and bows out needs `SIGSTOP`/`SIGCONT` injection. Covered by unit tests today.

## Open questions

- **Target filesystem and its measured Δ.** Fixes the takeover margin and decides whether at-most-once
  is achievable at all for a given deployment. Defaults ship at Δ = 5s, skew = 1s — conservative for a
  strongly-consistent FS, **too low for NFS with default attribute caching**, where Δ must be raised.
- **Whether a startup probe should measure Δ** rather than trusting a configured value.

(The `DurableContext.fence()` checkpoint question is resolved: the DLQ safety valve covers the goal
without new public API, so `fence()` is **not** being built — see the alternatives-not-taken section.)
