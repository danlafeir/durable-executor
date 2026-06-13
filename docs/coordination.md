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

## What this delivers — and what it does not

**Delivers (under the contract):**

- Recovery is **single-claimant** — `claim()` resolves concurrent recovery attempts to one winner.
- A stale owner **cannot corrupt the live owner's record** — it neither steals the commit rename nor
  pushes an actively-running record into the DLQ.
- The double-execution window is **shrunk to a real stall longer than `L + Δ + skew`**, not just a
  momentary race or a stale read within Δ.

**Does not deliver:**

- **At-most-once *effect* for an opaque method.** If an instance stalls mid-method, another takes the
  record over and re-runs the side effect, and the first instance later resumes and completes its own
  side effect, both happen. No close-time logic can prevent a side effect that already executed inside
  the method body. The self-fence bounds at-most-once *ownership* of the record, not the effect.

## The path to at-most-once *effect* (not yet built)

Two complementary options, neither of which the filesystem design adds on its own:

- **A user checkpoint** — e.g. `DurableContext.fence()` called immediately before the non-idempotent
  side effect, which throws (aborting *before* the effect) if the lease can't be proven fresh. This is
  the only thing that prevents the second side effect for an opaque method, because only the author
  knows where the effect is. It is net-new public API and is deferred to a separate decision.
- **A fencing token** — a monotonically increasing token handed out with the lease and checked by the
  downstream resource, which rejects a stale writer. This gives exactly-once effect *if and only if*
  the downstream honors it. A plain filesystem cannot mint a strong token cheaply; a DB/Redis/ZK
  backend can — which is why this pairs naturally with the SPI below.

## Held in reserve: the SPI seam

Non-filesystem coordination (DB advisory lock, Redis, etcd/ZooKeeper, conditional writes) belongs
behind a `CoordinationStrategy` SPI (`acquire / renew / release / isHeld / stillOwn / fenceToken`),
with the file lease as one pluggable implementation and the recovery/close logic above it unchanged.
The guarantee then scales to the primitive plugged in: plain FS → at-most-once ownership under the
timing contract; DB/ZK with conditional writes + honored tokens → exactly-once ownership and effect.
This is **not built** — a single filesystem implementation does not justify the abstraction; the seam
earns its place when a second backend or a token the FS can't provide actually appears.

## Validation gate

The README describes `shared-store` as **best-effort** and that language stays until the end-to-end
chaos suite (`spring-durable-executor-sample/chaos.sh`) demonstrates these properties under real pod
kills and induced stalls on the target filesystem. Discriminating unit tests cover each mechanism
(stomp regression, waited-takeover margin, monotonic self-fence, fenced-close-touches-nothing), but a
stronger guarantee claim in user-facing docs is earned by chaos validation, not by unit tests.

## Open questions

- **Target filesystem and its measured Δ.** Fixes the takeover margin and decides whether at-most-once
  is achievable at all for a given deployment. Defaults ship at Δ = 5s, skew = 1s — conservative for a
  strongly-consistent FS, **too low for NFS with default attribute caching**, where Δ must be raised.
- **Whether to ship the `DurableContext.fence()` checkpoint** — the only way to close the double-effect
  gap for opaque methods, weighed against adding public API.
- **Whether a startup probe should measure Δ** rather than trusting a configured value.
