# Operating shared-store: the DLQ-triage model

A practical guide to running `coordination: shared-store` — why coordinating over a shared volume is
hard, the trade-off this library makes (lean on the DLQ as a safety valve), and how to operate and
size that triage. For the design and correctness argument behind it, see
[coordination.md](coordination.md).

## Why a shared volume is hard

`shared-store` lets several replicas share one store directory (a `ReadWriteMany` / NFS volume). The
coordination problem underneath it is genuinely hard, for reasons that are properties of the storage,
not bugs to fix:

- **Crash vs. stall is undecidable.** When a record's lease expires, another replica cannot tell
  whether the owner *crashed* (safe to re-run) or merely *stalled* — a long GC pause, CPU starvation,
  a paused container, a partition to the NFS server — and is about to resume and finish. This is the
  classic distributed-systems impossibility (FLP): you cannot reliably detect a remote process's death
  over an unreliable channel. No timeout setting makes it decidable; a longer timeout only trades
  recovery latency for fewer false positives.
- **Reads can be stale (Δ).** On NFS, attribute caching means one replica may not see another's lease
  write for up to the cache bound (`acregmax`/`acdirmax`, often tens of seconds). The library models
  this as a configured visibility bound, `durable.visibility-lag` (Δ), and waits `lease-duration + Δ +
  clock-skew` before any takeover. Set Δ **too low and you risk premature takeover** (double work); set
  it high and recovery is slower. Measure it for your storage.
- **Clocks differ.** A lease's expiry is read from a file mtime stamped by a possibly differently-clocked
  owner, so `durable.clock-skew` is added to the takeover margin. Keep replicas on NTP.
- **No atomic compare-and-swap.** A filesystem offers atomic *rename*, not atomic CAS on file contents.
  Lease ownership is therefore check-then-act (write, read back), which is single-claimant only on a
  read-after-write filesystem — part of the [supported-storage contract](coordination.md#the-supported-storage-contract).

The library narrows the resulting double-execution window hard (owner-checked renewal, waited takeover,
a monotonic self-fence), but it cannot make the underlying signal decidable. Something has to give for
the residual ambiguity — and that is where the DLQ comes in.

## The trade-off: the DLQ as a safety valve

When recovery reclaims a record across instances, the action depends on `closeMode`:

| closeMode | On cross-instance takeover | Why |
|-----------|----------------------------|-----|
| `IDEMPOTENT` | **Re-run** | Safe to repeat, so just recover it — no triage. |
| `TRANSACTIONAL` (default) | **Route to the DLQ, do not re-run** | Non-idempotent and the previous owner's fate is ambiguous; re-running could double-execute. |

So the library **never automatically double-executes a `TRANSACTIONAL` method across instances.** The
price is explicit and intentional:

> **`TRANSACTIONAL` + `shared-store` is not self-healing.** A non-idempotent record whose owner crashed
> or stalled is **not** auto-recovered — it goes to the DLQ and waits for an operator to decide. Expect
> DLQ entries at volume proportional to your crash/stall rate (every rolling deploy, OOM kill, or long
> stall on an in-flight `TRANSACTIONAL` record produces one).

This is the same preference `CloseMode` already encodes — *prefer a DLQ entry over a double execution
for non-idempotent work* — extended from the single-process crash window to the cross-instance case.

## Triaging the DLQ

Each `TRANSACTIONAL` DLQ entry asks one question: **did the side effect already happen?** The library
cannot answer it (the effect is opaque application code); a human or an application-specific check can.
For each entry:

1. **Determine whether the effect landed** — query the downstream system, check an idempotency key, or
   inspect an audit/ledger row keyed by the execution id.
2. **Resolve it** via the [DLQ endpoint](../README.md#dead-letter-queue):
   - **Effect did not happen →** `POST {path}/{id}/requeue` to re-run it with a fresh retry budget.
   - **Effect already happened →** `DELETE {path}/{id}` to discard it.

> **Do not blind-requeue.** Requeuing an entry whose effect already happened is the one way to turn
> at-most-once into a double execution — that step is the operator's responsibility, exactly because
> only the application can tell. Automate the check (below) before automating the requeue.

## Reducing — and automating — the triage

The DLQ valve is correct but costs operator time. Lower that cost by producing fewer entries and by
making each entry cheaper to resolve:

- **Make the work idempotent and use `IDEMPOTENT`.** An idempotent method (upsert by key, a
  natural-key insert, a no-op-if-already-done check) is safe to re-run, recovers automatically, and
  never lands in the DLQ for ambiguity. This is the single biggest lever — convert triage into
  auto-recovery wherever the operation can be made repeatable.
- **Split the work (two-layer `@Durable`).** Keep a thin `IDEMPOTENT` dispatcher that just hands off,
  and put the `TRANSACTIONAL` boundary only around the genuinely non-idempotent step — so only that
  step can need triage, not the whole workflow. (This is the pattern the sample uses.)
- **Make adjudication automatic.** Use [stable execution IDs](../README.md#stable-execution-ids) and
  write a downstream idempotency/ledger row keyed by that id. A reconciliation job can then *deduce*,
  per DLQ entry, whether the effect landed and requeue-or-delete without a human — the DLQ becomes a
  queue your code drains, not a ticket queue.
- **Tune the margins to cut false positives.** Spurious takeovers (an owner that stalled briefly but
  finished) generate DLQ entries for work that actually completed. Right-size `lease-duration` and
  `visibility-lag` to your real stall and FS-visibility profile so brief hiccups don't trip a takeover.
- **Set `dlq-retention`** so auto-resolved or stale entries don't accumulate unbounded.
- **Or sidestep it entirely.** If you cannot tolerate triage and your work is non-idempotent, run
  `single-instance` behind an external lock (leader election, DB advisory lock) so exactly one replica
  is ever active against the store — then `TRANSACTIONAL` recovers automatically on restart, with no
  cross-instance ambiguity to adjudicate.

## Rule of thumb

- Non-idempotent work you can't make repeatable, and you can tolerate operator/reconciliation triage →
  `shared-store` + `TRANSACTIONAL`, with an automated reconciliation job draining the DLQ.
- Work you can make idempotent → `shared-store` + `IDEMPOTENT`, auto-recovered, no triage.
- Non-idempotent work you need auto-recovered with no triage → `single-instance` behind an external lock.
