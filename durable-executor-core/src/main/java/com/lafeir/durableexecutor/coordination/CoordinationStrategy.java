package com.lafeir.durableexecutor.coordination;

import java.time.Duration;
import java.util.Set;

/**
 * How the durable executor decides, across one or more instances, whether a pending record is being
 * run right now or is abandoned and may be recovered — and who is allowed to run it.
 *
 * <p>The record store ({@code DurableStore}) is pure file I/O; all ownership lives here. This lets the
 * coordination back end be swapped for an independent store — a database row lock, Redis {@code SET NX},
 * etcd/ZooKeeper — without touching the record format or the recovery/aspect logic. Supply your own
 * {@code @Bean CoordinationStrategy} to override the default; see
 * {@code docs/coordination-strategy.md} for how to implement one against an external store.
 *
 * <p>For any multi-instance back end, extend {@link AbstractLeaseCoordination} rather than implementing
 * this directly: it carries the monotonic self-fence (the load-bearing safety check) and owner-checked
 * renewal so each back end only implements a handful of raw ownership primitives and cannot accidentally
 * reintroduce the stall/stomp races those guards prevent.
 *
 * <p>Lifecycle, per execution:
 * <ul>
 *   <li>fresh invocation: {@link #markRunning} then {@link #acquire}</li>
 *   <li>recovery: {@link #claimForRecovery} (in the recovery scan) then {@link #markRunning} (in the aspect)</li>
 *   <li>before committing a successful method: {@link #stillOwns} (self-fence)</li>
 *   <li>on completion (success or failure): {@link #markStopped}</li>
 * </ul>
 */
public interface CoordinationStrategy {

    /** Acquire ownership for a freshly-started execution (the {@code @Durable} open path). */
    void acquire(String executionId);

    /**
     * Atomically claim a record for recovery by this instance. Returns false if another instance owns
     * it (the caller skips it). This is the authoritative race-winner; {@link #isActive} is only a
     * best-effort pre-filter.
     */
    boolean claimForRecovery(String executionId);

    /** Mark an execution as running in this instance — excludes it from recovery here and keeps it heartbeated. */
    void markRunning(String executionId);

    /** Mark an execution as no longer running in this instance and release ownership if still held. */
    void markStopped(String executionId);

    /**
     * Release ownership without a running mark — for recovery paths that claimed a record but will not
     * invoke it (an unresolvable target, or a non-idempotent record routed to the DLQ).
     */
    void release(String executionId);

    /**
     * Whether this instance can still <em>prove</em> it owns the execution — checked before committing a
     * successful method. Must return false if ownership cannot be proven (e.g. this instance stalled
     * past its lease), so the owner abandons a record another instance may have taken over rather than
     * committing on top of it.
     */
    boolean stillOwns(String executionId);

    /**
     * Whether the record is being run right now — in this instance or by a live owner elsewhere — and so
     * is not a recovery candidate. Best-effort; {@link #claimForRecovery} is the authoritative gate.
     */
    boolean isActive(String executionId);

    /** Heartbeat tick: refresh ownership of everything still running in this instance. No-op when single-instance. */
    void heartbeat();

    /**
     * Whether this coordinates more than one instance. Drives whether the heartbeat runs and whether a
     * reclaimed {@code TRANSACTIONAL} record is routed to the DLQ (ambiguous cross-instance takeover)
     * rather than re-run.
     */
    boolean isMultiInstance();

    /** Heartbeat period; only meaningful when {@link #isMultiInstance()} is true. */
    Duration heartbeatInterval();

    /**
     * Remove coordination state left behind by records that no longer exist, given the ids of records
     * that currently do. A no-op for back ends whose ownership entries expire on their own (TTLs).
     */
    default void sweep(Set<String> liveRecordIds) {
    }
}
