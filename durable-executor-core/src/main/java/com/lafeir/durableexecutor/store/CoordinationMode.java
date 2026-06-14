package com.lafeir.durableexecutor.store;

/**
 * How the recovery scan decides whether a pending record is abandoned (needs recovery)
 * or owned by a live execution.
 */
public enum CoordinationMode {

    /**
     * One active writer per store directory (e.g. a ReadWriteOnce volume or a dedicated disk).
     * Liveness is tracked in-memory only — deterministic and zero on-disk overhead. A second
     * instance writing to the same store concurrently is unsupported in this mode.
     */
    SINGLE_INSTANCE,

    /**
     * Multiple instances sharing one store directory (e.g. a ReadWriteMany / NFS volume).
     * Each in-flight record is stamped with an owner + heartbeat-renewed lease so another
     * instance does not re-run work that is still live elsewhere.
     *
     * <p><strong>{@code TRANSACTIONAL} is at-most-once across instances; {@code IDEMPOTENT} is
     * at-least-once.</strong> File-based coordination has inherent TOCTOU windows and stale-read
     * behaviour (notably on NFS), but for a {@code TRANSACTIONAL} method they cannot cause a double
     * execution: a reclaimed record is routed to the DLQ, never re-run, so a non-idempotent method is
     * never automatically executed twice across instances — independent of filesystem timing.
     * {@code IDEMPOTENT} records may be re-run across instances (safe by definition). For strict
     * single-writer isolation, run {@link #SINGLE_INSTANCE} behind an external lock.
     *
     * <p>See {@code docs/coordination.md} for the design — the supported-storage contract, the
     * self-fencing lease (owner-checked renewal, waited takeover, monotonic self-fence), and
     * precisely what at-most-once does and does not cover.
     */
    SHARED_STORE
}
