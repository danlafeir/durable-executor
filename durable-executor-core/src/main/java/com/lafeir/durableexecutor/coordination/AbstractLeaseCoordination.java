package com.lafeir.durableexecutor.coordination;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base for any multi-instance {@link CoordinationStrategy} backed by a per-execution ownership entry
 * (a lease). It carries the parts that are identical across every back end and are correctness-critical,
 * so a back end implements only the raw ownership primitives and cannot reintroduce the stall/stomp
 * races those parts prevent:
 *
 * <ul>
 *   <li><strong>Monotonic self-fence.</strong> {@link #stillOwns} is true only if less than the lease
 *       duration has elapsed, <em>by this instance's own monotonic clock</em>, since we last successfully
 *       took or renewed ownership — <em>and</em> the back end still reports us as owner. The monotonic
 *       half is the load-bearing guard: a stalled instance cannot trust a wall-clock comparison or a
 *       possibly-stale read to notice it lost ownership. It can only ever fence earlier, never falsely
 *       report ownership, so it is a safe backstop on top of any back end (including a strongly
 *       consistent one).</li>
 *   <li><strong>Owner-checked renewal.</strong> The heartbeat re-asserts ownership only for entries the
 *       back end still reports as ours, so an instance that stalled and was taken over cannot stomp its
 *       id back over the new owner.</li>
 *   <li>The in-process running set (which ids to heartbeat and to exclude from local recovery) and the
 *       monotonic last-renew timestamps.</li>
 * </ul>
 *
 * Back ends implement: {@link #writeOwner}, {@link #readOwner}, {@link #removeOwner}, {@link #tryClaim},
 * and {@link #isHeldByLiveOwner}; optionally {@link #sweepBackend}.
 */
public abstract class AbstractLeaseCoordination implements CoordinationStrategy {

    protected final String ownerId;
    private final Duration leaseDuration;
    private final Duration heartbeatInterval;

    private final Set<String> running = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> lastRenewNanos = new ConcurrentHashMap<>();

    protected AbstractLeaseCoordination(String ownerId, Duration leaseDuration) {
        this.ownerId = ownerId;
        this.leaseDuration = leaseDuration;
        long thirdMillis = Math.max(1, leaseDuration.toMillis() / 3);
        this.heartbeatInterval = Duration.ofMillis(thirdMillis);
    }

    // ── back-end primitives ─────────────────────────────────────────────────────────────────────

    /** Unconditionally set ownership of {@code executionId} to this instance and refresh its expiry. */
    protected abstract void writeOwner(String executionId);

    /** The current owner id of {@code executionId}, or null if unowned/expired/unreadable. */
    protected abstract String readOwner(String executionId);

    /** Remove the ownership entry for {@code executionId} only if this instance still owns it. */
    protected abstract void removeOwner(String executionId);

    /** Atomically take ownership of {@code executionId}; return true iff this instance now owns it. */
    protected abstract boolean tryClaim(String executionId);

    /** Whether a non-expired ownership entry exists for {@code executionId} (any owner). */
    protected abstract boolean isHeldByLiveOwner(String executionId);

    /** Optional cleanup of ownership entries for records no longer present. */
    protected void sweepBackend(Set<String> liveRecordIds) {
    }

    // ── strategy ────────────────────────────────────────────────────────────────────────────────

    @Override
    public final boolean isMultiInstance() {
        return true;
    }

    @Override
    public final Duration heartbeatInterval() {
        return heartbeatInterval;
    }

    @Override
    public void acquire(String executionId) {
        writeOwner(executionId);
        lastRenewNanos.put(executionId, System.nanoTime());
    }

    @Override
    public boolean claimForRecovery(String executionId) {
        boolean won = tryClaim(executionId);
        if (won) {
            lastRenewNanos.put(executionId, System.nanoTime());
        } else {
            lastRenewNanos.remove(executionId);
        }
        return won;
    }

    @Override
    public void markRunning(String executionId) {
        running.add(executionId);
    }

    @Override
    public void markStopped(String executionId) {
        running.remove(executionId);
        release(executionId);
    }

    @Override
    public void release(String executionId) {
        lastRenewNanos.remove(executionId);
        removeOwner(executionId);
    }

    @Override
    public boolean stillOwns(String executionId) {
        Long renewedAtNanos = lastRenewNanos.get(executionId);
        if (renewedAtNanos == null) {
            return false;
        }
        if (System.nanoTime() - renewedAtNanos > leaseDuration.toNanos()) {
            return false; // can no longer prove freshness — self-fence
        }
        return ownerId.equals(readOwner(executionId));
    }

    @Override
    public boolean isActive(String executionId) {
        return running.contains(executionId) || isHeldByLiveOwner(executionId);
    }

    @Override
    public void heartbeat() {
        for (String id : running) {
            // Owner-checked: only renew leases we still hold, so a stalled-then-reclaimed instance does
            // not stomp its id back over the new owner.
            if (ownerId.equals(readOwner(id))) {
                writeOwner(id);
                lastRenewNanos.put(id, System.nanoTime());
            }
        }
    }

    @Override
    public final void sweep(Set<String> liveRecordIds) {
        sweepBackend(liveRecordIds);
    }
}
