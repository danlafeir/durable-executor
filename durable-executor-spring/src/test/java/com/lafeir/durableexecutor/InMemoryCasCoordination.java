package com.lafeir.durableexecutor;

import com.lafeir.durableexecutor.coordination.AbstractLeaseCoordination;

import java.time.Duration;
import java.util.Map;

/**
 * A {@link com.lafeir.durableexecutor.coordination.CoordinationStrategy} backed by an external TTL + CAS
 * store, modelled in memory — the shape of a Redis ({@code SET NX PX}) or a row-with-expiry database
 * backend. The {@code store} map stands in for the external store and is <em>shared</em> across instances
 * (each with its own owner id), so several instances coordinate through it exactly as they would through
 * Redis. It exists to prove the SPI fits a non-filesystem back end and to drive deterministic recovery
 * tests without a real Redis; it is also the worked example referenced by the docs.
 *
 * <p>Only the five raw primitives are implemented here — the monotonic self-fence and owner-checked
 * renewal come from {@link AbstractLeaseCoordination}.
 */
public class InMemoryCasCoordination extends AbstractLeaseCoordination {

    /** An ownership entry with a TTL — like a Redis key with PX, or a DB row with an expires_at column. */
    public record Entry(String owner, long expiryNanos) {
    }

    private final Map<String, Entry> store;
    private final Duration lease;

    public InMemoryCasCoordination(String ownerId, Duration lease, Map<String, Entry> sharedStore) {
        super(ownerId, lease);
        this.store = sharedStore;
        this.lease = lease;
    }

    private boolean expired(Entry e) {
        return System.nanoTime() > e.expiryNanos();
    }

    private Entry fresh() {
        return new Entry(ownerId, System.nanoTime() + lease.toNanos());
    }

    @Override
    protected void writeOwner(String executionId) {
        store.put(executionId, fresh());
    }

    @Override
    protected String readOwner(String executionId) {
        Entry e = store.get(executionId);
        return (e == null || expired(e)) ? null : e.owner();
    }

    @Override
    protected void removeOwner(String executionId) {
        // Atomic owner-checked delete (Redis: DEL guarded by a Lua compare, or WATCH/MULTI).
        store.computeIfPresent(executionId, (k, e) -> ownerId.equals(e.owner()) ? null : e);
    }

    @Override
    protected boolean tryClaim(String executionId) {
        // Atomic compare-and-set: take it iff absent or expired (Redis: SET NX PX).
        Entry result = store.compute(executionId, (k, e) -> (e == null || expired(e)) ? fresh() : e);
        return ownerId.equals(result.owner());
    }

    @Override
    protected boolean isHeldByLiveOwner(String executionId) {
        Entry e = store.get(executionId);
        return e != null && !expired(e);
    }
}
