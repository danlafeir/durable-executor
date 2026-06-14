package com.lafeir.durableexecutor.coordination;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Coordination for a single active writer per store directory. Liveness is tracked entirely in memory:
 * a record running in this process is excluded from recovery here, and on restart any pending record
 * unambiguously means the previous run crashed, so it is recovered (re-run). There is no external
 * ownership, so {@link #claimForRecovery} and {@link #stillOwns} are always true and the heartbeat is a
 * no-op. A second instance writing to the same store concurrently is unsupported in this mode.
 */
public class SingleInstanceCoordination implements CoordinationStrategy {

    private final Set<String> running = ConcurrentHashMap.newKeySet();

    @Override
    public void acquire(String executionId) {
    }

    @Override
    public boolean claimForRecovery(String executionId) {
        return true;
    }

    @Override
    public void markRunning(String executionId) {
        running.add(executionId);
    }

    @Override
    public void markStopped(String executionId) {
        running.remove(executionId);
    }

    @Override
    public void release(String executionId) {
    }

    @Override
    public boolean stillOwns(String executionId) {
        return true;
    }

    @Override
    public boolean isActive(String executionId) {
        return running.contains(executionId);
    }

    @Override
    public void heartbeat() {
    }

    @Override
    public boolean isMultiInstance() {
        return false;
    }

    @Override
    public Duration heartbeatInterval() {
        return Duration.ZERO;
    }
}
