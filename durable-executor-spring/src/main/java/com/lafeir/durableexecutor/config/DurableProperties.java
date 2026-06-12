package com.lafeir.durableexecutor.config;

import com.lafeir.durableexecutor.store.CoordinationMode;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for durable execution.
 *
 * Example application.yml:
 *
 *   durable:
 *     store-path: /var/data/durable-executions
 */
@ConfigurationProperties(prefix = "durable")
public class DurableProperties {

    /**
     * Directory used to persist in-flight executions.
     * Relative paths are resolved from the JVM working directory.
     */
    private String storePath = "./durable-executions";

    /**
     * Directory where executions are written after all retry attempts fail.
     */
    private String deadLetterPath = "./durable-dlq";

    /**
     * Number of threads used for retry execution.
     */
    private int retryThreads = 2;

    /**
     * Total recovery attempts for a record before it is moved to the dead letter queue.
     */
    private int maxAttempts = 5;

    /**
     * Base delay before the first retry after a failed attempt. Subsequent delays grow
     * exponentially by {@link #retryBackoffMultiplier}, capped at {@link #retryBackoffMax}.
     */
    private java.time.Duration retryBackoff = java.time.Duration.ofSeconds(30);

    /**
     * Multiplier applied to the backoff delay after each failed attempt.
     */
    private double retryBackoffMultiplier = 2.0;

    /**
     * Upper bound on the backoff delay between attempts.
     */
    private java.time.Duration retryBackoffMax = java.time.Duration.ofMinutes(5);

    /**
     * Minimum age of a -deleted.msgpack file before it is considered stuck and routed to
     * the dead letter queue. Provides a safety window against a live finalizeDelete() call
     * being mistaken for a crash-interrupted cleanup.
     */
    private java.time.Duration stuckGracePeriod = java.time.Duration.ofSeconds(30);

    /**
     * Coordination mode for the recovery scan. SINGLE_INSTANCE (default) assumes one active
     * writer per store and tracks liveness in-memory only. SHARED_STORE adds a best-effort
     * file lease so multiple instances sharing one store directory do not re-run each other's
     * live executions.
     */
    private CoordinationMode coordination = CoordinationMode.SINGLE_INSTANCE;

    /**
     * SHARED_STORE only: how long a record's lease stays valid after its last heartbeat before
     * another instance may reclaim it. Renewed at roughly one third of this interval.
     */
    private java.time.Duration leaseDuration = java.time.Duration.ofMinutes(1);

    public String getStorePath() { return storePath; }
    public void setStorePath(String storePath) { this.storePath = storePath; }

    public String getDeadLetterPath() { return deadLetterPath; }
    public void setDeadLetterPath(String deadLetterPath) { this.deadLetterPath = deadLetterPath; }

    public int getRetryThreads() { return retryThreads; }
    public void setRetryThreads(int retryThreads) { this.retryThreads = retryThreads; }

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public java.time.Duration getRetryBackoff() { return retryBackoff; }
    public void setRetryBackoff(java.time.Duration retryBackoff) { this.retryBackoff = retryBackoff; }

    public double getRetryBackoffMultiplier() { return retryBackoffMultiplier; }
    public void setRetryBackoffMultiplier(double retryBackoffMultiplier) { this.retryBackoffMultiplier = retryBackoffMultiplier; }

    public java.time.Duration getRetryBackoffMax() { return retryBackoffMax; }
    public void setRetryBackoffMax(java.time.Duration retryBackoffMax) { this.retryBackoffMax = retryBackoffMax; }

    public java.time.Duration getStuckGracePeriod() { return stuckGracePeriod; }
    public void setStuckGracePeriod(java.time.Duration stuckGracePeriod) { this.stuckGracePeriod = stuckGracePeriod; }

    public CoordinationMode getCoordination() { return coordination; }
    public void setCoordination(CoordinationMode coordination) { this.coordination = coordination; }

    public java.time.Duration getLeaseDuration() { return leaseDuration; }
    public void setLeaseDuration(java.time.Duration leaseDuration) { this.leaseDuration = leaseDuration; }

    private DlqEndpoint dlqEndpoint = new DlqEndpoint();

    public DlqEndpoint getDlqEndpoint() { return dlqEndpoint; }
    public void setDlqEndpoint(DlqEndpoint dlqEndpoint) { this.dlqEndpoint = dlqEndpoint; }

    public static class DlqEndpoint {

        /** Whether to expose the dead letter queue HTTP endpoint. Disabled by default. */
        private boolean enabled = false;

        /** Base path for the dead letter queue endpoint. */
        private String path = "/durable/dlq";

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }

        public String getPath() { return path; }
        public void setPath(String path) { this.path = path; }
    }
}
