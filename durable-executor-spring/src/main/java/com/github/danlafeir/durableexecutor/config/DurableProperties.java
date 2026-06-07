package com.github.danlafeir.durableexecutor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for durable execution.
 *
 * Example application.yml:
 *
 *   durable:
 *     store-path: /var/data/durable-executions.json
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
     * Number of threads used for scheduled retry execution.
     */
    private int retryThreads = 2;

    public String getStorePath() { return storePath; }
    public void setStorePath(String storePath) { this.storePath = storePath; }

    public String getDeadLetterPath() { return deadLetterPath; }
    public void setDeadLetterPath(String deadLetterPath) { this.deadLetterPath = deadLetterPath; }

    public int getRetryThreads() { return retryThreads; }
    public void setRetryThreads(int retryThreads) { this.retryThreads = retryThreads; }

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
