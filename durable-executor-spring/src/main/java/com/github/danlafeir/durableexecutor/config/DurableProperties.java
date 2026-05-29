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
     * Path to the JSON file used to persist in-flight executions.
     * Relative paths are resolved from the JVM working directory.
     */
    private String storePath = "./durable-executions.json";

    public String getStorePath() { return storePath; }
    public void setStorePath(String storePath) { this.storePath = storePath; }
}
