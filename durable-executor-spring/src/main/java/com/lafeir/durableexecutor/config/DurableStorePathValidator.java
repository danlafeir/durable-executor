package com.lafeir.durableexecutor.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.nio.file.Path;

/**
 * Warns at startup when a store path looks ephemeral. The store is only as durable as the
 * directory behind it — a relative path resolves under the JVM working directory (the ephemeral
 * writable layer in a container), and {@code /tmp} is wiped on restart. Either silently defeats
 * "durable" execution. This only warns; a relative path can legitimately be a mounted volume, so
 * failing fast would be wrong. (A genuinely unwritable directory does fail fast, in DurableStore.)
 */
public class DurableStorePathValidator implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(DurableStorePathValidator.class);

    private final DurableProperties properties;

    public DurableStorePathValidator(DurableProperties properties) {
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() {
        warnIfEphemeral("durable.store-path", properties.getStorePath());
        warnIfEphemeral("durable.dead-letter-path", properties.getDeadLetterPath());
    }

    private void warnIfEphemeral(String property, String value) {
        String reason = ephemeralReason(Path.of(value));
        if (reason != null) {
            log.warn("{}='{}' {}; durable records there are lost on restart. "
                    + "Set an absolute path backed by a persistent volume.", property, value, reason);
        }
    }

    private String ephemeralReason(Path path) {
        if (!path.isAbsolute()) {
            return "is a relative path resolving to '" + path.toAbsolutePath()
                    + "' under the JVM working directory (the ephemeral writable layer in a container)";
        }
        String normalized = path.normalize().toString();
        if (normalized.equals("/tmp") || normalized.startsWith("/tmp/")
                || normalized.equals("/var/tmp") || normalized.startsWith("/var/tmp/")) {
            return "is under a temporary directory";
        }
        return null;
    }
}
