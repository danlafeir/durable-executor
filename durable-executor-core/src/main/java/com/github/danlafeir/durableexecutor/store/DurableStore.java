package com.github.danlafeir.durableexecutor.store;

import com.github.danlafeir.durableexecutor.model.DurableExecution;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * File-per-execution store for in-flight durable executions.
 *
 * Lifecycle:
 *   open:   {id}.msgpack         — method is in-flight
 *   commit: {id}-deleted.msgpack — method completed; rename is atomic and crash-safe
 *   cleanup: file deleted         — housekeeping after commit
 *
 * On recovery, any {id}-deleted.msgpack file that is older than the configured
 * stuckGrace period is considered stuck (JVM crashed between commit and cleanup)
 * and is routed to the dead letter queue rather than retried.
 */
public class DurableStore {

    private static final Logger log = LoggerFactory.getLogger(DurableStore.class);

    static final String PENDING_SUFFIX = ".msgpack";
    static final String DELETED_SUFFIX = "-deleted.msgpack";

    private final Path storeDir;
    private final ObjectMapper objectMapper;
    private final Duration stuckGrace;
    private volatile boolean dirInitialized = false;

    public DurableStore(Path storeDir, ObjectMapper objectMapper, Duration stuckGrace) {
        this.storeDir = storeDir;
        this.objectMapper = objectMapper;
        this.stuckGrace = stuckGrace;
    }

    public void save(DurableExecution execution) {
        try {
            ensureDirectory();
            Path tmp = storeDir.resolve(execution.getExecutionId() + ".tmp");
            Path file = storeDir.resolve(execution.getExecutionId() + PENDING_SUFFIX);
            objectMapper.writeValue(tmp.toFile(), execution);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new DurableStoreException("Failed to save execution " + execution.getExecutionId(), e);
        }
    }

    /**
     * Atomically renames {id}.msgpack to {id}-deleted.msgpack.
     * This is the crash-safe commit point for a successful execution.
     */
    public void markDeleted(String executionId) {
        Path source = storeDir.resolve(executionId + PENDING_SUFFIX);
        Path target = storeDir.resolve(executionId + DELETED_SUFFIX);
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new DurableStoreException("Failed to mark execution " + executionId + " as deleted", e);
        }
    }

    /**
     * Deletes the {id}-deleted.msgpack file. Throws on failure so callers can revert via unmarkDeleted().
     */
    public void finalizeDelete(String executionId) {
        try {
            Files.deleteIfExists(storeDir.resolve(executionId + DELETED_SUFFIX));
        } catch (IOException e) {
            throw new DurableStoreException("Failed to finalize delete of execution " + executionId, e);
        }
    }

    /**
     * Reverts a markDeleted() by atomically renaming {id}-deleted.msgpack back to {id}.msgpack.
     */
    public void unmarkDeleted(String executionId) {
        Path source = storeDir.resolve(executionId + DELETED_SUFFIX);
        Path target = storeDir.resolve(executionId + PENDING_SUFFIX);
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            log.error("Failed to revert deletion mark for execution {} — record may be in inconsistent state", executionId, e);
        }
    }

    /** Deletes the pending {id}.msgpack file directly. Used when moving a failed execution to the DLQ. */
    public void delete(String executionId) {
        try {
            Files.deleteIfExists(storeDir.resolve(executionId + PENDING_SUFFIX));
        } catch (IOException e) {
            log.error("Failed to delete execution {} from store", executionId, e);
        }
    }

    /** Returns all in-flight executions ({id}.msgpack files, excluding -deleted.msgpack). */
    public Map<String, DurableExecution> loadAll() {
        return load(PENDING_SUFFIX);
    }

    /** Returns all -deleted.msgpack files regardless of age. Used for test cleanup. */
    public Map<String, DurableExecution> loadAllDeleted() {
        return load(DELETED_SUFFIX);
    }

    /**
     * Single-pass directory scan. Returns pending executions and stuck-deleted executions
     * (those whose -deleted.msgpack file is older than the configured stuckGrace period).
     * Using this instead of separate loadAll() + loadAllDeleted() halves directory I/O per cycle
     * and ensures the age check is applied consistently.
     */
    public StoreScan scan() {
        if (!Files.exists(storeDir)) {
            return new StoreScan(new LinkedHashMap<>(), new LinkedHashMap<>());
        }
        Instant stuckCutoff = Instant.now().minus(stuckGrace);
        Map<String, DurableExecution> pending = new LinkedHashMap<>();
        Map<String, DurableExecution> stuckDeleted = new LinkedHashMap<>();
        try (Stream<Path> files = Files.list(storeDir)) {
            files.forEach(file -> {
                String name = file.getFileName().toString();
                try {
                    if (name.endsWith(DELETED_SUFFIX)) {
                        Instant modified = Files.getLastModifiedTime(file).toInstant();
                        if (modified.isBefore(stuckCutoff)) {
                            DurableExecution execution = objectMapper.readValue(file.toFile(), DurableExecution.class);
                            stuckDeleted.put(execution.getExecutionId(), execution);
                        }
                    } else if (name.endsWith(PENDING_SUFFIX)) {
                        DurableExecution execution = objectMapper.readValue(file.toFile(), DurableExecution.class);
                        pending.put(execution.getExecutionId(), execution);
                    }
                } catch (Exception e) {
                    log.warn("Skipping unreadable execution file {} ({})", name, e.getMessage());
                }
            });
        } catch (IOException e) {
            log.error("Failed to list durable store directory {}", storeDir, e);
        }
        return new StoreScan(pending, stuckDeleted);
    }

    private Map<String, DurableExecution> load(String suffix) {
        if (!Files.exists(storeDir)) {
            return new LinkedHashMap<>();
        }
        Map<String, DurableExecution> result = new LinkedHashMap<>();
        try (Stream<Path> files = Files.list(storeDir)) {
            files.filter(p -> {
                    String name = p.getFileName().toString();
                    // Exclude -deleted.msgpack when loading pending .msgpack files —
                    // DELETED_SUFFIX ends with PENDING_SUFFIX so a naive endsWith check
                    // would return both.
                    return name.endsWith(suffix)
                        && (suffix.equals(DELETED_SUFFIX) || !name.endsWith(DELETED_SUFFIX));
                })
                .forEach(file -> {
                    try {
                        DurableExecution execution = objectMapper.readValue(file.toFile(), DurableExecution.class);
                        result.put(execution.getExecutionId(), execution);
                    } catch (Exception e) {
                        log.warn("Skipping unreadable execution file {} ({})", file.getFileName(), e.getMessage());
                    }
                });
        } catch (IOException e) {
            log.error("Failed to list durable store directory {}", storeDir, e);
        }
        return result;
    }

    private void ensureDirectory() throws IOException {
        if (!dirInitialized) {
            Files.createDirectories(storeDir);
            dirInitialized = true;
        }
    }

    public record StoreScan(
        Map<String, DurableExecution> pending,
        Map<String, DurableExecution> stuckDeleted
    ) {}

    public static class DurableStoreException extends RuntimeException {
        public DurableStoreException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
