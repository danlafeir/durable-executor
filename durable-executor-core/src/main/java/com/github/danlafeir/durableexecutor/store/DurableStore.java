package com.github.danlafeir.durableexecutor.store;

import com.github.danlafeir.durableexecutor.model.DurableExecution;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * File-per-execution store for in-flight durable executions.
 *
 * Lifecycle:
 *   open:   {id}.msgpack        — method is in-flight
 *   commit: {id}-deleted.msgpack — method completed; rename is atomic and crash-safe
 *   cleanup: file deleted        — housekeeping after commit
 *
 * On recovery, any {id}-deleted.msgpack file indicates the method completed but
 * cleanup did not finish (JVM crashed between commit and cleanup). These are
 * "stuck" executions and should be routed to the dead letter queue rather than
 * retried.
 */
public class DurableStore {

    private static final Logger log = LoggerFactory.getLogger(DurableStore.class);

    private static final String PENDING_SUFFIX = ".msgpack";
    private static final String DELETED_SUFFIX = "-deleted.msgpack";

    private final Path storeDir;
    private final ObjectMapper objectMapper;

    public DurableStore(Path storeDir, ObjectMapper objectMapper) {
        this.storeDir = storeDir;
        this.objectMapper = objectMapper;
    }

    public void save(DurableExecution execution) {
        try {
            Files.createDirectories(storeDir);
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
     * If the JVM dies after this rename, the execution will not be retried.
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
     * Deletes the {id}-deleted.msgpack file. Call after markDeleted() to finish cleanup.
     */
    public void finalizeDelete(String executionId) {
        try {
            Files.deleteIfExists(storeDir.resolve(executionId + DELETED_SUFFIX));
        } catch (IOException e) {
            log.error("Failed to finalize delete of execution {}", executionId, e);
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

    /** Returns all in-flight executions ({id}.msgpack files). */
    public Map<String, DurableExecution> loadAll() {
        return load(PENDING_SUFFIX);
    }

    /** Returns all stuck-deleted executions ({id}-deleted.msgpack files). */
    public Map<String, DurableExecution> loadAllDeleted() {
        return load(DELETED_SUFFIX);
    }

    private Map<String, DurableExecution> load(String suffix) {
        if (!Files.exists(storeDir)) {
            return new LinkedHashMap<>();
        }
        Map<String, DurableExecution> result = new LinkedHashMap<>();
        try (Stream<Path> files = Files.list(storeDir)) {
            files.filter(p -> p.getFileName().toString().endsWith(suffix))
                 .forEach(file -> {
                     try {
                         DurableExecution execution = objectMapper.readValue(file.toFile(), DurableExecution.class);
                         result.put(execution.getExecutionId(), execution);
                     } catch (IOException e) {
                         log.warn("Skipping unreadable execution file {} ({})", file.getFileName(), e.getMessage());
                     }
                 });
        } catch (IOException e) {
            log.error("Failed to list durable store directory {}", storeDir, e);
        }
        return result;
    }

    public static class DurableStoreException extends RuntimeException {
        public DurableStoreException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
