package com.lafeir.durableexecutor.store;

import com.lafeir.durableexecutor.model.DurableExecution;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * File-per-execution store for in-flight durable executions — pure record I/O. Whether a pending record
 * is being run right now or is abandoned (and who may run it) is decided by a
 * {@link com.lafeir.durableexecutor.coordination.CoordinationStrategy}, not here.
 *
 * Lifecycle:
 *   open:   {id}.msgpack         — method is in-flight
 *   commit: {id}-deleted.msgpack — method completed; rename is atomic and crash-safe
 *   cleanup: file deleted         — housekeeping after commit
 *
 * On recovery, any {id}-deleted.msgpack file older than the configured stuckGrace period is considered
 * stuck (JVM crashed between commit and cleanup) and is routed to the dead letter queue rather than retried.
 */
public class DurableStore {

    private static final Logger log = LoggerFactory.getLogger(DurableStore.class);

    static final String PENDING_SUFFIX = ".msgpack";
    static final String DELETED_SUFFIX = "-deleted.msgpack";

    private final Path storeDir;
    private final ObjectMapper objectMapper;
    private final Duration stuckGrace;

    public DurableStore(Path storeDir, ObjectMapper objectMapper, Duration stuckGrace) {
        this.storeDir = storeDir;
        this.objectMapper = objectMapper;
        this.stuckGrace = stuckGrace;
        try {
            Files.createDirectories(storeDir);
            // Fail fast on a read-only store: createDirectories is a no-op on an existing directory,
            // so probe an actual write — otherwise durability silently fails at the first save().
            Path probe = Files.createTempFile(storeDir, ".durable-writecheck", ".tmp");
            Files.delete(probe);
        } catch (IOException e) {
            throw new DurableStoreException(
                    "Durable store directory " + storeDir + " is not writable; durable records cannot be persisted", e);
        }
    }

    public void save(DurableExecution execution) {
        String id = execution.getExecutionId();
        if (id.endsWith("-deleted")) {
            throw new DurableStoreException(
                    "executionId must not end with \"-deleted\" (conflicts with the commit-marker suffix): " + id, null);
        }
        try {
            Path tmp = storeDir.resolve(id + ".tmp");
            Path file = storeDir.resolve(id + PENDING_SUFFIX);
            objectMapper.writeValue(tmp.toFile(), execution);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new DurableStoreException("Failed to save execution " + id, e);
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
     * Deletes the {id}-deleted.msgpack commit marker. Throws on failure; the caller leaves the
     * marker in place so the stuck-grace scan routes it to the DLQ rather than re-running it.
     */
    public void finalizeDelete(String executionId) {
        try {
            Files.deleteIfExists(storeDir.resolve(executionId + DELETED_SUFFIX));
        } catch (IOException e) {
            throw new DurableStoreException("Failed to finalize delete of execution " + executionId, e);
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

    /** Sorted ids of all stored records (excluding commit markers), without deserializing them — for paging. */
    public List<String> listIds() {
        if (!Files.exists(storeDir)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(storeDir)) {
            return files.map(p -> p.getFileName().toString())
                    .filter(name -> name.endsWith(PENDING_SUFFIX) && !name.endsWith(DELETED_SUFFIX))
                    .map(name -> name.substring(0, name.length() - PENDING_SUFFIX.length()))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            log.error("Failed to list durable store directory {}", storeDir, e);
            return List.of();
        }
    }

    /** Loads a single record by id, or null if it is missing or unreadable. */
    public DurableExecution loadById(String executionId) {
        Path file = storeDir.resolve(executionId + PENDING_SUFFIX);
        if (!Files.exists(file)) {
            return null;
        }
        try {
            return objectMapper.readValue(file.toFile(), DurableExecution.class);
        } catch (IOException e) {
            log.warn("Failed to read execution {} ({})", executionId, e.getMessage());
            return null;
        }
    }

    /** Deletes records whose file was last modified before {@code now - age}; returns the count removed. */
    public int purgeOlderThan(Duration age) {
        Instant cutoff = Instant.now().minus(age);
        int purged = 0;
        for (String id : listIds()) {
            Path file = storeDir.resolve(id + PENDING_SUFFIX);
            try {
                if (Files.getLastModifiedTime(file).toInstant().isBefore(cutoff)) {
                    Files.deleteIfExists(file);
                    purged++;
                }
            } catch (IOException e) {
                log.warn("Failed to purge entry {} ({})", id, e.getMessage());
            }
        }
        return purged;
    }

    /**
     * Single-pass directory scan returning every pending execution and every stuck-deleted execution
     * (a -deleted.msgpack file older than the configured stuckGrace period). The recovery scan decides
     * which pending records are abandoned via the CoordinationStrategy — this returns them all.
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
                    // {id}.lease (coordination) and {id}.tmp (transient) files are ignored here.
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
