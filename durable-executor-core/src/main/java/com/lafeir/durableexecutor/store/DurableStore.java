package com.lafeir.durableexecutor.store;

import com.lafeir.durableexecutor.model.DurableExecution;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    static final String LEASE_SUFFIX = ".lease";

    private final Path storeDir;
    private final ObjectMapper objectMapper;
    private final Duration stuckGrace;
    private final CoordinationMode coordination;
    private final String ownerId;
    private final Duration leaseDuration;

    /**
     * Execution IDs currently running in this process. A pending {id}.msgpack file means
     * one of two things — a crashed execution that needs recovery, or one running right now —
     * and the file alone can't tell them apart. The interceptor registers an ID here for the
     * duration of a live call so scan() can exclude it from recovery candidates.
     */
    private final Set<String> live = ConcurrentHashMap.newKeySet();

    public DurableStore(Path storeDir, ObjectMapper objectMapper, Duration stuckGrace) {
        this(storeDir, objectMapper, stuckGrace, CoordinationMode.SINGLE_INSTANCE, "", Duration.ZERO);
    }

    public DurableStore(Path storeDir, ObjectMapper objectMapper, Duration stuckGrace,
                        CoordinationMode coordination, String ownerId, Duration leaseDuration) {
        this.storeDir = storeDir;
        this.objectMapper = objectMapper;
        this.stuckGrace = stuckGrace;
        this.coordination = coordination;
        this.ownerId = ownerId;
        this.leaseDuration = leaseDuration;
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

    public boolean isShared() {
        return coordination == CoordinationMode.SHARED_STORE;
    }

    public Duration getLeaseDuration() {
        return leaseDuration;
    }

    /** Marks an execution as actively running in this process so scan() will not offer it for recovery. */
    public void markLive(String executionId) {
        live.add(executionId);
    }

    /** Clears the live mark once the execution has completed, succeeded or failed. */
    public void markNotLive(String executionId) {
        live.remove(executionId);
    }

    /**
     * In SHARED_STORE mode, stamps {id}.lease with this instance's owner id so other instances
     * sharing the store skip the record while it is live here. No-op in SINGLE_INSTANCE mode.
     */
    public void acquireLease(String executionId) {
        if (isShared()) {
            writeLease(executionId);
        }
    }

    /** Releases the lease written by {@link #acquireLease}. No-op in SINGLE_INSTANCE mode. */
    public void releaseLease(String executionId) {
        if (!isShared()) {
            return;
        }
        try {
            Files.deleteIfExists(storeDir.resolve(executionId + LEASE_SUFFIX));
        } catch (IOException e) {
            log.warn("Failed to release lease for {}; it will expire after the lease duration", executionId, e);
        }
    }

    /**
     * Claims a record for recovery by this instance. Writes the lease and re-reads it: if a
     * concurrent instance won the race the read-back owner differs and this returns false, so
     * the caller skips the record. Always true in SINGLE_INSTANCE mode.
     */
    public boolean claim(String executionId) {
        if (!isShared()) {
            return true;
        }
        writeLease(executionId);
        return ownerId.equals(readLeaseOwner(executionId));
    }

    /** Renews the lease for every execution currently live in this process. No-op in SINGLE_INSTANCE mode. */
    public void renewLeases() {
        if (!isShared()) {
            return;
        }
        for (String id : live) {
            writeLease(id);
        }
    }

    private void writeLease(String executionId) {
        try {
            Path tmp = storeDir.resolve(executionId + LEASE_SUFFIX + ".tmp");
            Path file = storeDir.resolve(executionId + LEASE_SUFFIX);
            Files.write(tmp, ownerId.getBytes(StandardCharsets.UTF_8));
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            Files.setLastModifiedTime(file, FileTime.from(Instant.now()));
        } catch (IOException e) {
            log.warn("Failed to write lease for {}", executionId, e);
        }
    }

    private String readLeaseOwner(String executionId) {
        Path file = storeDir.resolve(executionId + LEASE_SUFFIX);
        try {
            return Files.exists(file) ? new String(Files.readAllBytes(file), StandardCharsets.UTF_8) : null;
        } catch (IOException e) {
            return null;
        }
    }

    private boolean isLeaseHeld(String executionId) {
        Path file = storeDir.resolve(executionId + LEASE_SUFFIX);
        try {
            if (!Files.exists(file)) {
                return false;
            }
            Instant expiry = Files.getLastModifiedTime(file).toInstant().plus(leaseDuration);
            return expiry.isAfter(Instant.now());
        } catch (IOException e) {
            return false;
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
                        String id = name.substring(0, name.length() - PENDING_SUFFIX.length());
                        if (live.contains(id)) {
                            return; // running in this process right now — not a crash to recover
                        }
                        if (isShared() && isLeaseHeld(id)) {
                            return; // running on another instance whose lease is still valid
                        }
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
