package com.lafeir.durableexecutor.coordination;

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
import java.util.Set;
import java.util.stream.Stream;

/**
 * Multi-instance coordination over a shared filesystem. Each in-flight record is stamped with an
 * {@code {id}.lease} sidecar file in the store directory containing the owner id; the file's mtime is the
 * heartbeat timestamp. Another instance reclaims a record only once its lease has been expired for the
 * full takeover margin {@code lease-duration + visibility-lag (Δ) + clock-skew}, which gives a still-live
 * owner time to renew or self-fence before anyone takes over.
 *
 * <p>The monotonic self-fence and owner-checked renewal live in {@link AbstractLeaseCoordination}; this
 * class only implements the raw lease-file primitives.
 *
 * <p>Sound only on storage that provides atomic rename, a bounded visibility lag Δ, read-after-write on
 * the lease file, and bounded clock skew — see {@code docs/coordination.md}.
 */
public class FileLeaseCoordination extends AbstractLeaseCoordination {

    private static final Logger log = LoggerFactory.getLogger(FileLeaseCoordination.class);
    private static final String LEASE_SUFFIX = ".lease";

    private final Path storeDir;
    private final Duration leaseDuration;
    private final Duration visibilityLag;
    private final Duration clockSkew;

    public FileLeaseCoordination(Path storeDir, String ownerId, Duration leaseDuration,
                                 Duration visibilityLag, Duration clockSkew) {
        super(ownerId, leaseDuration);
        this.storeDir = storeDir;
        this.leaseDuration = leaseDuration;
        this.visibilityLag = visibilityLag;
        this.clockSkew = clockSkew;
    }

    /**
     * How long after a lease's last heartbeat another instance must wait before reclaiming it: the lease
     * duration plus the storage's bounded visibility lag (Δ) and a clock-skew margin (the lease's expiry
     * is read from an mtime stamped by a possibly differently-clocked owner).
     */
    private Duration takeoverMargin() {
        return leaseDuration.plus(visibilityLag).plus(clockSkew);
    }

    private Path leaseFile(String executionId) {
        return storeDir.resolve(executionId + LEASE_SUFFIX);
    }

    @Override
    protected void writeOwner(String executionId) {
        try {
            Path file = leaseFile(executionId);
            // A unique temp per write: two instances claiming the same record concurrently (or this
            // instance's acquire racing its own heartbeat) must not share a {id}.lease.tmp, or one
            // writer's ATOMIC_MOVE consumes the file out from under the other — throwing
            // NoSuchFileException and leaving the lease's owner indeterminate.
            Path tmp = Files.createTempFile(storeDir, executionId + LEASE_SUFFIX, ".tmp");
            Files.write(tmp, ownerId.getBytes(StandardCharsets.UTF_8));
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            Files.setLastModifiedTime(file, FileTime.from(Instant.now()));
        } catch (IOException e) {
            log.warn("Failed to write lease for {}", executionId, e);
        }
    }

    @Override
    protected String readOwner(String executionId) {
        Path file = leaseFile(executionId);
        try {
            return Files.exists(file) ? new String(Files.readAllBytes(file), StandardCharsets.UTF_8) : null;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    protected void removeOwner(String executionId) {
        try {
            if (ownerId.equals(readOwner(executionId))) {
                Files.deleteIfExists(leaseFile(executionId));
            }
        } catch (IOException e) {
            log.warn("Failed to release lease for {}; it will expire after the lease duration", executionId, e);
        }
    }

    @Override
    protected boolean tryClaim(String executionId) {
        writeOwner(executionId);
        return ownerId.equals(readOwner(executionId));
    }

    @Override
    protected boolean isHeldByLiveOwner(String executionId) {
        Path file = leaseFile(executionId);
        try {
            if (!Files.exists(file)) {
                return false;
            }
            Instant expiry = Files.getLastModifiedTime(file).toInstant().plus(takeoverMargin());
            return expiry.isAfter(Instant.now());
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    protected void sweepBackend(Set<String> liveRecordIds) {
        if (!Files.exists(storeDir)) {
            return;
        }
        Instant now = Instant.now();
        try (Stream<Path> files = Files.list(storeDir)) {
            files.filter(p -> p.getFileName().toString().endsWith(LEASE_SUFFIX)).forEach(lease -> {
                String name = lease.getFileName().toString();
                String id = name.substring(0, name.length() - LEASE_SUFFIX.length());
                if (liveRecordIds.contains(id)) {
                    return; // backs a record that still exists
                }
                try {
                    if (Files.getLastModifiedTime(lease).toInstant().plus(takeoverMargin()).isAfter(now)) {
                        return; // still valid — the owner may be mid-write of the record
                    }
                    Files.deleteIfExists(lease);
                } catch (IOException e) {
                    log.warn("Failed to sweep orphaned lease {} ({})", id, e.getMessage());
                }
            });
        } catch (IOException e) {
            log.error("Failed to list durable store directory {} for lease sweep", storeDir, e);
        }
    }
}
