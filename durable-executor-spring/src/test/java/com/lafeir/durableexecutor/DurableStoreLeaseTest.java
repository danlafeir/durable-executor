package com.lafeir.durableexecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.lafeir.durableexecutor.model.DurableExecution;
import com.lafeir.durableexecutor.store.CoordinationMode;
import com.lafeir.durableexecutor.store.DurableStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class DurableStoreLeaseTest {

    @TempDir
    Path storeDir;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper(new MessagePackFactory())
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    private DurableStore shared(String owner, Duration lease) {
        return new DurableStore(storeDir, objectMapper, Duration.ZERO,
                CoordinationMode.SHARED_STORE, owner, lease);
    }

    private DurableExecution execution(String id) {
        return new DurableExecution(id, "C", "m", new String[0], new byte[0][], Instant.now());
    }

    @Test
    void aValidLeaseHeldByAnotherInstanceHidesThePendingRecord() {
        DurableStore a = shared("owner-A", Duration.ofSeconds(30));
        DurableStore b = shared("owner-B", Duration.ofSeconds(30));

        a.save(execution("x"));
        b.acquireLease("x"); // another live instance owns it

        assertThat(a.scan().pending())
                .as("a record under a valid lease elsewhere is not a recovery candidate")
                .doesNotContainKey("x");
    }

    @Test
    void anExpiredLeaseMakesThePendingRecordReclaimable() {
        DurableStore a = shared("owner-A", Duration.ofMillis(150));
        DurableStore b = shared("owner-B", Duration.ofMillis(150));

        a.save(execution("x"));
        b.acquireLease("x");

        await().atMost(2, SECONDS).untilAsserted(() ->
                assertThat(a.scan().pending())
                        .as("once the lease expires the crashed record can be reclaimed")
                        .containsKey("x"));
    }

    @Test
    void claimingARecordStampsOurOwnerSoOtherInstancesSkipIt() {
        DurableStore a = shared("owner-A", Duration.ofSeconds(30));
        DurableStore b = shared("owner-B", Duration.ofSeconds(30));

        a.save(execution("x"));

        assertThat(a.claim("x")).isTrue();
        assertThat(b.scan().pending())
                .as("after A claims the record, B must not also pick it up")
                .doesNotContainKey("x");
    }

    @Test
    void singleInstanceModeWritesNoLeaseAndIgnoresAnyLeaseFile() {
        DurableStore single = new DurableStore(storeDir, objectMapper, Duration.ZERO);

        single.save(execution("x"));
        single.acquireLease("x"); // no-op in single-instance mode

        assertThat(single.claim("x")).isTrue();
        assertThat(storeDir.resolve("x.lease")).doesNotExist();
        assertThat(single.scan().pending()).containsKey("x");
    }

    @Test
    void releaseLeavesALeaseReclaimedByAnotherInstanceInPlace() {
        DurableStore a = shared("owner-A", Duration.ofSeconds(30));
        DurableStore b = shared("owner-B", Duration.ofSeconds(30));

        a.save(execution("x"));
        a.acquireLease("x");
        b.acquireLease("x"); // A's lease expired and B reclaimed it (now owned by B, still running)

        a.releaseLease("x"); // A finishing late must not delete B's live lease

        assertThat(storeDir.resolve("x.lease")).exists();
    }

    @Test
    void renewalDoesNotStompALeaseAnotherInstanceTookOver() throws Exception {
        DurableStore a = shared("owner-A", Duration.ofSeconds(30));
        DurableStore b = shared("owner-B", Duration.ofSeconds(30));

        a.save(execution("x"));
        a.acquireLease("x");
        a.markLive("x");      // A still believes it is running x
        b.acquireLease("x");  // A stalled past its lease; B reclaimed it

        a.renewLeases();      // A's heartbeat resumes — must NOT stamp itself back over B

        assertThat(Files.readString(storeDir.resolve("x.lease")))
                .as("a heartbeat for a lease another instance has taken over must not steal it back")
                .isEqualTo("owner-B");
    }

    @Test
    void releaseRemovesOurOwnLease() {
        DurableStore a = shared("owner-A", Duration.ofSeconds(30));

        a.save(execution("x"));
        a.acquireLease("x");
        a.releaseLease("x");

        assertThat(storeDir.resolve("x.lease")).doesNotExist();
    }

    @Test
    void scanSweepsAnExpiredOrphanedLease() throws Exception {
        DurableStore a = shared("owner-A", Duration.ofSeconds(30));
        a.acquireLease("orphan"); // a lease with no pending record (e.g. its record was dead-lettered)
        Files.setLastModifiedTime(storeDir.resolve("orphan.lease"),
                FileTime.from(Instant.now().minusSeconds(3600)));

        a.scan();

        assertThat(storeDir.resolve("orphan.lease")).doesNotExist();
    }

    @Test
    void scanLeavesValidOrphansAndLeasesBackingARecordAlone() throws Exception {
        DurableStore a = shared("owner-A", Duration.ofSeconds(30));
        a.acquireLease("valid-orphan"); // orphan but not yet expired — owner may be mid-write
        a.save(execution("backed"));
        a.acquireLease("backed");
        Files.setLastModifiedTime(storeDir.resolve("backed.lease"),
                FileTime.from(Instant.now().minusSeconds(3600))); // expired, but the record still exists

        a.scan();

        assertThat(storeDir.resolve("valid-orphan.lease")).exists();
        assertThat(storeDir.resolve("backed.lease")).exists();
    }

    @Test
    void concurrentClaimsLeaveAWellFormedLeaseOwnedByOneClaimant() throws Exception {
        // This pins lease *integrity* under contention, not mutual exclusion. claim() is
        // write-then-read-back, so two simultaneous claims can both return true and cross-instance
        // double execution remains possible by design (shared-store is documented best-effort).
        // What must always hold: the unique-temp write leaves the lease file owned by exactly one
        // claimant — never empty, truncated, or lost to a temp-file collision.
        int n = 8;
        DurableStore writer = shared("writer", Duration.ofSeconds(30));
        writer.save(execution("x"));

        List<String> owners = IntStream.range(0, n).mapToObj(i -> "owner-" + i).toList();
        List<DurableStore> stores = owners.stream()
                .map(o -> shared(o, Duration.ofSeconds(30)))
                .toList();

        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch ready = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();
        for (DurableStore s : stores) {
            futures.add(pool.submit(() -> {
                ready.await();
                return s.claim("x");
            }));
        }
        ready.countDown();
        for (Future<Boolean> f : futures) {
            f.get(5, SECONDS);
        }
        pool.shutdown();

        String finalOwner = Files.readString(storeDir.resolve("x.lease"));
        assertThat(finalOwner).isIn(owners);
    }
}
