package com.lafeir.durableexecutor;

import com.lafeir.durableexecutor.coordination.FileLeaseCoordination;
import com.lafeir.durableexecutor.coordination.SingleInstanceCoordination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class CoordinationStrategyTest {

    @TempDir
    Path storeDir;

    // ── SingleInstanceCoordination ───────────────────────────────────────────────────────────────

    @Test
    void singleInstanceTracksLivenessInMemoryAndAlwaysOwns() {
        SingleInstanceCoordination s = new SingleInstanceCoordination();
        assertThat(s.isMultiInstance()).isFalse();
        assertThat(s.claimForRecovery("x")).isTrue(); // single writer always wins recovery
        assertThat(s.stillOwns("x")).isTrue();
        s.markRunning("x");
        assertThat(s.isActive("x")).isTrue();
        s.markStopped("x");
        assertThat(s.isActive("x")).isFalse();
    }

    // ── InMemoryCasCoordination — proves the SPI fits a TTL+CAS (Redis/DB) back end ──────────────

    private InMemoryCasCoordination cas(String owner, Map<String, InMemoryCasCoordination.Entry> shared) {
        return new InMemoryCasCoordination(owner, Duration.ofSeconds(30), shared);
    }

    @Test
    void casBackendGivesMutualExclusionAndReclaimAfterRelease() {
        Map<String, InMemoryCasCoordination.Entry> shared = new ConcurrentHashMap<>();
        InMemoryCasCoordination a = cas("A", shared);
        InMemoryCasCoordination b = cas("B", shared);

        assertThat(a.claimForRecovery("x")).isTrue();
        assertThat(b.claimForRecovery("x")).as("B cannot claim a record A holds").isFalse();
        assertThat(a.stillOwns("x")).isTrue();
        assertThat(b.stillOwns("x")).isFalse();
        assertThat(b.isActive("x")).as("held by a live owner elsewhere").isTrue();

        a.release("x");
        assertThat(b.claimForRecovery("x")).as("released — B can take it now").isTrue();
        assertThat(a.stillOwns("x")).isFalse();
    }

    @Test
    void casBackendConcurrentClaimsElectExactlyOneWinner() throws Exception {
        Map<String, InMemoryCasCoordination.Entry> shared = new ConcurrentHashMap<>();
        int n = 8;
        List<InMemoryCasCoordination> instances = IntStream.range(0, n)
                .mapToObj(i -> cas("owner-" + i, shared)).toList();

        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch ready = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();
        for (InMemoryCasCoordination s : instances) {
            futures.add(pool.submit(() -> {
                ready.await();
                return s.claimForRecovery("x");
            }));
        }
        ready.countDown();
        long winners = 0;
        for (Future<Boolean> f : futures) {
            if (f.get(5, SECONDS)) {
                winners++;
            }
        }
        pool.shutdown();
        assertThat(winners).as("a CAS back end elects exactly one recovery claimant").isEqualTo(1);
    }

    // ── FileLeaseCoordination ────────────────────────────────────────────────────────────────────

    private FileLeaseCoordination file(String owner, Duration lease, Duration delta, Duration skew) {
        return new FileLeaseCoordination(storeDir, owner, lease, delta, skew);
    }

    private void setLeaseMtime(String id, Instant when) throws Exception {
        Files.setLastModifiedTime(storeDir.resolve(id + ".lease"), FileTime.from(when));
    }

    @Test
    void fileLeaseStillOwnsUntilAnotherInstanceTakesOver() {
        FileLeaseCoordination a = file("A", Duration.ofSeconds(30), Duration.ZERO, Duration.ZERO);
        FileLeaseCoordination b = file("B", Duration.ofSeconds(30), Duration.ZERO, Duration.ZERO);
        a.acquire("x");
        assertThat(a.stillOwns("x")).isTrue();
        b.acquire("x"); // A stalled; B took the lease over
        assertThat(a.stillOwns("x")).as("the lease no longer names us").isFalse();
    }

    @Test
    void fileLeaseSelfFencesPastLeaseDurationEvenIfFileStillNamesUs() {
        FileLeaseCoordination a = file("A", Duration.ofMillis(50), Duration.ZERO, Duration.ZERO);
        a.acquire("x"); // nobody else touches the lease — the file keeps reading A
        await().atMost(2, SECONDS).untilAsserted(() ->
                assertThat(a.stillOwns("x"))
                        .as("past the lease duration we cannot prove freshness and must self-fence")
                        .isFalse());
    }

    @Test
    void fileLeaseHeartbeatDoesNotStompALeaseAnotherInstanceTookOver() throws Exception {
        FileLeaseCoordination a = file("A", Duration.ofSeconds(30), Duration.ZERO, Duration.ZERO);
        FileLeaseCoordination b = file("B", Duration.ofSeconds(30), Duration.ZERO, Duration.ZERO);
        a.acquire("x");
        a.markRunning("x");
        b.acquire("x");   // A stalled past its lease; B reclaimed it
        a.heartbeat();    // A's heartbeat resumes — must not steal it back

        assertThat(Files.readString(storeDir.resolve("x.lease"))).isEqualTo("B");
    }

    @Test
    void fileLeaseTakeoverWaitsTheFullVisibilityAndSkewMargin() throws Exception {
        Duration lease = Duration.ofSeconds(10);
        Duration delta = Duration.ofSeconds(30);
        Duration skew = Duration.ofSeconds(5);
        FileLeaseCoordination a = file("A", lease, delta, skew);
        a.acquire("x");

        // past the lease duration, but still within L + Δ + skew → not yet reclaimable
        setLeaseMtime("x", Instant.now().minus(lease).minusSeconds(1));
        assertThat(a.isActive("x")).as("within the takeover margin, still held").isTrue();

        // past the full margin → reclaimable
        setLeaseMtime("x", Instant.now().minus(lease).minus(delta).minus(skew).minusSeconds(1));
        assertThat(a.isActive("x")).as("past the full margin, reclaimable").isFalse();
    }

    @Test
    void fileLeaseSweepRemovesExpiredOrphansButKeepsLeasesBackingALiveRecord() throws Exception {
        FileLeaseCoordination a = file("A", Duration.ofSeconds(30), Duration.ZERO, Duration.ZERO);
        a.acquire("orphan");
        a.acquire("backed");
        setLeaseMtime("orphan", Instant.now().minusSeconds(3600));
        setLeaseMtime("backed", Instant.now().minusSeconds(3600));

        a.sweep(Set.of("backed")); // only "backed" still has a record

        assertThat(storeDir.resolve("orphan.lease")).doesNotExist();
        assertThat(storeDir.resolve("backed.lease")).exists();
    }
}
