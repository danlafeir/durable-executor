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

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

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
}
