package com.lafeir.durableexecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.lafeir.durableexecutor.DurableContext;
import com.lafeir.durableexecutor.annotation.Durable;
import com.lafeir.durableexecutor.aspect.AsyncReturnPolicy;
import com.lafeir.durableexecutor.aspect.DurableAspect;
import com.lafeir.durableexecutor.coordination.CoordinationStrategy;
import com.lafeir.durableexecutor.coordination.FileLeaseCoordination;
import com.lafeir.durableexecutor.model.DurableExecution;
import com.lafeir.durableexecutor.store.DurableStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The aspect's self-fence at close: if an instance lost its lease while the method ran (another
 * instance reclaimed and took the record over), it must mutate the record in no way — not commit,
 * delete, or dead-letter it — because the new owner is now authoritative. Verified against the real
 * aspect and a real file store via an AspectJ proxy (no Spring context or heartbeat thread, so the
 * outcome is deterministic).
 */
class DurableSelfFenceAspectTest {

    @TempDir
    Path storeDir;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper(new MessagePackFactory())
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    private DurableStore store() {
        return new DurableStore(storeDir, objectMapper, Duration.ZERO);
    }

    /** A multi-instance coordinator over the shared store dir, identified by owner — like a replica. */
    private CoordinationStrategy coordination(String owner) {
        return new FileLeaseCoordination(storeDir, owner, Duration.ofSeconds(30), Duration.ZERO, Duration.ZERO);
    }

    private Task proxy(DurableStore store, CoordinationStrategy coordination, Runnable insideMethod) {
        DurableAspect aspect = new DurableAspect(store, coordination, objectMapper, AsyncReturnPolicy.REJECT);
        AspectJProxyFactory factory = new AspectJProxyFactory(new Task(insideMethod));
        factory.addAspect(aspect);
        return factory.getProxy();
    }

    private DurableExecution execution(String id) {
        return new DurableExecution(id, "C", "m", new String[0], new byte[0][], Instant.now());
    }

    @Test
    void aFencedOwnerLeavesItsRecordUntouchedAtClose() {
        DurableStore store = store();
        CoordinationStrategy ours = coordination("owner-A");
        CoordinationStrategy intruder = coordination("intruder");
        // Mid-method, another instance reclaims the record and stamps ownership with its own id.
        Task proxy = proxy(store, ours, () -> intruder.acquire("fenced-x"));

        proxy.process();

        assertThat(store.loadAll())
                .as("an owner that lost its lease mid-run must not commit or delete the record it no longer owns")
                .containsKey("fenced-x");
    }

    @Test
    void aFencedOwnerLeavesItsRecordUntouchedWithACasBackend() {
        // The same fence as above but coordinating through a Redis/DB-style TTL+CAS backend — proves the
        // aspect's open / self-fence / close wiring is backend-agnostic, not specific to file leases.
        DurableStore store = store();
        Map<String, InMemoryCasCoordination.Entry> shared = new ConcurrentHashMap<>();
        CoordinationStrategy ours = new InMemoryCasCoordination("owner-A", Duration.ofSeconds(30), shared);
        CoordinationStrategy intruder = new InMemoryCasCoordination("intruder", Duration.ofSeconds(30), shared);
        Task proxy = proxy(store, ours, () -> intruder.acquire("fenced-x"));

        proxy.process();

        assertThat(store.loadAll())
                .as("the close gate fences on any coordination backend, not just file leases")
                .containsKey("fenced-x");
    }

    @Test
    void anOwnerThatKeptItsLeaseClosesTheRecordNormally() {
        DurableStore store = store();
        Task proxy = proxy(store, coordination("owner-A"), () -> { }); // no takeover — still ours at close

        proxy.process();

        assertThat(store.loadAll())
                .as("a normal close with the lease still held removes the pending record")
                .doesNotContainKey("fenced-x");
    }

    @Test
    void aFencedRecoveryThatMarksFailedTouchesNothingAndDoesNotThrow() {
        DurableStore store = store();
        CoordinationStrategy ours = coordination("owner-A");
        CoordinationStrategy intruder = coordination("intruder");

        store.save(execution("fenced-x")); // a record being recovered
        ours.acquire("fenced-x");           // we hold ownership (as claimForRecovery would) — monotonic baseline

        Task proxy = proxy(store, ours, () -> {
            intruder.acquire("fenced-x");      // another instance takes the record over mid-recovery
            DurableContext.markFailed("boom"); // and our recovery attempt signals failure
        });

        // Drive the aspect's recovery path so a markFailed() would otherwise throw MarkedFailedException
        // (which DurableRecovery routes into save/delete — corrupting the record's new owner).
        DurableAspect.RECOVERY_EXECUTION_ID.set("fenced-x");
        try {
            assertThatCode(proxy::process)
                    .as("a fenced recovery must self-fence before the markedFailed branch, not throw "
                            + "MarkedFailedException against a record it no longer owns")
                    .doesNotThrowAnyException();
        } finally {
            DurableAspect.RECOVERY_EXECUTION_ID.remove();
        }

        assertThat(store.loadAll())
                .as("the fenced recovery must leave the record intact for its new owner")
                .containsKey("fenced-x");
    }

    public static class Task {
        private final Runnable insideMethod;

        public Task(Runnable insideMethod) {
            this.insideMethod = insideMethod;
        }

        @Durable(executionId = "fenced-x")
        public void process() {
            insideMethod.run();
        }
    }
}
