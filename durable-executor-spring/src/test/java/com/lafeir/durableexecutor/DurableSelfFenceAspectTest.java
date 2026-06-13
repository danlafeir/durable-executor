package com.lafeir.durableexecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.lafeir.durableexecutor.annotation.Durable;
import com.lafeir.durableexecutor.aspect.AsyncReturnPolicy;
import com.lafeir.durableexecutor.aspect.DurableAspect;
import com.lafeir.durableexecutor.store.CoordinationMode;
import com.lafeir.durableexecutor.store.DurableStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

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

    private DurableStore shared(String owner) {
        return new DurableStore(storeDir, objectMapper, Duration.ZERO,
                CoordinationMode.SHARED_STORE, owner, Duration.ofSeconds(30),
                Duration.ZERO, Duration.ZERO);
    }

    private Task proxy(DurableStore store, Runnable insideMethod) {
        DurableAspect aspect = new DurableAspect(store, objectMapper, AsyncReturnPolicy.REJECT);
        AspectJProxyFactory factory = new AspectJProxyFactory(new Task(insideMethod));
        factory.addAspect(aspect);
        return factory.getProxy();
    }

    @Test
    void aFencedOwnerLeavesItsRecordUntouchedAtClose() {
        DurableStore store = shared("owner-A");
        DurableStore intruder = shared("intruder");
        // Mid-method, another instance reclaims the record and stamps the lease with its own owner.
        Task proxy = proxy(store, () -> intruder.acquireLease("fenced-x"));

        proxy.process();

        assertThat(store.loadAll())
                .as("an owner that lost its lease mid-run must not commit or delete the record it no longer owns")
                .containsKey("fenced-x");
    }

    @Test
    void anOwnerThatKeptItsLeaseClosesTheRecordNormally() {
        DurableStore store = shared("owner-A");
        Task proxy = proxy(store, () -> { }); // no takeover — the lease is still ours at close

        proxy.process();

        assertThat(store.loadAll())
                .as("a normal close with the lease still held removes the pending record")
                .doesNotContainKey("fenced-x");
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
