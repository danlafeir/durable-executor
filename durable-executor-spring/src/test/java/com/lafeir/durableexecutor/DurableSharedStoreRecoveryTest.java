package com.lafeir.durableexecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lafeir.durableexecutor.annotation.Durable;
import com.lafeir.durableexecutor.annotation.Durable.CloseMode;
import com.lafeir.durableexecutor.config.DurableAutoConfiguration;
import com.lafeir.durableexecutor.model.DurableExecution;
import com.lafeir.durableexecutor.store.DurableStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.springframework.test.context.TestPropertySource;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * In shared-store mode, recovery reclaiming a record from an expired lease is an ambiguous
 * cross-instance takeover (the previous owner may have crashed or merely stalled). A non-idempotent
 * (TRANSACTIONAL) method is routed to the DLQ for adjudication rather than re-run; an IDEMPOTENT one
 * is re-run as normal. (Single-instance recovery re-runs TRANSACTIONAL — covered by DurableExecutionTest.)
 */
@SpringBootTest(classes = {DurableSharedStoreRecoveryTest.TestConfig.class, DurableAutoConfiguration.class})
@TestPropertySource(properties = {
    "durable.store-path=${java.io.tmpdir}/durable-shared-${random.uuid}",
    "durable.dead-letter-path=${java.io.tmpdir}/durable-shared-dlq-${random.uuid}",
    "durable.coordination=shared-store",
    "durable.lease-duration=PT30S",
    "durable.stuck-grace-period=PT30S",
    "durable.max-attempts=5",
    "durable.retry-backoff=PT30S"
})
class DurableSharedStoreRecoveryTest {

    @Autowired
    private DurableStore durableStore;

    @Autowired
    @Qualifier("durableDeadLetterStore")
    private DurableStore deadLetterStore;

    @Autowired
    @Qualifier("durableObjectMapper")
    private ObjectMapper durableObjectMapper;

    @Autowired
    private ApplicationContext ctx;

    @Autowired
    private Recorder recorder;

    @Test
    void aReclaimedTransactionalRecordIsRoutedToTheDlqInsteadOfReExecuted() throws Exception {
        // Saved with no lease → immediately reclaimable in a shared-store scan (a crashed/stalled owner).
        durableStore.save(record("txn-id", "doTransactional"));

        triggerRecovery();

        await().atMost(5, SECONDS).untilAsserted(() -> {
            assertThat(deadLetterStore.loadAll()).containsKey("txn-id");
            assertThat(durableStore.loadAll()).isEmpty();
        });
        assertThat(recorder.wasInvoked("doTransactional"))
                .as("a non-idempotent record reclaimed across instances must not be re-executed")
                .isFalse();
    }

    @Test
    void aReclaimedIdempotentRecordIsReExecuted() throws Exception {
        durableStore.save(record("idem-id", "doIdempotent"));

        triggerRecovery();

        await().atMost(5, SECONDS).untilAsserted(() -> {
            assertThat(recorder.wasInvoked("doIdempotent"))
                    .as("an idempotent record is safe to repeat and is re-run on takeover")
                    .isTrue();
            assertThat(durableStore.loadAll()).isEmpty(); // closed normally after the re-run
        });
        assertThat(deadLetterStore.loadAll()).doesNotContainKey("idem-id");
    }

    private void triggerRecovery() {
        ctx.publishEvent(new ApplicationReadyEvent(
                new SpringApplication(TestConfig.class),
                new String[0],
                (ConfigurableApplicationContext) ctx,
                null));
    }

    private DurableExecution record(String id, String method) throws Exception {
        return new DurableExecution(
                id,
                OrderSvc.class.getName(),
                method,
                new String[]{"java.lang.String"},
                new byte[][]{durableObjectMapper.writeValueAsBytes("arg")},
                Instant.now());
    }

    @Configuration
    static class TestConfig {
        @Bean
        public Recorder recorder() {
            return new Recorder();
        }

        @Bean
        public OrderSvc orderSvc(Recorder recorder) {
            return new OrderSvc(recorder);
        }
    }

    static class Recorder {
        private final Set<String> invoked = ConcurrentHashMap.newKeySet();

        void record(String method) {
            invoked.add(method);
        }

        boolean wasInvoked(String method) {
            return invoked.contains(method);
        }
    }

    @Service
    static class OrderSvc {
        private final Recorder recorder;

        OrderSvc(Recorder recorder) {
            this.recorder = recorder;
        }

        @Durable(closeMode = CloseMode.TRANSACTIONAL)
        public void doTransactional(String arg) {
            recorder.record("doTransactional");
        }

        @Durable(closeMode = CloseMode.IDEMPOTENT)
        public void doIdempotent(String arg) {
            recorder.record("doIdempotent");
        }
    }
}
