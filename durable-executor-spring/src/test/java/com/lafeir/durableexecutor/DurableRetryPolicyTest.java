package com.lafeir.durableexecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lafeir.durableexecutor.DurableContext;
import com.lafeir.durableexecutor.annotation.Durable;
import com.lafeir.durableexecutor.config.DurableAutoConfiguration;
import com.lafeir.durableexecutor.model.DurableExecution;
import com.lafeir.durableexecutor.store.DurableStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.springframework.test.context.TestPropertySource;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = {DurableRetryPolicyTest.TestConfig.class, DurableAutoConfiguration.class})
@TestPropertySource(properties = {
    "durable.store-path=${java.io.tmpdir}/durable-retry-${random.uuid}",
    "durable.dead-letter-path=${java.io.tmpdir}/durable-retry-dlq-${random.uuid}",
    "durable.stuck-grace-period=PT30S",
    "durable.max-attempts=3",
    "durable.retry-backoff=PT0.05S",
    "durable.retry-backoff-multiplier=1.0",
    "durable.retry-backoff-max=PT0.05S"
})
class DurableRetryPolicyTest {

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

    @BeforeEach
    void reset() {
        FlakyService.invocations.set(0);
        FlakyService.failUntilAttempt = 0;
        durableStore.loadAll().keySet().forEach(durableStore::delete);
        durableStore.loadAllDeleted().keySet().forEach(durableStore::finalizeDelete);
        deadLetterStore.loadAll().keySet().forEach(deadLetterStore::delete);
    }

    private void savePending() throws Exception {
        durableStore.save(new DurableExecution(
                "flaky-id",
                FlakyService.class.getName(),
                "flaky",
                new String[]{"java.lang.String"},
                new byte[][]{durableObjectMapper.writeValueAsBytes("order-x")},
                Instant.now()));
    }

    private void triggerRecovery() {
        ctx.publishEvent(new ApplicationReadyEvent(
                new org.springframework.boot.SpringApplication(TestConfig.class),
                new String[0],
                (org.springframework.context.ConfigurableApplicationContext) ctx,
                null));
    }

    @Test
    void aTransientFailureIsRetriedWithBackoffUntilItSucceeds() throws Exception {
        FlakyService.failUntilAttempt = 2; // fail the first two attempts, succeed on the third
        savePending();

        triggerRecovery();

        await().atMost(5, SECONDS).untilAsserted(() -> {
            assertThat(FlakyService.invocations.get()).isEqualTo(3);
            assertThat(durableStore.loadAll()).as("record cleared after eventual success").isEmpty();
            assertThat(deadLetterStore.loadAll()).as("never dead-lettered").isEmpty();
        });
    }

    @Test
    void aRecordIsDeadLetteredOnlyAfterAttemptsAreExhausted() throws Exception {
        FlakyService.failUntilAttempt = Integer.MAX_VALUE; // always fail
        savePending();

        triggerRecovery();

        await().atMost(5, SECONDS).untilAsserted(() ->
                assertThat(deadLetterStore.loadAll()).containsKey("flaky-id"));

        assertThat(FlakyService.invocations.get())
                .as("exactly max-attempts attempts, no more")
                .isEqualTo(3);
        assertThat(durableStore.loadAll()).isEmpty();
        assertThat(deadLetterStore.loadAll().get("flaky-id").getAttempts()).isEqualTo(3);
    }

    @Test
    void markFailedDuringRecoveryIsAlsoBoundedByMaxAttempts() throws Exception {
        // markFailed() returns normally rather than throwing, but during recovery it is still a
        // failed attempt and must be subject to the same attempt budget as a thrown exception.
        durableStore.save(new DurableExecution(
                "markfail-id",
                FlakyService.class.getName(),
                "alwaysMarkFailed",
                new String[]{"java.lang.String"},
                new byte[][]{durableObjectMapper.writeValueAsBytes("order-x")},
                Instant.now()));

        triggerRecovery();

        await().atMost(5, SECONDS).untilAsserted(() ->
                assertThat(deadLetterStore.loadAll()).containsKey("markfail-id"));

        assertThat(FlakyService.invocations.get())
                .as("markFailed recovery is bounded, not retried forever")
                .isEqualTo(3);
        assertThat(durableStore.loadAll()).isEmpty();
    }

    @Configuration
    static class TestConfig {
        @Bean
        public FlakyService flakyService() {
            return new FlakyService();
        }
    }

    @Service
    static class FlakyService {

        static final AtomicInteger invocations = new AtomicInteger();
        static volatile int failUntilAttempt = 0;

        @Durable(executionId = "flaky-id")
        public void flaky(String orderId) {
            int n = invocations.incrementAndGet();
            if (n <= failUntilAttempt) {
                throw new RuntimeException("transient failure #" + n);
            }
        }

        @Durable(executionId = "markfail-id")
        public void alwaysMarkFailed(String orderId) {
            invocations.incrementAndGet();
            DurableContext.markFailed("always");
        }
    }
}
