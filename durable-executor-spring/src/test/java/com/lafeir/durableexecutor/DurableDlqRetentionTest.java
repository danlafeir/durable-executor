package com.lafeir.durableexecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lafeir.durableexecutor.model.DurableExecution;
import com.lafeir.durableexecutor.config.DurableAutoConfiguration;
import com.lafeir.durableexecutor.store.DurableStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;

import java.time.Instant;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = {DurableDlqRetentionTest.TestConfig.class, DurableAutoConfiguration.class})
@TestPropertySource(properties = {
    "durable.store-path=${java.io.tmpdir}/durable-retention-${random.uuid}",
    "durable.dead-letter-path=${java.io.tmpdir}/durable-retention-dlq-${random.uuid}",
    "durable.stuck-grace-period=PT30S",
    "durable.dlq-retention=PT0S"
})
class DurableDlqRetentionTest {

    @Autowired
    @Qualifier("durableDeadLetterStore")
    private DurableStore deadLetterStore;

    @Autowired
    @Qualifier("durableObjectMapper")
    private ObjectMapper durableObjectMapper;

    @Autowired
    private ApplicationContext ctx;

    @Test
    void expiredDeadLetterEntriesArePurgedOnTheRecoveryCycle() {
        deadLetterStore.save(new DurableExecution(
                "dlq-old", "C", "m", new String[0], new byte[0][], Instant.now()));
        assertThat(deadLetterStore.listIds()).contains("dlq-old");

        ctx.publishEvent(new ApplicationReadyEvent(
                new org.springframework.boot.SpringApplication(TestConfig.class),
                new String[0],
                (org.springframework.context.ConfigurableApplicationContext) ctx,
                null));

        await().atMost(5, SECONDS).untilAsserted(() ->
                assertThat(deadLetterStore.listIds()).isEmpty());
    }

    @Configuration
    static class TestConfig {
    }
}
