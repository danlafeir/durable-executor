package com.lafeir.durableexecutor;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lafeir.durableexecutor.annotation.Durable;
import com.lafeir.durableexecutor.config.DurableAutoConfiguration;
import com.lafeir.durableexecutor.model.DurableExecution;
import com.lafeir.durableexecutor.recovery.DurableRecovery;
import com.lafeir.durableexecutor.store.DurableStore;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = {DurableSignatureChangeTest.TestConfig.class, DurableAutoConfiguration.class})
@TestPropertySource(properties = {
    "durable.store-path=${java.io.tmpdir}/durable-sig-${random.uuid}",
    "durable.dead-letter-path=${java.io.tmpdir}/durable-sig-dlq-${random.uuid}",
    "durable.stuck-grace-period=PT30S",
    // A long backoff and budget: an unresolvable target would stay pending for 30s+ if it were
    // retried, so the assertion below only passes if it is dead-lettered immediately.
    "durable.max-attempts=5",
    "durable.retry-backoff=PT30S"
})
class DurableSignatureChangeTest {

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

    @Test
    void anUnresolvableTargetIsDeadLetteredImmediatelyWithAClearMessage() throws Exception {
        // The class and bean resolve, but the method does not — as if a deploy renamed it.
        durableStore.save(new DurableExecution(
                "sig-id",
                OrderService.class.getName(),
                "renamedAway",
                new String[]{"java.lang.String"},
                new byte[][]{durableObjectMapper.writeValueAsBytes("order-x")},
                Instant.now()));

        Logger recoveryLog = (Logger) LoggerFactory.getLogger(DurableRecovery.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        recoveryLog.addAppender(appender);
        try {
            ctx.publishEvent(new ApplicationReadyEvent(
                    new org.springframework.boot.SpringApplication(TestConfig.class),
                    new String[0],
                    (org.springframework.context.ConfigurableApplicationContext) ctx,
                    null));

            await().atMost(3, SECONDS).untilAsserted(() -> {
                assertThat(deadLetterStore.loadAll()).containsKey("sig-id");
                assertThat(durableStore.loadAll()).isEmpty();
            });
        } finally {
            recoveryLog.detachAppender(appender);
        }

        assertThat(appender.list)
                .anySatisfy(e -> {
                    assertThat(e.getLevel()).isEqualTo(Level.ERROR);
                    assertThat(e.getFormattedMessage())
                            .contains("could not be resolved")
                            .contains("renamedAway");
                });
    }

    @Configuration
    static class TestConfig {
        @Bean
        public OrderService orderService() {
            return new OrderService();
        }
    }

    @Service
    static class OrderService {
        @Durable
        public void processOrder(String orderId) {
        }
    }
}
