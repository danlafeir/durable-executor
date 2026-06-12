package com.lafeir.durableexecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lafeir.durableexecutor.annotation.Durable;
import com.lafeir.durableexecutor.config.DurableAutoConfiguration;
import com.lafeir.durableexecutor.model.DurableExecution;
import com.lafeir.durableexecutor.store.DurableStore;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = {DurableNestedRecoveryTest.TestConfig.class, DurableAutoConfiguration.class})
@TestPropertySource(properties = {
    "durable.store-path=${java.io.tmpdir}/durable-nested-${random.uuid}",
    "durable.dead-letter-path=${java.io.tmpdir}/durable-nested-dlq-${random.uuid}",
    "durable.stuck-grace-period=PT30S"
})
class DurableNestedRecoveryTest {

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
    void aNestedDurableCallDuringRecoveryOpensItsOwnRecord() throws Exception {
        OuterService.ran = false;
        InnerService.ran = false;
        InnerService.pendingDuringInner.clear();
        // While the nested inner method runs, snapshot the records currently on disk.
        InnerService.probe = () -> InnerService.pendingDuringInner.addAll(durableStore.loadAll().keySet());

        durableStore.save(new DurableExecution(
                "outer-id",
                OuterService.class.getName(),
                "outer",
                new String[]{"java.lang.String"},
                new byte[][]{durableObjectMapper.writeValueAsBytes("order-x")},
                Instant.now()));

        ctx.publishEvent(new ApplicationReadyEvent(
                new org.springframework.boot.SpringApplication(TestConfig.class),
                new String[0],
                (org.springframework.context.ConfigurableApplicationContext) ctx,
                null));

        await().atMost(5, SECONDS).untilAsserted(() -> {
            assertThat(OuterService.ran).isTrue();
            assertThat(InnerService.ran).isTrue();
            assertThat(durableStore.loadAll()).as("both records closed after recovery").isEmpty();
            assertThat(deadLetterStore.loadAll()).isEmpty();
        });

        assertThat(InnerService.pendingDuringInner)
                .as("nested call opens its own record rather than reusing the parent recovery id")
                .contains("inner-id")
                .contains("outer-id");
    }

    @Configuration
    static class TestConfig {
        @Bean
        public OuterService outerService(InnerService innerService) {
            return new OuterService(innerService);
        }

        @Bean
        public InnerService innerService() {
            return new InnerService();
        }
    }

    @Service
    static class OuterService {
        static volatile boolean ran;
        private final InnerService inner;

        OuterService(InnerService inner) {
            this.inner = inner;
        }

        @Durable(executionId = "outer-id")
        public void outer(String orderId) {
            inner.inner(orderId); // proxied call — goes through the aspect on this same thread
            ran = true;
        }
    }

    @Service
    static class InnerService {
        static volatile boolean ran;
        static Runnable probe = () -> {};
        static final Set<String> pendingDuringInner = ConcurrentHashMap.newKeySet();

        @Durable(executionId = "inner-id")
        public void inner(String orderId) {
            probe.run();
            ran = true;
        }
    }
}
