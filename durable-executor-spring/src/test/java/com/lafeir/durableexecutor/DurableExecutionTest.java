package com.lafeir.durableexecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = {DurableExecutionTest.TestConfig.class, DurableAutoConfiguration.class})
@TestPropertySource(properties = {
    "durable.store-path=${java.io.tmpdir}/durable-test-${random.uuid}",
    "durable.dead-letter-path=${java.io.tmpdir}/durable-dlq-${random.uuid}",
    "durable.stuck-grace-period=PT0S"
})
class DurableExecutionTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private DurableStore durableStore;

    @Autowired
    @Qualifier("durableDeadLetterStore")
    private DurableStore deadLetterStore;

    @Autowired
    @Qualifier("durableObjectMapper")
    private ObjectMapper durableObjectMapper;

    @BeforeEach
    void clearStore() {
        OrderService.processed.clear();
        durableStore.loadAll().keySet().forEach(durableStore::delete);
        durableStore.loadAllDeleted().keySet().forEach(durableStore::finalizeDelete);
        deadLetterStore.loadAll().keySet().forEach(deadLetterStore::delete);
    }

    @Test
    void storeIsEmptyAfterSuccessfulExecution() {
        orderService.processOrder("order-1", 42);

        assertThat(durableStore.loadAll()).isEmpty();
    }

    @Test
    void recordRemainsInStoreWhenMethodThrows() {
        assertThatThrownBy(() -> orderService.failingOrder("order-fail"))
                .isInstanceOf(RuntimeException.class);

        assertThat(durableStore.loadAll()).hasSize(1);
    }

    @Test
    void methodIsCalledWithCorrectArgs() {
        orderService.processOrder("order-2", 99);

        assertThat(OrderService.processed).contains("order-2:99");
    }

    @Test
    void recoveryReInvokesOpenExecutions(@Autowired ApplicationContext ctx) throws Exception {
        var pendingExecution = new DurableExecution(
                "recovery-test-id",
                OrderService.class.getName(),
                "processOrder",
                new String[]{"java.lang.String", "int"},
                new byte[][]{
                    durableObjectMapper.writeValueAsBytes("order-recovered"),
                    durableObjectMapper.writeValueAsBytes(7)
                },
                Instant.now()
        );
        durableStore.save(pendingExecution);
        assertThat(durableStore.loadAll()).hasSize(1);

        ctx.publishEvent(new ApplicationReadyEvent(
                new org.springframework.boot.SpringApplication(TestConfig.class),
                new String[0],
                (org.springframework.context.ConfigurableApplicationContext) ctx,
                null));

        await().atMost(5, SECONDS).untilAsserted(() -> {
            assertThat(OrderService.processed).contains("order-recovered:7");
            assertThat(durableStore.loadAll()).isEmpty();
        });
    }

    @Test
    void stuckDeletedExecutionIsMovedToDeadLetterQueue(@Autowired ApplicationContext ctx) throws Exception {
        // Simulate a crash between markDeleted() and finalizeDelete() by writing a -deleted file directly
        var execution = new DurableExecution(
                "stuck-delete-id",
                OrderService.class.getName(),
                "processOrder",
                new String[]{"java.lang.String", "int"},
                new byte[][]{
                    durableObjectMapper.writeValueAsBytes("order-stuck"),
                    durableObjectMapper.writeValueAsBytes(5)
                },
                Instant.now()
        );
        durableStore.save(execution);
        durableStore.markDeleted(execution.getExecutionId());

        ctx.publishEvent(new ApplicationReadyEvent(
                new org.springframework.boot.SpringApplication(TestConfig.class),
                new String[0],
                (org.springframework.context.ConfigurableApplicationContext) ctx,
                null));

        await().atMost(5, SECONDS).untilAsserted(() -> {
            assertThat(durableStore.loadAllDeleted()).isEmpty();
            assertThat(deadLetterStore.loadAll()).containsKey("stuck-delete-id");
        });
    }

    @Test
    void failedRecoveryMovesExecutionToDeadLetterQueue(@Autowired ApplicationContext ctx) throws Exception {
        var pendingExecution = new DurableExecution(
                "dlq-test-id",
                OrderService.class.getName(),
                "failingOrder",
                new String[]{"java.lang.String"},
                new byte[][]{durableObjectMapper.writeValueAsBytes("order-dlq")},
                Instant.now()
        );
        durableStore.save(pendingExecution);

        ctx.publishEvent(new ApplicationReadyEvent(
                new org.springframework.boot.SpringApplication(TestConfig.class),
                new String[0],
                (org.springframework.context.ConfigurableApplicationContext) ctx,
                null));

        await().atMost(5, SECONDS).untilAsserted(() -> {
            assertThat(durableStore.loadAll()).isEmpty();
            assertThat(deadLetterStore.loadAll()).containsKey("dlq-test-id");
        });
    }

    // ---- test fixtures ----

    @Configuration
    static class TestConfig {
        @Bean
        public OrderService orderService() {
            return new OrderService();
        }
    }

    @Service
    static class OrderService {

        static final List<String> processed = new ArrayList<>();

        @Durable
        public void processOrder(String orderId, int amount) {
            processed.add(orderId + ":" + amount);
        }

        @Durable
        public void failingOrder(String orderId) {
            throw new RuntimeException("intentional failure");
        }
    }
}
