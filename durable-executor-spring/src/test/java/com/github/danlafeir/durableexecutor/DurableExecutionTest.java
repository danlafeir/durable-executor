package com.github.danlafeir.durableexecutor;

import com.github.danlafeir.durableexecutor.annotation.Durable;
import com.github.danlafeir.durableexecutor.config.DurableAutoConfiguration;
import com.github.danlafeir.durableexecutor.store.DurableStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(classes = {DurableExecutionTest.TestConfig.class, DurableAutoConfiguration.class})
@TestPropertySource(properties = "durable.store-path=${java.io.tmpdir}/durable-test-${random.uuid}.json")
class DurableExecutionTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private DurableStore durableStore;

    @BeforeEach
    void clearStore() throws IOException {
        durableStore.loadAll().keySet().forEach(durableStore::delete);
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
    void recoveryReInvokesOpenExecutions(@Autowired ApplicationContext ctx) {
        var pendingExecution = new com.github.danlafeir.durableexecutor.model.DurableExecution(
                "recovery-test-id",
                OrderService.class.getName(),
                "processOrder",
                new String[]{"java.lang.String", "int"},
                new String[]{"\"order-recovered\"", "7"},
                java.time.Instant.now()
        );
        durableStore.save(pendingExecution);
        assertThat(durableStore.loadAll()).hasSize(1);

        ctx.publishEvent(new ApplicationReadyEvent(
                new org.springframework.boot.SpringApplication(TestConfig.class),
                new String[0],
                (org.springframework.context.ConfigurableApplicationContext) ctx,
                null));

        assertThat(OrderService.processed).contains("order-recovered:7");
        assertThat(durableStore.loadAll()).isEmpty();
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
