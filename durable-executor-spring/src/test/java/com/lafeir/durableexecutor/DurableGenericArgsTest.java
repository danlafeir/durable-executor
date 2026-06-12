package com.lafeir.durableexecutor;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
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
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = {DurableGenericArgsTest.TestConfig.class, DurableAutoConfiguration.class})
@TestPropertySource(properties = {
    "durable.store-path=${java.io.tmpdir}/durable-generic-${random.uuid}",
    "durable.dead-letter-path=${java.io.tmpdir}/durable-generic-dlq-${random.uuid}",
    "durable.stuck-grace-period=PT30S",
    "durable.max-attempts=2",
    "durable.retry-backoff=PT0.05S",
    "durable.retry-backoff-multiplier=1.0",
    "durable.retry-backoff-max=PT0.05S"
})
class DurableGenericArgsTest {

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
        ArgService.processedOrders.clear();
        ArgService.seenAnimal = null;
    }

    private void triggerRecovery() {
        ctx.publishEvent(new ApplicationReadyEvent(
                new org.springframework.boot.SpringApplication(TestConfig.class),
                new String[0],
                (org.springframework.context.ConfigurableApplicationContext) ctx,
                null));
    }

    @Test
    void aGenericListArgumentRecoversWithItsConcreteElementType() throws Exception {
        List<Order> orders = List.of(new Order("o1"), new Order("o2"));
        durableStore.save(new DurableExecution(
                "generic-id",
                ArgService.class.getName(),
                "processOrders",
                new String[]{"java.util.List"},
                new byte[][]{durableObjectMapper.writeValueAsBytes(orders)},
                Instant.now()));

        triggerRecovery();

        await().atMost(5, SECONDS).untilAsserted(() -> {
            // Without the generic-type fix the elements come back as LinkedHashMap, the for-each
            // cast throws ClassCastException, and the record is dead-lettered instead of processed.
            assertThat(ArgService.processedOrders).containsExactly("o1", "o2");
            assertThat(durableStore.loadAll()).isEmpty();
            assertThat(deadLetterStore.loadAll()).isEmpty();
        });
    }

    @Test
    void aPolymorphicArgumentWithJsonTypeInfoRecoversItsConcreteSubtype() throws Exception {
        Dog dog = new Dog();
        dog.name = "Rex";
        dog.breed = "lab";
        durableStore.save(new DurableExecution(
                "poly-id",
                ArgService.class.getName(),
                "processAnimal",
                new String[]{Animal.class.getName()},
                new byte[][]{durableObjectMapper.writeValueAsBytes(dog)},
                Instant.now()));

        triggerRecovery();

        await().atMost(5, SECONDS).untilAsserted(() ->
                assertThat(ArgService.seenAnimal).isEqualTo("Dog:lab"));
    }

    @Configuration
    static class TestConfig {
        @Bean
        public ArgService argService() {
            return new ArgService();
        }
    }

    @Service
    static class ArgService {
        static final List<String> processedOrders = new ArrayList<>();
        static volatile String seenAnimal;

        @Durable(executionId = "generic-id")
        public void processOrders(List<Order> orders) {
            for (Order order : orders) {
                processedOrders.add(order.id);
            }
        }

        @Durable(executionId = "poly-id")
        public void processAnimal(Animal animal) {
            seenAnimal = animal.getClass().getSimpleName()
                    + ":" + (animal instanceof Dog dog ? dog.breed : "");
        }
    }

    static class Order {
        public String id;
        public Order() {}
        public Order(String id) { this.id = id; }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = Dog.class, name = "dog"),
        @JsonSubTypes.Type(value = Cat.class, name = "cat")
    })
    abstract static class Animal {
        public String name;
    }

    static class Dog extends Animal {
        public String breed;
    }

    static class Cat extends Animal {
    }
}
