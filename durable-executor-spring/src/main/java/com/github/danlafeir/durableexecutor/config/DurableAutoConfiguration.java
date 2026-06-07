package com.github.danlafeir.durableexecutor.config;

import com.github.danlafeir.durableexecutor.aspect.DurableAspect;
import com.github.danlafeir.durableexecutor.recovery.DurableRecovery;
import com.github.danlafeir.durableexecutor.store.DurableStore;
import com.github.danlafeir.durableexecutor.web.DurableDeadLetterController;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication.Type;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@AutoConfiguration
@EnableAspectJAutoProxy
@EnableConfigurationProperties(DurableProperties.class)
public class DurableAutoConfiguration {

    @Bean(name = "durableObjectMapper")
    @ConditionalOnMissingBean(name = "durableObjectMapper")
    public ObjectMapper durableObjectMapper() {
        return new ObjectMapper(new MessagePackFactory())
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Bean
    @ConditionalOnMissingBean
    public DurableStore durableStore(DurableProperties properties,
                                     @Qualifier("durableObjectMapper") ObjectMapper durableObjectMapper) {
        return new DurableStore(Path.of(properties.getStorePath()), durableObjectMapper);
    }

    @Bean(name = "durableDeadLetterStore")
    @ConditionalOnMissingBean(name = "durableDeadLetterStore")
    public DurableStore durableDeadLetterStore(DurableProperties properties,
                                               @Qualifier("durableObjectMapper") ObjectMapper durableObjectMapper) {
        return new DurableStore(Path.of(properties.getDeadLetterPath()), durableObjectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public DurableAspect durableAspect(@Qualifier("durableStore") DurableStore durableStore,
                                       @Qualifier("durableObjectMapper") ObjectMapper durableObjectMapper) {
        return new DurableAspect(durableStore, durableObjectMapper);
    }

    @Bean(name = "durableScheduler")
    @ConditionalOnMissingBean(name = "durableScheduler")
    public ScheduledExecutorService durableScheduler(DurableProperties properties) {
        return Executors.newScheduledThreadPool(properties.getRetryThreads());
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnWebApplication(type = Type.SERVLET)
    @ConditionalOnProperty(prefix = "durable.dlq-endpoint", name = "enabled", havingValue = "true")
    public DurableDeadLetterController durableDeadLetterController(
            @Qualifier("durableDeadLetterStore") DurableStore durableDeadLetterStore) {
        return new DurableDeadLetterController(durableDeadLetterStore);
    }

    @Bean
    @ConditionalOnMissingBean
    public DurableRecovery durableRecovery(@Qualifier("durableStore") DurableStore durableStore,
                                           @Qualifier("durableDeadLetterStore") DurableStore durableDeadLetterStore,
                                           @Qualifier("durableObjectMapper") ObjectMapper durableObjectMapper,
                                           ApplicationContext applicationContext,
                                           @Qualifier("durableScheduler") ScheduledExecutorService durableScheduler) {
        return new DurableRecovery(durableStore, durableDeadLetterStore, durableObjectMapper, applicationContext, durableScheduler);
    }
}
