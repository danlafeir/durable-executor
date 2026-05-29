package com.github.danlafeir.durableexecutor.config;

import com.github.danlafeir.durableexecutor.aspect.DurableAspect;
import com.github.danlafeir.durableexecutor.recovery.DurableRecovery;
import com.github.danlafeir.durableexecutor.store.DurableStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import java.nio.file.Path;

@AutoConfiguration
@EnableAspectJAutoProxy
@EnableConfigurationProperties(DurableProperties.class)
public class DurableAutoConfiguration {

    @Bean(name = "durableObjectMapper")
    @ConditionalOnMissingBean(name = "durableObjectMapper")
    public ObjectMapper durableObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Bean
    @ConditionalOnMissingBean
    public DurableStore durableStore(DurableProperties properties,
                                     ObjectMapper durableObjectMapper) {
        return new DurableStore(Path.of(properties.getStorePath()), durableObjectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public DurableAspect durableAspect(DurableStore durableStore,
                                       ObjectMapper durableObjectMapper) {
        return new DurableAspect(durableStore, durableObjectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public DurableRecovery durableRecovery(DurableStore durableStore,
                                           ObjectMapper durableObjectMapper,
                                           ApplicationContext applicationContext) {
        return new DurableRecovery(durableStore, durableObjectMapper, applicationContext);
    }
}
