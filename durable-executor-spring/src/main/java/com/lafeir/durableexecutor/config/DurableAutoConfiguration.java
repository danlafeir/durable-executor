package com.lafeir.durableexecutor.config;

import com.lafeir.durableexecutor.aspect.AsyncReturnPolicy;
import com.lafeir.durableexecutor.aspect.DurableAspect;
import com.lafeir.durableexecutor.aspect.DurableAsyncReturnValidator;
import com.lafeir.durableexecutor.recovery.DurableRecovery;
import com.lafeir.durableexecutor.recovery.RetryPolicy;
import com.lafeir.durableexecutor.store.DurableStore;
import com.lafeir.durableexecutor.web.DurableDeadLetterController;
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
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@AutoConfiguration(after = JacksonAutoConfiguration.class)
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
        return new DurableStore(Path.of(properties.getStorePath()), durableObjectMapper,
                properties.getStuckGracePeriod(), properties.getCoordination(),
                UUID.randomUUID().toString(), properties.getLeaseDuration());
    }

    @Bean(name = "durableDeadLetterStore")
    @ConditionalOnMissingBean(name = "durableDeadLetterStore")
    public DurableStore durableDeadLetterStore(DurableProperties properties,
                                               @Qualifier("durableObjectMapper") ObjectMapper durableObjectMapper) {
        return new DurableStore(Path.of(properties.getDeadLetterPath()), durableObjectMapper, properties.getStuckGracePeriod());
    }

    @Bean
    @ConditionalOnMissingBean
    public DurableAspect durableAspect(@Qualifier("durableStore") DurableStore durableStore,
                                       @Qualifier("durableObjectMapper") ObjectMapper durableObjectMapper,
                                       DurableProperties properties) {
        return new DurableAspect(durableStore, durableObjectMapper, properties.getAsyncReturnPolicy());
    }

    /**
     * Reads the policy straight from the Environment (rather than the DurableProperties bean) so the
     * BeanPostProcessor can be created early without pulling regular beans into premature init.
     */
    @Bean
    public static DurableAsyncReturnValidator durableAsyncReturnValidator(Environment environment) {
        String value = environment.getProperty("durable.async-return-policy", "reject").trim();
        AsyncReturnPolicy policy;
        try {
            policy = value.isEmpty() ? AsyncReturnPolicy.REJECT : AsyncReturnPolicy.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid durable.async-return-policy '" + value
                    + "'; expected reject or allow");
        }
        return new DurableAsyncReturnValidator(policy);
    }

    /** Single-thread scheduler — used only for the 5-minute periodic trigger. */
    @Bean(name = "durableScheduler")
    @ConditionalOnMissingBean(name = "durableScheduler")
    public ScheduledExecutorService durableScheduler() {
        return Executors.newSingleThreadScheduledExecutor();
    }

    /** Thread pool used for the actual retry work, sized by durable.retry-threads (default 2). */
    @Bean(name = "durableRetryExecutor")
    @ConditionalOnMissingBean(name = "durableRetryExecutor")
    public ExecutorService durableRetryExecutor(DurableProperties properties) {
        return Executors.newFixedThreadPool(properties.getRetryThreads());
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
                                           @Qualifier("durableScheduler") ScheduledExecutorService durableScheduler,
                                           @Qualifier("durableRetryExecutor") ExecutorService durableRetryExecutor,
                                           DurableProperties properties) {
        RetryPolicy retryPolicy = new RetryPolicy(properties.getMaxAttempts(), properties.getRetryBackoff(),
                properties.getRetryBackoffMultiplier(), properties.getRetryBackoffMax());
        return new DurableRecovery(durableStore, durableDeadLetterStore, durableObjectMapper,
                applicationContext, durableScheduler, durableRetryExecutor, retryPolicy);
    }
}
