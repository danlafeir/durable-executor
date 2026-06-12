package com.lafeir.durableexecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lafeir.durableexecutor.annotation.Durable;
import com.lafeir.durableexecutor.aspect.AsyncReturnPolicy;
import com.lafeir.durableexecutor.aspect.DurableAspect;
import com.lafeir.durableexecutor.aspect.DurableAsyncReturnValidator;
import com.lafeir.durableexecutor.config.DurableAutoConfiguration;
import com.lafeir.durableexecutor.store.DurableStore;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class DurableAsyncReturnPolicyTest {

    @TempDir
    Path tmp;

    // ---- end-to-end: real autoconfiguration + startup contract ----

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(DurableAutoConfiguration.class))
                .withUserConfiguration(AsyncConfig.class)
                .withPropertyValues(
                        "durable.store-path=" + tmp.resolve("store"),
                        "durable.dead-letter-path=" + tmp.resolve("dlq"));
    }

    @Test
    void contextFailsToStartWhenAnAsyncDurableMethodIsPresentUnderReject() {
        runner().run(ctx -> {
            assertThat(ctx).hasFailed();
            assertThat(ctx.getStartupFailure()).hasStackTraceContaining("doAsync");
        });
    }

    @Test
    void contextStartsWhenPolicyIsAllow() {
        runner().withPropertyValues("durable.async-return-policy=allow")
                .run(ctx -> assertThat(ctx).hasNotFailed());
    }

    @Configuration
    static class AsyncConfig {
        @Bean
        AsyncBean asyncBean() {
            return new AsyncBean();
        }
    }

    // ---- startup validator (BeanPostProcessor) ----

    @Test
    void rejectFailsStartupForAsyncDurableMethod() {
        var validator = new DurableAsyncReturnValidator(AsyncReturnPolicy.REJECT);

        assertThatThrownBy(() -> validator.postProcessAfterInitialization(new AsyncBean(), "asyncBean"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("doAsync")
                .hasMessageContaining("CompletableFuture");
    }

    @Test
    void rejectAllowsSynchronousDurableMethods() {
        var validator = new DurableAsyncReturnValidator(AsyncReturnPolicy.REJECT);
        Object bean = new SyncBean();

        assertThat(validator.postProcessAfterInitialization(bean, "syncBean")).isSameAs(bean);
    }

    @Test
    void allowPermitsAsyncDurableMethod() {
        var validator = new DurableAsyncReturnValidator(AsyncReturnPolicy.ALLOW);
        Object bean = new AsyncBean();

        assertThat(validator.postProcessAfterInitialization(bean, "asyncBean")).isSameAs(bean);
    }

    // ---- aspect invocation guard (defence in depth) ----

    @Test
    void aspectRejectsAnAsyncMethodBeforeOpeningARecord() throws Throwable {
        DurableStore store = mock(DurableStore.class);
        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        MethodSignature signature = mock(MethodSignature.class);
        when(joinPoint.getSignature()).thenReturn(signature);
        when(signature.getReturnType()).thenReturn(CompletableFuture.class);
        when(signature.toShortString()).thenReturn("AsyncBean.doAsync()");

        DurableAspect aspect = new DurableAspect(store, mock(ObjectMapper.class), AsyncReturnPolicy.REJECT);

        assertThatThrownBy(() -> aspect.around(joinPoint, mock(Durable.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("async-return-policy=allow");
        verifyNoInteractions(store);    // never opened a durable record
        verify(joinPoint, never()).proceed(); // never ran the method
    }

    // ---- fixtures ----

    static class AsyncBean {
        @Durable
        public CompletableFuture<String> doAsync() {
            return CompletableFuture.completedFuture("x");
        }
    }

    static class SyncBean {
        @Durable
        public void doSync() {}

        @Durable
        public String doSyncValue() {
            return "x";
        }
    }
}
