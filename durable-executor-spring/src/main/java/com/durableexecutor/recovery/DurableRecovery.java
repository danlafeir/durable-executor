package com.durableexecutor.recovery;

import com.durableexecutor.aspect.DurableAspect;
import com.durableexecutor.model.DurableExecution;
import com.durableexecutor.store.DurableStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * On application startup, loads every open DurableExecution record and re-invokes
 * the corresponding Spring bean method so it can complete.
 *
 * Recovery runs after the application context is fully initialised (ApplicationReadyEvent)
 * so all beans are available for lookup.
 *
 * The method is invoked through the Spring proxy (obtained from ApplicationContext.getBean)
 * so @Transactional, @Retry, and other AOP advice still applies. The DurableAspect's
 * RECOVERY_EXECUTION_ID thread-local tells the aspect to reuse the existing record
 * instead of creating a duplicate.
 */
public class DurableRecovery implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger log = LoggerFactory.getLogger(DurableRecovery.class);

    private final DurableStore store;
    private final ObjectMapper objectMapper;
    private final ApplicationContext applicationContext;

    public DurableRecovery(DurableStore store, ObjectMapper objectMapper, ApplicationContext applicationContext) {
        this.store = store;
        this.objectMapper = objectMapper;
        this.applicationContext = applicationContext;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        Map<String, DurableExecution> pending = store.loadAll();
        if (pending.isEmpty()) {
            return;
        }
        log.info("Recovering {} pending durable execution(s)", pending.size());
        for (DurableExecution execution : pending.values()) {
            recoverSafely(execution);
        }
    }

    private void recoverSafely(DurableExecution execution) {
        try {
            log.info("Recovering execution {} → {}.{}()",
                    execution.getExecutionId(), execution.getTargetClassName(), execution.getMethodName());
            recover(execution);
        } catch (Exception e) {
            log.error("Recovery failed for execution {} — will retry on next boot. Cause: {}",
                    execution.getExecutionId(), e.getMessage(), e);
        }
    }

    private void recover(DurableExecution execution) throws Exception {
        Class<?> targetClass = Class.forName(execution.getTargetClassName());
        Class<?>[] paramTypes = resolveParamTypes(execution.getParameterTypeNames());
        Method method = targetClass.getMethod(execution.getMethodName(), paramTypes);

        Object[] args = deserializeArgs(execution.getSerializedArgs(), paramTypes);

        Object bean = applicationContext.getBean(targetClass);

        // setAccessible mirrors what Spring itself does when invoking methods reflectively
        method.setAccessible(true);

        DurableAspect.RECOVERY_EXECUTION_ID.set(execution.getExecutionId());
        try {
            method.invoke(bean, args);
        } finally {
            DurableAspect.RECOVERY_EXECUTION_ID.remove();
        }
    }

    private Class<?>[] resolveParamTypes(String[] typeNames) throws ClassNotFoundException {
        Class<?>[] types = new Class<?>[typeNames.length];
        for (int i = 0; i < typeNames.length; i++) {
            types[i] = resolvePrimitive(typeNames[i]);
        }
        return types;
    }

    private Object[] deserializeArgs(String[] serialized, Class<?>[] types) throws Exception {
        Object[] args = new Object[serialized.length];
        for (int i = 0; i < serialized.length; i++) {
            args[i] = objectMapper.readValue(serialized[i], types[i]);
        }
        return args;
    }

    private Class<?> resolvePrimitive(String name) throws ClassNotFoundException {
        return switch (name) {
            case "int"     -> int.class;
            case "long"    -> long.class;
            case "double"  -> double.class;
            case "float"   -> float.class;
            case "boolean" -> boolean.class;
            case "byte"    -> byte.class;
            case "short"   -> short.class;
            case "char"    -> char.class;
            default        -> Class.forName(name);
        };
    }
}
