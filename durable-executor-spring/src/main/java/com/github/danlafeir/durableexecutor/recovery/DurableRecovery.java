package com.github.danlafeir.durableexecutor.recovery;

import com.github.danlafeir.durableexecutor.aspect.DurableAspect;
import com.github.danlafeir.durableexecutor.model.DurableExecution;
import com.github.danlafeir.durableexecutor.store.DurableStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Retries open DurableExecution records on startup and every 5 minutes.
 *
 * Startup recovery runs synchronously on the ApplicationReadyEvent thread.
 * Scheduled recovery submits each retry to the thread pool so retries run
 * concurrently without blocking the scheduler. An in-flight set prevents the
 * same execution from being retried twice simultaneously across scheduler ticks.
 *
 * If a retry fails (either path), the record is moved to the dead letter store
 * and removed from the pending store.
 */
public class DurableRecovery implements ApplicationListener<ApplicationReadyEvent>, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(DurableRecovery.class);
    private static final long RETRY_INTERVAL_MINUTES = 5;

    private final DurableStore pendingStore;
    private final DurableStore deadLetterStore;
    private final ObjectMapper objectMapper;
    private final ApplicationContext applicationContext;
    private final ScheduledExecutorService scheduler;
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();

    public DurableRecovery(DurableStore pendingStore,
                           DurableStore deadLetterStore,
                           ObjectMapper objectMapper,
                           ApplicationContext applicationContext,
                           ScheduledExecutorService scheduler) {
        this.pendingStore = pendingStore;
        this.deadLetterStore = deadLetterStore;
        this.objectMapper = objectMapper;
        this.applicationContext = applicationContext;
        this.scheduler = scheduler;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        drainStuckDeleted();
        Map<String, DurableExecution> pending = pendingStore.loadAll();
        if (!pending.isEmpty()) {
            log.info("Recovering {} pending durable execution(s) on startup", pending.size());
            pending.values().forEach(this::retryExecution);
        }
        scheduler.scheduleAtFixedRate(this::runScheduledRecovery, RETRY_INTERVAL_MINUTES, RETRY_INTERVAL_MINUTES, TimeUnit.MINUTES);
    }

    private void runScheduledRecovery() {
        drainStuckDeleted();
        Map<String, DurableExecution> pending = pendingStore.loadAll();
        if (pending.isEmpty()) {
            return;
        }
        log.info("Scheduled check found {} pending durable execution(s)", pending.size());
        for (DurableExecution execution : pending.values()) {
            if (inFlight.add(execution.getExecutionId())) {
                scheduler.submit(() -> {
                    try {
                        retryExecution(execution);
                    } finally {
                        inFlight.remove(execution.getExecutionId());
                    }
                });
            }
        }
    }

    private void drainStuckDeleted() {
        Map<String, DurableExecution> stuck = pendingStore.loadAllDeleted();
        if (stuck.isEmpty()) {
            return;
        }
        log.warn("Found {} stuck-deleted execution(s); method completed but cleanup did not — moving to DLQ", stuck.size());
        for (DurableExecution execution : stuck.values()) {
            try {
                deadLetterStore.save(execution);
                pendingStore.finalizeDelete(execution.getExecutionId());
                log.info("Stuck execution {} moved to DLQ", execution.getExecutionId());
            } catch (Exception e) {
                log.error("Failed to move stuck execution {} to DLQ", execution.getExecutionId(), e);
            }
        }
    }

    private void retryExecution(DurableExecution execution) {
        try {
            log.info("Retrying execution {} → {}.{}()",
                    execution.getExecutionId(), execution.getTargetClassName(), execution.getMethodName());
            recover(execution);
        } catch (Exception e) {
            log.error("Execution {} failed, moving to dead letter queue. Cause: {}",
                    execution.getExecutionId(), e.getMessage(), e);
            sendToDeadLetter(execution);
        }
    }

    private void sendToDeadLetter(DurableExecution execution) {
        try {
            deadLetterStore.save(execution);
            pendingStore.delete(execution.getExecutionId());
            log.info("Execution {} moved to dead letter queue", execution.getExecutionId());
        } catch (Exception e) {
            log.error("Failed to move execution {} to dead letter queue", execution.getExecutionId(), e);
        }
    }

    private void recover(DurableExecution execution) throws Exception {
        Class<?> targetClass = Class.forName(execution.getTargetClassName());
        Class<?>[] paramTypes = resolveParamTypes(execution.getParameterTypeNames());
        Method method = targetClass.getMethod(execution.getMethodName(), paramTypes);
        Object[] args = deserializeArgs(execution.getSerializedArgs(), paramTypes);
        Object bean = applicationContext.getBean(targetClass);
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

    private Object[] deserializeArgs(byte[][] serialized, Class<?>[] types) throws Exception {
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

    @Override
    public void destroy() throws InterruptedException {
        scheduler.shutdown();
        if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
            scheduler.shutdownNow();
        }
    }
}
