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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Retries open DurableExecution records on startup and every 5 minutes.
 *
 * The periodic trigger runs on a dedicated single-thread ScheduledExecutorService so
 * long-running retries cannot starve the scheduler. Retry work runs on a separate
 * fixed-thread pool sized by durable.retry-threads.
 *
 * An in-flight set prevents the same execution from being submitted to the retry pool
 * twice across concurrent scheduler ticks.
 *
 * Stuck-deleted executions ({id}-deleted.msgpack files older than the configured grace
 * period) are routed to the dead letter queue rather than retried.
 */
public class DurableRecovery implements ApplicationListener<ApplicationReadyEvent>, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(DurableRecovery.class);
    private static final long RETRY_INTERVAL_MINUTES = 5;

    private final DurableStore pendingStore;
    private final DurableStore deadLetterStore;
    private final ObjectMapper objectMapper;
    private final ApplicationContext applicationContext;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService retryExecutor;
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();

    public DurableRecovery(DurableStore pendingStore,
                           DurableStore deadLetterStore,
                           ObjectMapper objectMapper,
                           ApplicationContext applicationContext,
                           ScheduledExecutorService scheduler,
                           ExecutorService retryExecutor) {
        this.pendingStore = pendingStore;
        this.deadLetterStore = deadLetterStore;
        this.objectMapper = objectMapper;
        this.applicationContext = applicationContext;
        this.scheduler = scheduler;
        this.retryExecutor = retryExecutor;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        runRecovery("startup");
        scheduler.scheduleAtFixedRate(
                () -> runRecovery("scheduled"),
                RETRY_INTERVAL_MINUTES, RETRY_INTERVAL_MINUTES, TimeUnit.MINUTES);
    }

    private void runRecovery(String trigger) {
        DurableStore.StoreScan scan = pendingStore.scan();
        drainStuckDeleted(scan.stuckDeleted());
        if (!scan.pending().isEmpty()) {
            log.info("[{}] Submitting {} pending execution(s) for retry", trigger, scan.pending().size());
            scan.pending().values().forEach(this::scheduleRetry);
        }
    }

    private void scheduleRetry(DurableExecution execution) {
        if (inFlight.add(execution.getExecutionId())) {
            try {
                retryExecutor.submit(() -> {
                    try {
                        retryExecution(execution);
                    } finally {
                        inFlight.remove(execution.getExecutionId());
                    }
                });
            } catch (RejectedExecutionException e) {
                inFlight.remove(execution.getExecutionId());
                log.debug("Retry submission rejected for {} (executor shutting down)", execution.getExecutionId());
            }
        }
    }

    private void drainStuckDeleted(Map<String, DurableExecution> stuck) {
        if (stuck.isEmpty()) {
            return;
        }
        log.warn("Found {} stuck-deleted execution(s); moving to DLQ", stuck.size());
        for (DurableExecution execution : stuck.values()) {
            // Write to DLQ first (safe to retry via REPLACE_EXISTING if finalize fails)
            try {
                deadLetterStore.save(execution);
            } catch (Exception e) {
                log.error("Failed to write execution {} to DLQ, will retry next cycle", execution.getExecutionId(), e);
                continue;
            }
            // Remove the stuck file — if this fails, the next cycle will overwrite the DLQ
            // entry (harmless) and retry the cleanup
            try {
                pendingStore.finalizeDelete(execution.getExecutionId());
                log.info("Stuck execution {} moved to DLQ", execution.getExecutionId());
            } catch (Exception e) {
                log.warn("Stuck execution {} written to DLQ but source file not removed; will retry cleanup next cycle",
                        execution.getExecutionId(), e);
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
        // getMethod() returns only public methods; setAccessible is unnecessary and
        // throws InaccessibleObjectException on named modules in JDK 17+
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
        retryExecutor.shutdown();
        if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
            scheduler.shutdownNow();
        }
        if (!retryExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
            retryExecutor.shutdownNow();
        }
    }
}
