package com.lafeir.durableexecutor.recovery;

import com.lafeir.durableexecutor.aspect.DurableAspect;
import com.lafeir.durableexecutor.model.DurableExecution;
import com.lafeir.durableexecutor.store.DurableStore;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;

import org.springframework.util.ClassUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Retries open DurableExecution records on startup and every 5 minutes.
 *
 * A failed attempt increments the record's attempt count and, if the {@link RetryPolicy}
 * budget is not yet exhausted, schedules the next attempt after an exponential backoff;
 * the new attempt count and due time are persisted so backoff survives a restart. Only once
 * the attempts are exhausted is the record moved to the dead letter queue. The periodic scan
 * acts as a crash-safety net that re-picks up any record whose due time has passed.
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
    private final RetryPolicy retryPolicy;
    private final Duration dlqRetention;
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();

    /**
     * Dedicated heartbeat thread, created only in SHARED_STORE mode. Kept off {@code scheduler}
     * so a slow {@code runRecovery} (full directory scan + inline DLQ writes, e.g. on NFS) can't
     * stall lease renewal long enough for this instance's live leases to lapse and be reclaimed.
     */
    private ScheduledExecutorService heartbeatScheduler;

    public DurableRecovery(DurableStore pendingStore,
                           DurableStore deadLetterStore,
                           ObjectMapper objectMapper,
                           ApplicationContext applicationContext,
                           ScheduledExecutorService scheduler,
                           ExecutorService retryExecutor,
                           RetryPolicy retryPolicy,
                           Duration dlqRetention) {
        this.pendingStore = pendingStore;
        this.deadLetterStore = deadLetterStore;
        this.objectMapper = objectMapper;
        this.applicationContext = applicationContext;
        this.scheduler = scheduler;
        this.retryExecutor = retryExecutor;
        this.retryPolicy = retryPolicy;
        this.dlqRetention = dlqRetention;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        retryExecutor.submit(() -> runRecovery("startup"));
        scheduler.scheduleAtFixedRate(
                () -> runRecovery("scheduled"),
                RETRY_INTERVAL_MINUTES, RETRY_INTERVAL_MINUTES, TimeUnit.MINUTES);
        if (pendingStore.isShared()) {
            long heartbeatMillis = Math.max(1, pendingStore.getLeaseDuration().toMillis() / 3);
            heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
            heartbeatScheduler.scheduleAtFixedRate(
                    this::renewLeases, heartbeatMillis, heartbeatMillis, TimeUnit.MILLISECONDS);
        }
    }

    private void renewLeases() {
        try {
            pendingStore.renewLeases();
        } catch (Exception e) {
            log.warn("Lease renewal cycle failed", e);
        }
    }

    private void runRecovery(String trigger) {
        DurableStore.StoreScan scan = pendingStore.scan();
        drainStuckDeleted(scan.stuckDeleted());
        purgeExpiredDeadLetters();
        if (!scan.pending().isEmpty()) {
            log.info("[{}] Submitting {} pending execution(s) for retry", trigger, scan.pending().size());
            scan.pending().values().forEach(this::scheduleRetry);
        }
    }

    private void purgeExpiredDeadLetters() {
        if (dlqRetention == null) {
            return;
        }
        int purged = deadLetterStore.purgeOlderThan(dlqRetention);
        if (purged > 0) {
            log.info("Purged {} dead-letter entr(ies) older than {}", purged, dlqRetention);
        }
    }

    private void scheduleRetry(DurableExecution execution) {
        Instant nextAttemptAt = execution.getNextAttemptAt();
        if (nextAttemptAt != null && nextAttemptAt.isAfter(Instant.now())) {
            return; // backing off — not due yet
        }
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
        } catch (DurableTargetUnresolvableException e) {
            // Permanent failure — retrying cannot resolve a method that no longer exists. Dead-letter
            // immediately rather than burning the retry budget; the entry can be requeued after the
            // signature is restored.
            log.error("{} Dead-lettering {} without retry.", e.getMessage(), execution.getExecutionId());
            sendToDeadLetter(execution);
        } catch (Exception e) {
            Throwable cause = e instanceof InvocationTargetException ite && ite.getCause() != null
                    ? ite.getCause() : e;
            handleFailedAttempt(execution, cause, e);
        }
    }

    private void handleFailedAttempt(DurableExecution execution, Throwable cause, Exception raw) {
        String id = execution.getExecutionId();
        execution.setAttempts(execution.getAttempts() + 1);
        if (retryPolicy.exhausted(execution.getAttempts())) {
            log.error("Execution {} failed after {} attempt(s), moving to dead letter queue. Cause: {}",
                    id, execution.getAttempts(), cause.getMessage(), raw);
            sendToDeadLetter(execution);
            return;
        }
        Duration backoff = retryPolicy.backoffAfter(execution.getAttempts());
        execution.setNextAttemptAt(Instant.now().plus(backoff));
        try {
            // Persist the new attempt count and due time so the backoff survives a restart;
            // the periodic scan re-picks it up if the in-memory schedule below is lost.
            pendingStore.save(execution);
        } catch (Exception persistError) {
            log.error("Failed to persist retry state for {}; it will be retried on the next scan", id, persistError);
            return;
        }
        log.warn("Execution {} failed (attempt {}/{}); retrying in {}. Cause: {}",
                id, execution.getAttempts(), retryPolicy.maxAttempts(), backoff, cause.getMessage());
        scheduleNextAttempt(execution, backoff);
    }

    private void scheduleNextAttempt(DurableExecution execution, Duration backoff) {
        try {
            scheduler.schedule(() -> scheduleRetry(execution), backoff.toMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            log.debug("Next retry not scheduled for {} (scheduler shutting down); the recovery scan will resume it",
                    execution.getExecutionId());
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
        // In SHARED_STORE mode, claim the record before re-invoking. If a concurrent instance
        // won the claim, skip quietly (no exception → not dead-lettered); the owner will run it.
        if (!pendingStore.claim(execution.getExecutionId())) {
            log.debug("Execution {} claimed by another instance; skipping recovery", execution.getExecutionId());
            return;
        }
        Class<?> targetClass;
        Method method;
        Object bean;
        try {
            targetClass = Class.forName(execution.getTargetClassName());
            Class<?>[] paramTypes = resolveParamTypes(execution.getParameterTypeNames());
            method = targetClass.getMethod(execution.getMethodName(), paramTypes);
            bean = applicationContext.getBean(targetClass);
        } catch (ClassNotFoundException | NoSuchMethodException | NoSuchBeanDefinitionException e) {
            // Only genuinely permanent resolution failures. Do NOT widen to BeansException — a
            // transient BeanCreationException must keep falling through to the retry path below.
            // We claimed the lease above but will never invoke (no aspect to release it), so release
            // it here before dead-lettering rather than leaving it for the orphan sweep.
            pendingStore.releaseLease(execution.getExecutionId());
            throw new DurableTargetUnresolvableException(execution, e);
        }
        // Deserialize against the method's *generic* parameter types, not the erased classes, so
        // List<Order> comes back as List<Order> rather than List<LinkedHashMap>. @JsonTypeInfo-
        // annotated parameter types also round-trip to their concrete subtype.
        Object[] args = deserializeArgs(execution.getSerializedArgs(), method.getGenericParameterTypes());
        // setAccessible is required when the method's declaring class has non-public
        // visibility (e.g. a public method inside a package-private enclosing type).
        // On named-module deployments it may throw InaccessibleObjectException — we
        // log a warning and proceed; method.invoke() will fail with a clear error if
        // the method truly isn't accessible.
        try {
            method.setAccessible(true);
        } catch (RuntimeException e) {
            log.warn("Could not setAccessible on {}; recovery may fail if the method is not otherwise accessible. "
                    + "Ensure @Durable methods are in exported packages when using named modules.", method);
        }
        DurableAspect.RECOVERY_EXECUTION_ID.set(execution.getExecutionId());
        try {
            method.invoke(bean, args);
        } finally {
            DurableAspect.RECOVERY_EXECUTION_ID.remove();
        }
    }

    private Class<?>[] resolveParamTypes(String[] typeNames) throws ClassNotFoundException {
        ClassLoader classLoader = applicationContext.getClassLoader();
        Class<?>[] types = new Class<?>[typeNames.length];
        for (int i = 0; i < typeNames.length; i++) {
            types[i] = ClassUtils.forName(typeNames[i], classLoader);
        }
        return types;
    }

    private Object[] deserializeArgs(byte[][] serialized, Type[] genericTypes) throws Exception {
        Object[] args = new Object[serialized.length];
        for (int i = 0; i < serialized.length; i++) {
            JavaType javaType = objectMapper.getTypeFactory().constructType(genericTypes[i]);
            args[i] = objectMapper.readValue(serialized[i], javaType);
        }
        return args;
    }

    /** A record's @Durable target class/method/bean no longer resolves — a permanent (non-retryable) failure. */
    static class DurableTargetUnresolvableException extends RuntimeException {
        DurableTargetUnresolvableException(DurableExecution execution, Throwable cause) {
            super(String.format(
                    "@Durable target %s.%s(%s) could not be resolved — it was likely renamed, removed, or "
                    + "re-signatured by a deploy (record created %s). The record cannot be recovered; drain "
                    + "in-flight @Durable executions before changing their signatures.",
                    execution.getTargetClassName(), execution.getMethodName(),
                    String.join(", ", execution.getParameterTypeNames()), execution.getCreatedAt()), cause);
        }
    }

    @Override
    public void destroy() throws InterruptedException {
        if (heartbeatScheduler != null) {
            heartbeatScheduler.shutdownNow();
        }
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
