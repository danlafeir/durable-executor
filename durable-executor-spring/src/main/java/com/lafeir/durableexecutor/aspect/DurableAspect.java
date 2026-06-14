package com.lafeir.durableexecutor.aspect;

import com.lafeir.durableexecutor.DurableContext;
import com.lafeir.durableexecutor.annotation.Durable;
import com.lafeir.durableexecutor.coordination.CoordinationStrategy;
import com.lafeir.durableexecutor.model.DurableExecution;
import com.lafeir.durableexecutor.store.DurableStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.UUID;

/**
 * Intercepts @Durable methods and maintains the open/close lifecycle in the DurableStore.
 *
 * Open  (before proceed): serialise arguments and write a DurableExecution record.
 * Close (after proceed):  delete the record on successful completion.
 * Failure:                leave the record in place so recovery can retry on next boot.
 *
 * Recovery re-uses the existing record ID via RECOVERY_EXECUTION_ID so that the aspect
 * does not create a second record for the same logical execution.
 */
@Aspect
public class DurableAspect {

    private static final Logger log = LoggerFactory.getLogger(DurableAspect.class);

    /**
     * Set by DurableRecovery before invoking a recovered method so that the aspect
     * reuses the existing store record instead of creating a new one.
     */
    public static final ThreadLocal<String> RECOVERY_EXECUTION_ID = new ThreadLocal<>();

    private final DurableStore store;
    private final CoordinationStrategy coordination;
    private final ObjectMapper objectMapper;
    private final AsyncReturnPolicy asyncReturnPolicy;

    public DurableAspect(DurableStore store, CoordinationStrategy coordination,
                         ObjectMapper objectMapper, AsyncReturnPolicy asyncReturnPolicy) {
        this.store = store;
        this.coordination = coordination;
        this.objectMapper = objectMapper;
        this.asyncReturnPolicy = asyncReturnPolicy;
    }

    @Around("@annotation(durable)")
    public Object around(ProceedingJoinPoint joinPoint, Durable durable) throws Throwable {
        // Clear any stale signal left by a prior invocation on a pooled thread.
        DurableContext.clear();

        // Defence in depth alongside the startup validator: never open a record for an async method
        // under REJECT, since it would be closed at hand-off and lose the still-running work.
        if (asyncReturnPolicy == AsyncReturnPolicy.REJECT) {
            Class<?> returnType = ((MethodSignature) joinPoint.getSignature()).getReturnType();
            if (AsyncReturnTypes.isAsync(returnType)) {
                throw new IllegalStateException("@Durable method "
                        + joinPoint.getSignature().toShortString() + " returns the asynchronous type "
                        + returnType.getName() + "; only synchronous methods are supported. "
                        + "Set durable.async-return-policy=allow to opt out.");
            }
        }

        String recoveryId = RECOVERY_EXECUTION_ID.get();
        boolean isRecovery = recoveryId != null;
        if (isRecovery) {
            // Consume the recovery id immediately so a nested @Durable call on this same thread
            // starts its own record instead of being treated as a recovery of this (the parent's)
            // execution. The captured locals below drive this invocation's behaviour.
            RECOVERY_EXECUTION_ID.remove();
        }

        String executionId = isRecovery ? recoveryId : resolveId(durable);

        // Mark running before the record exists on disk and keep it marked until the close
        // completes, so a concurrent recovery scan never mistakes this still-running
        // execution for an abandoned one and re-invokes it.
        coordination.markRunning(executionId);
        try {
            if (!isRecovery) {
                // Acquire ownership before the record exists so a sharing instance can't see an
                // unowned pending file and reclaim it in the gap between save and acquire (recovery
                // claims ownership itself before re-invoking, so the recovery path skips this).
                coordination.acquire(executionId);
                store.save(buildRecord(joinPoint, executionId));
                log.debug("Durable execution opened: {}", executionId);
            }

            // Isolate the business method — only method exceptions belong in this catch.
            Object result;
            boolean markedFailed = false;
            String failureReason = null;
            try {
                result = joinPoint.proceed();
                markedFailed = DurableContext.isMarkedFailed();
                failureReason = DurableContext.getFailureReason();
            } catch (Throwable t) {
                log.warn("Durable execution {} failed; record kept for recovery. Cause: {}", executionId, t.getMessage());
                throw t;
            } finally {
                DurableContext.clear();
            }

            // Self-fence before touching the record in any way. If we lost the lease while the method
            // ran — this instance stalled long enough for another to reclaim the record and take it
            // over — that other instance is now authoritative. Anything we do to the record from here
            // (commit, delete, dead-letter, or re-persist a failed attempt) would corrupt its state:
            // steal the commit rename out from under it, or push a record it is actively running into
            // the DLQ. A fenced owner mutates the record in no way and bows out; releaseLease and
            // markNotLive in the finally are owner-checked and safe. This must precede the markedFailed
            // branch — on the recovery path that branch throws, which would otherwise route a fenced
            // owner into handleFailedAttempt (save/delete) against a record it no longer owns. (Bounds
            // at-most-once *ownership*; a side effect already performed in the method body cannot be
            // undone here.)
            if (coordination.isMultiInstance() && !coordination.stillOwns(executionId)) {
                log.warn("Durable execution {} lost its lease while running (this instance likely stalled); "
                        + "another instance is now authoritative — leaving the record untouched.", executionId);
                return result;
            }

            if (markedFailed) {
                String reason = (failureReason != null && !failureReason.isEmpty()) ? failureReason : "(no reason given)";
                if (isRecovery) {
                    // During recovery a markFailed() is a failed attempt — surface it so the retry
                    // policy counts it and eventually dead-letters, rather than re-running forever.
                    log.warn("Durable recovery {} signalled failed via DurableContext; counts as a failed attempt. Reason: {}", executionId, reason);
                    throw new DurableContext.MarkedFailedException(reason);
                }
                log.warn("Durable execution {} signalled failed via DurableContext; record kept for recovery. Reason: {}", executionId, reason);
                return result;
            }

            // Method succeeded — close the record. Close is best-effort cleanup: an I/O error
            // here must not fail the caller (the method already returned) and must not leave a
            // pending record that recovery would re-run.
            if (durable.closeMode() == Durable.CloseMode.TRANSACTIONAL) {
                // Two-phase close: atomic rename to commit-marker, then delete.
                // If the JVM crashes between the two steps the marker is picked up by
                // recovery and routed to the DLQ (no retry). Prefer for non-idempotent methods.
                try {
                    store.markDeleted(executionId);
                    try {
                        store.finalizeDelete(executionId);
                    } catch (Exception e) {
                        // Marker is the commit point; leave it for the stuck-grace scan to route to
                        // the DLQ (no retry). Reverting to pending here would re-run a succeeded method.
                        log.warn("finalizeDelete failed for {}; commit marker left for DLQ reconciliation. Cause: {}",
                                executionId, e.getMessage());
                    }
                } catch (Exception e) {
                    // Could not write the commit marker. The method already succeeded, so delete the
                    // pending record directly so recovery does not re-run it. (Best-effort: if the
                    // store is entirely unwritable this delete also fails and recovery retries.)
                    log.warn("markDeleted failed for {} after the method succeeded; deleting the pending "
                            + "record directly to avoid a re-run. Cause: {}", executionId, e.getMessage());
                    store.delete(executionId);
                }
            } else {
                // Single-step close: direct delete of the pending record.
                // If the JVM crashes before the delete completes the method is retried once.
                // Prefer for idempotent methods — avoids DLQ noise from stuck commit-markers.
                store.delete(executionId);
            }
            log.debug("Durable execution closed: {}", executionId);
            return result;
        } finally {
            coordination.markStopped(executionId);
        }
    }

    private String resolveId(Durable durable) {
        String declared = durable.executionId();
        return declared.isEmpty() ? UUID.randomUUID().toString() : declared;
    }

    private DurableExecution buildRecord(ProceedingJoinPoint joinPoint, String executionId) throws Exception {
        MethodSignature sig = (MethodSignature) joinPoint.getSignature();
        Method method = sig.getMethod();
        Object[] args = joinPoint.getArgs();
        Class<?>[] paramTypes = method.getParameterTypes();

        byte[][] serializedArgs = new byte[args.length][];
        String[] paramTypeNames = new String[paramTypes.length];

        for (int i = 0; i < args.length; i++) {
            serializedArgs[i] = objectMapper.writeValueAsBytes(args[i]);
            paramTypeNames[i] = paramTypes[i].getName();
        }

        Class<?> userClass = org.springframework.util.ClassUtils.getUserClass(joinPoint.getTarget().getClass());

        return new DurableExecution(
                executionId,
                userClass.getName(),
                method.getName(),
                paramTypeNames,
                serializedArgs,
                Instant.now()
        );
    }
}
