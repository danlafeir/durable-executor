package com.github.danlafeir.durableexecutor.aspect;

import com.github.danlafeir.durableexecutor.annotation.Durable;
import com.github.danlafeir.durableexecutor.model.DurableExecution;
import com.github.danlafeir.durableexecutor.store.DurableStore;
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
    private final ObjectMapper objectMapper;

    public DurableAspect(DurableStore store, ObjectMapper objectMapper) {
        this.store = store;
        this.objectMapper = objectMapper;
    }

    @Around("@annotation(durable)")
    public Object around(ProceedingJoinPoint joinPoint, Durable durable) throws Throwable {
        String recoveryId = RECOVERY_EXECUTION_ID.get();
        boolean isRecovery = recoveryId != null;

        String executionId = isRecovery ? recoveryId : resolveId(durable);

        if (!isRecovery) {
            store.save(buildRecord(joinPoint, executionId));
            log.debug("Durable execution opened: {}", executionId);
        }

        try {
            Object result = joinPoint.proceed();
            store.delete(executionId);
            log.debug("Durable execution closed: {}", executionId);
            return result;
        } catch (Throwable t) {
            log.warn("Durable execution {} failed; record kept for recovery. Cause: {}", executionId, t.getMessage());
            throw t;
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

        String[] serializedArgs = new String[args.length];
        String[] paramTypeNames = new String[paramTypes.length];

        for (int i = 0; i < args.length; i++) {
            serializedArgs[i] = objectMapper.writeValueAsString(args[i]);
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
