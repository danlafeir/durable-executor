package com.lafeir.durableexecutor.annotation;

import java.lang.annotation.*;

/**
 * Marks a method for durable execution.
 *
 * When a @Durable method is called its arguments are persisted (open).
 * When it completes successfully the record is removed (close).
 * On process restart any open records are recovered by re-invoking the method.
 *
 * The method must be on a managed bean so that the framework integration can intercept it.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Durable {

    /**
     * Optional stable execution ID. When empty a UUID is generated per invocation.
     * Supply a fixed or derived value to correlate a recovery record with a specific
     * logical operation. Concurrent live calls with the same ID are not deduplicated —
     * idempotency of the method body is the caller's responsibility.
     * Note: IDs must not end with {@code -deleted} (reserved for the commit-marker suffix).
     */
    String executionId() default "";

    /**
     * Close-path behaviour on successful execution. Defaults to {@link CloseMode#TRANSACTIONAL}.
     *
     * @see CloseMode
     */
    CloseMode closeMode() default CloseMode.TRANSACTIONAL;

    /**
     * Controls how a successfully completed durable execution's store record is removed.
     *
     * <p><b>{@link #TRANSACTIONAL} (default):</b> two-phase close. The record is atomically
     * renamed to a commit-marker file ({@code {id}-deleted.msgpack}) before the backing
     * file is removed. A JVM crash between those two steps leaves the marker behind;
     * recovery routes it to the dead letter queue rather than retrying. Prefer this when
     * the method is <em>not</em> safely idempotent — a DLQ entry is better than a double
     * execution.
     *
     * <p><b>{@link #IDEMPOTENT}:</b> single-step close. The record is deleted directly.
     * A JVM crash between the method returning and the delete completing causes one extra
     * retry on the next startup. Prefer this when the method <em>is</em> idempotent —
     * a retry is harmless and you avoid DLQ noise from stuck commit-marker files.
     */
    enum CloseMode {
        TRANSACTIONAL,
        IDEMPOTENT
    }
}
