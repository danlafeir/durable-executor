package com.lafeir.durableexecutor.aspect;

/**
 * How {@code @Durable} treats methods whose return type is asynchronous (a {@code Future},
 * {@code CompletionStage}, or a reactive {@code Publisher}). The framework closes the durable
 * record when the intercepted method returns — for an async method that is when the work is
 * <em>handed off</em>, not when it completes, so the record would be removed while the work is
 * still running and a crash would lose it.
 */
public enum AsyncReturnPolicy {

    /**
     * Default. Reject async {@code @Durable} methods: fail fast at startup (and again if one is
     * somehow invoked) rather than silently dropping durability for them.
     */
    REJECT,

    /**
     * Opt out of the check and run the method as-is. Durability is <strong>not</strong> guaranteed
     * for the async portion — the record is closed at hand-off. The caller is responsible for the
     * async work's reliability.
     */
    ALLOW
}
