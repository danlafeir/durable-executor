package com.lafeir.durableexecutor;

/**
 * Signals a failure to the {@code @Durable} framework from within a method body,
 * for use when throwing an exception is not the failure signal — e.g. functional
 * styles that return {@code Result}/{@code Either} types.
 *
 * <p>Call {@link #markFailed()} (or {@link #markFailed(String)}) before returning
 * from a {@code @Durable} method. The aspect will leave the durable record open
 * so recovery retries the method on the next startup.
 *
 * <p><strong>Transaction caveat:</strong> unlike throwing an exception,
 * {@code markFailed()} does <em>not</em> roll back an active {@code @Transactional}
 * context. The method has returned normally, so Spring commits the transaction before
 * the aspect inspects the flag. Recovery will re-invoke the method on top of
 * already-committed state. Only use {@code markFailed()} when:
 * <ul>
 *   <li>the method is idempotent and a re-run on committed state is safe, or</li>
 *   <li>you have explicitly marked the transaction for rollback via
 *       {@code TransactionAspectSupport.currentTransactionStatus().setRollbackOnly()}
 *       before returning.</li>
 * </ul>
 */
public final class DurableContext {

    private static final ThreadLocal<String> FAILURE_REASON = new ThreadLocal<>();

    private DurableContext() {}

    /**
     * Signals that the current {@code @Durable} execution should be treated as a failure.
     * The durable record is kept open for recovery retry.
     */
    public static void markFailed() {
        FAILURE_REASON.set("");
    }

    /**
     * Signals failure with a reason string included in the aspect's log output.
     */
    public static void markFailed(String reason) {
        FAILURE_REASON.set(reason != null ? reason : "");
    }

    /** Returns {@code true} if {@link #markFailed()} was called on this thread. */
    public static boolean isMarkedFailed() {
        return FAILURE_REASON.get() != null;
    }

    /** Returns the failure reason, or {@code null} if not marked. */
    public static String getFailureReason() {
        return FAILURE_REASON.get();
    }

    /**
     * Clears the failure signal. Called by {@code DurableAspect} — user code
     * does not need to call this.
     */
    public static void clear() {
        FAILURE_REASON.remove();
    }

    /**
     * Thrown internally by the framework when a method signals {@link #markFailed()} during a
     * recovery re-invocation, so the failure feeds the same attempt-counting and backoff path as
     * a thrown exception. Never thrown back to a live caller — a live {@code markFailed()} returns
     * normally and leaves the record for recovery.
     */
    public static final class MarkedFailedException extends RuntimeException {
        public MarkedFailedException(String reason) {
            super("Durable method signalled failure via markFailed(): " + reason);
        }
    }
}
