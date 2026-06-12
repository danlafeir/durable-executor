package com.lafeir.durableexecutor.recovery;

import java.time.Duration;

/**
 * Recovery retry policy: how many attempts before dead-lettering and how long to back off
 * between them. Backoff is exponential — {@code baseBackoff * multiplier^(attempts-1)} —
 * capped at {@code maxBackoff}.
 */
public record RetryPolicy(int maxAttempts, Duration baseBackoff, double multiplier, Duration maxBackoff) {

    /** True once {@code attempts} failures have used up the budget and the record should be dead-lettered. */
    public boolean exhausted(int attempts) {
        return attempts >= maxAttempts;
    }

    /** Delay before the next attempt, given {@code attempts} failures so far (>= 1). */
    public Duration backoffAfter(int attempts) {
        double millis = baseBackoff.toMillis() * Math.pow(multiplier, attempts - 1);
        long capped = (long) Math.min(millis, maxBackoff.toMillis());
        return Duration.ofMillis(Math.max(1, capped));
    }
}
