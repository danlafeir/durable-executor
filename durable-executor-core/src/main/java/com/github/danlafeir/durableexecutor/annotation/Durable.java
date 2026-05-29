package com.github.danlafeir.durableexecutor.annotation;

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
     * Supply a fixed or derived value to make the execution idempotent across
     * re-deliveries (only the first open record wins).
     */
    String executionId() default "";
}
