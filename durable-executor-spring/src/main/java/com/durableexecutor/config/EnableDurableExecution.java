package com.durableexecutor.config;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Activates durable execution support on a @Configuration class.
 *
 * Spring Boot users get this automatically via autoconfiguration.
 * Plain Spring users should add this annotation to their root @Configuration class:
 *
 *   @Configuration
 *   @EnableDurableExecution
 *   public class AppConfig { ... }
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(DurableAutoConfiguration.class)
public @interface EnableDurableExecution {}
