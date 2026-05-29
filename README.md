# durable-executor

Durable execution for JVM applications. Persists in-flight method invocations to disk so they survive process crashes and are automatically retried on next startup.

## Modules

| Module | Artifact | Description |
|--------|----------|-------------|
| `durable-executor-core` | `com.durableexecutor:durable-executor-core` | Framework-agnostic core: `@Durable` annotation, execution model, file-backed store |
| `durable-executor-spring` | `com.durableexecutor:durable-executor-spring` | Spring Boot integration: AOP aspect, autoconfiguration, startup recovery |

## How it works

1. A `@Durable` method is called — arguments are serialized and written to a JSON store file (open)
2. The method executes normally
3. On success the record is deleted (close)
4. If the process crashes mid-execution the record remains; on next startup `DurableRecovery` re-invokes the method with the original arguments

## Spring Boot usage

Add the Spring module to your dependencies:

```kotlin
// build.gradle.kts
dependencies {
    implementation("com.durableexecutor:durable-executor-spring:0.1.0")
}
```

Annotate any Spring-managed bean method:

```java
@Service
public class OrderService {

    @Durable
    public void processOrder(String orderId, BigDecimal amount) {
        // safe to crash here — will be retried on next boot
    }
}
```

Spring Boot picks up the autoconfiguration automatically. No further setup required.

### Configuration

```yaml
durable:
  store-path: /var/data/durable-executions.json  # default: ./durable-executions.json
```

### Plain Spring (no Boot)

```java
@Configuration
@EnableDurableExecution
public class AppConfig { }
```

### Stable execution IDs

By default a UUID is generated per invocation. Supply a fixed ID to make the execution idempotent across re-deliveries:

```java
@Durable(executionId = "daily-reconciliation")
public void reconcile() { ... }
```

## Requirements

- Java 21+
- Spring Boot 3.x (for the Spring module)

## Adding a new framework integration

1. Add a new submodule to `settings.gradle.kts`:
   ```kotlin
   include("durable-executor-quarkus")
   ```
2. Depend on `durable-executor-core` for the annotation, model, and store:
   ```kotlin
   dependencies {
       api(project(":durable-executor-core"))
   }
   ```
3. Implement an interceptor and a startup recovery hook using your framework's extension points.
