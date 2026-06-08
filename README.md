# durable-executor

Durable execution for JVM applications. Persists in-flight method invocations to disk so they survive process crashes and are automatically retried on next startup.

## Modules

| Module | Artifact | Description |
|--------|----------|-------------|
| `durable-executor-core` | `com.github.danlafeir:durable-executor-core` | Framework-agnostic core: `@Durable` annotation, execution model, file-backed store |
| `durable-executor-spring` | `com.github.danlafeir:durable-executor-spring` | Spring Boot integration: AOP aspect, autoconfiguration, startup recovery |

## How it works

1. A `@Durable` method is called — arguments are serialized and written to a store file (open)
2. The method executes normally
3. On success the record is atomically committed and cleaned up (close)
4. If the process crashes mid-execution the record remains; on next startup `DurableRecovery` re-invokes the method with the original arguments

Records that fail every recovery attempt are moved to a dead letter queue (DLQ) rather than retried indefinitely.

## Spring Boot usage

```kotlin
// build.gradle.kts
dependencies {
    implementation("com.github.danlafeir:durable-executor-spring:0.1.0")
}
```

```xml
<!-- pom.xml -->
<dependency>
    <groupId>com.github.danlafeir</groupId>
    <artifactId>durable-executor-spring</artifactId>
    <version>0.1.0</version>
</dependency>
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
  store-path: /var/data/durable-executions   # default: ./durable-executions
  dead-letter-path: /var/data/durable-dlq    # default: ./durable-dlq
  retry-threads: 2                           # thread pool size for recovery
  stuck-grace-period: PT30S                  # age threshold for stuck-deleted files

  dlq-endpoint:
    enabled: true                            # expose GET /durable/dlq (default: false)
    path: /durable/dlq                       # endpoint base path
```

`stuck-grace-period` controls how long a commit-marker file (`{id}-deleted.msgpack`) must exist before it is treated as a crash remnant and routed to the DLQ. This window protects against a live `finalizeDelete()` call being misread as a crash.

### Plain Spring (no Boot)

```java
@Configuration
@EnableDurableExecution
public class AppConfig { }
```

### Stable execution IDs

By default a UUID is generated per invocation. Supply a fixed or derived value to correlate a recovery record with a specific logical operation:

```java
@Durable(executionId = "daily-reconciliation")
public void reconcile() { ... }
```

Concurrent live calls with the same ID are not deduplicated — idempotency of the method body is the caller's responsibility. IDs must not end with `-deleted` (reserved for the internal commit-marker suffix).

## Requirements

- Java 21+
- Spring Boot 3.x (for the Spring module)

## Testing

`durable-executor-spring` ships with integration tests that run against a real file store:

| Test | What it covers |
|------|----------------|
| `DurableExecutionTest` | Happy path, failure retention, recovery re-invocation, stuck-deleted → DLQ, failed-recovery → DLQ |
| `DurableDeadLetterControllerTest` | DLQ HTTP endpoint response shape |

### End-to-end chaos validation

[spring-durable-executor-sample](../spring-durable-executor-sample) is an order-processing service that demonstrates and chaos-tests this library against a real Kubernetes cluster. It runs a four-step workflow per order and uses `chaos.sh` to kill random pods (and periodically Postgres) while orders are in-flight, then validates with `validate.sh` that every order eventually reaches `FULFILLED` and all durable stores are empty.

The sample exercises the two-layer `@Durable` pattern: one annotation on the dispatcher (covers the window before the 202 goes out) and one on the processing method (covers the long-running workflow). See the [sample README](../spring-durable-executor-sample/README.md) for setup and chaos test instructions.

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
