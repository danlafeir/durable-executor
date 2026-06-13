# durable-executor

Durable execution for JVM applications. Persists in-flight method invocations to disk so they survive process crashes and are automatically retried on next startup.

## Modules

| Module | Artifact | Description |
|--------|----------|-------------|
| `durable-executor-core` | `com.lafeir:durable-executor-core` | Framework-agnostic core: `@Durable` annotation, execution model, file-backed store |
| `durable-executor-spring` | `com.lafeir:durable-executor-spring` | Spring Boot integration: AOP aspect, autoconfiguration, startup recovery |

## How it works

1. A `@Durable` method is called — arguments are serialized and written to a store file (open)
2. The method executes normally
3. On success the record is atomically committed and cleaned up (close)
4. If the process crashes mid-execution the record remains; on next startup `DurableRecovery` re-invokes the method with the original arguments

Records that fail every recovery attempt are moved to a dead letter queue (DLQ) rather than retried indefinitely.

### Method arguments

Arguments are serialized with Jackson (MessagePack) and, on recovery, deserialized against the method's declared **generic** parameter types — so `List<Order>`, `Map<String, Order>`, and the like are reconstructed with their concrete element types rather than raw `LinkedHashMap`s. Arguments must be Jackson-serializable. For a parameter whose declared type is a supertype, annotate the base type with `@JsonTypeInfo` so the concrete subtype round-trips. Without it: an interface or abstract declared type can't be constructed and the record is dead-lettered, while a concrete supertype deserializes as the base type, silently dropping subtype-specific fields.

### Changing a @Durable signature across deploys

A record stores the target's class name, method name, and parameter types, and recovery re-invokes it by reflection. If a deploy **renames, removes, or re-signatures** a `@Durable` method (or removes its bean), in-flight records for it can no longer be resolved — they are dead-lettered immediately (no wasted retries) with a message naming the unresolvable target. After restoring the method you can [requeue](#dead-letter-queue) those entries. To avoid this, drain in-flight `@Durable` executions before changing a signature, or keep the old signature until they clear.

## Spring Boot usage

```kotlin
// build.gradle.kts
dependencies {
    implementation("com.lafeir:durable-executor-spring:0.1.0")
}
```

```xml
<!-- pom.xml -->
<dependency>
    <groupId>com.lafeir</groupId>
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
  coordination: single-instance              # single-instance (default) | shared-store
  lease-duration: PT1M                       # shared-store only: lease TTL (default 1m)
  visibility-lag: PT5S                       # shared-store only: bounded delay before one instance sees another's lease write — Δ (default 5s)
  clock-skew: PT1S                           # shared-store only: assumed clock-skew bound between instances (default 1s)
  max-attempts: 5                            # recovery attempts before DLQ (default 5)
  retry-backoff: PT30S                       # base backoff delay (default 30s)
  retry-backoff-multiplier: 2.0              # backoff growth per attempt (default 2.0)
  retry-backoff-max: PT5M                    # backoff ceiling (default 5m)
  async-return-policy: reject                # reject (default) | allow

  dlq-retention: P7D                         # purge DLQ entries older than this (default: keep forever)

  dlq-endpoint:
    enabled: true                            # expose the DLQ HTTP endpoint (default: false)
    path: /durable/dlq                       # endpoint base path
```

`stuck-grace-period` controls how long a commit-marker file (`{id}-deleted.msgpack`) must exist before it is treated as a crash remnant and routed to the DLQ. This window protects against a live `finalizeDelete()` call being misread as a crash.

### Persistent storage

The store is only as durable as the directory behind it. The default `store-path`/`dead-letter-path` are **relative** and resolve under the JVM working directory — in a container that is the ephemeral writable layer, so records are lost on restart and "durable" execution is an illusion. **Mount a persistent volume and set absolute paths to it.** On startup the library warns when a configured path is relative or under `/tmp`, and **fails fast** if the directory is not writable (a non-writable store cannot persist anything).

### Retry policy

A recovery attempt that fails does **not** immediately dead-letter the record. The attempt count is incremented and persisted, and the next attempt is scheduled after an exponential backoff — `retry-backoff × retry-backoff-multiplier^(attempt-1)`, capped at `retry-backoff-max`. Only once `max-attempts` is reached does the record move to the DLQ. The defaults retry at 30s, 1m, 2m, 4m, then dead-letter on the fifth failure.

Backoff state lives on the record, so it survives a restart: the next attempt resumes from the persisted due time rather than starting over. The DLQ entry includes the final `attempts` count.

### Dead letter queue

Records that exhaust their retries land in the DLQ. When `dlq-endpoint.enabled=true`, the following endpoints are exposed under `dlq-endpoint.path` (default `/durable/dlq`):

| Method & path | Effect |
|---------------|--------|
| `GET {path}?offset=&limit=` | List entries (paged; default `limit=100`). Ids are listed cheaply and only the requested page is deserialized. The total is returned in the `X-Total-Count` header. |
| `POST {path}/{id}/requeue` | Move an entry back to the pending store with a fresh retry budget (`attempts` reset) so recovery retries it. Use after fixing the cause. Returns `202`, or `404` if the id is unknown. |
| `DELETE {path}/{id}` | Discard an entry. Returns `204`. |

`dlq-retention` auto-purges entries older than the configured age (by file modification time) on the periodic recovery cycle; unset (the default) keeps them forever.

> **Secure the endpoint.** It is disabled by default and exposes internal class/method names; `requeue` and `delete` mutate state. When you enable it, protect the path with your application's security (e.g. Spring Security) — the library does not authenticate requests.

### Synchronous methods only

`@Durable` closes the record when the intercepted method **returns**. For a method that returns an asynchronous handle — `Future`, `CompletionStage`/`CompletableFuture`, or a reactive `Publisher` (`Mono`/`Flux`) — that is when the work is *handed off*, not when it finishes, so the record would be removed while the work is still running and a crash would lose it.

`async-return-policy` controls this:

| Value | Behaviour |
|-------|-----------|
| `reject` (default) | The application **fails to start** if any `@Durable` method returns an async type, with an error naming the method. Make the method synchronous (do the work before returning), or move `@Durable` to a synchronous method that wraps it. |
| `allow` | Run the method as-is and log a startup warning. Durability is **not** guaranteed for the async portion — the record is closed at hand-off. Use only when the async work is reliable by other means. |

### Coordination mode

A pending `{id}.msgpack` file means one of two things — an execution that crashed and needs recovery, or one that is running *right now* — and the file alone cannot distinguish them. `coordination` selects how the recovery scan tells them apart, so you can match it to your deployment topology:

| Mode | Use when | How |
|------|----------|-----|
| `single-instance` (default) | One active writer per store directory — a `ReadWriteOnce` volume, a dedicated disk, or a single replica. | Liveness is tracked in memory. Deterministic, zero on-disk overhead. The recovery scan never re-runs an execution that is still in-flight in this process. |
| `shared-store` | Multiple replicas sharing one store directory — a `ReadWriteMany` / NFS volume. | Each in-flight record is stamped with a `{id}.lease` file (owner + heartbeat-renewed expiry). Another instance skips a record whose lease is still valid and only reclaims it once the lease has been expired for the full takeover margin — `lease-duration + visibility-lag + clock-skew` after the owner last heartbeat — so a slow-to-propagate renewal from a still-live owner can't trigger a premature takeover. |

> **`shared-store` is best-effort, not race-free.** File-based coordination over a shared filesystem has inherent check-then-act windows and stale-read behaviour (notably on NFS), so a narrow window of cross-instance double execution remains possible. For strict exactly-once *across instances*, run `single-instance` behind an external lock (a leader election, a database advisory lock, etc.) so only one replica is ever active against a given store.

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

### Close mode

`@Durable` supports two close strategies, controlled by `closeMode`:

| Mode | Default | Close mechanism | JVM-crash-after-success result |
|------|---------|-----------------|-------------------------------|
| `TRANSACTIONAL` | ✓ | Atomic rename to commit-marker, then delete | Marker file → DLQ entry (no retry) |
| `IDEMPOTENT` | | Direct delete of the pending record | Pending file → one retry on next startup |

```java
// Default — prefers DLQ entry over double execution (for non-idempotent methods)
@Durable
public void chargeCard(String orderId, BigDecimal amount) { ... }

// Explicit idempotent — prefers retry over DLQ noise (for safe-to-repeat methods)
@Durable(closeMode = Durable.CloseMode.IDEMPOTENT)
public void sendWelcomeEmail(String userId) { ... }
```

Both modes guarantee **at-least-once execution**. The difference is what happens in the narrow window between a method returning and the close operation completing: `TRANSACTIONAL` routes that case to the DLQ; `IDEMPOTENT` routes it to a retry.

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
