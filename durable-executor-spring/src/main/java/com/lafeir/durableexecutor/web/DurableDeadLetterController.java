package com.lafeir.durableexecutor.web;

import com.lafeir.durableexecutor.model.DurableExecution;
import com.lafeir.durableexecutor.store.DurableStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

@RestController
public class DurableDeadLetterController {

    private static final Logger log = LoggerFactory.getLogger(DurableDeadLetterController.class);

    private final DurableStore pendingStore;
    private final DurableStore deadLetterStore;

    public DurableDeadLetterController(DurableStore pendingStore, DurableStore deadLetterStore) {
        this.pendingStore = pendingStore;
        this.deadLetterStore = deadLetterStore;
    }

    /** Paged listing. Lists ids cheaply, then deserializes only the requested page. */
    @GetMapping("${durable.dlq-endpoint.path:/durable/dlq}")
    public ResponseEntity<List<DeadLetterEntry>> getDeadLetterQueue(
            @RequestParam(name = "offset", defaultValue = "0") int offset,
            @RequestParam(name = "limit", defaultValue = "100") int limit) {
        List<String> ids = deadLetterStore.listIds();
        List<DeadLetterEntry> page = ids.stream()
                .skip(Math.max(0, offset))
                .limit(Math.max(0, limit))
                .map(deadLetterStore::loadById)
                .filter(Objects::nonNull)
                .map(DeadLetterEntry::from)
                .toList();
        return ResponseEntity.ok().header("X-Total-Count", Integer.toString(ids.size())).body(page);
    }

    /** Moves a dead-lettered record back to the pending store with a fresh retry budget. */
    @PostMapping("${durable.dlq-endpoint.path:/durable/dlq}/{id}/requeue")
    public ResponseEntity<Void> requeue(@PathVariable("id") String id) {
        DurableExecution record = deadLetterStore.loadById(id);
        if (record == null) {
            return ResponseEntity.notFound().build();
        }
        record.setAttempts(0);
        record.setNextAttemptAt(null);
        pendingStore.save(record);   // write to pending first so a crash here cannot lose the record
        deadLetterStore.delete(id);
        log.info("Requeued dead-letter entry {} for recovery", id);
        return ResponseEntity.accepted().build();
    }

    /** Discards a dead-lettered entry. */
    @DeleteMapping("${durable.dlq-endpoint.path:/durable/dlq}/{id}")
    public ResponseEntity<Void> delete(@PathVariable("id") String id) {
        deadLetterStore.delete(id);
        return ResponseEntity.noContent().build();
    }

    public record DeadLetterEntry(
            String executionId,
            String targetClassName,
            String methodName,
            String[] parameterTypeNames,
            Instant createdAt,
            int attempts
    ) {
        static DeadLetterEntry from(DurableExecution e) {
            return new DeadLetterEntry(
                    e.getExecutionId(),
                    e.getTargetClassName(),
                    e.getMethodName(),
                    e.getParameterTypeNames(),
                    e.getCreatedAt(),
                    e.getAttempts()
            );
        }
    }
}
