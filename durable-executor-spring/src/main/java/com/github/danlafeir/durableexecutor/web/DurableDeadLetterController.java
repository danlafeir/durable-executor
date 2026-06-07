package com.github.danlafeir.durableexecutor.web;

import com.github.danlafeir.durableexecutor.model.DurableExecution;
import com.github.danlafeir.durableexecutor.store.DurableStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Collection;

@RestController
public class DurableDeadLetterController {

    private final DurableStore deadLetterStore;

    public DurableDeadLetterController(DurableStore deadLetterStore) {
        this.deadLetterStore = deadLetterStore;
    }

    @GetMapping("${durable.dlq-endpoint.path:/durable/dlq}")
    public Collection<DeadLetterEntry> getDeadLetterQueue() {
        return deadLetterStore.loadAll().values().stream()
                .map(DeadLetterEntry::from)
                .toList();
    }

    public record DeadLetterEntry(
            String executionId,
            String targetClassName,
            String methodName,
            String[] parameterTypeNames,
            Instant createdAt
    ) {
        static DeadLetterEntry from(DurableExecution e) {
            return new DeadLetterEntry(
                    e.getExecutionId(),
                    e.getTargetClassName(),
                    e.getMethodName(),
                    e.getParameterTypeNames(),
                    e.getCreatedAt()
            );
        }
    }
}
