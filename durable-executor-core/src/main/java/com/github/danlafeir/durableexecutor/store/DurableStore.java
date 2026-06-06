package com.github.danlafeir.durableexecutor.store;

import com.github.danlafeir.durableexecutor.model.DurableExecution;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * File-per-execution store for in-flight durable executions.
 *
 * Each execution is written to its own {executionId}.json file inside the store
 * directory. Because each file is independent, concurrent writes from multiple
 * threads require no locking. Writes are atomic (write to .tmp, then rename).
 */
public class DurableStore {

    private static final Logger log = LoggerFactory.getLogger(DurableStore.class);

    private final Path storeDir;
    private final ObjectMapper objectMapper;

    public DurableStore(Path storeDir, ObjectMapper objectMapper) {
        this.storeDir = storeDir;
        this.objectMapper = objectMapper;
    }

    public void save(DurableExecution execution) {
        try {
            Files.createDirectories(storeDir);
            Path tmp = storeDir.resolve(execution.getExecutionId() + ".tmp");
            Path file = storeDir.resolve(execution.getExecutionId() + ".json");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), execution);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new DurableStoreException("Failed to save execution " + execution.getExecutionId(), e);
        }
    }

    public void delete(String executionId) {
        try {
            Files.deleteIfExists(storeDir.resolve(executionId + ".json"));
        } catch (IOException e) {
            log.error("Failed to delete execution {} from store", executionId, e);
        }
    }

    public Map<String, DurableExecution> loadAll() {
        if (!Files.exists(storeDir)) {
            return new LinkedHashMap<>();
        }
        Map<String, DurableExecution> result = new LinkedHashMap<>();
        try (Stream<Path> files = Files.list(storeDir)) {
            files.filter(p -> p.getFileName().toString().endsWith(".json"))
                 .forEach(file -> {
                     try {
                         DurableExecution execution = objectMapper.readValue(file.toFile(), DurableExecution.class);
                         result.put(execution.getExecutionId(), execution);
                     } catch (IOException e) {
                         log.warn("Skipping unreadable execution file {} ({})", file.getFileName(), e.getMessage());
                     }
                 });
        } catch (IOException e) {
            log.error("Failed to list durable store directory {}", storeDir, e);
        }
        return result;
    }

    public static class DurableStoreException extends RuntimeException {
        public DurableStoreException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
