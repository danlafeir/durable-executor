package com.github.danlafeir.durableexecutor.store;

import com.github.danlafeir.durableexecutor.model.DurableExecution;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Thread-safe, file-backed store for in-flight durable executions.
 *
 * Writes go through an atomic rename (write to tmp, then move) to avoid
 * leaving a partially-written file on crash.
 */
public class DurableStore {

    private static final Logger log = LoggerFactory.getLogger(DurableStore.class);

    private final Path storePath;
    private final ObjectMapper objectMapper;
    private final MapType mapType;

    public DurableStore(Path storePath, ObjectMapper objectMapper) {
        this.storePath = storePath;
        this.objectMapper = objectMapper;
        this.mapType = objectMapper.getTypeFactory()
                .constructMapType(LinkedHashMap.class, String.class, DurableExecution.class);
    }

    public synchronized void save(DurableExecution execution) {
        try {
            Map<String, DurableExecution> all = readFile();
            all.put(execution.getExecutionId(), execution);
            writeFile(all);
        } catch (IOException e) {
            throw new DurableStoreException("Failed to save execution " + execution.getExecutionId(), e);
        }
    }

    public synchronized void delete(String executionId) {
        try {
            Map<String, DurableExecution> all = readFile();
            if (all.remove(executionId) != null) {
                writeFile(all);
            }
        } catch (IOException e) {
            log.error("Failed to delete execution {} from store", executionId, e);
        }
    }

    public synchronized Map<String, DurableExecution> loadAll() {
        try {
            return readFile();
        } catch (IOException e) {
            log.error("Failed to read durable store from {}", storePath, e);
            return new LinkedHashMap<>();
        }
    }

    private Map<String, DurableExecution> readFile() throws IOException {
        if (!Files.exists(storePath)) {
            return new LinkedHashMap<>();
        }
        try {
            return objectMapper.readValue(storePath.toFile(), mapType);
        } catch (IOException e) {
            log.warn("Durable store at {} is unreadable ({}); treating as empty. " +
                    "Original file preserved for manual inspection.", storePath, e.getMessage());
            return new LinkedHashMap<>();
        }
    }

    private void writeFile(Map<String, DurableExecution> executions) throws IOException {
        Path parent = storePath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path tmp = storePath.resolveSibling(storePath.getFileName() + ".tmp");
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), executions);
        Files.move(tmp, storePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    public static class DurableStoreException extends RuntimeException {
        public DurableStoreException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
