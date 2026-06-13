package com.lafeir.durableexecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.lafeir.durableexecutor.model.DurableExecution;
import com.lafeir.durableexecutor.store.DurableStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class DurableStoreDlqOpsTest {

    @TempDir
    Path dir;

    private DurableStore store;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory())
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        store = new DurableStore(dir, objectMapper, Duration.ZERO);
    }

    private DurableExecution execution(String id) {
        return new DurableExecution(id, "C", "m", new String[0], new byte[0][], Instant.now());
    }

    @Test
    void listIdsReturnsSortedIdsWithoutDeserializing() {
        store.save(execution("b"));
        store.save(execution("a"));

        assertThat(store.listIds()).containsExactly("a", "b");
    }

    @Test
    void loadByIdReturnsTheRecordOrNull() {
        store.save(execution("x"));

        assertThat(store.loadById("x")).extracting(DurableExecution::getExecutionId).isEqualTo("x");
        assertThat(store.loadById("missing")).isNull();
    }

    @Test
    void purgeOlderThanRemovesOnlyStaleEntries() throws IOException {
        store.save(execution("old"));
        store.save(execution("fresh"));
        Files.setLastModifiedTime(dir.resolve("old.msgpack"),
                FileTime.from(Instant.now().minusSeconds(3600)));

        int purged = store.purgeOlderThan(Duration.ofMinutes(30));

        assertThat(purged).isEqualTo(1);
        assertThat(store.listIds()).containsExactly("fresh");
    }
}
