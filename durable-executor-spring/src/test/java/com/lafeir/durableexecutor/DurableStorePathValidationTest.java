package com.lafeir.durableexecutor;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lafeir.durableexecutor.config.DurableProperties;
import com.lafeir.durableexecutor.config.DurableStorePathValidator;
import com.lafeir.durableexecutor.store.DurableStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class DurableStorePathValidationTest {

    @TempDir
    Path tmp;

    private String warningsFor(String storePath, String dlqPath) {
        DurableProperties props = new DurableProperties();
        props.setStorePath(storePath);
        props.setDeadLetterPath(dlqPath);

        Logger logger = (Logger) LoggerFactory.getLogger(DurableStorePathValidator.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            new DurableStorePathValidator(props).afterPropertiesSet();
        } finally {
            logger.detachAppender(appender);
        }
        return appender.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.joining("\n"));
    }

    @Test
    void warnsWhenStorePathIsRelative() {
        String warnings = warningsFor("./durable-executions", "/var/data/dlq");

        assertThat(warnings)
                .contains("durable.store-path")
                .contains("relative")
                .contains("persistent volume");
        assertThat(warnings).doesNotContain("durable.dead-letter-path");
    }

    @Test
    void warnsWhenPathIsUnderTmp() {
        String warnings = warningsFor("/tmp/durable", "/var/data/dlq");

        assertThat(warnings).contains("durable.store-path").contains("temporary directory");
    }

    @Test
    void silentWhenPathsAreAbsoluteAndPersistent() {
        assertThat(warningsFor("/var/data/durable", "/var/data/dlq")).isEmpty();
    }

    // ---- writability fail-fast (DurableStore) ----

    @Test
    void constructionSucceedsOnAWritableDirAndLeavesNoProbeFile() throws IOException {
        Path dir = tmp.resolve("store");
        new DurableStore(dir, new ObjectMapper(), Duration.ZERO);

        try (var entries = Files.list(dir)) {
            assertThat(entries).as("the write-probe file is cleaned up").isEmpty();
        }
    }

    @Test
    void constructionFailsFastWhenStoreDirIsNotWritable() throws IOException {
        Path readOnly = Files.createDirectory(tmp.resolve("readonly"));
        assumeTrue(readOnly.toFile().setWritable(false, false), "could not make directory read-only");
        assumeTrue(notActuallyWritable(readOnly), "directory still writable (running as root?); skipping");

        assertThatThrownBy(() -> new DurableStore(readOnly, new ObjectMapper(), Duration.ZERO))
                .isInstanceOf(DurableStore.DurableStoreException.class)
                .hasMessageContaining("not writable");
    }

    private boolean notActuallyWritable(Path dir) {
        try {
            Path probe = Files.createTempFile(dir, "x", null);
            Files.deleteIfExists(probe);
            return false;
        } catch (IOException expected) {
            return true;
        }
    }
}
