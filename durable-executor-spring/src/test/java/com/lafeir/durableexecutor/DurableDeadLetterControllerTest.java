package com.lafeir.durableexecutor;

import com.lafeir.durableexecutor.config.DurableAutoConfiguration;
import com.lafeir.durableexecutor.model.DurableExecution;
import com.lafeir.durableexecutor.store.DurableStore;
import com.lafeir.durableexecutor.web.DurableDeadLetterController;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class DurableDeadLetterControllerTest {

    @Mock
    DurableStore pendingStore;

    @Mock
    DurableStore deadLetterStore;

    MockMvc mockMvc;

    @BeforeEach
    void setup() {
        mockMvc = MockMvcBuilders.standaloneSetup(
                new DurableDeadLetterController(pendingStore, deadLetterStore)).build();
    }

    private DurableExecution execution(String id) {
        return new DurableExecution(id, "com.example.OrderService", "processOrder",
                new String[]{"java.lang.String"}, new byte[][]{}, Instant.parse("2026-01-01T00:00:00Z"));
    }

    @Test
    void returnsEmptyListWhenDlqIsEmpty() throws Exception {
        when(deadLetterStore.listIds()).thenReturn(List.of());

        mockMvc.perform(get("/durable/dlq"))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Total-Count", "0"))
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    void returnsDlqEntries() throws Exception {
        when(deadLetterStore.listIds()).thenReturn(List.of("exec-1"));
        when(deadLetterStore.loadById("exec-1")).thenReturn(execution("exec-1"));

        mockMvc.perform(get("/durable/dlq"))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Total-Count", "1"))
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].executionId").value("exec-1"))
                .andExpect(jsonPath("$[0].methodName").value("processOrder"));
    }

    @Test
    void paginatesAndLoadsOnlyTheRequestedPage() throws Exception {
        when(deadLetterStore.listIds()).thenReturn(List.of("a", "b", "c"));
        when(deadLetterStore.loadById("b")).thenReturn(execution("b"));

        mockMvc.perform(get("/durable/dlq").param("offset", "1").param("limit", "1"))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Total-Count", "3"))
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].executionId").value("b"));

        verify(deadLetterStore, never()).loadById("a");
        verify(deadLetterStore, never()).loadById("c");
    }

    @Test
    void requeueMovesEntryToPendingWithAFreshRetryBudget() throws Exception {
        DurableExecution dead = execution("exec-1");
        dead.setAttempts(5);
        dead.setNextAttemptAt(Instant.parse("2026-02-01T00:00:00Z"));
        when(deadLetterStore.loadById("exec-1")).thenReturn(dead);

        mockMvc.perform(post("/durable/dlq/exec-1/requeue"))
                .andExpect(status().isAccepted());

        ArgumentCaptor<DurableExecution> saved = ArgumentCaptor.forClass(DurableExecution.class);
        verify(pendingStore).save(saved.capture());
        assertThat(saved.getValue().getAttempts()).isZero();
        assertThat(saved.getValue().getNextAttemptAt()).isNull();
        verify(deadLetterStore).delete("exec-1");
    }

    @Test
    void requeueReturns404WhenEntryIsMissing() throws Exception {
        when(deadLetterStore.loadById("missing")).thenReturn(null);

        mockMvc.perform(post("/durable/dlq/missing/requeue"))
                .andExpect(status().isNotFound());

        verify(pendingStore, never()).save(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void deleteRemovesTheEntry() throws Exception {
        mockMvc.perform(delete("/durable/dlq/exec-1"))
                .andExpect(status().isNoContent());

        verify(deadLetterStore).delete("exec-1");
    }

    @Test
    void theEndpointBeanWiresUpThroughAutoconfigWhenEnabledInAWebApp(@TempDir Path tmp) {
        new WebApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(DurableAutoConfiguration.class))
                .withPropertyValues(
                        "durable.dlq-endpoint.enabled=true",
                        "durable.store-path=" + tmp.resolve("store"),
                        "durable.dead-letter-path=" + tmp.resolve("dlq"))
                .run(ctx -> assertThat(ctx).hasSingleBean(DurableDeadLetterController.class));
    }
}
