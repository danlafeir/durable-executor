package com.github.danlafeir.durableexecutor;

import com.github.danlafeir.durableexecutor.model.DurableExecution;
import com.github.danlafeir.durableexecutor.store.DurableStore;
import com.github.danlafeir.durableexecutor.web.DurableDeadLetterController;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.LinkedHashMap;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class DurableDeadLetterControllerTest {

    @Mock
    DurableStore deadLetterStore;

    MockMvc mockMvc;

    @BeforeEach
    void setup() {
        mockMvc = MockMvcBuilders.standaloneSetup(new DurableDeadLetterController(deadLetterStore)).build();
    }

    @Test
    void returnsEmptyListWhenDlqIsEmpty() throws Exception {
        when(deadLetterStore.loadAll()).thenReturn(new LinkedHashMap<>());

        mockMvc.perform(get("/durable/dlq"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    void returnsDlqEntries() throws Exception {
        var dlq = new LinkedHashMap<String, DurableExecution>();
        dlq.put("exec-1", new DurableExecution(
                "exec-1",
                "com.example.OrderService",
                "processOrder",
                new String[]{"java.lang.String"},
                new byte[][]{},
                Instant.parse("2026-01-01T00:00:00Z")
        ));
        when(deadLetterStore.loadAll()).thenReturn(dlq);

        mockMvc.perform(get("/durable/dlq"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].executionId").value("exec-1"))
                .andExpect(jsonPath("$[0].targetClassName").value("com.example.OrderService"))
                .andExpect(jsonPath("$[0].methodName").value("processOrder"));
    }
}
