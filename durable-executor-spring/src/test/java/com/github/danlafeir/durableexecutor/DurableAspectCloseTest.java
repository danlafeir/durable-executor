package com.github.danlafeir.durableexecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.danlafeir.durableexecutor.annotation.Durable;
import com.github.danlafeir.durableexecutor.aspect.DurableAspect;
import com.github.danlafeir.durableexecutor.store.DurableStore;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Verifies the close-path branch in DurableAspect: transactional vs. non-transactional.
 * Uses a mocked store so the assertions target which store operations are invoked,
 * not just the end-state (which is "store empty" in both cases).
 */
@ExtendWith(MockitoExtension.class)
class DurableAspectCloseTest {

    @Mock DurableStore store;
    @Mock ObjectMapper objectMapper;
    @Mock ProceedingJoinPoint joinPoint;
    @Mock MethodSignature signature;
    @Mock Durable durable;

    DurableAspect aspect;

    @BeforeEach
    void setup() throws Throwable {
        aspect = new DurableAspect(store, objectMapper);

        when(joinPoint.getSignature()).thenReturn(signature);
        when(signature.getMethod()).thenReturn(DurableAspectCloseTest.class.getDeclaredMethod("target"));
        when(joinPoint.getArgs()).thenReturn(new Object[0]);
        when(joinPoint.getTarget()).thenReturn(this);
        when(durable.executionId()).thenReturn("test-id");
    }

    void target() {}

    @Test
    void transactionalCloseUsesMarkDeletedNeverDirectDelete() throws Throwable {
        doReturn(null).when(joinPoint).proceed();
        when(durable.closeMode()).thenReturn(Durable.CloseMode.TRANSACTIONAL);

        aspect.around(joinPoint, durable);

        verify(store).markDeleted("test-id");
        verify(store, never()).delete(any());
    }

    @Test
    void idempotentCloseDeletesDirectlyNeverMarkDeleted() throws Throwable {
        doReturn(null).when(joinPoint).proceed();
        when(durable.closeMode()).thenReturn(Durable.CloseMode.IDEMPOTENT);

        aspect.around(joinPoint, durable);

        verify(store).delete("test-id");
        verify(store, never()).markDeleted(any());
        verify(store, never()).finalizeDelete(any());
    }

    @Test
    void failurePathIsUnaffectedByCloseMode() throws Throwable {
        doThrow(new RuntimeException("boom")).when(joinPoint).proceed();

        try {
            aspect.around(joinPoint, durable);
        } catch (RuntimeException ignored) {}

        verify(store, never()).delete(any());
        verify(store, never()).markDeleted(any());
    }
}
