package com.lafeir.durableexecutor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DurableExecution {

    private String executionId;
    private String targetClassName;
    private String methodName;
    private String[] parameterTypeNames;
    private byte[][] serializedArgs;
    private Instant createdAt;
    private int attempts;
    private Instant nextAttemptAt;

    public DurableExecution() {}

    public DurableExecution(
            String executionId,
            String targetClassName,
            String methodName,
            String[] parameterTypeNames,
            byte[][] serializedArgs,
            Instant createdAt) {
        this.executionId = executionId;
        this.targetClassName = targetClassName;
        this.methodName = methodName;
        this.parameterTypeNames = parameterTypeNames;
        this.serializedArgs = serializedArgs;
        this.createdAt = createdAt;
    }

    public String getExecutionId() { return executionId; }
    public void setExecutionId(String executionId) { this.executionId = executionId; }

    public String getTargetClassName() { return targetClassName; }
    public void setTargetClassName(String targetClassName) { this.targetClassName = targetClassName; }

    public String getMethodName() { return methodName; }
    public void setMethodName(String methodName) { this.methodName = methodName; }

    public String[] getParameterTypeNames() { return parameterTypeNames; }
    public void setParameterTypeNames(String[] parameterTypeNames) { this.parameterTypeNames = parameterTypeNames; }

    public byte[][] getSerializedArgs() { return serializedArgs; }
    public void setSerializedArgs(byte[][] serializedArgs) { this.serializedArgs = serializedArgs; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    /** Number of recovery attempts made so far. 0 for a record that has not yet been retried. */
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }

    /** Earliest time the next recovery attempt may run, or null when due immediately. */
    public Instant getNextAttemptAt() { return nextAttemptAt; }
    public void setNextAttemptAt(Instant nextAttemptAt) { this.nextAttemptAt = nextAttemptAt; }
}
