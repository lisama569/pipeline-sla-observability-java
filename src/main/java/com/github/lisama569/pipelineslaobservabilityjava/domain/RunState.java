package com.github.lisama569.pipelineslaobservabilityjava.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 单次运行的聚合生命周期状态（可变，仅在所属流水线锁内访问）。
 */
public final class RunState {

    private final String pipelineId;
    private final String runId;
    private RunStatus status = RunStatus.RUNNING;
    private Instant scheduledTime;
    private Instant startedAt;
    private Instant finishedAt;
    private EventType terminalType;
    private long firstIngestSeq;
    private long lastIngestSeq;
    private final List<String> eventIds = new ArrayList<>();

    public RunState(String pipelineId, String runId) {
        this.pipelineId = pipelineId;
        this.runId = runId;
    }

    public String getPipelineId() { return pipelineId; }
    public String getRunId() { return runId; }
    public RunStatus getStatus() { return status; }
    public void setStatus(RunStatus status) { this.status = status; }
    public Instant getScheduledTime() { return scheduledTime; }
    public void setScheduledTime(Instant scheduledTime) { this.scheduledTime = scheduledTime; }
    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public Instant getFinishedAt() { return finishedAt; }
    public void setFinishedAt(Instant finishedAt) { this.finishedAt = finishedAt; }
    public EventType getTerminalType() { return terminalType; }
    public void setTerminalType(EventType terminalType) { this.terminalType = terminalType; }
    public long getFirstIngestSeq() { return firstIngestSeq; }
    public void setFirstIngestSeq(long firstIngestSeq) { this.firstIngestSeq = firstIngestSeq; }
    public long getLastIngestSeq() { return lastIngestSeq; }
    public void setLastIngestSeq(long lastIngestSeq) { this.lastIngestSeq = lastIngestSeq; }
    public List<String> getEventIds() { return eventIds; }
}
