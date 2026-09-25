package com.github.lisama569.pipelineslaobservabilityjava.store;

import com.github.lisama569.pipelineslaobservabilityjava.domain.Alert;
import com.github.lisama569.pipelineslaobservabilityjava.domain.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.domain.MetricType;
import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineConfig;
import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineEvaluation;
import com.github.lisama569.pipelineslaobservabilityjava.domain.RunState;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 单条流水线的全部进程内状态。
 * 所有访问必须在 PipelineRegistry 提供的该流水线锁内进行（除构造器中只读字段外）。
 */
public final class PipelineState {

    private volatile PipelineConfig config;

    /** eventId -> 事件，幂等与矛盾检测 */
    private final Map<String, JobRunEvent> eventsById = new HashMap<>();
    /** 按 (eventTime, ingestSeq) 有序的事件，保证乱序写入后评估仍确定 */
    private final TreeMap<EventOrderKey, JobRunEvent> orderedEvents = new TreeMap<>();
    /** runId -> 运行聚合状态 */
    private final Map<String, RunState> runs = new HashMap<>();
    /** 历史评估结论（有界环形淘汰） */
    private final Deque<PipelineEvaluation> evaluations = new ArrayDeque<>();
    /** 未恢复告警：metricType -> Alert，同维度至多一个 */
    private final Map<MetricType, Alert> activeAlerts = new LinkedHashMap<>();
    /** 已恢复告警（有界保留） */
    private final Deque<Alert> recoveredAlerts = new ArrayDeque<>();

    private long evaluationSeq;

    public PipelineState(PipelineConfig config) {
        this.config = config;
    }

    public PipelineConfig getConfig() { return config; }
    public void setConfig(PipelineConfig config) { this.config = config; }
    public Map<String, JobRunEvent> getEventsById() { return eventsById; }
    public TreeMap<EventOrderKey, JobRunEvent> getOrderedEvents() { return orderedEvents; }
    public Map<String, RunState> getRuns() { return runs; }
    public Deque<PipelineEvaluation> getEvaluations() { return evaluations; }
    public Map<MetricType, Alert> getActiveAlerts() { return activeAlerts; }
    public Deque<Alert> getRecoveredAlerts() { return recoveredAlerts; }

    public long getEvaluationSeq() { return evaluationSeq; }
    public long nextEvaluationSeq() { return ++evaluationSeq; }

    /** 有序事件的复合键：eventTime 优先，同刻按接收序号决胜。 */
    public record EventOrderKey(java.time.Instant eventTime, long ingestSeq)
            implements Comparable<EventOrderKey> {
        @Override
        public int compareTo(EventOrderKey o) {
            int c = eventTime.compareTo(o.eventTime);
            return c != 0 ? c : Long.compare(ingestSeq, o.ingestSeq);
        }
    }

    public Optional<PipelineEvaluation> latestEvaluation() {
        return Optional.ofNullable(evaluations.peekLast());
    }

    public List<PipelineEvaluation> evaluationList() {
        return new ArrayList<>(evaluations);
    }
}
