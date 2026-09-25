package com.github.lisama569.pipelineslaobservabilityjava.store;

import com.github.lisama569.pipelineslaobservabilityjava.domain.PipelineConfig;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * 流水线注册表与全局接收序号分配器。
 * 每条流水线一把锁：上报与评估在同一锁内串行化，跨流水线完全并发。
 */
@Component
public class PipelineRegistry {

    private final ConcurrentHashMap<String, PipelineState> pipelines = new ConcurrentHashMap<>();
    /** 全局单调接收序号，不依赖墙上时钟，时钟回拨不影响先后判定 */
    private final AtomicLong ingestSequence = new AtomicLong(0);

    public long nextIngestSeq() {
        return ingestSequence.incrementAndGet();
    }

    public PipelineState register(PipelineConfig config) {
        PipelineState state = pipelines.computeIfAbsent(
                config.pipelineId(), id -> new PipelineState(config));
        synchronized (state) {
            state.setConfig(config);
        }
        return state;
    }

    public Optional<PipelineState> find(String pipelineId) {
        return Optional.ofNullable(pipelines.get(pipelineId));
    }

    public List<PipelineState> all() {
        return pipelines.values().stream().toList();
    }

    /** 在某条流水线的锁内执行并返回结果。 */
    public <T> T withLock(String pipelineId, java.util.function.Function<PipelineState, T> action) {
        PipelineState state = pipelines.get(pipelineId);
        if (state == null) {
            return null;
        }
        synchronized (state) {
            return action.apply(state);
        }
    }
}
