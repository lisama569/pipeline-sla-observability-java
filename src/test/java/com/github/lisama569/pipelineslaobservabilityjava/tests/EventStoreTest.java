package com.github.lisama569.pipelineslaobservabilityjava.tests;

import com.github.lisama569.pipelineslaobservabilityjava.config.SlaProperties;
import com.github.lisama569.pipelineslaobservabilityjava.model.EventType;
import com.github.lisama569.pipelineslaobservabilityjava.model.IngestResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.IngestStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.JobRunEvent;
import com.github.lisama569.pipelineslaobservabilityjava.model.RejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.model.RunStatus;
import com.github.lisama569.pipelineslaobservabilityjava.service.EventStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class EventStoreTest {

    private static final Instant T0 = Instant.parse("2026-09-26T00:00:00Z");

    private EventStore store;

    @BeforeEach
    void setUp() {
        SlaProperties props = new SlaProperties();
        props.setRetention(Duration.ofHours(2));
        store = new EventStore(props);
    }

    private JobRunEvent started(String run, Instant scheduled, Instant occurred) {
        return new JobRunEvent("p1", run, EventType.STARTED, null, scheduled, occurred);
    }

    private JobRunEvent finished(String run, RunStatus status, Instant scheduled, Instant occurred) {
        return new JobRunEvent("p1", run, EventType.FINISHED, status, scheduled, occurred);
    }

    private void log(JobRunEvent e, IngestResult r) {
        System.out.println("[EventStoreTest] 输入=" + e + " -> 判定=" + r.status()
                + (r.reason() != null ? "/" + r.reason() : "") + " 依据=" + r.detail());
    }

    @Test
    void duplicateReportIsIdempotent() {
        JobRunEvent e = started("r1", T0, T0.plusSeconds(5));
        IngestResult first = store.ingest(e);
        log(e, first);
        assertEquals(IngestStatus.ACCEPTED, first.status());

        IngestResult second = store.ingest(e);
        log(e, second);
        assertEquals(IngestStatus.DUPLICATE, second.status());
        assertNull(second.reason());
        // 重复上报不得重复计数
        assertEquals(1, store.eventsInWindow("p1", T0.minusSeconds(60), T0.plusSeconds(60)).size());
    }

    @Test
    void conflictingReportIsRejected() {
        log(started("r1", T0, T0.plusSeconds(5)), store.ingest(started("r1", T0, T0.plusSeconds(5))));
        JobRunEvent conflict = started("r1", T0, T0.plusSeconds(7)); // 同键不同 occurredAt
        IngestResult r = store.ingest(conflict);
        log(conflict, r);
        assertEquals(IngestStatus.REJECTED, r.status());
        assertEquals(RejectReason.CONFLICTING_EVENT, r.reason());
        assertEquals(1, store.eventsInWindow("p1", T0.minusSeconds(60), T0.plusSeconds(60)).size());
    }

    @Test
    void outOfOrderEventsAreAcceptedAndSorted() {
        IngestResult r1 = store.ingest(finished("r1", RunStatus.SUCCESS, T0, T0.plusSeconds(100)));
        log(finished("r1", RunStatus.SUCCESS, T0, T0.plusSeconds(100)), r1);
        assertEquals(IngestStatus.ACCEPTED, r1.status());

        // 乱序：STARTED 晚到，但事件时间早于 FINISHED —— 合法，按事件时间归位
        JobRunEvent lateStart = started("r1", T0, T0.plusSeconds(50));
        IngestResult r2 = store.ingest(lateStart);
        log(lateStart, r2);
        assertEquals(IngestStatus.ACCEPTED, r2.status());

        var events = store.eventsInWindow("p1", T0, T0.plusSeconds(200));
        assertEquals(2, events.size());
        assertEquals(EventType.STARTED, events.get(0).eventType(), "应按事件时间升序");
        assertEquals(EventType.FINISHED, events.get(1).eventType());
    }

    @Test
    void clockRegressionContradictionIsRejected() {
        log(started("r1", T0, T0.plusSeconds(100)), store.ingest(started("r1", T0, T0.plusSeconds(100))));
        // FINISHED 的事件时间早于已接受的 STARTED —— 时钟回拨矛盾
        JobRunEvent badFinish = finished("r1", RunStatus.SUCCESS, T0, T0.plusSeconds(50));
        IngestResult r = store.ingest(badFinish);
        log(badFinish, r);
        assertEquals(IngestStatus.REJECTED, r.status());
        assertEquals(RejectReason.EVENT_TIME_REGRESSION, r.reason());
    }

    @Test
    void missingStatusAndUnknownPipelineAreDistinguished() {
        JobRunEvent noStatus = new JobRunEvent("p1", "r1", EventType.FINISHED, null, T0, T0);
        IngestResult r1 = store.ingest(noStatus);
        log(noStatus, r1);
        assertEquals(RejectReason.MISSING_STATUS, r1.reason());

        SlaProperties strict = new SlaProperties();
        strict.setRequireRegisteredPipelines(true);
        strict.getPipelines().put("registered", new SlaProperties.PipelineRule());
        EventStore strictStore = new EventStore(strict);
        JobRunEvent unknown = new JobRunEvent("ghost", "r1", EventType.STARTED, null, T0, T0);
        IngestResult r2 = strictStore.ingest(unknown);
        log(unknown, r2);
        assertEquals(RejectReason.UNKNOWN_PIPELINE, r2.reason());
    }

    @Test
    void eventsBeyondRetentionAreRejectedAndStorageIsBounded() {
        // 推进水位线到 T0+2h
        log(started("r-new", T0.plus(Duration.ofHours(2)), T0.plus(Duration.ofHours(2))),
                store.ingest(started("r-new", T0.plus(Duration.ofHours(2)), T0.plus(Duration.ofHours(2)))));
        // 迟到事件：事件时间早于 水位线-retention(2h)=T0
        JobRunEvent tooLate = started("r-old", T0.minusSeconds(10), T0.minusSeconds(10));
        IngestResult r = store.ingest(tooLate);
        log(tooLate, r);
        assertEquals(IngestStatus.REJECTED, r.status());
        assertEquals(RejectReason.BEYOND_RETENTION, r.reason());
        assertEquals(1, store.eventsInWindow("p1", Instant.EPOCH, Instant.now().plusSeconds(86400)).size(),
                "保留下界之前不得留存事件");
    }
}
