package com.github.lisama569.pipelineslaobservabilityjava.web;

import com.github.lisama569.pipelineslaobservabilityjava.domain.IngestResult;
import com.github.lisama569.pipelineslaobservabilityjava.ingestion.EventReport;
import com.github.lisama569.pipelineslaobservabilityjava.ingestion.IngestService;
import java.util.List;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 事件上报接口（骨架）。 */
@RestController
@RequestMapping("/api/v1/events")
public class IngestController {

    private final IngestService ingestService;

    public IngestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @PostMapping
    public IngestResult report(@RequestBody EventReport report) {
        return ingestService.report(report);
    }

    @PostMapping("/batch")
    public List<IngestResult> reportBatch(@RequestBody List<EventReport> reports) {
        return ingestService.reportBatch(reports);
    }
}
