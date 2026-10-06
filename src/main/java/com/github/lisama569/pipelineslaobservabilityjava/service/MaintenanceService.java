package com.github.lisama569.pipelineslaobservabilityjava.service;

import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceRejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceWindow;
import com.github.lisama569.pipelineslaobservabilityjava.model.MetricType;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 计划内维护 / 静默窗口管理。
 *
 * <p>语义约定：
 * <ul>
 *   <li>窗口按事件时间口径判断：[start, end)；重放同一批事件得到一致的静默结论，
 *       不读取墙钟。</li>
 *   <li>幂等键 = pipelineId|metric(或 ALL)|start|end，重复声明返回既有记录（DUPLICATE），
 *       不产生第二份。</li>
 *   <li>声明与撤销可并发：全部状态迁移在监视器锁内完成，不会互相覆盖或丢数据。</li>
 *   <li>metricType 为 null 表示静默该流水线的全部指标；指定指标时只静默该指标，
 *       其它指标的告警行为不受影响。</li>
 * </ul>
 */
@Service
public class MaintenanceService {

    private final Object lock = new Object();
    /** windowId -> 窗口；windowId 即幂等键。 */
    private final Map<String, MaintenanceWindow> windows = new LinkedHashMap<>();

    /** 声明一个维护窗口（幂等）。 */
    public MaintenanceResult declare(String pipelineId, MetricType metricType,
                                     Instant start, Instant end, String note) {
        synchronized (lock) {
            if (pipelineId == null || pipelineId.isBlank() || start == null || end == null) {
                return MaintenanceResult.rejected(MaintenanceRejectReason.MISSING_FIELDS,
                        "pipelineId/start/end 为必填: pipelineId=" + pipelineId
                                + " start=" + start + " end=" + end);
            }
            if (!end.isAfter(start)) {
                return MaintenanceResult.rejected(MaintenanceRejectReason.INVALID_TIME_RANGE,
                        "end 必须严格晚于 start: start=" + start + " end=" + end);
            }
            String key = MaintenanceWindow.keyOf(pipelineId, metricType, start, end);
            MaintenanceWindow existing = windows.get(key);
            if (existing != null) {
                return MaintenanceResult.of(MaintenanceStatus.DUPLICATE, existing,
                        "相同维护窗口已存在，幂等忽略: " + key);
            }
            MaintenanceWindow window = new MaintenanceWindow(key, pipelineId, metricType, start, end, note);
            windows.put(key, window);
            return MaintenanceResult.of(MaintenanceStatus.DECLARED, window, "已声明维护窗口: " + key);
        }
    }

    /** 撤销一个维护窗口（按幂等键定位；目标不存在时返回 NOT_FOUND）。 */
    public MaintenanceResult revoke(String pipelineId, MetricType metricType, Instant start, Instant end) {
        synchronized (lock) {
            if (pipelineId == null || pipelineId.isBlank() || start == null || end == null) {
                return MaintenanceResult.rejected(MaintenanceRejectReason.MISSING_FIELDS,
                        "pipelineId/start/end 为必填: pipelineId=" + pipelineId
                                + " start=" + start + " end=" + end);
            }
            String key = MaintenanceWindow.keyOf(pipelineId, metricType, start, end);
            MaintenanceWindow removed = windows.remove(key);
            if (removed == null) {
                return MaintenanceResult.of(MaintenanceStatus.NOT_FOUND, null,
                        "维护窗口不存在，无需撤销: " + key);
            }
            return MaintenanceResult.of(MaintenanceStatus.REVOKED, removed, "已撤销维护窗口: " + key);
        }
    }

    /** 事件时间 at 上，该流水线的该指标是否处于维护静默中。 */
    public boolean isSuppressed(String pipelineId, MetricType metric, Instant at) {
        if (at == null) {
            return false;
        }
        synchronized (lock) {
            for (MaintenanceWindow w : windows.values()) {
                if (w.pipelineId().equals(pipelineId) && w.covers(metric, at)) {
                    return true;
                }
            }
            return false;
        }
    }

    /** 当前生效的维护窗口（可按流水线过滤，按 pipelineId+start 排序，输出确定）。 */
    public List<MaintenanceWindow> list(String pipelineId) {
        synchronized (lock) {
            return windows.values().stream()
                    .filter(w -> pipelineId == null || w.pipelineId().equals(pipelineId))
                    .sorted(Comparator.comparing(MaintenanceWindow::pipelineId)
                            .thenComparing(MaintenanceWindow::start)
                            .thenComparing(MaintenanceWindow::windowId))
                    .toList();
        }
    }
}
