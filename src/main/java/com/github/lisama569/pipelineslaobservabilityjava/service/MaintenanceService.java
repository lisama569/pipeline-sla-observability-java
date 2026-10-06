package com.github.lisama569.pipelineslaobservabilityjava.service;

import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceRejectReason;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceResult;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceStatus;
import com.github.lisama569.pipelineslaobservabilityjava.model.MaintenanceWindow;
import com.github.lisama569.pipelineslaobservabilityjava.model.MetricType;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 计划内维护 / 静默窗口登记处。
 *
 * <p>语义约定：
 * <ul>
 *   <li>windowId 为幂等去重键：同 id 同内容重复声明 = DUPLICATE（无副作用），
 *       同 id 不同内容 = 拒绝 CONFLICTING_WINDOW。</li>
 *   <li>是否处于维护按事件时间口径判断（[start, end)），不读取墙钟，
 *       同一批输入事件重放结论一致。</li>
 *   <li>声明与撤销可并发，所有状态迁移在监视器锁内完成，不互相覆盖、不丢数据。</li>
 * </ul>
 */
@Service
public class MaintenanceService {

    private final Map<String, MaintenanceWindow> windowsById = new LinkedHashMap<>();

    /** 声明一个维护窗口（幂等）。 */
    public synchronized MaintenanceResult declare(MaintenanceWindow window) {
        MaintenanceResult invalid = validate(window);
        if (invalid != null) {
            return invalid;
        }
        MaintenanceWindow existing = windowsById.get(window.windowId());
        if (existing != null) {
            if (existing.equals(window)) {
                return MaintenanceResult.of(MaintenanceStatus.DUPLICATE, existing,
                        "同 id 同内容，幂等忽略: " + window.windowId());
            }
            return MaintenanceResult.rejected(MaintenanceRejectReason.CONFLICTING_WINDOW,
                    "同 id 不同内容: " + window.windowId() + " 既有=" + existing + " 新=" + window);
        }
        windowsById.put(window.windowId(), window);
        return MaintenanceResult.of(MaintenanceStatus.DECLARED, window, "已登记: " + window.windowId());
    }

    /** 撤销一个维护窗口；撤销不存在的 id 返回 NOT_FOUND（可区分）。 */
    public synchronized MaintenanceResult revoke(String windowId) {
        if (windowId == null || windowId.isBlank()) {
            return MaintenanceResult.rejected(MaintenanceRejectReason.INVALID_WINDOW, "windowId 为空");
        }
        MaintenanceWindow removed = windowsById.remove(windowId);
        if (removed == null) {
            return MaintenanceResult.of(MaintenanceStatus.NOT_FOUND, null, "窗口不存在: " + windowId);
        }
        return MaintenanceResult.of(MaintenanceStatus.REVOKED, removed, "已撤销: " + windowId);
    }

    /** 当前已登记的全部维护窗口。 */
    public synchronized List<MaintenanceWindow> list() {
        return new ArrayList<>(windowsById.values());
    }

    /** 判断 (pipelineId, metric) 在事件时间 at 是否处于维护静默中。 */
    public boolean isSuppressed(String pipelineId, MetricType metric, Instant at) {
        if (at == null) {
            return false;
        }
        synchronized (this) {
            for (MaintenanceWindow w : windowsById.values()) {
                if (w.covers(pipelineId, metric, at)) {
                    return true;
                }
            }
            return false;
        }
    }

    private MaintenanceResult validate(MaintenanceWindow window) {
        if (window == null
                || window.windowId() == null || window.windowId().isBlank()
                || window.pipelineId() == null || window.pipelineId().isBlank()
                || window.start() == null || window.end() == null) {
            return MaintenanceResult.rejected(MaintenanceRejectReason.INVALID_WINDOW,
                    "必填字段缺失: " + window);
        }
        if (!window.start().isBefore(window.end())) {
            return MaintenanceResult.rejected(MaintenanceRejectReason.INVALID_WINDOW,
                    "维护时间段非法（start 必须早于 end）: start=" + window.start() + " end=" + window.end());
        }
        return null;
    }
}
