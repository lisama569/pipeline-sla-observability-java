package com.github.lisama569.pipelineslaobservabilityjava.model;

/**
 * 维护窗口声明 / 撤销的处理结果。
 *
 * @param status 处理状态
 * @param reason 拒绝原因（仅 REJECTED 时非空）
 * @param window 涉及的窗口（撤销时可能为 null）
 * @param detail 人类可读的判定依据
 */
public record MaintenanceResult(MaintenanceStatus status, MaintenanceRejectReason reason,
                                MaintenanceWindow window, String detail) {

    public static MaintenanceResult of(MaintenanceStatus status, MaintenanceWindow window, String detail) {
        return new MaintenanceResult(status, null, window, detail);
    }

    public static MaintenanceResult rejected(MaintenanceRejectReason reason, String detail) {
        return new MaintenanceResult(MaintenanceStatus.REJECTED, reason, null, detail);
    }
}
