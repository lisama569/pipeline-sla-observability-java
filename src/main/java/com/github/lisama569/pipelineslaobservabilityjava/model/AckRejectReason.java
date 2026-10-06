package com.github.lisama569.pipelineslaobservabilityjava.model;

/** 告警认领被拒绝的可区分原因。 */
public enum AckRejectReason {
    /** 认领人缺失。 */
    MISSING_ACK_BY,
    /** 告警不存在。 */
    ALERT_NOT_FOUND,
    /** 告警已恢复，不允许再认领。 */
    ALERT_ALREADY_RESOLVED
}
