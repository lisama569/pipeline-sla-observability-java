package com.github.lisama569.pipelineslaobservabilityjava.model;

/** 告警认领的处理状态。 */
public enum AckStatus {
    /** 认领成功。 */
    ACKNOWLEDGED,
    /** 该告警已被认领过，幂等忽略（不产生额外变化）。 */
    ALREADY_ACKNOWLEDGED,
    /** 告警不存在。 */
    ALERT_NOT_FOUND,
    /** 告警已恢复，拒绝认领。 */
    ALERT_ALREADY_RESOLVED,
    /** 请求参数非法（如认领人为空）。 */
    INVALID_REQUEST
}
