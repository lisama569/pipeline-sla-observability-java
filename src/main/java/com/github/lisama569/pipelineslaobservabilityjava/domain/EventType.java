package com.github.lisama569.pipelineslaobservabilityjava.domain;

/**
 * 作业运行事件类型。
 */
public enum EventType {
    /** 运行开始 */
    STARTED,
    /** 运行成功结束（终态） */
    SUCCEEDED,
    /** 运行失败结束（终态） */
    FAILED
}
