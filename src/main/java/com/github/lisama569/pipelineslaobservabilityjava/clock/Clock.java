package com.github.lisama569.pipelineslaobservabilityjava.clock;

import java.time.Instant;

/**
 * 可注入时钟：生产用系统时钟，测试用可控时钟，保证评估可复现。
 */
public interface Clock {
    Instant now();
}
