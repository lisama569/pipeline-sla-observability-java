package com.github.lisama569.pipelineslaobservabilityjava.clock;

import java.time.Instant;
import org.springframework.stereotype.Component;

/**
 * 墙上时钟实现。
 */
@Component
public class SystemClock implements Clock {
    @Override
    public Instant now() {
        return Instant.now();
    }
}
