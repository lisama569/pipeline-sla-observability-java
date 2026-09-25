package com.github.lisama569.pipelineslaobservabilityjava.tests.support;

import com.github.lisama569.pipelineslaobservabilityjava.clock.Clock;
import java.time.Instant;

/** 测试用可控时钟：显式推进/回拨，保证评估结论可复现。 */
public class MutableClock implements Clock {

    private Instant now;

    public MutableClock(Instant now) {
        this.now = now;
    }

    @Override
    public Instant now() {
        return now;
    }

    public void set(Instant instant) {
        this.now = instant;
    }

    public void advanceSeconds(long seconds) {
        this.now = this.now.plusSeconds(seconds);
    }

    /** 模拟时钟回拨。 */
    public void rewindSeconds(long seconds) {
        this.now = this.now.minusSeconds(seconds);
    }
}
