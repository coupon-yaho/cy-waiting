package com.kafkick.waiting.control;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * 승계 첫 회차를 앞 리더의 마지막 발행에서 한 틱 떨어뜨린다. <b>정상 인계는 틈이 짧다</b> —
 * 새 리더가 곧 첫 회차를 돌면 두 리더의 한 틱 몫이 1초 안에 겹쳐 뒷단 유입이 두 배가 된다.
 * 리더가 죽은 승계는 마지막 발행이 이미 리스 넘게 지나 기다리지 않는다.
 */
public final class HandoverSpacing implements BooleanSupplier {

    /** 발행 시각이 초 단위로 실린다. 실제 발행은 그 초 안 어디쯤이라 한 초를 더한다. */
    private static final Duration PUBLISHED_ROUNDING = Duration.ofSeconds(1);

    private final Clock clock;
    private final Duration tick;

    /** 이 시각 전에는 리더로 안 친다. 승계마다 새로 건다. */
    private volatile Instant notBefore = Instant.MIN;

    private HandoverSpacing(Clock clock, Duration tick) {
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
        this.tick = Objects.requireNonNull(tick, "tick 은 필수다");
    }

    public static HandoverSpacing of(Clock clock, Duration tick) {
        return new HandoverSpacing(clock, tick);
    }

    /** 리더가 됐다. @param lastPublished 이 노드가 본 마지막 발행 시각. 본 적 없으면 null */
    public void armedFrom(Instant lastPublished) {
        notBefore = lastPublished == null
                ? Instant.MIN : lastPublished.plus(PUBLISHED_ROUNDING).plus(tick);
    }

    @Override
    public boolean getAsBoolean() {
        return !clock.instant().isBefore(notBefore);
    }
}
