package com.kafkick.waiting.adapter.redis;

import io.lettuce.core.resource.Delay;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 상한까지 두 배씩 늘고, 늘 절반은 지키고 절반만 흔드는 재연결 지연. <b>라이브러리 것을 안 쓴다</b> —
 * 그쪽은 0 부터 난수를 뽑은 뒤 상한으로 잘라, 시도가 쌓이면 전 노드가 상한 한 박자에 몰린다.
 */
final class CappedJitterDelay extends Delay {

    /** 두 배로 늘리는 횟수의 상한. 이보다 크면 곱이 넘친다. */
    private static final int MAX_DOUBLINGS = 30;

    private final long baseNanos;
    private final long capNanos;

    private CappedJitterDelay(Duration base, Duration cap) {
        this.baseNanos = Objects.requireNonNull(base, "base 는 필수다").toNanos();
        this.capNanos = Objects.requireNonNull(cap, "cap 은 필수다").toNanos();
        if (baseNanos <= 0 || capNanos < baseNanos) {
            throw new IllegalArgumentException("0 < base <= cap 이어야 한다: base=%s cap=%s"
                    .formatted(base, cap));
        }
    }

    static CappedJitterDelay of(Duration base, Duration cap) {
        return new CappedJitterDelay(base, cap);
    }

    @Override
    public Duration createDelay(long attempt) {
        int doublings = (int) Math.clamp(attempt - 1, 0, MAX_DOUBLINGS);
        long ceiling = Math.min(capNanos, baseNanos << doublings);
        long half = ceiling / 2;
        return Duration.ofNanos(half + ThreadLocalRandom.current().nextLong(ceiling - half + 1));
    }
}
