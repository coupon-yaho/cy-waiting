package com.kafkick.waiting.control;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * 리더가 <b>되는 순간</b>과 <b>잃는 순간</b>에 한 번씩 알린다.
 *
 * <p>승계마다 초기화해야 하는 상태를 그 구별 없이 두면, 비리더 구간에 얼어
 * 있던 값을 자기 것으로 이어 쓴다.
 */
public final class LeadershipEdge implements BooleanSupplier {

    private final BooleanSupplier source;
    private final Runnable onGained;
    private final Runnable onLost;
    private final AtomicBoolean held = new AtomicBoolean();

    private LeadershipEdge(BooleanSupplier source, Runnable onGained, Runnable onLost) {
        this.source = Objects.requireNonNull(source, "source 는 필수다");
        this.onGained = Objects.requireNonNull(onGained, "onGained 는 필수다");
        this.onLost = Objects.requireNonNull(onLost, "onLost 는 필수다");
    }

    public static LeadershipEdge of(BooleanSupplier source, Runnable onGained, Runnable onLost) {
        return new LeadershipEdge(source, onGained, onLost);
    }

    @Override
    public boolean getAsBoolean() {
        boolean now = source.getAsBoolean();
        if (!now) {
            if (held.compareAndSet(true, false)) {
                onLost.run();
            }
        } else if (held.compareAndSet(false, true)) {
            onGained.run();
        }
        return now;
    }
}
