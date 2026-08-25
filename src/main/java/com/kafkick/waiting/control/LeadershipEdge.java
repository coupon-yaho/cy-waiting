package com.kafkick.waiting.control;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * 리더가 <b>되는 순간</b>에 한 번 알린다.
 *
 * <p>매 틱 참인 것과 방금 참이 된 것은 다르다. 승계마다 초기화해야 하는 상태를
 * 그 구별 없이 두면, 비리더 구간에 얼어 있던 값을 자기 것으로 이어 쓴다.
 */
public final class LeadershipEdge implements BooleanSupplier {

    private final BooleanSupplier source;
    private final Runnable onGained;
    private final AtomicBoolean held = new AtomicBoolean();

    private LeadershipEdge(BooleanSupplier source, Runnable onGained) {
        this.source = Objects.requireNonNull(source, "source 는 필수다");
        this.onGained = Objects.requireNonNull(onGained, "onGained 는 필수다");
    }

    public static LeadershipEdge of(BooleanSupplier source, Runnable onGained) {
        return new LeadershipEdge(source, onGained);
    }

    @Override
    public boolean getAsBoolean() {
        boolean now = source.getAsBoolean();
        if (!now) {
            held.set(false);
        } else if (held.compareAndSet(false, true)) {
            onGained.run();
        }
        return now;
    }
}
