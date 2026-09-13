package com.kafkick.waiting.control;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * 리더가 <b>되는 순간</b>과 <b>잃는 순간</b>에 한 번씩 알린다. 승계마다 초기화해야
 * 하는 상태를 그 구별 없이 두면, 비리더 구간에 얼어 있던 값을 자기 것으로 이어 쓴다.
 */
public final class LeadershipEdge implements BooleanSupplier {

    private final BooleanSupplier source;
    private final LongSupplier term;
    private final Runnable onGained;
    private final Runnable onLost;
    private final AtomicBoolean held = new AtomicBoolean();

    /** 알린 임기. 0 은 모른다는 뜻이다. */
    private final AtomicLong heldTerm = new AtomicLong();

    private LeadershipEdge(BooleanSupplier source, LongSupplier term, Runnable onGained,
            Runnable onLost) {
        this.source = Objects.requireNonNull(source, "source 는 필수다");
        this.term = Objects.requireNonNull(term, "term 은 필수다");
        this.onGained = Objects.requireNonNull(onGained, "onGained 는 필수다");
        this.onLost = Objects.requireNonNull(onLost, "onLost 는 필수다");
    }

    /**
     * @param term 지금 임기. <b>틱 사이의 짧은 승계는 참·거짓으로 안 보인다</b> — 표본이
     *             틱마다 한 번이라 번호가 바뀐 것으로 가른다. 모르면 0 이다
     */
    public static LeadershipEdge of(BooleanSupplier source, LongSupplier term,
            Runnable onGained, Runnable onLost) {
        return new LeadershipEdge(source, term, onGained, onLost);
    }

    @Override
    public boolean getAsBoolean() {
        boolean now = source.getAsBoolean();
        if (!now) {
            if (held.compareAndSet(true, false)) {
                onLost.run();
            }
            return false;
        }
        long current = term.getAsLong();
        if (held.compareAndSet(false, true)) {
            heldTerm.set(current);
            onGained.run();
            return true;
        }
        // **모르는 번호로는 안 가른다.** 잃는 순간 번호가 먼저 0 이 되는 창이 있어,
        // 거기서 가르면 없는 승계를 만들고 다음 틱에 잃음을 한 번 더 알린다.
        long previous = heldTerm.get();
        if (current > 0 && previous > 0 && current != previous
                && heldTerm.compareAndSet(previous, current)) {
            onLost.run();
            onGained.run();
        } else if (previous <= 0 && current > 0) {
            heldTerm.compareAndSet(previous, current);
        }
        return true;
    }
}
