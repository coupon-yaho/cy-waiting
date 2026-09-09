package com.kafkick.waiting.control;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * 문을 잠글 때까지는 리더로 안 친다.
 *
 * <p>잠금이 끝나기 전에 회차가 돌면, 새 리더가 안 만지는 쿠폰에 유령의 지연된 몫이
 * 그대로 들어간다 — 같은 초에 두 리더의 몫이 나가면 초과 발급이다.
 */
public final class SealGate implements BooleanSupplier {

    private final BooleanSupplier leader;

    /**
     * 시작한 잠금의 세대. <b>참·거짓으로는 못 센다</b> — 승계가 잦으면 첫 잠금이
     * 끝나기 전에 다음 승계가 오고, 그때 첫 잠금의 완료가 문을 열어 버린다.
     */
    private final AtomicLong started = new AtomicLong();

    /** 끝난 잠금의 세대. 시작한 것보다 뒤처져 있으면 아직 잠그는 중이다. */
    private final AtomicLong finished = new AtomicLong();

    private SealGate(BooleanSupplier leader) {
        this.leader = Objects.requireNonNull(leader, "leader 는 필수다");
    }

    public static SealGate of(BooleanSupplier leader) {
        return new SealGate(leader);
    }

    /**
     * 잠그기 시작했다. 끝날 때까지 이 노드는 배분을 안 돈다.
     *
     * @return 이 잠금의 세대. {@link #sealed(long)} 에 그대로 넘긴다
     */
    public long sealing() {
        return started.incrementAndGet();
    }

    /**
     * 잠금이 끝났다. <b>못 잠가도 연다</b> — 여기서 멈추면 아무도 배분을 안 돌아
     * 줄이 통째로 멎고, 못 잠근 쿠폰은 적용이 그 자리에서 다시 막는다.
     */
    public void sealed(long generation) {
        finished.accumulateAndGet(generation, Math::max);
    }

    @Override
    public boolean getAsBoolean() {
        return leader.getAsBoolean() && finished.get() >= started.get();
    }
}
