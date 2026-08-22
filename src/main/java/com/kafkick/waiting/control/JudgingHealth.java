package com.kafkick.waiting.control;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * <b>내가 판정할 수 있는가만 본다.</b>
 *
 * <p>의존성 상태를 넣으면 공유 장애가 전 노드 동시 이탈로 번진다. 레디스가
 * 흔들릴 때 전 노드가 한꺼번에 빠지면 100% 장애다 — 낡은 재료로 판정하는 것보다
 * 훨씬 나쁘다.
 */
public final class JudgingHealth implements HealthIndicator {

    private final SnapshotHolder holder;

    /** 종료 신호를 받았나. LB 가 먼저 빼야 진행 중인 요청이 5xx 로 안 샌다. */
    private final AtomicBoolean draining = new AtomicBoolean();

    private JudgingHealth(SnapshotHolder holder) {
        this.holder = Objects.requireNonNull(holder, "holder 는 필수다");
    }

    public static JudgingHealth of(SnapshotHolder holder) {
        return new JudgingHealth(holder);
    }

    public void draining() {
        draining.set(true);
    }

    /**
     * 못 받는 경우는 둘뿐이다 — <b>드레이닝</b>과 <b>판정 재료 없음</b>.
     *
     * <p>받아오기 실패도, 재료가 낡은 것도 여기서 안 뺀다. 공유인지 국소인지
     * 못 가리기 때문이고, 못 가리면 안 빼는 것이 안전한 쪽이다.
     */
    @Override
    public Health health() {
        Health.Builder builder = detailed(draining.get() || holder.isBeforeFirstTick()
                ? Health.outOfService()
                : Health.up());
        return builder.build();
    }

    private Health.Builder detailed(Health.Builder builder) {
        return builder
                .withDetail("coupons", holder.current().coupons().size())
                .withDetail("fetchAgeSec", holder.fetchAge().toSeconds())
                .withDetail("dataAgeSec", holder.dataAge().toSeconds())
                .withDetail("tickAgeSec", tickAgeSeconds())
                .withDetail("fetchStale", holder.isFetchStale())
                .withDetail("dataStale", holder.isDataStale())
                .withDetail("clockAhead", holder.isClockAhead())
                .withDetail("draining", draining.get());
    }

    /** 한 번도 안 돌았으면 무한이라 초로 못 바꾼다. */
    private Object tickAgeSeconds() {
        return holder.isBeforeFirstTick() ? "없음" : holder.tickAge().toSeconds();
    }
}
