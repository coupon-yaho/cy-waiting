package com.kafkick.waiting.control;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
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
    private final ShutdownState shutdown;

    private JudgingHealth(SnapshotHolder holder, ShutdownState shutdown) {
        this.holder = Objects.requireNonNull(holder, "holder 는 필수다");
        this.shutdown = Objects.requireNonNull(shutdown, "shutdown 은 필수다");
    }

    public static JudgingHealth of(SnapshotHolder holder, ShutdownState shutdown) {
        return new JudgingHealth(holder, shutdown);
    }

    /**
     * 못 받는 경우는 둘뿐이다 — <b>드레이닝</b>과 <b>판정 재료 없음</b>.
     *
     * <p>받아오기 실패도, 재료가 낡은 것도 여기서 안 뺀다. 공유인지 국소인지
     * 못 가리기 때문이고, 못 가리면 안 빼는 것이 안전한 쪽이다.
     */
    @Override
    public Health health() {
        Health.Builder builder = detailed(shutdown.isDraining() || !hasSnapshot()
                ? Health.outOfService()
                : Health.up());
        return builder.build();
    }

    /**
     * 판정 재료가 있나. <b>루프가 돌았는가로 재면 안 된다.</b>
     *
     * <p>받아오기에 실패해도 루프는 돈다 — 그게 공유 장애를 노드별 신호로 안
     * 흘리는 방법이다. 그걸로 재료 유무를 재면, 레디스가 죽은 채 뜬 노드가
     * 빈 스냅샷으로 받기 시작해 전 쿠폰이 매진으로 보인다.
     */
    private boolean hasSnapshot() {
        return !holder.current().publishedAt().equals(Instant.EPOCH);
    }

    private Health.Builder detailed(Health.Builder builder) {
        return builder
                .withDetail("coupons", holder.current().coupons().size())
                .withDetail("fetchAgeSec", ageOrUnknown(holder.fetchAge()))
                .withDetail("dataAgeSec", ageOrUnknown(holder.dataAge()))
                .withDetail("tickAgeSec", tickAgeSeconds())
                .withDetail("fetchStale", holder.isFetchStale())
                .withDetail("dataStale", holder.isDataStale())
                .withDetail("clockAhead", holder.isClockAhead())
                .withDetail("hasSnapshot", hasSnapshot())
                .withDetail("draining", shutdown.isDraining());
    }

    /**
     * 한 번도 안 돌았으면 {@code -1} 이다.
     *
     * <p>문자열과 수를 오가면 이 값을 파싱하는 관제 도구가 깨진다. 나이가
     * 음수일 수는 없으니 그 자리가 비었다는 뜻으로 쓴다.
     */
    private long tickAgeSeconds() {
        return holder.isBeforeFirstTick() ? -1 : holder.tickAge().toSeconds();
    }

    /** 재료가 없으면 나이도 없다. 기동 직후에 56년이 찍히면 관제 축이 깨진다. */
    private long ageOrUnknown(Duration age) {
        return hasSnapshot() ? age.toSeconds() : -1;
    }
}
