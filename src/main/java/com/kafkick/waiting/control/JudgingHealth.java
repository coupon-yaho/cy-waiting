package com.kafkick.waiting.control;

import java.util.Objects;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * <b>내가 판정할 수 있는가만 본다.</b> 의존성 상태를 넣으면 공유 장애가 전 노드
 * 동시 이탈로 번진다 — 낡은 재료로 판정하는 것보다 훨씬 나쁘다.
 *
 * <p>이름을 못 박는다. 헬스 그룹이 빈 이름으로 지목하므로 기본 이름에 맡기면
 * 클래스 이름을 고치는 것만으로 그룹에서 빠진다.
 */
@Component("judging")
public final class JudgingHealth implements HealthIndicator {

    /** 나이가 없다는 뜻. 문자열과 수를 오가면 이 값을 파싱하는 관제가 깨진다. */
    private static final long UNKNOWN = -1;

    private final SnapshotHolder holder;
    private final ShutdownState shutdown;

    JudgingHealth(SnapshotHolder holder, ShutdownState shutdown) {
        this.holder = Objects.requireNonNull(holder, "holder 는 필수다");
        this.shutdown = Objects.requireNonNull(shutdown, "shutdown 은 필수다");
    }

    /** 패키지 밖 시험이 부른다. 스프링은 위의 생성자를 쓴다. */
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
        // **한 번만 읽는다.** 판정과 진단이 다른 순간에서 오면, 못 받는다면서
        // 재료는 있다고 하는 응답이 나온다 — 첫 스냅샷이 도착하는 순간이다.
        SnapshotHolder.View view = holder.view();
        boolean hasSnapshot = hasSnapshot(view);
        Health.Builder builder = shutdown.isDraining() || !hasSnapshot
                ? Health.outOfService()
                : Health.up();
        return builder
                .withDetail("coupons", view.snapshot().coupons().size())
                .withDetail("hasSnapshot", hasSnapshot)
                .withDetail("fetchAgeSec", hasSnapshot ? view.fetchAge().toSeconds() : UNKNOWN)
                .withDetail("dataAgeSec", hasSnapshot ? view.dataAge().toSeconds() : UNKNOWN)
                .withDetail("tickAgeSec",
                        view.isBeforeFirstTick() ? UNKNOWN : view.tickAge().toSeconds())
                .withDetail("fetchStale", holder.isFetchStale(view))
                .withDetail("dataStale", holder.isDataStale(view))
                .withDetail("clockAhead", view.clockAhead())
                .withDetail("draining", shutdown.isDraining())
                .build();
    }

    /**
     * 판정 재료가 있나. <b>루프가 돌았는가로 재면 안 된다.</b>
     *
     * <p>받아오기에 실패해도 루프는 돈다 — 그게 공유 장애를 노드별 신호로 안
     * 흘리는 방법이다. 그걸로 재면 레디스가 죽은 채 뜬 노드가 빈 스냅샷으로
     * 받기 시작해 전 쿠폰이 매진으로 보인다.
     */
    private boolean hasSnapshot(SnapshotHolder.View view) {
        return view.snapshot().isPublished();
    }
}
