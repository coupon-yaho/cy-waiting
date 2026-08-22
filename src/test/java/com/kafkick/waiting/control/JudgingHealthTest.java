package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

/**
 * <b>내가 판정할 수 있는가만 본다.</b>
 *
 * <p>의존성 상태를 넣으면 공유 장애가 전 노드 동시 이탈로 번진다. 레디스가 흔들릴
 * 때 전 노드가 한꺼번에 빠지면 그건 100% 장애다 — 낡은 재료로 판정하는 것보다
 * 훨씬 나쁘다.
 */
class JudgingHealthTest {

    private static final Duration FETCH_STALE = Duration.ofSeconds(2);
    private static final Duration DATA_STALE = Duration.ofSeconds(5);

    private final AtomicReference<Instant> now =
            new AtomicReference<>(Instant.ofEpochSecond(1_700_000_000L));

    private final Clock clock = new Clock() {
        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    };

    private final SnapshotHolder holder = SnapshotHolder.of(FETCH_STALE, DATA_STALE, clock);

    private void 시간을_흘린다(Duration 만큼) {
        now.updateAndGet(t -> t.plus(만큼));
    }

    private GatewaySnapshot 스냅샷() {
        return new GatewaySnapshot(Map.of(), GatewaySnapshot.EMPTY.meta(), now.get());
    }

    private Health 판정() {
        return JudgingHealth.of(holder).health();
    }

    @Test
    @DisplayName("첫_스냅샷_전에는_못_받는다")
    void 첫_스냅샷_전에는_못_받는다() {
        // 판정 재료가 없으면 통과도 대기도 못 만든다. 그 상태로 트래픽을 받으면
        // 전 쿠폰이 매진으로 보인다.
        assertThat(판정().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    }

    @Test
    @DisplayName("받아_왔으면_받는다")
    void 받아_왔으면_받는다() {
        holder.replace(스냅샷());

        assertThat(판정().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    @DisplayName("못_받아도_계속_받는다")
    void 못_받아도_계속_받는다() {
        // **공유인지 국소인지 못 가린다.** 못 가리면 안 뺀다 — 공유 원인이면
        // 전 노드가 동시에 빠져 100% 장애가 된다.
        //
        // 루프는 돌지만 받아오기만 실패하는 상태다 — 회전은 갱신되고 받아온
        // 시각은 안 갱신된다.
        holder.replace(스냅샷());
        시간을_흘린다(FETCH_STALE.plusSeconds(1));
        holder.loopTicked();

        assertThat(판정().getStatus()).isEqualTo(Status.UP);
        assertThat(판정().getDetails()).containsEntry("fetchStale", false);
    }

    @Test
    @DisplayName("루프가_멎어도_받는_것은_유지한다")
    void 루프가_멎어도_받는_것은_유지한다() {
        // 루프 정지는 이 프로세스의 결함이라 재기동으로 다룬다. 받는 것에서
        // 빼면 재기동 전에 트래픽만 잃고, 그 사이 남은 노드에 부하가 몰린다.
        holder.replace(스냅샷());
        시간을_흘린다(FETCH_STALE.plusSeconds(1));

        assertThat(판정().getStatus()).isEqualTo(Status.UP);
        assertThat(판정().getDetails()).containsEntry("fetchStale", true);
    }

    @Test
    @DisplayName("재료가_낡아도_계속_받는다")
    void 재료가_낡아도_계속_받는다() {
        // 전 노드가 같은 값을 본다. 여기서 빼면 100% 장애다. 낡았다는 사실은
        // 진단에만 싣는다.
        holder.replace(스냅샷());
        시간을_흘린다(DATA_STALE.plusSeconds(1));
        holder.loopTicked();

        assertThat(판정().getStatus()).isEqualTo(Status.UP);
        assertThat(판정().getDetails()).containsEntry("dataStale", true);
    }

    @Test
    @DisplayName("종료_신호를_받으면_안_받는다")
    void 종료_신호를_받으면_안_받는다() {
        // 드레이닝이다. LB 가 먼저 빼야 진행 중인 요청이 5xx 로 안 샌다.
        holder.replace(스냅샷());
        JudgingHealth health = JudgingHealth.of(holder);

        health.draining();

        assertThat(health.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    }

    @Test
    @DisplayName("진단에_두_나이와_틱을_싣는다")
    void 진단에_두_나이와_틱을_싣는다() {
        // 낡음이 두 종류라 하나만 실으면 원인을 못 가린다. 루프가 멎은 것과
        // 스케줄러가 멎은 것은 대응이 다르다.
        // **두 나이가 달라야 구별이 된다.** 같은 값이면 어느 쪽을 실어도 통과해서,
        // 루프가 멎은 것과 스케줄러가 멎은 것을 가리는 목적이 사라진다.
        holder.replace(new GatewaySnapshot(Map.of(), GatewaySnapshot.EMPTY.meta(),
                now.get().minusSeconds(4)));
        시간을_흘린다(Duration.ofSeconds(3));

        Map<String, Object> 진단 = 판정().getDetails();

        assertThat(진단).containsKeys("fetchAgeSec", "dataAgeSec", "tickAgeSec", "coupons");
        assertThat(진단.get("fetchAgeSec")).isEqualTo(3L);
        assertThat(진단.get("dataAgeSec")).isEqualTo(7L);
        assertThat(진단.get("tickAgeSec")).isEqualTo(3L);
    }

    @Test
    @DisplayName("시계가_갈리면_드러낸다")
    void 시계가_갈리면_드러낸다() {
        // 발행 시각이 미래면 나이가 음수가 되어 낡음이 영영 거짓이 된다.
        // 조용히 보정하면 스케줄러가 죽어도 아무도 모른다.
        holder.replace(new GatewaySnapshot(Map.of(), GatewaySnapshot.EMPTY.meta(),
                now.get().plusSeconds(10)));

        assertThat(판정().getDetails()).containsEntry("clockAhead", true);
    }
}
