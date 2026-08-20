package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.MutableClock;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 낡음을 <b>두 종류로</b> 구분한다 (4.1).
 *
 * <p>섞으면 스케줄러 장애가 전 게이트웨이 동시 이탈로 번져 100% 장애가 된다.
 * 로컬 수신 시각만 쓰면 스케줄러가 죽어도 게이트웨이는 같은 해시를 계속 받아
 * <b>"방금 갱신했다" 고 스스로를 속인다.</b>
 */
class SnapshotHolderTest {

    private static final Instant 지금 = Instant.parse("2026-08-20T00:00:00Z");
    private static final Duration FETCH_STALE = Duration.ofSeconds(2);
    private static final Duration DATA_STALE = Duration.ofSeconds(5);

    private static Clock 고정시계(Instant at) {
        return Clock.fixed(at, ZoneOffset.UTC);
    }

    private static SnapshotHolder 홀더(Instant at) {
        return SnapshotHolder.of(FETCH_STALE, DATA_STALE, 고정시계(at));
    }

    @Test
    @DisplayName("첫_갱신_전에는_비어_있고_둘_다_낡았다")
    void 첫_갱신_전에는_비어_있고_둘_다_낡았다() {
        // 판정 재료가 없으면 못 받는다. EPOCH 이라 어떤 임계로도 낡음이다.
        SnapshotHolder holder = 홀더(지금);

        assertThat(holder.current().coupons()).isEmpty();
        assertThat(holder.current().publishedAt()).isEqualTo(Instant.EPOCH);
        assertThat(holder.isFetchStale()).isTrue();
        assertThat(holder.isDataStale()).isTrue();
    }

    @Test
    @DisplayName("갱신하면_두_나이가_각각_0이_된다")
    void 갱신하면_두_나이가_각각_0이_된다() {
        SnapshotHolder holder = 홀더(지금);

        holder.replace(스냅샷(지금));

        assertThat(holder.fetchAge()).isEqualTo(Duration.ZERO);
        assertThat(holder.dataAge()).isEqualTo(Duration.ZERO);
    }

    @Test
    @DisplayName("스케줄러가_멎으면_dataStale만_참이다")
    void 스케줄러가_멎으면_dataStale만_참이다() {
        // 이 노드의 갱신 루프는 멀쩡하다 — 낡은 값을 계속 잘 받아 오고 있다.
        // 여기서 503 을 내면 전 노드가 동시에 빠져 100% 장애가 된다.
        SnapshotHolder holder = SnapshotHolder.of(
                FETCH_STALE, DATA_STALE, 고정시계(지금.plusSeconds(10)));

        holder.replace(스냅샷(지금.minusSeconds(10)));

        assertThat(holder.isFetchStale()).isFalse();
        assertThat(holder.isDataStale()).isTrue();
    }

    @Test
    @DisplayName("이_노드의_루프가_멎으면_fetchStale이다")
    void 이_노드의_루프가_멎으면_fetchStale이다() {
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = SnapshotHolder.of(FETCH_STALE, DATA_STALE, clock);
        holder.replace(스냅샷(지금));

        clock.앞으로(Duration.ofSeconds(3));

        assertThat(holder.isFetchStale()).isTrue();
    }

    @Test
    @DisplayName("경계값은_임계와_같을_때_아직_낡지_않았다")
    void 경계값은_임계와_같을_때_아직_낡지_않았다() {
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = SnapshotHolder.of(FETCH_STALE, DATA_STALE, clock);
        holder.replace(스냅샷(지금));

        clock.앞으로(FETCH_STALE);
        assertThat(holder.isFetchStale()).isFalse();

        clock.앞으로(Duration.ofMillis(1));
        assertThat(holder.isFetchStale()).isTrue();
    }


    @Test
    @DisplayName("발행_시각이_미래면_나이를_0으로_본다")
    void 발행_시각이_미래면_나이를_0으로_본다() {
        // 리더 시계가 앞서면 나이가 음수가 되고 dataStale 이 영영 거짓이 된다
        // — 스케줄러가 죽어도 아무 노드가 fail-open 에 못 들어간다.
        SnapshotHolder holder = 홀더(지금);

        holder.replace(스냅샷(지금.plusSeconds(10)));

        assertThat(holder.dataAge()).isEqualTo(Duration.ZERO);
        assertThat(holder.isDataStale()).isFalse();
        assertThat(holder.시계가_앞섰나()).isTrue();
    }

    @Test
    @DisplayName("스냅샷은_밖에서_못_바꾼다")
    void 스냅샷은_밖에서_못_바꾼다() {
        // **가변 맵을 넣고 원본을 흔든다.** Map.of 를 넣으면 방어 복사를
        // 지워도 이 시험이 통과한다 — 이미 불변인 것을 다시 확인할 뿐이다.
        Map<String, CouponState> 원본 = new HashMap<>();
        원본.put("c1", CouponState.always(100));
        SnapshotHolder holder = 홀더(지금);
        holder.replace(new GatewaySnapshot(원본, new SnapshotMeta(1000, 3), 지금));

        원본.put("c2", CouponState.always(50));
        원본.remove("c1");

        assertThat(holder.current().coupons()).containsOnlyKeys("c1");
        assertThatThrownBy(() -> holder.current().coupons().put("c2", null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("갱신_직후에는_fetchStale이_아니다")
    void 갱신_직후에는_fetchStale이_아니다() {
        // 스냅샷과 수신 시각을 따로 두면 읽는 쪽이 새 스냅샷과 옛 시각을
        // 함께 본다. 방금 갱신했는데 503 이 나가는 경로다.
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = SnapshotHolder.of(FETCH_STALE, DATA_STALE, clock);
        holder.replace(스냅샷(지금.minusSeconds(60)));

        clock.앞으로(Duration.ofSeconds(30));
        holder.replace(스냅샷(지금.plusSeconds(30)));

        assertThat(holder.isFetchStale()).isFalse();
        assertThat(holder.fetchAge()).isEqualTo(Duration.ZERO);
    }

    @Test
    @DisplayName("교체는_통째로_일어난다")
    void 교체는_통째로_일어난다() {
        // 키별로 갈아 끼우면 "이 판정이 얼마나 낡았나" 를 말할 수 없다.
        SnapshotHolder holder = 홀더(지금);
        holder.replace(스냅샷(지금));

        holder.replace(new GatewaySnapshot(Map.of(), new SnapshotMeta(0, 1), 지금));

        assertThat(holder.current().coupons()).isEmpty();
    }

    private static GatewaySnapshot 스냅샷(Instant publishedAt) {
        Map<String, CouponState> coupons = Map.of("c1", CouponState.always(100));
        return new GatewaySnapshot(coupons, new SnapshotMeta(1000, 3), publishedAt);
    }
}
