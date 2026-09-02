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
 * 로컬 수신 시각으로 재면 스케줄러가 죽어도 게이트웨이는 같은 해시를 계속 받아
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

        assertThat(holder.tickAge()).isEqualTo(Duration.ZERO);
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
    @DisplayName("발행_시각이_미래면_곧바로_낡음이다")
    void 발행_시각이_미래면_곧바로_낡음이다() {
        // 나이를 0 으로만 보정하면 갱신이 멎어도 임계가 지날 때까지 최신으로
        // 취급된다 — 그동안 아무 노드도 fail-open 에 못 들어간다.
        SnapshotHolder holder = 홀더(지금);

        holder.replace(스냅샷(지금.plusSeconds(10)));

        assertThat(holder.dataAge()).isEqualTo(Duration.ZERO);
        assertThat(holder.isClockAhead()).isTrue();
        assertThat(holder.isDataStale()).isTrue();
    }


    @Test
    @DisplayName("루프가_도는_동안은_못_받아도_낡지_않았다")
    void 루프가_도는_동안은_못_받아도_낡지_않았다() {
        // **공유 원인을 노드별 신호로 흘리면 전 노드가 동시에 빠진다.**
        // 레디스가 모두에게 느리면 아무도 못 받는데, 그걸 이 노드의 문제로
        // 세면 로테이션에 남는 노드가 없다 — 100% 장애다.
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = SnapshotHolder.of(FETCH_STALE, DATA_STALE, clock);
        holder.replace(스냅샷(지금));

        clock.앞으로(Duration.ofSeconds(30));
        holder.loopTicked();   // 시도는 했다. 못 받았을 뿐이다

        assertThat(holder.isFetchStale()).isFalse();
    }

    @Test
    @DisplayName("하트비트는_찍힌_그_시각으로_늙는다")
    void 하트비트는_찍힌_그_시각으로_늙는다() {
        // 루프가 멎으면 낡음이 되어야 하는데, 그러려면 하트비트가 **찍힌 시각**
        // 이어야 한다. 미래를 찍거나 상수를 찍으면 영영 안 늙어 멎은 노드가
        // 멀쩡하다고 답한다 — 재기동 신호가 통째로 죽는다.
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = SnapshotHolder.of(FETCH_STALE, DATA_STALE, clock);
        holder.replace(스냅샷(지금));
        holder.loopTicked();

        clock.앞으로(FETCH_STALE.plusMillis(1));

        assertThat(holder.isFetchStale()).isTrue();
    }

    @Test
    @DisplayName("아직_안_돈_것과_돌다_멎은_것은_갈라진다")
    void 아직_안_돈_것과_돌다_멎은_것은_갈라진다() {
        // 둘 다 tickAge 로는 낡음이다. **그런데 대응이 반대다** — 아직 안 돈
        // 것을 재기동 신호로 쓰면 첫 회차를 못 돈 파드가 죽고, 다시 떠도 또 첫 회차
        // 전이라 또 죽는다. 크래시 루프다. 그래서 갈라 볼 수단이 있어야 한다.
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = SnapshotHolder.of(FETCH_STALE, DATA_STALE, clock);

        assertThat(holder.isFetchStale()).isTrue();
        assertThat(holder.isBeforeFirstTick()).isTrue();

        holder.loopTicked();
        clock.앞으로(FETCH_STALE.plusSeconds(1));

        assertThat(holder.isFetchStale()).isTrue();
        assertThat(holder.isBeforeFirstTick()).isFalse();
    }

    @Test
    @DisplayName("실패한_갱신은_수신_시각을_안_움직인다")
    void 실패한_갱신은_수신_시각을_안_움직인다() {
        // fetchAge 는 판정에서 빠졌지만 헬스 detail 에 남는다. 하트비트와 같이
        // 움직이면 사람이 "루프가 도는데 왜 못 받나" 를 가릴 수단을 잃는다.
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = SnapshotHolder.of(FETCH_STALE, DATA_STALE, clock);
        holder.replace(스냅샷(지금));

        clock.앞으로(Duration.ofSeconds(30));
        holder.loopTicked();

        assertThat(holder.tickAge()).isZero();
        assertThat(holder.fetchAge()).isEqualTo(Duration.ofSeconds(30));
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
        assertThat(holder.tickAge()).isEqualTo(Duration.ZERO);
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

    /**
     * <b>나이를 두 벽시계의 차로 재지 않는다.</b> 발행 시각은 리더가 찍고 나이는
     * 각 노드가 재므로, 보정 없이는 시계가 어긋난 만큼 같은 스냅샷이 노드마다
     * 다르게 낡는다 — 어떤 노드는 fail-open 으로 열리고 어떤 노드는 안 열린다.
     */
    @Test
    @DisplayName("나이는_받아온_순간에_한_번만_잰다")
    void 나이는_받아온_순간에_한_번만_잰다() {
        // 이 노드 시계가 레디스보다 100 초 앞선다.
        MutableClock 앞선_시계 = MutableClock.at(지금.plusSeconds(100));
        SnapshotHolder holder = SnapshotHolder.of(
                Duration.ofSeconds(3), Duration.ofSeconds(5), 앞선_시계);

        // 레디스 기준으로는 방금 발행된 재료다.
        holder.replace(스냅샷(지금.minusSeconds(2)), 지금.getEpochSecond());

        assertThat(holder.view().dataAge()).isEqualTo(Duration.ofSeconds(2));
    }

    /** 받아온 뒤로는 이 노드가 흐른 만큼만 더한다. 그건 한 시계로 잰 값이다. */
    @Test
    @DisplayName("받아온_뒤_흐른_시간이_더해진다")
    void 받아온_뒤_흐른_시간이_더해진다() {
        MutableClock 시계 = MutableClock.at(지금.plusSeconds(100));
        SnapshotHolder holder = SnapshotHolder.of(
                Duration.ofSeconds(3), Duration.ofSeconds(5), 시계);
        holder.replace(스냅샷(지금.minusSeconds(2)), 지금.getEpochSecond());

        시계.앞으로(Duration.ofSeconds(3));

        assertThat(holder.view().dataAge()).isEqualTo(Duration.ofSeconds(5));
    }
}
