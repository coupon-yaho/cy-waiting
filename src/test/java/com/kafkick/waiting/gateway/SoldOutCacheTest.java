package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 매진 negative cache — <b>노드 로컬 기록</b> (7.2 · B-10·B-11).
 *
 * <p>재료가 아직 재고를 말하는 창에서 몰려온 요청이 전부 뒷단까지 가는 것을
 * 끊습니다. 영구 보관은 안 합니다 — 재입고된 쿠폰이 영영 막힙니다 (B-11).
 */
class SoldOutCacheTest {

    /** 재료의 발행 시각. <b>레디스 시계</b>다 — 수명 계산과 다른 축이다. */
    private static final Instant 발행 = Instant.parse("2026-08-28T00:00:00Z");

    private static final Duration TTL = Duration.ofSeconds(10);

    /** 단조 시계를 손으로 민다. 실제로 자면 시험이 장비 속도에 걸린다 (TS-4). */
    private final AtomicLong 나노 = new AtomicLong();

    private SoldOutCache 캐시() {
        return SoldOutCache.of(TTL, 100, 나노::get);
    }

    private void 흐른다(Duration d) {
        나노.addAndGet(d.toNanos());
    }

    /** 관찰하지 않았으면 막지 않습니다. 안 그러면 첫 요청부터 전부 매진이 됩니다. */
    @Test
    @DisplayName("관찰하지_않은_쿠폰은_매진이_아니다")
    void 관찰하지_않은_쿠폰은_매진이_아니다() {
        assertThat(캐시().soldOut("c1")).isFalse();
    }

    /** <b>노드당 최초 1건만 뒷단에 닿습니다.</b> 20대면 20건입니다. */
    @Test
    @DisplayName("한_번_관찰하면_그_뒤로는_매진이다")
    void 한_번_관찰하면_그_뒤로는_매진이다() {
        SoldOutCache 캐시 = 캐시();

        assertThat(캐시.observed("c1", 발행)).as("새로 무장").isTrue();

        assertThat(캐시.soldOut("c1")).isTrue();
    }

    /** 두 번째 관찰은 무장이 아닙니다. 로그를 쿠폰당 한 번만 찍으려는 것입니다 (LG-3). */
    @Test
    @DisplayName("이미_무장_중이면_새_무장이_아니다")
    void 이미_무장_중이면_새_무장이_아니다() {
        SoldOutCache 캐시 = 캐시();
        캐시.observed("c1", 발행);

        assertThat(캐시.observed("c1", 발행)).isFalse();
    }

    /** 다른 쿠폰까지 막으면 한 쿠폰의 매진이 전체를 멈춥니다. */
    @Test
    @DisplayName("관찰은_그_쿠폰에만_걸린다")
    void 관찰은_그_쿠폰에만_걸린다() {
        SoldOutCache 캐시 = 캐시();

        캐시.observed("c1", 발행);

        assertThat(캐시.soldOut("c2")).isFalse();
    }

    /**
     * <b>수명은 단조 시계로 잽니다</b> (M1).
     *
     * <p>벽시계로 재면 NTP 보정 한 번에 그 노드의 방패가 전부 같은 순간
     * 풀립니다. 경계에서 살아 있고 그 다음에 죽습니다 — 한쪽만 재면 부호를
     * 뒤집어도 통과합니다.
     */
    @Test
    @DisplayName("절대_TTL_이_지나면_스스로_풀린다")
    void 절대_TTL_이_지나면_스스로_풀린다() {
        SoldOutCache 캐시 = 캐시();
        캐시.observed("c1", 발행);

        흐른다(TTL);
        assertThat(캐시.soldOut("c1")).as("경계에서는 아직").isTrue();

        나노.incrementAndGet();
        assertThat(캐시.soldOut("c1")).as("경계 뒤").isFalse();
    }

    /**
     * <b>나중에 발행된 재료가 재고를 말하면 풉니다</b> (7.2.4).
     *
     * <p>양쪽 다 재료의 발행 시각이라 <b>같은 시계 영역</b>입니다. 한쪽이 노드
     * 벽시계면 시계 스큐만큼 판정이 어긋납니다.
     */
    @Test
    @DisplayName("나중_재료가_재고를_말하면_TTL_전에_풀린다")
    void 나중_재료가_재고를_말하면_TTL_전에_풀린다() {
        SoldOutCache 캐시 = 캐시();
        캐시.observed("c1", 발행);
        흐른다(Duration.ofSeconds(3));
        캐시.soldOut("c1");
        캐시.soldOut("c1");

        SoldOutCache.Released 푼_것 = 캐시.restocked("c1", 발행.plusSeconds(1)).orElseThrow();

        assertThat(캐시.soldOut("c1")).isFalse();
        assertThat(푼_것.elapsed()).as("끊고 있던 시간").isEqualTo(Duration.ofSeconds(3));
        assertThat(푼_것.blocked()).as("끊은 건수").isEqualTo(2);
    }

    /**
     * <b>같은 재료로는 안 풉니다.</b>
     *
     * <p>캐시가 존재하는 창이 바로 "재료는 아직 재고를 말하는데 뒷단은 이미
     * 매진" 인 창입니다. 발행 시각을 안 보면 그 재료가 곧바로 관찰을 지워,
     * 캐시가 아무것도 안 막습니다.
     */
    @Test
    @DisplayName("관찰보다_먼저_발행된_재료로는_안_푼다")
    void 관찰보다_먼저_발행된_재료로는_안_푼다() {
        SoldOutCache 캐시 = 캐시();
        캐시.observed("c1", 발행);

        assertThat(캐시.restocked("c1", 발행)).as("같은 재료").isEmpty();
        assertThat(캐시.restocked("c1", 발행.minusSeconds(1))).as("먼저 발행된 재료").isEmpty();

        assertThat(캐시.soldOut("c1")).isTrue();
    }

    /** 무장하지 않은 쿠폰을 푸는 것은 아무 일도 아닙니다. 로그도 안 찍습니다. */
    @Test
    @DisplayName("무장하지_않았으면_풀_것이_없다")
    void 무장하지_않았으면_풀_것이_없다() {
        assertThat(캐시().restocked("c1", 발행.plusSeconds(1))).isEmpty();
    }

    /**
     * <b>키가 클라이언트 입력에서 옵니다.</b> 상한이 없으면 아무 문자열이나
     * 던져 메모리를 무한히 늘릴 수 있습니다.
     */
    @Test
    @DisplayName("키_상한을_넘으면_새_관찰을_안_받는다")
    void 키_상한을_넘으면_새_관찰을_안_받는다() {
        SoldOutCache 캐시 = SoldOutCache.of(TTL, 2, 나노::get);

        캐시.observed("c1", 발행);
        캐시.observed("c2", 발행);

        assertThat(캐시.observed("c3", 발행)).as("자리가 없으면 안 받는다").isFalse();
        assertThat(캐시.soldOut("c1")).as("먼저 온 것은 남는다").isTrue();
        assertThat(캐시.soldOut("c3")).isFalse();
    }

    /**
     * <b>죽은 항목이 자리를 잡고 있으면 안 됩니다.</b> 상한에 걸려 새 관찰을
     * 못 받는데, 정작 들어 있는 것은 이미 만료된 것일 수 있습니다.
     */
    @Test
    @DisplayName("만료된_항목의_자리는_새_관찰이_쓴다")
    void 만료된_항목의_자리는_새_관찰이_쓴다() {
        SoldOutCache 캐시 = SoldOutCache.of(TTL, 1, 나노::get);
        캐시.observed("c1", 발행);
        흐른다(TTL.plusSeconds(1));

        assertThat(캐시.observed("c2", 발행)).isTrue();

        assertThat(캐시.soldOut("c2")).isTrue();
    }

    /** 0 이하 상한은 상한이 아닙니다. 값으로 끄면 그 사실이 설정 어디에도 안 드러납니다. */
    @Test
    @DisplayName("상한이_없는_캐시는_만들_수_없다")
    void 상한이_없는_캐시는_만들_수_없다() {
        assertThatThrownBy(() -> SoldOutCache.of(TTL, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("키 상한");
        assertThatThrownBy(() -> SoldOutCache.of(Duration.ZERO, 100))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TTL");
    }
}
