package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 매진 negative cache — <b>노드 로컬 기록</b> (7.2 · B-10·B-11).
 *
 * <p>재료가 아직 재고를 말하는 창에서 몰려온 요청이 전부 뒷단까지 가는 것을
 * 끊습니다. 영구 보관은 안 합니다 — 재입고된 쿠폰이 영영 막힙니다 (B-11).
 */
class SoldOutCacheTest {

    private static final Instant 지금 = Instant.parse("2026-08-28T00:00:00Z");
    private static final Duration TTL = Duration.ofSeconds(10);

    private SoldOutCache 캐시() {
        return SoldOutCache.of(TTL, 100);
    }

    /** 관찰하지 않았으면 막지 않습니다. 안 그러면 첫 요청부터 전부 매진이 됩니다. */
    @Test
    @DisplayName("관찰하지_않은_쿠폰은_매진이_아니다")
    void 관찰하지_않은_쿠폰은_매진이_아니다() {
        assertThat(캐시().soldOut("c1", 지금)).isFalse();
    }

    /**
     * <b>노드당 최초 1건만 뒷단에 닿습니다.</b> 20대면 20건입니다.
     */
    @Test
    @DisplayName("한_번_관찰하면_그_뒤로는_매진이다")
    void 한_번_관찰하면_그_뒤로는_매진이다() {
        SoldOutCache 캐시 = 캐시();

        캐시.observed("c1", 지금);

        assertThat(캐시.soldOut("c1", 지금)).isTrue();
    }

    /** 다른 쿠폰까지 막으면 한 쿠폰의 매진이 전체를 멈춥니다. */
    @Test
    @DisplayName("관찰은_그_쿠폰에만_걸린다")
    void 관찰은_그_쿠폰에만_걸린다() {
        SoldOutCache 캐시 = 캐시();

        캐시.observed("c1", 지금);

        assertThat(캐시.soldOut("c2", 지금)).isFalse();
    }

    /**
     * <b>절대 TTL 은 안전판입니다</b> (7.2.5).
     *
     * <p>해제 신호(재입고 관찰)를 놓쳐도 무한히 지속되면 안 됩니다. 경계에서
     * 살아 있고 그 다음에 죽습니다 — 한쪽만 재면 부호를 뒤집어도 통과합니다.
     */
    @Test
    @DisplayName("절대_TTL_이_지나면_스스로_풀린다")
    void 절대_TTL_이_지나면_스스로_풀린다() {
        SoldOutCache 캐시 = 캐시();
        캐시.observed("c1", 지금);

        assertThat(캐시.soldOut("c1", 지금.plus(TTL))).as("경계에서는 아직").isTrue();
        assertThat(캐시.soldOut("c1", 지금.plus(TTL).plusMillis(1))).as("경계 뒤").isFalse();
    }

    /**
     * <b>나중에 발행된 재료가 재고를 말하면 풉니다</b> (7.2.4).
     *
     * <p>TTL 을 기다리면 재고가 돌아온 쿠폰이 그 시간만큼 막힙니다.
     */
    @Test
    @DisplayName("나중_재료가_재고를_말하면_TTL_전에_풀린다")
    void 나중_재료가_재고를_말하면_TTL_전에_풀린다() {
        SoldOutCache 캐시 = 캐시();
        캐시.observed("c1", 지금);

        캐시.restocked("c1", 지금.plusSeconds(1));

        assertThat(캐시.soldOut("c1", 지금)).isFalse();
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
        캐시.observed("c1", 지금);

        캐시.restocked("c1", 지금);

        assertThat(캐시.soldOut("c1", 지금)).isTrue();
    }

    /**
     * <b>키가 클라이언트 입력에서 옵니다.</b> 상한이 없으면 아무 문자열이나
     * 던져 메모리를 무한히 늘릴 수 있습니다.
     */
    @Test
    @DisplayName("키_상한을_넘으면_새_관찰을_안_받는다")
    void 키_상한을_넘으면_새_관찰을_안_받는다() {
        SoldOutCache 캐시 = SoldOutCache.of(TTL, 2);

        캐시.observed("c1", 지금);
        캐시.observed("c2", 지금);
        캐시.observed("c3", 지금);

        assertThat(캐시.soldOut("c1", 지금)).as("먼저 온 것은 남는다").isTrue();
        assertThat(캐시.soldOut("c3", 지금)).as("자리가 없으면 안 받는다").isFalse();
    }

    /**
     * <b>죽은 항목이 자리를 잡고 있으면 안 됩니다.</b> 상한에 걸려 새 관찰을
     * 못 받는데, 정작 들어 있는 것은 이미 만료된 것일 수 있습니다.
     */
    @Test
    @DisplayName("만료된_항목의_자리는_새_관찰이_쓴다")
    void 만료된_항목의_자리는_새_관찰이_쓴다() {
        SoldOutCache 캐시 = SoldOutCache.of(TTL, 1);
        캐시.observed("c1", 지금);
        Instant 나중 = 지금.plus(TTL).plusSeconds(1);

        캐시.observed("c2", 나중);

        assertThat(캐시.soldOut("c2", 나중)).isTrue();
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
