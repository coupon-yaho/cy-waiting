package com.kafkick.waiting.domain.coupon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 노드 수는 분모로 쓰인다. 0 이 들어오면 판정 전체가 터진다. */
class SnapshotMetaTest {

    @Test
    @DisplayName("게이트웨이_수가_0이면_1로_취급한다")
    void 게이트웨이_수가_0이면_1로_취급한다() {
        assertThat(new SnapshotMeta(1000, 0).effectiveGatewayCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("게이트웨이_수가_음수여도_1로_취급한다")
    void 게이트웨이_수가_음수여도_1로_취급한다() {
        assertThat(new SnapshotMeta(1000, -5).effectiveGatewayCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("게이트웨이_수가_양수면_그대로_쓴다")
    void 게이트웨이_수가_양수면_그대로_쓴다() {
        assertThat(new SnapshotMeta(1000, 10).effectiveGatewayCount()).isEqualTo(10);
    }

    @Test
    @DisplayName("전역_여유가_음수면_생성에_실패한다")
    void 전역_여유가_음수면_생성에_실패한다() {
        assertThatThrownBy(() -> new SnapshotMeta(-1, 10))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("폴링_배수가_1미만이면_1로_정규화된다")
    void 폴링_배수가_1미만이면_1로_정규화된다() {
        // 거부가 아니라 정규화다. 1 미만은 폴링을 더 자주 하라는 뜻이 되는데
        // 그건 예산을 늘리는 방향이라 의미가 없다.
        assertThat(new SnapshotMeta(1000, 1, null, 0.3).pollScale()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("폴링_배수가_1이상이면_그대로_둔다")
    void 폴링_배수가_1이상이면_그대로_둔다() {
        assertThat(new SnapshotMeta(1000, 1, null, 2.5).pollScale()).isEqualTo(2.5);
    }

    @Test
    @DisplayName("배수를_안_실으면_1이다")
    void 배수를_안_실으면_1이다() {
        // 크게 잡으면 예산이 멀쩡한데도 전원이 뜸하게 묻고, 그만큼 차례가
        // 온 사실을 늦게 안다.
        assertThat(new SnapshotMeta(1000, 1).pollScale()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("폴링_배수가_유한하지_않으면_생성에_실패한다")
    void 폴링_배수가_유한하지_않으면_생성에_실패한다() {
        // NaN 은 비교가 전부 false 라 Math.max 를 그냥 통과한다. 그대로 두면
        // 폴링 간격이 조용히 NaN 이 되어 대기자가 폴링을 멈춘다.
        assertThatThrownBy(() -> new SnapshotMeta(1000, 1, null, Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
        // 무한은 간격을 상한으로 밀어 올린다. 계산된 값이 흘러드는 자리가 됐으니
        // 둘 다 막는다.
        assertThatThrownBy(() -> new SnapshotMeta(1000, 1, null, Double.POSITIVE_INFINITY))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
