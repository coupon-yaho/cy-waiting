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
}
