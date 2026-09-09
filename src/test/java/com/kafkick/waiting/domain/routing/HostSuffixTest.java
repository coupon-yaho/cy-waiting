package com.kafkick.waiting.domain.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 허용 목적지의 이름 항목.
 *
 * <p><b>기동에서 검증한다.</b> 못 맞을 항목을 조용히 받으면 전면 거절이 설정
 * 오타로 일어나고, 증상은 기동 성공에 라우팅 후보 0 이다.
 */
@Tag("unit")
class HostSuffixTest {

    @Test
    @DisplayName("점으로_시작하면_그_망_전부다")
    void 점으로_시작하면_그_망_전부다() {
        HostSuffix 접미사 = HostSuffix.parse(".Internal");

        assertThat(접미사.matches("a.internal")).isTrue();
        assertThat(접미사.matches("internal")).isTrue();
        assertThat(접미사.matches("evil-internal")).isFalse();
    }

    @Test
    @DisplayName("점이_없으면_그_이름_하나다")
    void 점이_없으면_그_이름_하나다() {
        HostSuffix 접미사 = HostSuffix.parse("coupon-be");

        assertThat(접미사.matches("coupon-be")).isTrue();
        assertThat(접미사.matches("a.coupon-be")).isFalse();
    }

    /** 정규 생성자도 막는다. 팩토리만 검증하면 못 맞을 값이 그대로 샌다. */
    @Test
    @DisplayName("못_맞을_이름은_거절한다")
    void 못_맞을_이름은_거절한다() {
        assertThatThrownBy(() -> new HostSuffix("*.internal"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HostSuffix("a..b"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HostSuffix("a.b."))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HostSuffix("."))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HostSuffix("  "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 형제 파서들이 다 null 을 받으므로 여기만 터지면 다음 호출부에서 갈린다. */
    @Test
    @DisplayName("빈_값도_거절한다")
    void 빈_값도_거절한다() {
        assertThatThrownBy(() -> HostSuffix.parse(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> HostSuffix.parse(""))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
