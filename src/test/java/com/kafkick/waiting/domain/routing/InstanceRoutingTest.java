package com.kafkick.waiting.domain.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 스냅샷에 실어 전 노드에 보내는 인스턴스 한 줄.
 *
 * <p>보고는 리더만 읽는다. 요청 경로가 레디스를 안 치므로(불변식 1) 라우팅에
 * 필요한 것만 골라 판정 재료에 실어야 한다.
 */
@Tag("unit")
class InstanceRoutingTest {

    private static final InstanceAddress 주소 =
            InstanceAddress.parse("10.0.1.7:8080").orElseThrow();

    @Test
    @DisplayName("구분자가_없으면_실을_수_있다")
    void 구분자가_없으면_실을_수_있다() {
        assertThat(new InstanceRouting("be-1", 주소, 200).encodable()).isTrue();
    }

    /**
     * <b>식별자는 밖에서 온다.</b> 구분자가 섞이면 그 줄이 통째로 어긋나 다른
     * 인스턴스의 여유가 엉뚱한 주소에 붙는다.
     */
    @Test
    @DisplayName("구분자가_섞이면_못_싣는다")
    void 구분자가_섞이면_못_싣는다() {
        assertThat(new InstanceRouting("be,1", 주소, 200).encodable()).isFalse();
        assertThat(new InstanceRouting("be|1", 주소, 200).encodable()).isFalse();
    }

    @Test
    @DisplayName("빈_식별자는_못_싣는다")
    void 빈_식별자는_못_싣는다() {
        assertThat(new InstanceRouting("", 주소, 200).encodable()).isFalse();
        assertThat(new InstanceRouting("   ", 주소, 200).encodable()).isFalse();
    }

    @Test
    @DisplayName("빠진_값은_거절한다")
    void 빠진_값은_거절한다() {
        assertThatThrownBy(() -> new InstanceRouting(null, 주소, 200))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new InstanceRouting("be-1", null, 200))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new InstanceRouting("be-1", 주소, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
