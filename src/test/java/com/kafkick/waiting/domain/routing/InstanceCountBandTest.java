package com.kafkick.waiting.domain.routing;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 인스턴스 수가 가정(A7) 안인가.
 *
 * <p><b>자동으로 전략을 안 바꾼다</b> (R-9). 임계 근처에서 진동하고, 롤링 배포가
 * 정확히 그 구간을 지난다.
 */
@Tag("unit")
class InstanceCountBandTest {

    @Test
    @DisplayName("가정_안이면_안_알린다")
    void 가정_안이면_안_알린다() {
        assertThat(InstanceCountBand.of(10).withinAssumption()).isTrue();
        assertThat(InstanceCountBand.of(15).withinAssumption()).isTrue();
        assertThat(InstanceCountBand.of(20).withinAssumption()).isTrue();
    }

    /** 3~5 대면 무작위 둘을 뽑는 이득이 없다. 라운드로빈을 권한다. */
    @Test
    @DisplayName("적으면_아래_대역이다")
    void 적으면_아래_대역이다() {
        assertThat(InstanceCountBand.of(3)).isEqualTo(InstanceCountBand.BELOW);
        assertThat(InstanceCountBand.of(9)).isEqualTo(InstanceCountBand.BELOW);
        assertThat(InstanceCountBand.of(3).withinAssumption()).isFalse();
    }

    /** 50 대면 전수 조회 비용을 다시 본다. */
    @Test
    @DisplayName("많으면_위_대역이다")
    void 많으면_위_대역이다() {
        assertThat(InstanceCountBand.of(21)).isEqualTo(InstanceCountBand.ABOVE);
        assertThat(InstanceCountBand.of(50)).isEqualTo(InstanceCountBand.ABOVE);
        assertThat(InstanceCountBand.of(50).withinAssumption()).isFalse();
    }

    /** <b>0 도 범위 밖이다.</b> 보낼 곳이 없다는 것 자체가 알려야 할 상태다. */
    @Test
    @DisplayName("한_대도_없으면_범위_밖이다")
    void 한_대도_없으면_범위_밖이다() {
        assertThat(InstanceCountBand.of(0).withinAssumption()).isFalse();
    }

    /** 사유에 실제 대수가 실린다. 안 실으면 로그를 보고도 무엇이 문제인지 모른다. */
    @Test
    @DisplayName("사유에_대수가_실린다")
    void 사유에_대수가_실린다() {
        assertThat(InstanceCountBand.of(3).describe(3)).contains("3 대").contains("round-robin");
        assertThat(InstanceCountBand.of(50).describe(50)).contains("50 대");
        assertThat(InstanceCountBand.of(12).describe(12)).contains("12 대");
    }
}
