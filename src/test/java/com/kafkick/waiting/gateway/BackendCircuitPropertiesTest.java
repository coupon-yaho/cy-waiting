package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 서킷 설정은 <b>코드가 아니라 설정에 둔다.</b>
 *
 * <p>실측 전에는 맞는 값을 모른다. 코드에 박으면 재조정마다 배포가 필요하고,
 * 배포가 필요하면 장애 중에는 못 고친다 — 정작 고쳐야 할 때가 그때다.
 */
class BackendCircuitPropertiesTest {

    private static BackendCircuitProperties 기본() {
        return new BackendCircuitProperties(null, null, null, null, null, null, null);
    }

    /**
     * <b>건수 창을 쓰면 안 된다.</b> 100K RPS 에서 건수 100 은 수 ms 분량이라,
     * GC 한 번이나 순간 변동에 서킷이 열린다.
     */
    @Test
    @DisplayName("기본값은_시간_기반_창이다")
    void 기본값은_시간_기반_창이다() {
        assertThat(기본().slidingWindowSize()).isEqualTo(Duration.ofSeconds(10));
    }

    /** 표본이 적을 때 열면 오픈 직후 첫 몇 건으로 전 노드가 서킷을 연다. */
    @Test
    @DisplayName("표본이_모자라면_안_연다")
    void 표본이_모자라면_안_연다() {
        assertThat(기본().minimumNumberOfCalls()).isEqualTo(20);
    }

    /**
     * <b>느린 호출을 실패로 센다.</b> 타임아웃만 보면 타임아웃 직전까지 느려진
     * 인스턴스가 전부 성공으로 집계되어 서킷이 안 열린다 (6.1.8).
     */
    @Test
    @DisplayName("느림_임계가_타임아웃보다_낮다")
    void 느림_임계가_타임아웃보다_낮다() {
        BackendCircuitProperties p = 기본();

        assertThat(p.slowCallDurationThreshold()).isLessThan(p.timeout());
    }

    /**
     * <b>회복을 늦어도 두 번째 시도에 판정한다.</b> G8.12 가 half-open 회복
     * 시도를 2회 이하로 못 박는다 — 대기가 길면 그 안에 못 든다.
     */
    @Test
    @DisplayName("열린_뒤_대기가_회복_판정을_막지_않는다")
    void 열린_뒤_대기가_회복_판정을_막지_않는다() {
        assertThat(기본().waitDurationInOpenState()).isEqualTo(Duration.ofSeconds(5));
    }

    /** 서킷이 회복을 판정할 표본 수. 유입 억제는 판정 쪽이 따로 한다. */
    @Test
    @DisplayName("반쯤_열린_상태의_표본_수를_정한다")
    void 반쯤_열린_상태의_표본_수를_정한다() {
        assertThat(기본().permittedNumberOfCallsInHalfOpenState()).isEqualTo(10);
    }

    /**
     * <b>비율은 백분율이다.</b> 0.5 로 적으면 반올림돼 0% 가 되고, 그러면 첫
     * 실패에 서킷이 열린다 — 기동은 성공한다.
     */
    @Test
    @DisplayName("비율이_범위를_벗어나면_기동을_막는다")
    void 비율이_범위를_벗어나면_기동을_막는다() {
        assertThatThrownBy(() -> new BackendCircuitProperties(
                null, null, 0f, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackendCircuitProperties(
                null, null, 101f, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>느림 임계가 타임아웃보다 크면 그 설정이 죽은 값이 된다.</b> 타임아웃이
     * 먼저 끊어 느린 호출이 한 건도 안 집계되고, 운영자는 켰다고 믿는다.
     */
    @Test
    @DisplayName("느림_임계가_타임아웃_이상이면_기동을_막는다")
    void 느림_임계가_타임아웃_이상이면_기동을_막는다() {
        assertThatThrownBy(() -> new BackendCircuitProperties(
                null, null, null, Duration.ofSeconds(3), null, Duration.ofSeconds(3), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("느림");
    }
}
