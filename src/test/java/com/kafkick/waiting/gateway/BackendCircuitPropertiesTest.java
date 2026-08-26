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
 *
 * <p><b>여기서는 값을 안 정하고 검증만 한다.</b> 코드에도 기본값이 있으면 yml 의
 * 키를 하나 잘못 적어도 조용히 그 값으로 떨어진다. 실제로 그 오타를 시험이 못
 * 잡았다. 실린 값 자체는 {@code GatewayWiringTest} 가 뜬 컨텍스트에서 본다.
 */
class BackendCircuitPropertiesTest {

    private static final Duration 창 = Duration.ofSeconds(10);
    private static final Duration 타임아웃 = Duration.ofSeconds(3);
    private static final Duration 느림 = Duration.ofMillis(1500);
    private static final Duration 대기 = Duration.ofSeconds(5);

    /** 온전한 한 벌. 시험마다 한 자리씩 흠집을 낸다. */
    private static BackendCircuitProperties 값(Float 실패비율, Duration 타임아웃값,
            Duration 느림임계, Float 느림비율) {
        return new BackendCircuitProperties(창, 20,
                실패비율 == null ? 50f : 실패비율,
                타임아웃값 == null ? 타임아웃 : 타임아웃값,
                느림임계 == null ? 느림 : 느림임계,
                느림비율 == null ? 50f : 느림비율,
                대기, 10);
    }

    /**
     * <b>안 적으면 기동을 막는다.</b> 조용히 채우면 yml 의 오타가 그대로 운영에
     * 나가고, 운영자는 자기가 적은 값으로 돌고 있다고 믿는다.
     */
    @Test
    @DisplayName("값을_안_적으면_기동을_막는다")
    void 값을_안_적으면_기동을_막는다() {
        assertThatThrownBy(() -> new BackendCircuitProperties(
                null, 20, 50f, 타임아웃, 느림, 50f, 대기, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sliding-window-size");
        assertThatThrownBy(() -> new BackendCircuitProperties(
                창, null, 50f, 타임아웃, 느림, 50f, 대기, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minimum-number-of-calls");
        assertThatThrownBy(() -> new BackendCircuitProperties(
                창, 20, 50f, 타임아웃, 느림, 50f, null, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("wait-duration-in-open-state");
    }

    /**
     * <b>비율은 백분율이다.</b> 0.5 로 적으면 반올림돼 0% 가 되고, 그러면 첫
     * 실패에 서킷이 열린다 — 기동은 성공한다.
     */
    @Test
    @DisplayName("비율이_범위를_벗어나면_기동을_막는다")
    void 비율이_범위를_벗어나면_기동을_막는다() {
        assertThatThrownBy(() -> 값(0f, null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> 값(101f, null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        // 느림 비율도 같은 규칙을 탄다. 하나만 걸면 다른 쪽이 조용히 0 이 된다.
        assertThatThrownBy(() -> 값(null, null, null, 0f))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>느림 임계가 타임아웃 이상이면 그 설정이 죽은 값이 된다.</b> 타임아웃이
     * 먼저 끊어 느린 호출이 한 건도 안 집계되고, 운영자는 켰다고 믿는다 (6.1.8).
     */
    @Test
    @DisplayName("느림_임계가_타임아웃_이상이면_기동을_막는다")
    void 느림_임계가_타임아웃_이상이면_기동을_막는다() {
        assertThatThrownBy(() -> 값(null, Duration.ofSeconds(3), Duration.ofSeconds(3), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("느림");
    }

    /** 온전한 한 벌은 받아들인다. 거절만 재면 전부 거절해도 통과한다. */
    @Test
    @DisplayName("온전한_설정은_받아들인다")
    void 온전한_설정은_받아들인다() {
        assertThat(값(null, null, null, null).slowCallDurationThreshold())
                .isEqualTo(느림);
    }

    /** 표본 하한이 0 이면 첫 한 건으로 서킷이 열린다. */
    @Test
    @DisplayName("표본_하한이_0이면_기동을_막는다")
    void 표본_하한이_0이면_기동을_막는다() {
        assertThatThrownBy(() -> new BackendCircuitProperties(
                창, 0, 50f, 타임아웃, 느림, 50f, 대기, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackendCircuitProperties(
                창, 20, 50f, 타임아웃, 느림, 50f, 대기, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
