package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 합성 프로브의 노브.
 *
 * <p><b>기본은 꺼짐이다.</b> 뒷단이 부하를 받는 헬스 경로를 내기 전에 켜면 정적
 * 200 을 보고 서킷이 거짓으로 닫힌다 (CY-890).
 */
@Tag("unit")
class BackendProbePropertiesTest {

    @Test
    @DisplayName("안_적으면_기본값이_선다")
    void 안_적으면_기본값이_선다() {
        BackendProbeProperties p = new BackendProbeProperties(false, null, null);

        assertThat(p.enabled()).isFalse();
        assertThat(p.path()).isEqualTo("/actuator/health");
        assertThat(p.interval()).isEqualTo(Duration.ofSeconds(1));
    }

    /** 경로가 아닌 값을 받으면 baseUrl 이 통째로 바뀐다 — 프로브가 남의 집을 친다. */
    @Test
    @DisplayName("경로가_아니면_거절한다")
    void 경로가_아니면_거절한다() {
        assertThatThrownBy(() -> new BackendProbeProperties(true, "http://evil", null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackendProbeProperties(true, "health", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>간격이 성기면 회복이 그만큼 밀린다.</b> 열린 뒤 허가가 나는 순간을 놓치면
     * 다음 회차까지 통째로 기다린다.
     */
    @Test
    @DisplayName("간격의_범위를_본다")
    void 간격의_범위를_본다() {
        assertThatThrownBy(() -> new BackendProbeProperties(true, null, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackendProbeProperties(true, null, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackendProbeProperties(true, null, Duration.ofMinutes(2)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
