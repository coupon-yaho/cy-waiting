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

    /**
     * <b>경로에 기본값이 없다.</b> 정적 200 을 주는 경로가 기본으로 실리면 켜는 데
     * 한 줄이면 되고, 그 한 줄이 계획서가 적어 둔 거짓 회복을 그대로 만든다.
     */
    @Test
    @DisplayName("경로는_반드시_적는다")
    void 경로는_반드시_적는다() {
        assertThatThrownBy(() -> new BackendProbeProperties(true, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackendProbeProperties(true, "  ", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("간격은_안_적으면_일초다")
    void 간격은_안_적으면_일초다() {
        BackendProbeProperties p = new BackendProbeProperties(false, "/health", null);

        assertThat(p.enabled()).isFalse();
        assertThat(p.path()).isEqualTo("/health");
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
        assertThatThrownBy(() -> new BackendProbeProperties(true, "/health", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() ->
                new BackendProbeProperties(true, "/health", Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        // 열린 뒤 반쯤 열리기까지가 5초다. 그보다 성기면 그 순간을 놓친다.
        assertThatThrownBy(() ->
                new BackendProbeProperties(true, "/health", Duration.ofSeconds(6)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
