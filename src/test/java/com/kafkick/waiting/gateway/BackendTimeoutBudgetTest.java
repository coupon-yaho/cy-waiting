package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

/**
 * 상한 사다리를 <b>배포되는 파일에서</b> 읽어 못 박는다.
 *
 * <p>다섯 값이 서로를 모른 채 각자 서 있다 — 연결·응답 상한은 한 record 에,
 * 서킷의 느림 임계는 다른 record 에, 격벽의 두 값은 필터의 상수에 있다. 하나만
 * 고쳐도 사다리가 뒤집히는데 그 사실이 어디에도 안 걸려 있었다.
 */
class BackendTimeoutBudgetTest {

    /** 리눅스의 첫 SYN 재전송. 이보다 짧으면 잠깐 밀린 멀쩡한 대를 접는다. */
    private static final Duration SYN_RETRANSMIT = Duration.ofSeconds(1);

    /** 연결 실패에 무는 재시도가 한 번 더 간다. */
    private static final int ATTEMPTS = 2;

    private Binder 운영설정() throws IOException {
        List<PropertySource<?>> sources = new YamlPropertySourceLoader()
                .load("application", new ClassPathResource("application.yml"));
        return new Binder(ConfigurationPropertySources.from(sources));
    }

    private GatewayRoutes.Backend 뒷단() throws IOException {
        return 운영설정().bind("waiting.backend", GatewayRoutes.Backend.class)
                .orElseThrow(() -> new AssertionError("waiting.backend 가 없다"));
    }

    private Duration 느림_임계() throws IOException {
        return 운영설정()
                .bind("waiting.backend.circuit.slow-call-duration-threshold", Duration.class)
                .orElseThrow(() -> new AssertionError("slow-call-duration-threshold 가 없다"));
    }

    /**
     * <b>아래는 SYN 재전송이다.</b> 그보다 짧으면 accept 큐가 잠깐 넘친 멀쩡한
     * 인스턴스를 실패로 접는다 — 그 큐가 넘치는 순간이 하필 회복 구간이라,
     * 회복이 2차 장애가 된다. 근거는
     * `ai/journal/2026/09/AIJ-0214-connect-timeout.md` 에 있다.
     */
    @Test
    @DisplayName("연결_상한이_SYN_재전송보다_길다")
    void 연결_상한이_SYN_재전송보다_길다() throws IOException {
        assertThat(뒷단().connectTimeout()).isGreaterThan(SYN_RETRANSMIT);
    }

    /**
     * <b>위는 서킷의 느림 임계다.</b> 재시도가 서킷 안쪽이라, 죽은 대를 먼저 고른
     * 요청은 연결 상한만큼 늦게 <b>성공</b>한다. 그것이 느림으로 집계되면 모든
     * 요청이 200 인데 서킷이 열리고, 그때 판정이 한산한 쿠폰까지 줄로 보낸다 —
     * 한산한 쿠폰은 줄 없이 지나가야 한다.
     */
    @Test
    @DisplayName("느리게_성공한_요청이_느림으로_안_세어진다")
    void 느리게_성공한_요청이_느림으로_안_세어진다() throws IOException {
        assertThat(뒷단().connectTimeout()).isLessThan(느림_임계());
    }

    /**
     * <b>재시도까지 격벽이 막기 시작하는 지연 안이어야 한다.</b> 넘으면 그 요청은
     * 서킷에 닿기도 전에 격벽에서 잘리고, 잘린 것은 창에 안 쌓인다 — 멎은 뒷단의
     * 서킷이 영영 안 열린다.
     */
    @Test
    @DisplayName("연결_시도_전부가_격벽_지연_안이다")
    void 연결_시도_전부가_격벽_지연_안이다() throws IOException {
        assertThat(뒷단().connectTimeout().multipliedBy(ATTEMPTS))
                .isLessThanOrEqualTo(AdmissionGatewayFilter.BLOCKING_DELAY);
    }

    /**
     * <b>연결과 응답을 더한 것이 격벽 시한 안이어야 한다.</b> 응답 상한은 시도마다
     * 따로 걸리므로 최악은 둘의 합이다. 격벽이 먼저 끊으면 서킷에 가는 것이 오류가
     * 아니라 취소이고, 취소는 창에 안 쌓인다.
     */
    @Test
    @DisplayName("연결과_응답의_합이_격벽_시한_안이다")
    void 연결과_응답의_합이_격벽_시한_안이다() throws IOException {
        GatewayRoutes.Backend backend = 뒷단();

        assertThat(backend.connectTimeout().plus(backend.responseTimeout()))
                .isLessThan(AdmissionGatewayFilter.MAX_IN_FLIGHT);
    }
}
