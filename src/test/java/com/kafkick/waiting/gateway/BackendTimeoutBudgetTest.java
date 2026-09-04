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

    /**
     * 뒷단이 정상일 때의 응답 여유. <b>이 가정을 값으로 남긴다</b> — 살아난
     * 재시도의 값은 연결 대기와 이것의 합이고, 그것이 느림 임계 아래여야 한다.
     */
    private static final Duration HEALTHY_RESPONSE = Duration.ofMillis(300);

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
     * <b>위는 서킷의 느림 임계다.</b> 재시도가 서킷 안쪽이라, 안 붙는 대를 먼저
     * 고른 요청은 <b>연결 대기와 정상 응답을 더한 값</b>으로 성공한다. 서킷은
     * 그 합을 재므로, 연결 상한만 견주면 정상 응답이 조금만 길어도 느림으로
     * 세어진다 — 그때 모든 요청이 정상인데 서킷이 열리고, 판정이 한산한 쿠폰까지
     * 줄로 보낸다.
     */
    @Test
    @DisplayName("살아난_재시도가_느림으로_안_세어진다")
    void 살아난_재시도가_느림으로_안_세어진다() throws IOException {
        assertThat(뒷단().connectTimeout().plus(HEALTHY_RESPONSE))
                .as("안 붙는 대를 먼저 고른 요청의 값")
                .isLessThan(느림_임계());
    }

    /**
     * <b>느림 임계는 격벽이 막기 시작하는 지연 아래여야 한다.</b> 위로 가면 느린
     * 뒷단이 서킷에 집계되기 전에 격벽이 먼저 막고, 막힌 것은 창에 안 쌓인다.
     */
    @Test
    @DisplayName("느림_임계가_격벽_지연_아래다")
    void 느림_임계가_격벽_지연_아래다() throws IOException {
        assertThat(느림_임계()).isLessThan(AdmissionGatewayFilter.BLOCKING_DELAY);
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

    private com.kafkick.waiting.routing.RoutingProperties 라우팅() throws IOException {
        return 운영설정().bind("waiting.routing",
                        com.kafkick.waiting.routing.RoutingProperties.class)
                .orElseThrow(() -> new AssertionError("waiting.routing 이 없다"));
    }

    /**
     * <b>배제는 응답 상한보다 오래 가야 한다.</b> 멎은 대로 간 요청은 그 상한이
     * 지나야 실패로 관측된다. 배제가 먼저 풀리면 직전 실패가 세어지기도 전에
     * 그 대가 후보로 돌아오고, 돌아와서 다시 상한만큼 매달린다.
     */
    @Test
    @DisplayName("배제가_응답_상한보다_길다")
    void 배제가_응답_상한보다_길다() throws IOException {
        assertThat(라우팅().outlierEjectFor()).isGreaterThan(뒷단().responseTimeout());
    }

    /**
     * <b>되돌리는 램프가 배제보다 길어야 한다.</b> 짧으면 복귀가 사실상 절벽이라,
     * 배제 동안 트래픽이 0 이던 대가 돌아오는 순간 전량을 받는다.
     */
    @Test
    @DisplayName("램프가_배제보다_길다")
    void 램프가_배제보다_길다() throws IOException {
        assertThat(라우팅().coldStartRamp()).isGreaterThan(라우팅().outlierEjectFor());
    }

    /**
     * <b>최악의 성공 경로가 격벽 시한 안이어야 한다.</b> 실패한 연결 대기, 두 번째
     * 연결, 그리고 응답까지다 — 응답 상한은 시도마다 따로 걸린다. 격벽이 먼저
     * 끊으면 서킷에 가는 것이 오류가 아니라 취소이고, 취소는 창에 안 쌓인다.
     */
    @Test
    @DisplayName("시도_전부와_응답의_합이_격벽_시한_안이다")
    void 시도_전부와_응답의_합이_격벽_시한_안이다() throws IOException {
        GatewayRoutes.Backend backend = 뒷단();

        assertThat(backend.connectTimeout().multipliedBy(ATTEMPTS)
                        .plus(backend.responseTimeout()))
                .as("연결 %d 번과 응답을 더한 값", ATTEMPTS)
                .isLessThan(AdmissionGatewayFilter.MAX_IN_FLIGHT);
    }
}
