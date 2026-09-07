package com.kafkick.waiting.gateway;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.ChannelOption;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

/**
 * 합성 프로브 배선. <b>기본은 꺼짐이다</b> — 뒷단이 부하를 받는 헬스 경로를 내기
 * 전에 켜면 정적 200 을 보고 서킷이 거짓으로 닫힌다 (CY-890).
 */
@Configuration
@EnableConfigurationProperties(BackendProbeProperties.class)
@ConditionalOnProperty(prefix = "waiting.backend.probe", name = "enabled", havingValue = "true")
public class BackendProbeConfig {

    /**
     * <b>게이트웨이의 뒷단 클라이언트를 안 재사용한다.</b> 그쪽은 라우팅·재시도·격벽이
     * 붙어 있어 프로브 한 건이 그 장치들을 다 지난다 — 재는 것이 달라진다. 상한은
     * 같은 값을 쓴다. 안 주면 연결 30초에 응답 무한이 서고 회차가 안 끝난다.
     */
    @Bean
    BackendProbe backendProbe(GatewayRoutes.Backend backend, BackendProbeProperties probe,
            CircuitBreakerRegistry circuits) {
        HttpClient http = HttpClient.create().option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                (int) backend.connectTimeout().toMillis());
        WebClient client = WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(http))
                .baseUrl(backend.uri())
                .build();
        // **없는 이름을 만들지 않는다.** `circuitBreaker(name)` 은 없으면 새로
        // 만드는데, 그 유령은 영원히 닫혀 있고 배선 게이지까지 1 로 올린다.
        return BackendProbe.of(() -> circuits.find(GatewayRoutes.CIRCUIT),
                () -> client.get().uri(probe.path())
                        .retrieve()
                        // 2xx 가 아니면 실패다. 뒷단이 살아 답을 준 것과 제 일을
                        // 할 수 있는 것은 다르다.
                        .onStatus(HttpStatusCode::isError,
                                response -> response.createException().flatMap(Mono::error))
                        .bodyToMono(Void.class)
                        .then()
                        // **반쯤 열린 시한보다 짧아야 한다.** 넘으면 그 회차의 답이
                        // 다음 구간에 표본으로 얹힌다 — 허가를 안 쓴 유령 표본이다.
                        .timeout(backend.responseTimeout()));
    }

    @Bean
    BackendProbeLoop backendProbeLoop(BackendProbe probe, BackendProbeProperties properties,
            MeterRegistry meters) {
        // **안 도는 것과 칠 일이 없는 것을 가른다.** 셋 다 0 이면 루프가 죽은 것이다.
        Gauge.builder("waiting.probe.passed", probe, BackendProbe::passed)
                .description("합성 프로브가 표본을 채운 회차 수")
                .strongReference(true).register(meters);
        Gauge.builder("waiting.probe.failed", probe, BackendProbe::failed)
                .description("합성 프로브가 실패로 표본을 채운 회차 수")
                .strongReference(true).register(meters);
        Gauge.builder("waiting.probe.skipped", probe, BackendProbe::skipped)
                .description("반쯤 열리지 않아 안 친 회차 수. 루프 생존의 신호다")
                .strongReference(true).register(meters);
        return BackendProbeLoop.of(probe::probe, properties.interval());
    }
}
