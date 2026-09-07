package com.kafkick.waiting.gateway;

import com.kafkick.waiting.routing.RoutingProperties;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.ChannelOption;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
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
            CircuitBreakerRegistry circuits, ObjectProvider<RoutingProperties> routing) {
        // **라우팅과 같이 못 켠다.** 트래픽은 lb:// 로 흩어지는데 프로브는 고정 주소
        // 하나만 치고, 그 결과는 전 인스턴스를 덮는 같은 서킷에 남는다 — 그 한 대가
        // 죽으면 멀쩡한 나머지로 갈 발급이 통째로 폴백으로 떨어진다.
        RoutingProperties on = routing.getIfAvailable();
        if (on != null && on.enabled()) {
            throw new IllegalStateException(
                    "라우팅과 합성 프로브를 같이 켤 수 없다 — 프로브가 한 대만 보고 "
                            + "그 결과가 전 인스턴스의 서킷에 남는다");
        }
        // **상한도 전용 스케줄러에서 잰다.** 공용 풀을 쓰면 트래픽이 몰릴 때 이
        // 타이머가 요청 처리 뒤에 줄을 서 상한이 실제보다 늦게 끊는다.
        Scheduler timer = Schedulers.newSingle("backend-probe-timeout", true);
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
                        // **2xx 만 성공이다.** 뒷단이 살아 답을 준 것과 제 일을 할 수
                        // 있는 것은 다르고, 3xx 도 그렇다 — 헬스 경로가 로그인
                        // 페이지로 넘기면 그 302 가 회복 표본이 된다.
                        .onStatus(status -> !status.is2xxSuccessful(),
                                response -> response.createException().flatMap(Mono::error))
                        .bodyToMono(Void.class)
                        .then()
                        // **반쯤 열린 시한보다 짧아야 한다.** 넘으면 그 회차의 답이
                        // 다음 구간에 표본으로 얹힌다 — 허가를 안 쓴 유령 표본이다.
                        .timeout(backend.responseTimeout(), timer));
    }

    @Bean
    BackendProbeLoop backendProbeLoop(BackendProbe probe, BackendProbeProperties properties,
            MeterRegistry meters) {
        return BackendProbeLoop.of(probe.bind(meters)::probe, properties.interval());
    }
}
