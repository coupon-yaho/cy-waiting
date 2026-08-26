package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.http.HttpMethod;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 지표를 밖에서 긁어 갈 수 있는가.
 *
 * <p><b>만드는 것과 내보내는 것은 다르다.</b> Micrometer 로 세고 있어도 수집기가
 * 읽을 자리가 없으면 아무도 못 본다. 그때 대시보드는 비어 있고, 사고 중에야 그
 * 사실을 안다.
 */
@Tag("context")
@SpringBootTest
class MetricsExposureTest {

    @Autowired
    private PrometheusMeterRegistry registry;

    @Autowired
    private RouteLocator locator;

    /** 레지스트리가 프로메테우스 형식으로 내놓아야 수집기가 읽는다. */
    @Test
    @DisplayName("프로메테우스_형식으로_긁힌다")
    void 프로메테우스_형식으로_긁힌다() {
        registry.counter("waiting.admission", "outcome", "PASS_UNDER_CAP", "cause", "none")
                .increment();

        assertThat(registry.scrape()).contains("waiting_admission_total");
    }

    /**
     * <b>같은 이름에 태그 키 집합이 둘이면 등록을 거절한다.</b> 단순 레지스트리는
     * 받아 주므로 이 시험 없이는 운영에서야 드러난다.
     */
    @Test
    @DisplayName("판정_지표의_태그_집합이_한_벌이다")
    void 판정_지표의_태그_집합이_한_벌이다() {
        registry.counter("waiting.admission", "outcome", "a", "cause", "none").increment();
        registry.counter("waiting.admission", "outcome", "b", "cause", "io").increment();

        assertThat(registry.scrape()).contains("waiting_admission_total");
    }

    /**
     * <b>관리 경로가 프록시 대상이면 안 된다.</b> 라우트가 잡으면 그 요청이 뒷단
     * 쿠폰 서비스로 가고, 지표는 밖에서 못 읽는데 뒷단만 이상한 요청을 받는다.
     */
    @Test
    @DisplayName("관리_경로는_라우트가_안_잡는다")
    void 관리_경로는_라우트가_안_잡는다() {
        ServerWebExchange 관리 = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.GET, "/actuator/prometheus"));

        assertThat(locator.getRoutes().collectList().block())
                .noneSatisfy(r -> assertThat(잡는가(r, 관리)).isTrue());
    }

    private boolean 잡는가(Route route, ServerWebExchange exchange) {
        return Boolean.TRUE.equals(Mono.from(route.getPredicate().apply(exchange)).block());
    }
}
