package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import java.util.List;
import org.springframework.http.codec.HttpMessageWriter;
import org.springframework.web.reactive.function.server.HandlerStrategies;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.reactive.result.view.ViewResolver;

/**
 * 서킷이 열렸을 때 사용자가 받는 것.
 *
 * <p><b>이것이 없으면 서킷이 열리는 순간 404 가 나간다.</b> 프레임워크는 fallback
 * 주소로 넘길 뿐이고, 그 주소를 아무도 안 받으면 "없는 경로" 가 된다. 재고가 없다는
 * 뜻으로 읽히므로 사용자는 다시 오지 않는다 — 뒷단은 잠깐 흔들렸을 뿐인데.
 *
 * <p>장애 때만 드러나는 실패라 일반 시험으로는 안 걸린다.
 */
class BackendFallbackTest {

    private static final Instant 지금 = Instant.parse("2026-08-26T00:00:00Z");

    private final MeterRegistry meters = new SimpleMeterRegistry();

    private final BackendFallback fallback = BackendFallback.of(
            Clock.fixed(지금, ZoneOffset.UTC), meters, () -> 0.5);

    private static final HandlerStrategies 기본 = HandlerStrategies.withDefaults();

    private MockServerWebExchange 넘어온_요청() {
        return MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST, "/fallback/issue"));
    }

    /**
     * <b>실제로 실어 보내는 데까지 간다.</b> 만들어진 응답만 보면 라우트가 그것을
     * 못 싣는 형태여도 초록이다.
     */
    private void 답한다(BackendFallback f, MockServerWebExchange exchange) {
        f.respond(ServerRequest.create(exchange, 기본.messageReaders()))
                .flatMap(r -> r.writeTo(exchange, new ServerResponse.Context() {
                    @Override
                    public List<HttpMessageWriter<?>> messageWriters() {
                        return 기본.messageWriters();
                    }

                    @Override
                    public List<ViewResolver> viewResolvers() {
                        return List.of();
                    }
                }))
                .block();
    }

    /** 뒷단이 못 받는 것과 없는 것은 다르다. 404 는 사용자에게 매진으로 읽힌다. */
    @Test
    @DisplayName("서킷이_열리면_503을_낸다")
    void 서킷이_열리면_503을_낸다() {
        MockServerWebExchange exchange = 넘어온_요청();

        답한다(fallback, exchange);

        assertThat(exchange.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    /**
     * <b>안내가 없으면 전원이 같은 순간에 다시 온다.</b> 서킷이 닫히자마자
     * 재포화되어 다시 열리고, 그 진동이 회복을 막는다.
     */
    @Test
    @DisplayName("다시_올_시각을_알려_준다")
    void 다시_올_시각을_알려_준다() {
        MockServerWebExchange exchange = 넘어온_요청();

        답한다(fallback, exchange);

        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .isNotNull()
                .satisfies(v -> assertThat(Integer.parseInt(v)).isPositive());
    }

    /**
     * <b>줄에 선 사람에게는 자리가 그대로라고 말해야 한다.</b> 안 그러면 다시
     * 줄을 서려 하고, 그건 자기 자리를 버리는 일이다.
     */
    @Test
    @DisplayName("순번이_유지된다고_안내한다")
    void 순번이_유지된다고_안내한다() {
        MockServerWebExchange exchange = 넘어온_요청();

        답한다(fallback, exchange);

        assertThat(exchange.getResponse().getBodyAsString().block())
                .contains("대기 순번");
    }

    /**
     * <b>같은 값을 주면 전원이 같은 순간에 온다.</b> 서킷이 닫히는 순간 전체가
     * 한꺼번에 몰려 약한 뒷단을 다시 무너뜨린다.
     */
    @Test
    @DisplayName("다시_올_시각을_흩는다")
    void 다시_올_시각을_흩는다() {
        BackendFallback 이른_쪽 = BackendFallback.of(
                Clock.fixed(지금, ZoneOffset.UTC), meters, () -> 0.0);
        BackendFallback 늦은_쪽 = BackendFallback.of(
                Clock.fixed(지금, ZoneOffset.UTC), meters, () -> 1.0);

        MockServerWebExchange a = 넘어온_요청();
        MockServerWebExchange b = 넘어온_요청();
        답한다(이른_쪽, a);
        답한다(늦은_쪽, b);

        assertThat(a.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .isNotEqualTo(b.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER));
    }

    /** 얼마나 열렸는지는 지표로만 안다. 로그는 요청마다 남길 수 없다. */
    @Test
    @DisplayName("넘어온_횟수를_센다")
    void 넘어온_횟수를_센다() {
        답한다(fallback, 넘어온_요청());
        답한다(fallback, 넘어온_요청());

        assertThat(meters.counter("waiting.backend.fallback", "outcome", "open").count())
                .isEqualTo(2);
    }

    /**
     * <b>핸들러가 있어도 아무도 그 주소로 안 보내면 소용없다.</b> 서킷 필터는
     * {@code forward:} 로 넘길 뿐이라, 받는 라우트가 없으면 404 다 — 핸들러를
     * 만들어 둔 것이 오히려 안심을 준다.
     */
    @Test
    @DisplayName("fallback_주소를_받는_라우트가_있다")
    void fallback_주소를_받는_라우트가_있다() {
        RouterFunction<ServerResponse> routes = new BackendFallbackRoutes().fallbackRoutes(fallback);

        ServerRequest 넘어옴 = ServerRequest.create(
                MockServerWebExchange.from(
                        MockServerHttpRequest.method(HttpMethod.POST, "/fallback/issue")),
                HandlerStrategies.withDefaults().messageReaders());

        assertThat(routes.route(넘어옴).blockOptional()).isPresent();
    }

    /** 남의 경로까지 잡으면 안 된다. 잡으면 그 경로가 통째로 503 이 된다. */
    @Test
    @DisplayName("fallback_라우트가_남의_경로를_안_잡는다")
    void fallback_라우트가_남의_경로를_안_잡는다() {
        RouterFunction<ServerResponse> routes = new BackendFallbackRoutes().fallbackRoutes(fallback);

        ServerRequest 발급 = ServerRequest.create(
                MockServerWebExchange.from(
                        MockServerHttpRequest.method(HttpMethod.POST, "/api/v1/coupons/c1/issue")),
                HandlerStrategies.withDefaults().messageReaders());

        assertThat(routes.route(발급).blockOptional()).isEmpty();
    }
}
