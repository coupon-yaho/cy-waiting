package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * 본문이 <b>끝없이 느린</b> 뒷단을 끊습니다 (6.2 · CY-633).
 *
 * <p>응답 상한은 읽기 사이의 간격을 봅니다. 그 상한보다 촘촘히 흘리면 영영 안 걸리고,
 * 헤더가 이미 나간 뒤라 판정 쪽 시한도 커넥션을 못 끊습니다 — 실측으로 40초를
 * 기다려도 안 끝났습니다.
 */
class BodyDeadlineTest {

    private static final Duration 상한 = Duration.ofMillis(200);

    private MockServerWebExchange 조회() {
        return MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.GET, "/api/v1/coupons"));
    }

    /** 끝없이 흘리는 본문을 상한에서 끊습니다. 안 끊으면 커넥션이 영영 붙잡힙니다. */
    @Test
    @DisplayName("끝없는_본문을_상한에서_끊는다")
    void 끝없는_본문을_상한에서_끊는다() {
        MockServerWebExchange exchange = 조회();
        BodyDeadline deadline = BodyDeadline.of(상한);

        StepVerifier.create(deadline.filter(exchange, ex ->
                        ex.getResponse().writeWith(Flux.interval(Duration.ofMillis(20))
                                .map(i -> ex.getResponse().bufferFactory().wrap("x".getBytes())))))
                .expectError()
                .verify(Duration.ofSeconds(5));
    }

    /** 상한 안에 끝나는 본문은 그대로 흘립니다. 안 그러면 정상 응답을 죽입니다. */
    @Test
    @DisplayName("제_시간에_끝나는_본문은_그대로_흘린다")
    void 제_시간에_끝나는_본문은_그대로_흘린다() {
        MockServerWebExchange exchange = 조회();
        AtomicBoolean 흘렀다 = new AtomicBoolean();

        StepVerifier.create(BodyDeadline.of(상한).filter(exchange, ex ->
                        ex.getResponse().writeWith(Mono.fromSupplier(() -> {
                            흘렀다.set(true);
                            return ex.getResponse().bufferFactory().wrap("목록".getBytes());
                        }))))
                .verifyComplete();

        assertThat(흘렀다).isTrue();
        assertThat(exchange.getResponse().getBodyAsString().block()).isEqualTo("목록");
    }

    /** 상한이 0 이하면 끊는 것이 아니다. 값으로 끄면 그 사실이 안 드러난다. */
    @Test
    @DisplayName("상한이_0이하면_기동을_막는다")
    void 상한이_0이하면_기동을_막는다() {
        assertThat(잡는다(() -> BodyDeadline.of(Duration.ZERO)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Throwable 잡는다(Runnable r) {
        try {
            r.run();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }
}
