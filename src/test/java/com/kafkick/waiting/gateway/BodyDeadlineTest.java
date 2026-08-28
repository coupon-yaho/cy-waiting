package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
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
 * <p><b>응답 상한은 헤더가 오기까지만 잽니다.</b> 그 뒤로는 아무것도 안 재므로
 * 본문을 한 바이트도 안 보내도 안 걸리고, 헤더가 나간 뒤라 판정 쪽 시한도 커넥션을
 * 못 끊습니다 — 실측으로 40초를 기다려도 안 끝났습니다.
 */
class BodyDeadlineTest {

    private static final Duration 상한 = Duration.ofSeconds(2);

    /** 응답 상한. 본문 상한은 이보다 길어야 한다. */
    private static final Duration 응답 = Duration.ofSeconds(1);

    private final MeterRegistry meters = new SimpleMeterRegistry();

    private MockServerWebExchange 조회() {
        return MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.GET, "/api/v1/coupons"));
    }

    private double 끊은_건수() {
        return meters.counter("waiting.backend.body.cut").count();
    }

    /**
     * 끝없이 흘리는 본문을 상한에서 끊습니다.
     *
     * <p><b>가상 시간으로 잽니다</b> (TS-4). 실시간으로 재면 이 시험이 장비
     * 속도에 걸리고, 그때 나오는 실패는 회귀가 아니라 잡음입니다.
     */
    @Test
    @DisplayName("끝없는_본문을_상한에서_끊는다")
    void 끝없는_본문을_상한에서_끊는다() {
        MockServerWebExchange exchange = 조회();
        BodyDeadline deadline = BodyDeadline.of(상한, 응답, meters);

        StepVerifier.withVirtualTime(() -> deadline.filter(exchange, ex ->
                        ex.getResponse().writeWith(Flux.interval(Duration.ofMillis(200))
                                .map(i -> ex.getResponse().bufferFactory()
                                        .wrap("x".getBytes())))))
                .thenAwait(상한.plusSeconds(1))
                // **타입까지 봅니다** (TS-11). 다른 이유로 터져도 통과하면,
                // 이 시험이 재는 것은 "끊긴다" 가 아니라 "터진다" 입니다.
                .expectError(TimeoutException.class)
                .verify();

        assertThat(끊은_건수()).as("끊은 건수").isEqualTo(1);
    }

    /** 상한 안에 끝나는 본문은 그대로 흘립니다. 안 그러면 정상 응답을 죽입니다. */
    @Test
    @DisplayName("제_시간에_끝나는_본문은_그대로_흘린다")
    void 제_시간에_끝나는_본문은_그대로_흘린다() {
        MockServerWebExchange exchange = 조회();
        AtomicBoolean 흘렀다 = new AtomicBoolean();

        StepVerifier.create(BodyDeadline.of(상한, 응답, meters).filter(exchange, ex ->
                        ex.getResponse().writeWith(Mono.fromSupplier(() -> {
                            흘렀다.set(true);
                            return ex.getResponse().bufferFactory().wrap("목록".getBytes());
                        }))))
                .verifyComplete();

        assertThat(흘렀다).isTrue();
        assertThat(exchange.getResponse().getBodyAsString().block()).isEqualTo("목록");
        assertThat(끊은_건수()).as("안 끊었으면 안 센다").isZero();
    }

    /**
     * <b>스트리밍 응답도 끊습니다.</b>
     *
     * <p>쓰기 필터는 미디어 타입이 스트리밍이면 <code>writeAndFlushWith</code> 로
     * 갑니다. 한쪽만 덮으면 <code>text/event-stream</code> 헤더 하나로 이 보호
     * 장치가 통째로 무효가 됩니다 — 뒷단이 잘못된 상태로 무너지면 그 모양입니다.
     */
    @Test
    @DisplayName("스트리밍_본문도_상한에서_끊는다")
    void 스트리밍_본문도_상한에서_끊는다() {
        MockServerWebExchange exchange = 조회();
        BodyDeadline deadline = BodyDeadline.of(상한, 응답, meters);

        StepVerifier.withVirtualTime(() -> deadline.filter(exchange, ex ->
                        ex.getResponse().writeAndFlushWith(
                                Flux.interval(Duration.ofMillis(200))
                                        .map(i -> Flux.just(ex.getResponse().bufferFactory()
                                                .wrap("x".getBytes()))))))
                .thenAwait(상한.plusSeconds(1))
                .expectError(TimeoutException.class)
                .verify();

        assertThat(끊은_건수()).as("끊은 건수").isEqualTo(1);
    }

    /**
     * <b>스트리밍으로 회복해도 구간이 닫혀야 합니다.</b>
     *
     * <p>안 닫히면 다음 구간의 진입 로그가 영영 안 찍히고, 해제 요약도 안
     * 나옵니다 — 사고 관측이 첫 판 이후로 통째로 멎습니다.
     */
    @Test
    @DisplayName("스트리밍으로_회복해도_구간이_닫힌다")
    void 스트리밍으로_회복해도_구간이_닫힌다() {
        BodyDeadline deadline = BodyDeadline.of(상한, 응답, meters);
        StepVerifier.withVirtualTime(() -> deadline.filter(조회(), ex ->
                        ex.getResponse().writeAndFlushWith(Flux.interval(Duration.ofMillis(200))
                                .map(i -> Flux.just(ex.getResponse().bufferFactory()
                                        .wrap("x".getBytes()))))))
                .thenAwait(상한.plusSeconds(1))
                .expectError(TimeoutException.class)
                .verify();

        // 스트리밍으로 정상 응답 하나가 지나면 구간이 닫힌다.
        MockServerWebExchange 회복 = 조회();
        StepVerifier.create(deadline.filter(회복, ex ->
                        ex.getResponse().writeAndFlushWith(Flux.just(Flux.just(
                                ex.getResponse().bufferFactory().wrap("목록".getBytes()))))))
                .verifyComplete();

        // 닫혔으면 다음 끊김이 다시 새 구간이 된다 — 건수가 1 부터 다시 센다.
        StepVerifier.withVirtualTime(() -> deadline.filter(조회(), ex ->
                        ex.getResponse().writeAndFlushWith(Flux.interval(Duration.ofMillis(200))
                                .map(i -> Flux.just(ex.getResponse().bufferFactory()
                                        .wrap("x".getBytes()))))))
                .thenAwait(상한.plusSeconds(1))
                .expectError(TimeoutException.class)
                .verify();

        assertThat(끊은_건수()).as("끊은 건수").isEqualTo(2);
        // **구간이 닫혔어야 둘로 센다.** 안 닫히면 두 번째 끊김이 같은 구간에
        // 묻히고, 그때부터 진입 로그도 해제 요약도 안 나온다.
        assertThat(meters.counter("waiting.backend.body.episode").count())
                .as("구간 수").isEqualTo(2);
    }

    /** 상한이 0 이하면 끊는 것이 아니다. 값으로 끄면 그 사실이 안 드러난다. */
    @Test
    @DisplayName("상한이_0_이하면_거절한다")
    void 상한이_0_이하면_거절한다() {
        assertThatThrownBy(() -> BodyDeadline.of(Duration.ZERO, 응답, meters))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("본문 상한");
    }

    /**
     * <b>응답 상한보다 짧으면 기동을 막습니다.</b>
     *
     * <p>짧으면 정상 속도로 흐르는 큰 응답을 이쪽이 먼저 죽입니다. 시험으로만
     * 두면 배포 설정 한 줄이 순서를 뒤집고 그 사실은 장애 때만 드러납니다.
     */
    @Test
    @DisplayName("응답_상한보다_짧으면_기동을_막는다")
    void 응답_상한보다_짧으면_기동을_막는다() {
        assertThatThrownBy(() -> BodyDeadline.of(응답.minusMillis(1), 응답, meters))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("응답 상한");
        assertThatThrownBy(() -> BodyDeadline.of(응답, 응답, meters))
                .as("같아도 안 된다 — 어느 쪽이 먼저인지가 정해져야 한다")
                .isInstanceOf(IllegalArgumentException.class);
    }
}
