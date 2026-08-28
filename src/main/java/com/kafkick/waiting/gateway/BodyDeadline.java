package com.kafkick.waiting.gateway;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import org.reactivestreams.Publisher;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 본문이 <b>끝없이 느린</b> 뒷단을 끊습니다.
 *
 * <p>응답 상한은 읽기 사이의 간격을 봅니다. 그 상한보다 촘촘히 흘리면 영영 안
 * 걸리고, 헤더가 이미 나간 뒤라 판정 쪽 시한도 커넥션을 못 끊습니다 — 실측으로
 * 40초를 기다려도 안 끝났습니다.
 */
// **서킷 안쪽에 못 둔다.** 응답을 감싸려면 프레임워크의 쓰기 필터보다 앞이어야
// 하고, 그 자리는 서킷보다 바깥이다. 여기서 끊은 것은 서킷 창에 안 쌓인다.
public final class BodyDeadline implements GatewayFilter {

    private final Duration limit;

    /**
     * 끊은 건수.
     *
     * <p><b>로그가 아니라 지표다</b> (LG-1). 끊기는 것은 요청 단위라 로그로
     * 남기면 그게 곧 요청당 로그다. 그런데 안 세면 오탐이 나기 시작한 순간을
     * 못 본다 — 큰 응답이 정상 속도로 흐르다 걸리는 것이 그 위험이다.
     */
    private final Counter cuts;

    private BodyDeadline(Duration limit, MeterRegistry meters) {
        Objects.requireNonNull(limit, "limit 는 필수다");
        Objects.requireNonNull(meters, "meters 는 필수다");
        // **0 이면 끊는 것이 아닙니다.** 값으로 끄면 그 사실이 설정 어디에도
        // 안 드러나고, 커넥션이 붙잡히는 것이 정상으로 읽힙니다.
        if (limit.isNegative() || limit.isZero()) {
            throw new IllegalArgumentException("본문 상한은 양수여야 한다: " + limit);
        }
        this.limit = limit;
        this.cuts = meters.counter("waiting.backend.body.cut");
    }

    /**
     * 응답 상한과의 앞뒤를 <b>기동에서</b> 못 박는다.
     *
     * <p>시험으로만 두면 배포 설정 한 줄이 순서를 뒤집고, 그 사실은 장애 때만
     * 드러난다. 짧으면 정상 속도로 흐르는 큰 응답을 이쪽이 먼저 죽인다.
     */
    // 격벽 시한(15초)과는 겨루지 않는다. 그쪽은 **자리**를 반납하고 이쪽은
    // **커넥션**을 끊는다 — 헤더가 나간 뒤라 격벽 시한으로는 커넥션을 못 끊는
    // 것이 이 필터가 존재하는 이유다. 실측에서 자리는 15초에 돌아왔고 커넥션은
    // 24.5초에 끊겼다. 둘 사이 구간에 새는 것은 자리가 아니라 클라이언트다.
    public static BodyDeadline of(Duration limit, Duration responseTimeout,
            MeterRegistry meters) {
        if (limit.compareTo(responseTimeout) <= 0) {
            throw new IllegalArgumentException(
                    "본문 상한이 응답 상한보다 길어야 한다: 본문=%s 응답=%s"
                            .formatted(limit, responseTimeout));
        }
        return new BodyDeadline(limit, meters);
    }

    /**
     * 본문 쓰기 <b>전체</b>에 시한을 겁니다.
     *
     * <p>조각 사이에만 걸면 못 잡습니다 — 상한보다 촘촘히 꾸준히 흘리는 뒷단이
     * 정확히 그 구멍이고, 실측에서 40초를 기다려도 안 끝난 것이 그 모양입니다.
     */
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpResponseDecorator cut =
                new ServerHttpResponseDecorator(exchange.getResponse()) {
                    @Override
                    public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
                        // **예외를 지연 생성한다** (EX-3). `Mono.error(Throwable)`
                        // 은 즉시 평가라, 안 걸리는 응답까지 전부 스택트레이스를
                        // 채운다 — 보호 장치가 부하가 된다.
                        return super.writeWith(body)
                                .timeout(limit, Mono.error(() -> new TimeoutException(
                                        "뒷단 본문이 " + limit + " 안에 안 끝났다")))
                                // **여기서 끝난다.** 응답은 이미 커밋됐으므로
                                // 번역할 수 없다. 셀 수는 있고, 그게 이 보호
                                // 장치를 튜닝할 유일한 근거다.
                                .doOnError(TimeoutException.class, e -> cuts.increment());
                    }
                };
        return chain.filter(exchange.mutate().response(cut).build());
    }
}
