package com.kafkick.waiting.gateway;

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

    private BodyDeadline(Duration limit) {
        Objects.requireNonNull(limit, "limit 는 필수다");
        // **0 이면 끊는 것이 아닙니다.** 값으로 끄면 그 사실이 설정 어디에도
        // 안 드러나고, 커넥션이 붙잡히는 것이 정상으로 읽힙니다.
        if (limit.isNegative() || limit.isZero()) {
            throw new IllegalArgumentException("본문 상한은 양수여야 한다: " + limit);
        }
        this.limit = limit;
    }

    public static BodyDeadline of(Duration limit) {
        return new BodyDeadline(limit);
    }

    /** 이 상한. 설정에서 온 값이라 배포 없이 같이 움직입니다. */
    public Duration limit() {
        return limit;
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
                        return super.writeWith(body).timeout(limit,
                                Mono.error(new TimeoutException(
                                        "뒷단 본문이 " + limit + " 안에 안 끝났다")));
                    }
                };
        return chain.filter(exchange.mutate().response(cut).build());
    }
}
