package com.kafkick.waiting.gateway;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import org.reactivestreams.Publisher;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 뒷단이 낸 매진 응답을 <b>관찰만</b> 한다 (7.2.2 · B-10).
 *
 * <p>응답을 바꾸지 않는다. 게이트웨이가 매진 응답을 스스로 만들면 그 순간
 * 게이트웨이의 존재가 드러난다 — 뒷단이 낸 것을 그대로 흘려보낸다.
 */
public final class SoldOutObserver implements GatewayFilter {

    /** 매진 사유 코드. <b>상태만으로는 못 가린다</b> — 다른 409 가 섞인다. */
    private static final String SOLD_OUT_CODE = "COUPON-306";

    /**
     * 코드를 찾아볼 앞부분 길이.
     *
     * <p>오류 봉투는 짧고 코드가 앞에 온다. 상한이 없으면 뒷단이 큰 본문을
     * 409 로 낼 때 그 전부를 문자열로 만든다.
     */
    private static final int PREFIX = 512;

    private static final PathPattern PATH = new PathPatternParser()
            .parse("/api/v1/coupons/{couponId}/**");

    private final SoldOutCache cache;
    private final Clock clock;

    private SoldOutObserver(SoldOutCache cache, Clock clock) {
        this.cache = Objects.requireNonNull(cache, "cache 는 필수다");
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
    }

    public static SoldOutObserver of(SoldOutCache cache, Clock clock) {
        return new SoldOutObserver(cache, clock);
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String couponId = couponOf(exchange);
        if (couponId == null) {
            // **못 뽑은 것을 한 자리에 몰아 담지 않는다.** 그 이름이 매진으로
            // 굳고, 다음 판정이 그것을 읽는다.
            return chain.filter(exchange);
        }
        ServerHttpResponse original = exchange.getResponse();
        ServerHttpResponseDecorator watched = new ServerHttpResponseDecorator(original) {
            @Override
            public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
                // **흘려보내면서 본다.** 삼켰다가 다시 쓰면 상한을 넘긴 응답을
                // 되돌릴 방법이 없고, 그때는 이 관찰이 사고의 원인이 된다.
                //
                // 첫 조각만 본다. 코드는 봉투 앞에 있고, 조각을 모으면 그것이
                // 곧 버퍼링이다.
                return super.writeWith(Flux.from(body)
                        .doOnNext(buffer -> inspect(original, buffer, couponId)));
            }
        };
        return chain.filter(exchange.mutate().response(watched).build());
    }

    /**
     * 매진 봉투인가.
     *
     * <p><b>상태와 사유를 함께 본다.</b> 상태만 보면 중복 발급 같은 다른 409 가
     * 그 쿠폰을 끊고, 본문만 보면 매진을 설명하는 200 이 같은 일을 한다.
     */
    private void inspect(ServerHttpResponse response, DataBuffer buffer, String couponId) {
        if (!HttpStatus.CONFLICT.equals(response.getStatusCode())) {
            return;
        }
        // 읽기 위치를 안 옮긴다. 옮기면 뒷사람이 빈 조각을 받는다.
        int length = Math.min(buffer.readableByteCount(), PREFIX);
        String head = buffer.toString(buffer.readPosition(), length, StandardCharsets.UTF_8);
        if (head.contains(SOLD_OUT_CODE)) {
            cache.observed(couponId, clock.instant());
        }
    }

    private String couponOf(ServerWebExchange exchange) {
        PathPattern.PathMatchInfo vars =
                PATH.matchAndExtract(exchange.getRequest().getPath().pathWithinApplication());
        if (vars == null) {
            return null;
        }
        Map<String, String> byName = vars.getUriVariables();
        return byName.get("couponId");
    }
}
