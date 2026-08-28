package com.kafkick.waiting.gateway;

import com.kafkick.waiting.control.SnapshotHolder;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.web.server.ServerWebExchange;
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

    private static final String METRIC = "waiting.soldout.observed";

    private static final Logger log = LoggerFactory.getLogger(SoldOutObserver.class);

    private final SoldOutCache cache;
    private final Supplier<Instant> publishedAt;
    private final MeterRegistry meters;

    private SoldOutObserver(SoldOutCache cache, Supplier<Instant> publishedAt,
            MeterRegistry meters) {
        this.cache = Objects.requireNonNull(cache, "cache 는 필수다");
        this.publishedAt = Objects.requireNonNull(publishedAt, "publishedAt 은 필수다");
        this.meters = Objects.requireNonNull(meters, "meters 는 필수다");
    }

    /**
     * <b>노드 시계가 아니라 재료의 발행 시각을 심는다.</b>
     *
     * <p>해제가 발행 시각끼리 비교하므로, 무장도 같은 시계 영역이라야 한다 —
     * 섞으면 두 시계의 차가 그대로 판정에 실린다.
     */
    public static SoldOutObserver ofSnapshot(SoldOutCache cache, SnapshotHolder holder,
            MeterRegistry meters) {
        return new SoldOutObserver(cache, () -> holder.view().snapshot().publishedAt(), meters);
    }

    /** 발행 시각원을 직접 받는다. 고정하지 못하면 해제 비교를 못 잰다 (TS-4). */
    public static SoldOutObserver ofPublishedAt(SoldOutCache cache,
            Supplier<Instant> publishedAt, MeterRegistry meters) {
        return new SoldOutObserver(cache, publishedAt, meters);
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
        Prefix prefix = new Prefix();
        ServerHttpResponseDecorator watched = new ServerHttpResponseDecorator(original) {
            @Override
            public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
                // **흘려보내면서 본다.** 삼켰다가 다시 쓰면 상한을 넘긴 응답을
                // 되돌릴 방법이 없고, 그때는 이 관찰이 사고의 원인이 된다.
                return super.writeWith(Flux.from(body)
                        .doOnNext(buffer -> inspect(exchange, buffer, couponId, prefix)));
            }

            @Override
            public Mono<Void> writeAndFlushWith(
                    Publisher<? extends Publisher<? extends DataBuffer>> body) {
                // **이쪽도 덮는다.** 스트리밍 미디어 타입이면 쓰기 필터가 여기로
                // 온다 — 안 덮으면 그 응답의 관찰이 통째로 지나간다.
                return super.writeAndFlushWith(Flux.from(body)
                        .map(part -> Flux.from(part)
                                .doOnNext(buffer -> inspect(exchange, buffer, couponId, prefix))));
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
    private void inspect(ServerWebExchange exchange, DataBuffer buffer, String couponId,
            Prefix prefix) {
        if (!HttpStatus.CONFLICT.equals(exchange.getResponse().getStatusCode())) {
            return;
        }
        // **정말로 뒷단에 닿은 응답인가.** 이 필터는 쓰기 필터보다 바깥이라
        // 판정·서킷보다도 바깥이다. 안 가르면 게이트웨이 자신이 낸 매진
        // (사다리 1번의 `COUPON-306`)을 되먹여, 뒷단이 살아나도 안 풀린다.
        //
        // 이 표시는 라우팅 필터만 심고, 서킷 폴백은 재디스패치 전에 지운다.
        if (exchange.getAttribute(ServerWebExchangeUtils.CLIENT_RESPONSE_ATTR) == null) {
            return;
        }
        if (!prefix.append(buffer) || !prefix.contains(SOLD_OUT_CODE)) {
            return;
        }
        boolean armed = cache.observed(couponId, publishedAt.get());
        // **새 무장과 새는 것을 가른다.** 무장한 뒤로는 노드당 1건만 새야
        // 하므로, `already` 가 계속 오르는 것이 곧 방패가 안 듣는다는 신호다.
        // 태그를 안 달면 그 둘이 한 수치에 뭉쳐 구별이 안 된다.
        meters.counter(METRIC, "result", armed ? "armed" : "already").increment();
        if (armed) {
            // **쌍의 앞쪽이다** (LG-2). 뒤쪽은 판정이 풀 때 찍는다. 쿠폰당 한
            // 번만 찍히므로 매진이 몰려도 로그가 안 넘친다 (LG-3).
            log.info("매진 관찰 — 쿠폰 {} 의 발급을 뒷단이 거절했다. 이 노드는 끊는다",
                    couponId);
        }
    }

    /**
     * 앞부분만 모은다.
     *
     * <p><b>조각 하나만 보면 코드가 경계에 걸려 안 보인다.</b> 그렇다고 전부
     * 모으면 그것이 곧 버퍼링이다. 상한까지만 이어 붙인다.
     */
    private static final class Prefix {

        private final StringBuilder head = new StringBuilder();
        private int bytes;
        private boolean done;

        /** 더 모았으면 참. 이미 상한을 채웠거나 답이 난 뒤면 거짓이다. */
        boolean append(DataBuffer buffer) {
            if (done || bytes >= PREFIX) {
                return false;
            }
            // **바이트로 센다.** 글자 수로 세면 한글 봉투에서 창이 세 배로
            // 늘어, 상한이 뜻하는 바가 코드마다 달라진다.
            int take = Math.min(buffer.readableByteCount(), PREFIX - bytes);
            if (take <= 0) {
                return false;
            }
            // 읽기 위치를 안 옮긴다. 옮기면 뒷사람이 빈 조각을 받는다.
            //
            // 조각 경계가 여러 바이트 문자를 가를 수 있다. 찾는 것이 아스키라
            // 잘린 꼬리가 치환 문자가 되어도 검색에는 영향이 없다.
            head.append(buffer.toString(buffer.readPosition(), take, StandardCharsets.UTF_8));
            bytes += take;
            return true;
        }

        boolean contains(String needle) {
            if (head.indexOf(needle) >= 0) {
                done = true;
                return true;
            }
            return false;
        }
    }

    /**
     * 쿠폰 이름을 <b>판정과 같은 출처에서</b> 뽑는다.
     *
     * <p>경로를 다시 파면 담는 키와 읽는 키의 출처가 둘이 된다 — 라우트 술어가
     * 한 번만 느슨해지면 캐시가 조용히 0% 적중이 되고, 상한을 클라이언트가 고른
     * 문자열로 채울 수 있다. 라우트를 안 탄 요청이 자동으로 걸러지는 것은 덤이다.
     */
    private String couponOf(ServerWebExchange exchange) {
        Object vars = exchange.getAttribute(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE);
        if (!(vars instanceof Map<?, ?> byName)) {
            return null;
        }
        return byName.get("couponId") instanceof String id ? id : null;
    }
}
