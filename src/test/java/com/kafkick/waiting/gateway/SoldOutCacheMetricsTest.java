package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Flux;

/**
 * 캐시가 차오르는 중인지를 <b>막히기 전에</b> 본다 (7.2.7).
 *
 * <p>상한에 닿으면 새 관찰을 못 받고, 그때부터 뒷단이 다시 다 맞습니다. 막힌
 * 뒤에 오르는 카운터로는 그 순간을 못 봅니다.
 */
class SoldOutCacheMetricsTest {

    private static final Instant 지금 = Instant.parse("2026-08-28T00:00:00Z");

    private final MeterRegistry meters = new SimpleMeterRegistry();

    /** 라우트를 탄 요청에 뒷단이 매진을 답한다. */
    private void 매진을_답한다(SoldOutObserver observer) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.post("/api/v1/coupons/c1/issue"));
        // 라우팅 필터가 심는 값 둘. 없으면 관찰자가 아무것도 안 한다.
        exchange.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE,
                Map.of("couponId", "c1"));
        exchange.getAttributes().put(ServerWebExchangeUtils.CLIENT_RESPONSE_ATTR, "뒷단 응답");

        observer.filter(exchange, e -> {
            e.getResponse().setStatusCode(HttpStatus.CONFLICT);
            return e.getResponse().writeWith(Flux.just(
                    e.getResponse().bufferFactory().wrap(
                            """
                            {"success":false,"error":{"code":"COUPON-306"}}"""
                                    .getBytes(StandardCharsets.UTF_8))));
        }).block();
    }

    private double 게이지(String name) {
        return meters.get(name).gauge().value();
    }

    @Test
    @DisplayName("담긴_수와_상한을_함께_낸다")
    void 담긴_수와_상한을_함께_낸다() {
        SoldOutCache 캐시 = SoldOutCache.of(Duration.ofSeconds(10), 7);
        캐시.bindMetrics(meters);

        캐시.observed("c1", 지금);
        캐시.observed("c2", 지금);

        assertThat(게이지("waiting.soldout.cache.size")).as("담긴 수").isEqualTo(2);
        // **상한을 같이 안 내면 담긴 수만으로는 여유를 모른다.** 7 이 큰지
        // 작은지는 상한을 봐야 알 수 있다.
        assertThat(게이지("waiting.soldout.cache.capacity")).as("상한").isEqualTo(7);
    }

    /**
     * <b>언제 채워졌는지는 게이지로 못 봅니다.</b>
     *
     * <p>담긴 수는 지금 몇 개인지만 말합니다. 매진이 언제 시작됐는지, 관찰이
     * 얼마나 자주 들어오는지는 카운터라야 답합니다.
     */
    @Test
    @DisplayName("관찰_횟수를_따로_센다")
    void 관찰_횟수를_따로_센다() {
        SoldOutCache 캐시 = SoldOutCache.of(Duration.ofSeconds(10), 7);
        SoldOutObserver observer = SoldOutObserver.ofPublishedAt(캐시, () -> 지금, meters);
        매진을_답한다(observer);

        // **존재 검사로 재지 않는다.** `counter(...)` 는 없으면 만들어 주므로,
        // 프로덕션이 그 태그를 한 번도 안 써도 0 이 나온다 (TS-11).
        assertThat(meters.find("waiting.soldout.observed").tag("result", "armed")
                .counter().count()).as("새 무장").isEqualTo(1);
        assertThat(meters.find("waiting.soldout.observed").tag("result", "already").counter())
                .as("아직 안 샜다").isNull();
    }

    /**
     * <b>무장 뒤에 새는 것을 값으로 봅니다.</b>
     *
     * <p>무장한 뒤로는 노드당 1건만 새야 합니다. <code>already</code> 가 계속
     * 오르는 것이 곧 방패가 안 듣는다는 신호인데, 태그를 안 가르면 그 둘이 한
     * 수치에 뭉쳐 구별이 안 됩니다.
     */
    @Test
    @DisplayName("두_번째_관찰은_새는_것으로_센다")
    void 두_번째_관찰은_새는_것으로_센다() {
        SoldOutCache 캐시 = SoldOutCache.of(Duration.ofSeconds(10), 7);
        SoldOutObserver observer = SoldOutObserver.ofPublishedAt(캐시, () -> 지금, meters);

        매진을_답한다(observer);
        매진을_답한다(observer);

        assertThat(meters.find("waiting.soldout.observed").tag("result", "armed")
                .counter().count()).as("새 무장").isEqualTo(1);
        assertThat(meters.find("waiting.soldout.observed").tag("result", "already")
                .counter().count()).as("새는 중").isEqualTo(1);
    }

    /** 게이지는 살아 있는 값이어야 합니다. 등록 시점 값이 박히면 늘 그 값입니다. */
    @Test
    @DisplayName("게이지가_뒤이은_변화를_따라간다")
    void 게이지가_뒤이은_변화를_따라간다() {
        SoldOutCache 캐시 = SoldOutCache.of(Duration.ofSeconds(10), 7);
        캐시.bindMetrics(meters);
        캐시.observed("c1", 지금);

        캐시.restocked("c1", 지금.plusSeconds(1));

        assertThat(게이지("waiting.soldout.cache.size")).isZero();
    }
}
