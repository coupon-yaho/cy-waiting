package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.AdmissionDecision;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.queue.EntryToken;
import com.kafkick.waiting.domain.queue.QueueToken;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 관찰 → 캐시 → 판정을 <b>한 줄로 꿴다</b> (7.2).
 *
 * <p>세 조각을 따로 재면 <b>둘이 다른 캐시를 봐도 전부 초록</b>입니다. 그러면
 * 뒷단 409 는 아무도 안 읽는 맵에 쌓이고 판정은 영원히 빈 캐시를 봅니다 —
 * 지표로도 "매진이 없었다" 와 구별되지 않습니다.
 */
class SoldOutEndToEndTest {

    private static final Instant 지금 = Instant.parse("2026-08-28T00:00:00Z");
    private static final String COUPON = "c1";
    private static final String MEMBER = "812934";
    private static final String SECRET = "not-a-real-secret-0123456789abcdef";

    private final MeterRegistry meters = new SimpleMeterRegistry();
    private final FakeQueuePort 줄 = FakeQueuePort.create();
    private final SnapshotHolder holder = SnapshotHolder.of(
            Duration.ofSeconds(3), Duration.ofSeconds(10), Clock.fixed(지금, ZoneOffset.UTC));
    private final SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(10_000);

    /** <b>하나뿐이다.</b> 관찰자와 판정이 이것을 같이 본다 — 그게 이 시험의 전부다. */
    private final SoldOutCache 캐시 = SoldOutCache.of(Duration.ofSeconds(30), 100);

    private final SoldOutObserver observer =
            SoldOutObserver.ofSnapshot(캐시, holder, meters);

    private final AdmissionGatewayFilter admission = AdmissionGatewayFilter.of(
            holder, AdmissionDecider.of(limiter, 0.5), Clock.fixed(지금, ZoneOffset.UTC),
            meters, 줄, QueueToken.of(SECRET), limiter, EntryToken.of(SECRET),
            IdempotencyKey.of(SECRET), 캐시);

    private final AtomicInteger 뒷단_횟수 = new AtomicInteger();

    private MockServerWebExchange 요청() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                        "/api/v1/coupons/" + COUPON + "/issue").header("X-Member-Id", MEMBER));
        exchange.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE,
                Map.of("couponId", COUPON));
        return exchange;
    }

    /** 판정을 태우고, 통과하면 뒷단이 매진을 답하는 것까지 이어 붙인다. */
    private MockServerWebExchange 태운다(boolean 뒷단이_매진) {
        MockServerWebExchange exchange = 요청();
        admission.filter(exchange, e -> {
            뒷단_횟수.incrementAndGet();
            if (!뒷단이_매진) {
                return Mono.empty();
            }
            // 라우팅 필터가 심는 표시. 이게 있어야 관찰자가 뒷단 응답으로 본다.
            e.getAttributes().put(ServerWebExchangeUtils.CLIENT_RESPONSE_ATTR, "뒷단 응답");
            return observer.filter(e, e2 -> {
                e2.getResponse().setStatusCode(HttpStatus.CONFLICT);
                return e2.getResponse().writeWith(Flux.just(e2.getResponse().bufferFactory()
                        .wrap("""
                                {"success":false,"error":{"code":"COUPON-306"}}"""
                                .getBytes(StandardCharsets.UTF_8))));
            });
        }).block();
        return exchange;
    }

    private void 재고가_있다고_심는다() {
        holder.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.idle(1_000)), new SnapshotMeta(1_000, 1), 지금));
    }

    /**
     * <b>첫 요청은 배우고, 두 번째부터 끊는다.</b>
     *
     * <p>관찰자와 판정이 다른 캐시를 보면 두 번째도 뒷단으로 갑니다.
     */
    @Test
    @DisplayName("뒷단이_한_번_매진을_답하면_다음부터_안_간다")
    void 뒷단이_한_번_매진을_답하면_다음부터_안_간다() {
        재고가_있다고_심는다();

        태운다(true);
        assertThat(뒷단_횟수).as("첫 요청은 배우러 간다").hasValue(1);

        MockServerWebExchange 두번째 = 태운다(true);

        assertThat(뒷단_횟수).as("노드당 최초 1건만 닿는다").hasValue(1);
        assertThat(두번째.getResponse().getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(두번째.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.REJECT_SOLD_OUT);
    }

    /** 대조 — 뒷단이 매진을 안 답하면 계속 간다. 없으면 "늘 막는다" 로도 통과한다. */
    @Test
    @DisplayName("뒷단이_매진을_안_답하면_계속_간다")
    void 뒷단이_매진을_안_답하면_계속_간다() {
        재고가_있다고_심는다();

        태운다(false);
        태운다(false);

        assertThat(뒷단_횟수).hasValue(2);
    }

    /**
     * <b>나중에 발행된 재료가 재고를 말하면 다시 간다.</b>
     *
     * <p>배운 것을 못 잊으면 재입고된 쿠폰이 이 노드에서만 안 팔립니다.
     */
    @Test
    @DisplayName("재입고를_보면_다시_뒷단으로_간다")
    void 재입고를_보면_다시_뒷단으로_간다() {
        holder.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.idle(1_000)), new SnapshotMeta(1_000, 1),
                지금.minusSeconds(1)));
        태운다(true);
        태운다(true);
        assertThat(뒷단_횟수).hasValue(1);

        재고가_있다고_심는다();
        태운다(false);

        assertThat(뒷단_횟수).as("재입고 뒤에는 간다").hasValue(2);
    }
}
