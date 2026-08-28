package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.AdmissionDecision;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.queue.EntryToken;
import com.kafkick.waiting.domain.queue.QueueToken;
import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import reactor.core.publisher.Mono;

/**
 * 매진 negative cache 가 <b>판정 앞단에서</b> 걸린다 (7.2.3).
 *
 * <p>스냅샷은 최대 1.5초 낡습니다. 그 창에서 재료는 아직 재고가 있다고 말하는데
 * 뒷단은 이미 매진입니다. 노드당 최초 1건만 뒷단에 닿아야 합니다 — 20대면 20건.
 */
class SoldOutCacheAdmissionTest {

    private static final Instant 지금 = Instant.parse("2026-08-28T00:00:00Z");
    private static final String COUPON = "c1";
    private static final String MEMBER = "812934";
    private static final double IDLE_RATIO = 0.5;

    private final MeterRegistry meters = new SimpleMeterRegistry();
    private final FakeQueuePort 줄 = FakeQueuePort.create();
    private final SnapshotHolder holder = SnapshotHolder.of(
            Duration.ofSeconds(3), Duration.ofSeconds(10), Clock.fixed(지금, ZoneOffset.UTC));
    private final SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(10_000);
    private final SoldOutCache 캐시 = SoldOutCache.of(Duration.ofSeconds(10), 100);
    private final AtomicInteger 뒷단_횟수 = new AtomicInteger();

    private final AdmissionGatewayFilter filter = AdmissionGatewayFilter.of(
            holder, AdmissionDecider.of(limiter, IDLE_RATIO),
            Clock.fixed(지금, ZoneOffset.UTC), meters, 줄,
            QueueToken.of("not-a-real-secret-0123456789abcdef"), limiter,
            EntryToken.of("not-a-real-secret-0123456789abcdef"),
            IdempotencyKey.of("not-a-real-secret-0123456789abcdef"), 캐시);

    private MockServerWebExchange 태운다() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                        "/api/v1/coupons/" + COUPON + "/issue")
                        .header("X-Member-Id", MEMBER));
        exchange.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE,
                Map.of("couponId", COUPON));
        filter.filter(exchange, e -> {
            뒷단_횟수.incrementAndGet();
            return Mono.empty();
        }).block();
        return exchange;
    }

    /** 재료는 아직 재고가 있다고 말하는 상태를 심는다. 그래야 캐시가 유일한 근거다. */
    private void 재고가_있다고_심는다() {
        holder.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.idle(1_000)), new SnapshotMeta(1, 1), 지금));
    }

    /** 대조 — 관찰이 없으면 그대로 간다. 없으면 "늘 막는다" 로도 통과한다. */
    @Test
    @DisplayName("관찰이_없으면_뒷단으로_간다")
    void 관찰이_없으면_뒷단으로_간다() {
        재고가_있다고_심는다();

        태운다();

        assertThat(뒷단_횟수).hasValue(1);
    }

    /**
     * <b>노드당 최초 1건만 닿습니다.</b> 재료가 아직 재고를 말하는 1.5초 창에서
     * 몰려온 요청이 전부 뒷단까지 가는 것을 이것이 끊습니다.
     */
    @Test
    @DisplayName("관찰한_뒤에는_뒷단에_안_간다")
    void 관찰한_뒤에는_뒷단에_안_간다() {
        재고가_있다고_심는다();
        캐시.observed(COUPON, 지금);

        MockServerWebExchange exchange = 태운다();

        assertThat(뒷단_횟수).as("뒷단 도달").hasValue(0);
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(exchange.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.REJECT_SOLD_OUT);
    }

    /**
     * <b>재고가 돌아오면 캐시가 풀립니다</b> (7.2.4).
     *
     * <p>스냅샷이 <code>stock&gt;0</code> 이라고 말하는 순간이 해제 신호입니다.
     * TTL 을 기다리면 재입고된 쿠폰이 그 시간만큼 막힙니다.
     */
    @Test
    @DisplayName("재료가_재고를_말하면_관찰이_풀린다")
    void 재료가_재고를_말하면_관찰이_풀린다() {
        재고가_있다고_심는다();
        캐시.observed(COUPON, 지금);
        태운다();

        // 두 번째 요청은 첫 요청이 푼 뒤라 그대로 간다.
        태운다();

        assertThat(뒷단_횟수).as("두 번째는 간다").hasValue(1);
        assertThat(캐시.soldOut(COUPON, 지금)).isFalse();
    }

    /**
     * <b>재료가 매진을 말하면 풀지 않습니다.</b> 해제는 재입고를 봤을 때만이고,
     * 재고 0 은 재입고가 아닙니다 — 여기서 풀면 캐시가 매 요청 스스로 지워집니다.
     */
    @Test
    @DisplayName("재료가_매진이면_관찰을_안_푼다")
    void 재료가_매진이면_관찰을_안_푼다() {
        holder.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.closed(100)), new SnapshotMeta(1, 1), 지금));
        캐시.observed(COUPON, 지금);

        태운다();

        assertThat(캐시.soldOut(COUPON, 지금)).isTrue();
    }
}
