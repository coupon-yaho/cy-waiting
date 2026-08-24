package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.AdmissionDecision;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 판정 재료를 <b>로컬 스냅샷에서만</b> 읽는다.
 *
 * <p>요청마다 레디스를 치면 제어 평면을 만든 이유가 사라진다. 그리고 스냅샷에
 * 없는 쿠폰을 그대로 흘리면 레디스 키가 무한히 생긴다.
 */
class AdmissionGatewayFilterTest {

    private static final String COUPON = "c1";

    private final SnapshotHolder holder = SnapshotHolder.of(
            Duration.ofSeconds(3), Duration.ofSeconds(10), Clock.systemUTC());
    private final AdmissionGatewayFilter filter = AdmissionGatewayFilter.of(
            holder, AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(10_000), 0.2));

    private final AtomicReference<Boolean> 뒷단에_닿음 = new AtomicReference<>(false);

    private MockServerWebExchange 태운다(String couponId) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                        "/api/v1/coupons/" + couponId + "/issue"));
        exchange.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE,
                Map.of("couponId", couponId));
        filter.filter(exchange, e -> {
            뒷단에_닿음.set(true);
            return Mono.empty();
        }).block();
        return exchange;
    }

    private void 스냅샷을_심는다(CouponState state) {
        holder.replace(new GatewaySnapshot(
                state == null ? Map.of() : Map.of(COUPON, state),
                new SnapshotMeta(1_000, 1), Instant.now()));
    }

    @Test
    @DisplayName("스냅샷에_없는_쿠폰은_뒷단에_안_간다")
    void 스냅샷에_없는_쿠폰은_뒷단에_안_간다() {
        // **레디스 키 무한 생성을 막는 자리다.** 없는 쿠폰을 그대로 흘리면
        // 아무 문자열이나 큐를 하나씩 만든다.
        스냅샷을_심는다(null);

        MockServerWebExchange exchange = 태운다(COUPON);

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(뒷단에_닿음).hasValue(false);
    }

    @Test
    @DisplayName("첫_스냅샷_전에는_404_를_안_낸다")
    void 첫_스냅샷_전에는_404_를_안_낸다() {
        // 기동 직후 재료가 없다고 전면 404 를 내면, 뜨자마자 모든 쿠폰이
        // 없는 것이 된다 — 재기동해도 같은 구간을 또 지난다.
        MockServerWebExchange exchange = 태운다(COUPON);

        assertThat(exchange.getResponse().getStatusCode()).isNotEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("경로변수가_없으면_400_이다")
    void 경로변수가_없으면_400_이다() {
        // 라우트에서 변수 이름을 빼면 판정할 쿠폰이 없다. 그대로 흘리면
        // 판정이 사라진 채로 기동만 성공한다.
        스냅샷을_심는다(CouponStates.idle(100));
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST, "/api/v1/coupons/c1/issue"));

        filter.filter(exchange, e -> {
            뒷단에_닿음.set(true);
            return Mono.empty();
        }).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(뒷단에_닿음).hasValue(false);
    }

    @Test
    @DisplayName("한산한_쿠폰은_그대로_지나간다")
    void 한산한_쿠폰은_그대로_지나간다() {
        // **이 경로가 R1 이다.** 안 몰리는 쿠폰까지 줄을 세우면 제품이 성립하지 않는다.
        스냅샷을_심는다(CouponStates.idle(100));

        MockServerWebExchange exchange = 태운다(COUPON);

        assertThat(뒷단에_닿음).hasValue(true);
        assertThat(exchange.getResponse().getStatusCode()).isNull();
    }

    @Test
    @DisplayName("매진은_뒷단에_안_간다")
    void 매진은_뒷단에_안_간다() {
        // 재고가 없으면 여기서 끝낸다. 레디스도 뒷단도 안 친다.
        스냅샷을_심는다(CouponStates.closed(0));

        MockServerWebExchange exchange = 태운다(COUPON);

        assertThat(뒷단에_닿음).hasValue(false);
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    @DisplayName("판정값을_요청에_남긴다")
    void 판정값을_요청에_남긴다() {
        // 응답을 쓰는 쪽이 무엇 때문에 그렇게 됐는지 알아야 한다. 다시 판정하면
        // 두 번 세고, 그 사이 상태가 바뀌면 서로 다른 답을 낸다.
        스냅샷을_심는다(CouponStates.idle(100));

        MockServerWebExchange exchange = 태운다(COUPON);

        assertThat(exchange.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.PASS_UNDER_CAP);
    }
}
