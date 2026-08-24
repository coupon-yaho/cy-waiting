package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.MutableClock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import java.time.ZoneOffset;
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

    /** 고정 시계. 실제 시계를 쓰면 낡음 판정이 장비 속도에 걸린다 (TS-4). */
    private static final Instant 지금 = Instant.parse("2026-08-24T00:00:00Z");

    private final MeterRegistry meters = new SimpleMeterRegistry();

    private final SnapshotHolder holder = SnapshotHolder.of(
            Duration.ofSeconds(3), Duration.ofSeconds(10),
            Clock.fixed(지금, ZoneOffset.UTC));
    private final AdmissionGatewayFilter filter = AdmissionGatewayFilter.of(
            holder, AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(10_000), 0.2),
            Clock.fixed(지금, ZoneOffset.UTC), meters);

    private final AtomicReference<Boolean> 뒷단에_닿음 = new AtomicReference<>(false);

    private AdmissionDecision 태운다(AdmissionGatewayFilter f, String couponId) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                        "/api/v1/coupons/" + couponId + "/issue"));
        exchange.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE,
                Map.of("couponId", couponId));
        f.filter(exchange, e -> Mono.empty()).block();
        return exchange.getAttribute(AdmissionGatewayFilter.DECISION);
    }

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
                new SnapshotMeta(1_000, 1), 지금));
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

        // 404 가 아닌 것만 보면 400 이나 503 으로 끊어도 통과한다.
        assertThat(뒷단에_닿음).as("판정을 미루고 그대로 흘린다").hasValue(true);
        assertThat(exchange.getResponse().getStatusCode()).isNull();
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
    @DisplayName("대기_판정은_아직_뒷단으로_보낸다")
    void 대기_판정은_아직_뒷단으로_보낸다() {
        // 큐 등록과 순번 응답이 아직 없다. 여기서 붙잡으면 못 만든 응답을
        // 기다리는 사람이 생긴다 — 만들어지면 이 시험이 바뀐다 (CY-402).
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));

        MockServerWebExchange exchange = 태운다(COUPON);

        assertThat(exchange.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .matches(AdmissionDecision::isEnqueue, "대기 판정");
        assertThat(뒷단에_닿음).hasValue(true);
        assertThat(exchange.getResponse().getStatusCode()).isNull();
    }

    @Test
    @DisplayName("거절_사유마다_다른_응답이다")
    void 거절_사유마다_다른_응답이다() {
        // **뭉치면 운영자가 엉뚱한 것을 조인다.** 매진은 끝난 것이고, 큐 만원은
        // 잠시 뒤 다시 오면 되고, 과부하는 노드를 늘려야 한다 — 셋이 다르다.
        assertThat(AdmissionGatewayFilter.statusOf(AdmissionDecision.REJECT_SOLD_OUT))
                .isEqualTo(HttpStatus.CONFLICT);
        assertThat(AdmissionGatewayFilter.statusOf(AdmissionDecision.REJECT_QUEUE_FULL))
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(AdmissionGatewayFilter.statusOf(AdmissionDecision.REJECT_OVERLOAD))
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Test
    @DisplayName("차례가_온_사람은_큐로_안_돌린다")
    void 차례가_온_사람은_큐로_안_돌린다() {
        // 토큰을 들고 왔는데 노드 상한을 넘은 경우다. 어느 술어에도 안 걸려서
        // 그냥 두면 조용히 통과한다 — 상한을 넘겼는데 지나가는 것이다.
        assertThat(AdmissionGatewayFilter.statusOf(AdmissionDecision.RETRY_TOKEN))
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    @Test
    @DisplayName("모든_판정값에_대응이_있다")
    void 모든_판정값에_대응이_있다() {
        // 값을 늘리고 분기를 안 늘리면 그 판정이 조용히 통과한다. 늘리는 순간 걸린다.
        for (AdmissionDecision d : AdmissionDecision.values()) {
            // 어떤 값인지까지 본다. 있기만 하면 엉뚱한 코드가 붙어도 안 걸린다.
            assertThat(AdmissionGatewayFilter.statusOf(d)).as("판정 %s", d)
                    .isIn(HttpStatus.OK, HttpStatus.ACCEPTED, HttpStatus.CONFLICT,
                            HttpStatus.TOO_MANY_REQUESTS, HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    @Test
    @DisplayName("사유별로_센다")
    void 사유별로_센다() {
        // **요청마다 로그를 남기지 않는다.** 낡음 구간에서 없는 쿠폰을 반복해
        // 부르면 로그가 폭주하고, 그때 정작 봐야 할 것이 묻힌다.
        스냅샷을_심는다(null);
        태운다(COUPON);
        스냅샷을_심는다(CouponStates.idle(100));
        태운다(COUPON);

        assertThat(meters.counter("waiting.admission", "outcome", "unknown-coupon").count())
                .isEqualTo(1);
        assertThat(meters.counter("waiting.admission", "outcome", "PASS_UNDER_CAP").count())
                .isEqualTo(1);
    }

    @Test
    @DisplayName("쿠폰_식별자를_라벨에_안_넣는다")
    void 쿠폰_식별자를_라벨에_안_넣는다() {
        // 인증이 없어 아무 문자열이나 들어온다. 라벨에 넣으면 지표 하나가
        // 메모리를 밀어낸다.
        스냅샷을_심는다(CouponStates.idle(100));
        태운다(COUPON);

        // **태그를 정확히 못 박는다.** 값만 보면 식별자를 키로 쓸 때 안 걸리고,
        // 키만 보면 값으로 쓸 때 안 걸린다.
        assertThat(meters.getMeters())
                .singleElement()
                .satisfies(m -> assertThat(m.getId().getTags())
                        .containsExactly(Tag.of("outcome", "PASS_UNDER_CAP")));
    }

    @Test
    @DisplayName("리미터_윈도를_지금_시각으로_센다")
    void 리미터_윈도를_지금_시각으로_센다() {
        // **스냅샷 발행 시각을 쓰면 배분이 멎는 순간 윈도가 영영 안 넘어간다.**
        // 상한만큼 쓴 뒤부터 전부 막힌다 — 열어 줘야 할 구간에서 정반대로 조인다.
        //
        // 발행 시각을 과거로 고정해 두고, 시계만 흘려 두 윈도가 갈리는지 본다.
        Instant 낡은_발행 = 지금.minusSeconds(3_600);
        MutableClock 시계 = MutableClock.at(지금);
        AdmissionGatewayFilter 시계를_쓰는_필터 = AdmissionGatewayFilter.of(
                holder, AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(10), 0.2), 시계, meters);
        holder.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.idle(1_000_000)),
                new SnapshotMeta(1, 1), 낡은_발행));

        // 상한이 1 이라 같은 윈도에서 두 번째는 막힌다.
        AdmissionDecision 첫째 = 태운다(시계를_쓰는_필터, COUPON);
        AdmissionDecision 둘째 = 태운다(시계를_쓰는_필터, COUPON);
        시계.앞으로(Duration.ofSeconds(2));
        AdmissionDecision 시계가_흐른_뒤 = 태운다(시계를_쓰는_필터, COUPON);

        assertThat(첫째.isPass()).as("첫 요청").isTrue();
        assertThat(둘째.isPass()).as("같은 윈도의 둘째").isFalse();
        assertThat(시계가_흐른_뒤.isPass()).as("윈도가 넘어간 뒤").isTrue();
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
