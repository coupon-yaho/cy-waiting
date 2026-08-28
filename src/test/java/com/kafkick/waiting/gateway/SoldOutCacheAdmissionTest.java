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
    private final EntryToken entryTokens = EntryToken.of("not-a-real-secret-0123456789abcdef");
    private final AtomicInteger 뒷단_횟수 = new AtomicInteger();

    private final AdmissionGatewayFilter filter = AdmissionGatewayFilter.of(
            holder, AdmissionDecider.of(limiter, IDLE_RATIO),
            Clock.fixed(지금, ZoneOffset.UTC), meters, 줄,
            QueueToken.of("not-a-real-secret-0123456789abcdef"), limiter,
            entryTokens,
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
        // **판정 분포에도 잡혀야 한다.** 뒷단 유입이 0 인데 판정 카운터에도
        // 안 잡히면, 사고 때 어디서 끊겼는지를 못 찾는다.
        assertThat(meters.counter("waiting.admission", "outcome", "REJECT_SOLD_OUT",
                "cause", "none").count()).as("판정 계수").isEqualTo(1);
        assertThat(meters.counter("waiting.soldout.cache.hit").count())
                .as("캐시 적중").isEqualTo(1);
    }

    /**
     * <b>입장 토큰을 들고 와도 끊습니다.</b>
     *
     * <p>관찰은 사다리 1번 자리라 토큰 검사(2번)보다 위입니다. 뒤로 내리면 장애
     * 구간에 쌓인 토큰이 한꺼번에 오는 F8 구간에서 매진 쿠폰의 뒷단이 그대로
     * 열립니다 — R3 이 존재하는 이유가 정확히 그 트래픽입니다.
     */
    @Test
    @DisplayName("입장_토큰을_들고_와도_매진이면_끊는다")
    void 입장_토큰을_들고_와도_매진이면_끊는다() {
        재고가_있다고_심는다();
        캐시.observed(COUPON, 지금);
        String 입장 = entryTokens.issue(COUPON, MEMBER, 지금);

        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                        "/api/v1/coupons/" + COUPON + "/issue?entryToken=" + 입장)
                        .header("X-Member-Id", MEMBER));
        exchange.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE,
                Map.of("couponId", COUPON));
        filter.filter(exchange, e -> {
            뒷단_횟수.incrementAndGet();
            return Mono.empty();
        }).block();

        assertThat(뒷단_횟수).as("뒷단 도달").hasValue(0);
        assertThat(exchange.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.REJECT_SOLD_OUT);
    }

    /**
     * <b>항상 대기 모드여도 끊습니다.</b> 3번(`ALWAYS`)이 관찰보다 위로 가면
     * 그 모드를 건 쿠폰만 매진 뒤에도 줄이 계속 자랍니다.
     */
    @Test
    @DisplayName("항상_대기_모드여도_매진이면_끊는다")
    void 항상_대기_모드여도_매진이면_끊는다() {
        holder.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.always(1_000)), new SnapshotMeta(1, 1), 지금));
        캐시.observed(COUPON, 지금);

        MockServerWebExchange exchange = 태운다();

        assertThat(뒷단_횟수).as("뒷단 도달").hasValue(0);
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
        // 관찰 당시 손에 들고 있던 재료.
        holder.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.idle(1_000)), new SnapshotMeta(1, 1),
                지금.minusSeconds(1)));
        캐시.observed(COUPON, 지금.minusSeconds(1));
        태운다();
        assertThat(뒷단_횟수).as("관찰 뒤 첫 요청은 막힌다").hasValue(0);

        // **나중에 발행된 재료**가 재고를 말한다. 같은 재료로 풀면 캐시가
        // 존재하는 창에서 스스로 지워져 아무것도 안 막는다.
        재고가_있다고_심는다();
        태운다();

        assertThat(뒷단_횟수).as("재입고 뒤에는 간다").hasValue(1);
        assertThat(캐시.soldOut(COUPON)).isFalse();
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
        // **한 판 앞선 재료로 무장한다.** 같은 시각으로 두면 발행 시각 비교에서
        // 먼저 걸려, 재고 가드는 한 번도 안 불린다 — 지워도 통과한다.
        캐시.observed(COUPON, 지금.minusSeconds(1));

        태운다();

        assertThat(캐시.soldOut(COUPON)).isTrue();
    }

    /**
     * <b>재료가 낡아도 매진은 계속 끊습니다.</b>
     *
     * <p>사다리 1번(스냅샷 `stock<=0`)은 낡음을 견딥니다. 뒷단이 낸 409 는 그보다
     * 훨씬 직접적인 증거인데, 약한 증거만 낡음을 견디면 비대칭입니다.
     *
     * <p>그리고 낡음이 뜨는 순간은 전 노드가 <b>같은 초에</b> 뒤집힙니다 — 그때
     * 매진 보호가 통째로 꺼지면 뒷단 유입이 0 에서 노드 상한으로 한 번에 뜁니다.
     */
    @Test
    @DisplayName("재료가_낡아도_매진은_계속_끊는다")
    void 재료가_낡아도_매진은_계속_끊는다() {
        // **낡은 재료 하나만 손에 든 상태다.** 발행 시각은 단조 증가하므로
        // 새 재료가 옛 판으로 갈리는 일은 제어 평면이 안 만든다.
        holder.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.idle(1_000)), new SnapshotMeta(1, 1),
                지금.minusSeconds(60)));
        캐시.observed(COUPON, 지금.minusSeconds(60));

        MockServerWebExchange exchange = 태운다();

        assertThat(뒷단_횟수).as("뒷단 도달").hasValue(0);
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    /**
     * <b>낡은 재료로는 풀지도 않습니다.</b>
     *
     * <p>낡음은 못 믿겠다는 뜻인데, 못 믿는 재료로 방패를 부수는 것만 허용하면
     * 비대칭입니다. 특히 회복 첫 판에 도착한 스냅샷이 장애 중 쌓인 관찰을 전부
     * 지우면, 회복 순간에 다시 한 번 뒷단으로 몰립니다.
     */
    @Test
    @DisplayName("낡은_재료로는_관찰을_안_푼다")
    void 낡은_재료로는_관찰을_안_푼다() {
        holder.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.idle(1_000)), new SnapshotMeta(1, 1),
                지금.minusSeconds(60)));
        캐시.observed(COUPON, 지금.minusSeconds(120));

        태운다();

        assertThat(캐시.soldOut(COUPON)).as("낡은 재료가 방패를 부수지 않는다").isTrue();
    }
}
