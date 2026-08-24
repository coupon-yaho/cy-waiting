package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.queue.QueueState;
import com.kafkick.waiting.domain.queue.QueueToken;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 순번 조회. <b>대상을 토큰으로 특정한다</b> — 회원 헤더로 고르면 헤더 하나
 * 바꿔서 남의 순번을 본다.
 */
class QueueStatusFilterTest {

    private static final String COUPON = "c1";
    private static final String MEMBER = "812934";
    private static final Instant 지금 = Instant.parse("2026-08-24T00:00:00Z");

    private final MeterRegistry meters = new SimpleMeterRegistry();
    private final FakeQueuePort 줄 = FakeQueuePort.create();
    private final QueueToken tokens = QueueToken.of("not-a-real-secret-0123456789abcdef");
    private final SnapshotHolder holder = SnapshotHolder.of(
            Duration.ofSeconds(3), Duration.ofSeconds(10), Clock.fixed(지금, ZoneOffset.UTC));
    private final AtomicBoolean 다음으로_감 = new AtomicBoolean();

    private final QueueStatusFilter filter = QueueStatusFilter.of(
            holder, 줄, tokens, Clock.fixed(지금, ZoneOffset.UTC), meters, () -> 0.5);

    private void 스냅샷을_심는다(CouponState state) {
        holder.replace(new GatewaySnapshot(Map.of(COUPON, state), new SnapshotMeta(1, 1), 지금));
    }

    private MockServerWebExchange 조회한다(String path) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get(path).header("X-Member-Id", MEMBER));
        filter.filter(exchange, e -> {
            다음으로_감.set(true);
            return Mono.empty();
        }).block();
        return exchange;
    }

    private MockServerWebExchange 토큰으로_조회한다(String token) {
        return 조회한다("/api/v1/coupons/" + COUPON + "/queue?queueToken=" + token);
    }

    @Test
    @DisplayName("남의_경로는_그대로_흘려보낸다")
    void 남의_경로는_그대로_흘려보낸다() {
        조회한다("/api/v1/coupons/" + COUPON + "/issue");

        assertThat(다음으로_감).isTrue();
        assertThat(줄.왕복()).isZero();
    }

    @Test
    @DisplayName("토큰이_없으면_줄을_안_친다")
    void 토큰이_없으면_줄을_안_친다() {
        // 아무나 물어도 조회가 돌면 그 자체가 레디스 부하가 된다.
        MockServerWebExchange exchange = 조회한다("/api/v1/coupons/" + COUPON + "/queue");

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(줄.왕복()).isZero();
        assertThat(다음으로_감).isFalse();
    }

    @Test
    @DisplayName("헤더만_바꿔서는_남의_순번을_못_본다")
    void 헤더만_바꿔서는_남의_순번을_못_본다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, "남", 0, 지금).block();
        String 내_토큰 = tokens.issue(COUPON, MEMBER, 지금);

        // 헤더에 남의 식별자를 넣어도 토큰이 가리키는 사람만 본다.
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest
                        .get("/api/v1/coupons/" + COUPON + "/queue?queueToken=" + 내_토큰)
                        .header("X-Member-Id", "남"));
        filter.filter(exchange, e -> Mono.empty()).block();

        // 나는 줄에 없다. 남의 자리가 아니라 내 상태가 나와야 한다.
        assertThat(exchange.getResponse().getBodyAsString().block()).contains("CLOSED");
    }

    @Test
    @DisplayName("기다리는_중이면_순번을_준다")
    void 기다리는_중이면_순번을_준다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, "앞사람", 0, 지금).block();
        줄.enqueue(COUPON, MEMBER, 0, 지금).block();

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(exchange.getResponse().getBodyAsString().block())
                .contains("\"status\":\"WAITING\"")
                .contains("\"position\":1");
        assertThat(exchange.getResponse().getHeaders().getCacheControl()).isEqualTo("no-store");
    }

    @Test
    @DisplayName("차례가_오면_그렇게_말한다")
    void 차례가_오면_그렇게_말한다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, MEMBER, 0, 지금).block();
        줄.차례가_왔다();

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(exchange.getResponse().getBodyAsString().block())
                .contains("\"status\":\"ADMITTED\"");
    }

    @Test
    @DisplayName("차례는_맨_앞부터_온다")
    void 차례는_맨_앞부터_온다() {
        // 뒤에 선 사람까지 입장이 되면 줄이 통째로 한꺼번에 들어간다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, "앞사람", 0, 지금).block();
        줄.enqueue(COUPON, MEMBER, 0, 지금).block();
        줄.차례가_왔다();

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(exchange.getResponse().getBodyAsString().block())
                .contains("\"status\":\"WAITING\"");
    }

    @Test
    @DisplayName("다음에_물을_때를_알려_준다")
    void 다음에_물을_때를_알려_준다() {
        // 안 알려 주면 각자 마음대로 두드린다. 그 부하를 정하는 것은 서버다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, MEMBER, 0, 지금).block();

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .isEqualTo("1");
    }

    @Test
    @DisplayName("조회가_실패해도_다시_오라고_한다")
    void 조회가_실패해도_다시_오라고_한다() {
        // 순번은 레디스에 남아 있다. 다시 물으면 되므로 줄에서 빼지 않는다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.터진다(new IllegalStateException("레디스가 죽었다"));

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(exchange.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .isEqualTo("30");
    }

    @Test
    @DisplayName("재료를_못_믿으면_자주_묻게_하지_않는다")
    void 재료를_못_믿으면_자주_묻게_하지_않는다() {
        // ETA 를 모르는데 1초 간격을 주면, 배분이 멎은 구간에 폴링만 몰린다.
        holder.replace(new GatewaySnapshot(Map.of(COUPON, CouponStates.queueing(10, 1_000, 100)),
                new SnapshotMeta(1, 1), 지금.minusSeconds(3_600)));
        // 앞에 사람이 있어야 배분 속도가 답에 들어간다. 맨 앞이면 늘 0 초다.
        줄.enqueue(COUPON, "앞사람", 0, 지금).block();
        줄.enqueue(COUPON, MEMBER, 0, 지금).block();

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .isEqualTo("30");
    }

    @Test
    @DisplayName("모르는_쿠폰도_자주_묻게_하지_않는다")
    void 모르는_쿠폰도_자주_묻게_하지_않는다() {
        // 스냅샷에 없으면 배분 속도를 모른다. 모를수록 자주 묻게 하면 안 된다.
        줄.enqueue(COUPON, "앞사람", 0, 지금).block();
        줄.enqueue(COUPON, MEMBER, 0, 지금).block();

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .isEqualTo("30");
    }

    @Test
    @DisplayName("줄에_없으면_끝났다고_한다")
    void 줄에_없으면_끝났다고_한다() {
        // 매진으로 지워졌거나 이탈로 빠졌다. 어느 쪽이든 다시 서야 한다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(exchange.getResponse().getBodyAsString().block())
                .contains("\"status\":\"CLOSED\"");
        // 끝난 사람에게 다시 오라고 하지 않는다.
        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .isNull();
    }

    @Test
    @DisplayName("쿠폰_식별자를_라벨에_안_넣는다")
    void 쿠폰_식별자를_라벨에_안_넣는다() {
        // 인증이 없어 아무 문자열이나 들어온다. 라벨에 넣으면 지표가 메모리를 밀어낸다.
        조회한다("/api/v1/coupons/" + COUPON + "/queue");

        assertThat(meters.getMeters()).singleElement().satisfies(m ->
                assertThat(m.getId().getTags()).noneMatch(t -> t.getValue().contains(COUPON)));
    }
}
