package com.kafkick.waiting.gateway;

import static com.kafkick.waiting.adapter.redis.QueueRedisPort.NO_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.queue.EntryToken;
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

    private final SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(10);

    private final EntryToken entryTokens = EntryToken.of("not-a-real-secret-0123456789abcdef");

    private final QueueStatusFilter filter = QueueStatusFilter.of(
            holder, 줄, tokens, Clock.fixed(지금, ZoneOffset.UTC), meters, () -> 0.5, limiter, entryTokens);

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

    /** 실물이 거절하는 값을 픽스처가 받아 주면 그 회귀를 시험이 못 본다. */
    @Test
    @DisplayName("잘못된_상한은_픽스처도_거절한다")
    void 잘못된_상한은_픽스처도_거절한다() {
        assertThatThrownBy(() -> 줄.enqueue(COUPON, MEMBER, -2, 지금).block())
                .isInstanceOf(IllegalArgumentException.class);
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
        줄.enqueue(COUPON, "남", NO_LIMIT, 지금).block();
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
        줄.enqueue(COUPON, "앞사람", NO_LIMIT, 지금).block();
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(exchange.getResponse().getBodyAsString().block())
                .contains("\"status\":\"WAITING\"")
                .contains("\"position\":1");
        assertThat(exchange.getResponse().getHeaders().getCacheControl()).isEqualTo("no-store");
    }

    @Test
    @DisplayName("차례가_오면_입장_토큰을_준다")
    void 차례가_오면_입장_토큰을_준다() {
        // **여기서 발급한다.** 배분 때 미리 만들면 안 돌아온 사람 몫이 버려지고,
        // 그만큼 뒷사람이 늦게 들어간다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();
        줄.차례가_왔다();

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));
        String 본문 = exchange.getResponse().getBodyAsString().block();

        assertThat(본문)
                .contains("\"status\":\"ADMITTED\"")
                .contains("\"expiresIn\":180");
        assertThat(entryTokens.verify(입장_토큰(본문), COUPON, 지금)).contains(MEMBER);
    }

    /** 기다린 사람 것이어야 한다. 남의 토큰으로 통하면 줄이 무의미해진다. */
    @Test
    @DisplayName("입장_토큰은_그_사람_것이다")
    void 입장_토큰은_그_사람_것이다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();
        줄.차례가_왔다();

        String 받은_것 = 입장_토큰(토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금))
                .getResponse().getBodyAsString().block());

        assertThat(entryTokens.verify(받은_것, "다른쿠폰", 지금)).isEmpty();
        assertThat(entryTokens.verify(받은_것, COUPON, 지금.plusSeconds(EntryToken.TTL_SEC + 1)))
                .isEmpty();
    }

    private String 입장_토큰(String 본문) {
        int from = 본문.indexOf("\"entryToken\":\"") + "\"entryToken\":\"".length();
        return 본문.substring(from, 본문.indexOf('"', from));
    }

    @Test
    @DisplayName("차례는_맨_앞부터_온다")
    void 차례는_맨_앞부터_온다() {
        // 뒤에 선 사람까지 입장이 되면 줄이 통째로 한꺼번에 들어간다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, "앞사람", NO_LIMIT, 지금).block();
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();
        줄.차례가_왔다();

        // **앞사람이 실제로 들어갔는지 함께 본다.** 뒷사람만 보면 아무도
        // 입장 못 시켜도 통과한다 — 그러면 재려던 것을 안 재게 된다.
        assertThat(토큰으로_조회한다(tokens.issue(COUPON, "앞사람", 지금))
                .getResponse().getBodyAsString().block())
                .contains("\"status\":\"ADMITTED\"");
        assertThat(토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금))
                .getResponse().getBodyAsString().block())
                .contains("\"status\":\"WAITING\"")
                .contains("\"position\":1");
    }

    @Test
    @DisplayName("다음에_물을_때를_알려_준다")
    void 다음에_물을_때를_알려_준다() {
        // 안 알려 주면 각자 마음대로 두드린다. 그 부하를 정하는 것은 서버다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();

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
        줄.enqueue(COUPON, "앞사람", NO_LIMIT, 지금).block();
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .isEqualTo("30");
    }

    @Test
    @DisplayName("모르는_쿠폰도_자주_묻게_하지_않는다")
    void 모르는_쿠폰도_자주_묻게_하지_않는다() {
        // 스냅샷에 없으면 배분 속도를 모른다. 모를수록 자주 묻게 하면 안 된다.
        줄.enqueue(COUPON, "앞사람", NO_LIMIT, 지금).block();
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();

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

    /**
     * 폴링은 읽기가 아니라 쓰기다. 토큰은 줄을 서면 누구나 받고 한 시간 사니,
     * 상한이 없으면 토큰 몇 개로 공유 레디스에 무제한 쓰기를 넣을 수 있다.
     */
    @Test
    @DisplayName("조회에도_초당_상한이_있다")
    void 조회에도_초당_상한이_있다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();
        String 토큰 = tokens.issue(COUPON, MEMBER, 지금);

        // 예산을 다 쓴다. 조회 예산은 판정과 나뉘어 있다.
        for (int i = 0; i < 20_000; i++) {
            토큰으로_조회한다(토큰);
        }

        // **상한값을 정확히 못 박는다.** 넘긴 요청만 보면 그 전에 막혀도 통과한다.
        // 등록 한 번과 허용된 조회 2만 번이다.
        assertThat(줄.왕복()).isEqualTo(20_001);
        MockServerWebExchange 넘긴_것 = 토큰으로_조회한다(토큰);

        assertThat(넘긴_것.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        // 다시 올 때를 안 알려 주면 막힌 사람들이 곧바로 되돌아온다.
        assertThat(넘긴_것.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .isEqualTo("30");
        // 상한을 넘으면 레디스를 아예 안 친다. 치고 나서 막으면 막는 뜻이 없다.
        assertThat(줄.왕복()).isEqualTo(20_001);
    }

    /** 폴링이 발급 예산을 갉아먹으면, 기다리는 사람이 많을수록 통과가 줄어든다. */
    @Test
    @DisplayName("조회_예산은_판정과_나뉜다")
    void 조회_예산은_판정과_나뉜다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();
        // 판정의 노드 예산을 통째로 쓴다.
        for (int i = 0; i < 100_000; i++) {
            limiter.tryAcquire(AdmissionDecider.GLOBAL_KEY, 100_000, 지금.getEpochSecond());
        }

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.OK);
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
