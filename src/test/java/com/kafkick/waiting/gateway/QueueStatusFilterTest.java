package com.kafkick.waiting.gateway;

import static com.kafkick.waiting.gateway.QueuePort.NO_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Tag;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.queue.ErrorBackoff;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.coupon.SnapshotMetas;
import com.kafkick.waiting.domain.queue.EntryToken;
import com.kafkick.waiting.domain.queue.QueueState;
import com.kafkick.waiting.domain.queue.QueueToken;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.DoubleSupplier;
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
    private static final ObjectMapper JSON = new ObjectMapper();

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

    /** 밀 수 있는 시계. 백오프는 실패가 이어진 시간으로 오르므로 시각이 흘러야 잰다. */
    private final AtomicReference<Instant> 흐르는_지금 = new AtomicReference<>(지금);

    private final Clock 흐름 = new Clock() {
        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return 흐르는_지금.get();
        }
    };

    private void 시계를_민다(Duration 만큼) {
        흐르는_지금.updateAndGet(now -> now.plus(만큼));
    }

    private String 물러날_초(QueueStatusFilter 대상) {
        MockServerWebExchange exchange =
                토큰으로_조회한다(대상, tokens.issue(COUPON, MEMBER, 흐르는_지금.get()));
        return exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER);
    }

    private void 스냅샷을_심는다(CouponState state) {
        holder.replace(new GatewaySnapshot(Map.of(COUPON, state), new SnapshotMeta(1, 1), 지금));
    }

    /**
     * 자리를 다 쓴 리미터.
     *
     * <p><b>전제를 여기서 확인한다</b> (TS-9). 리미터가 자리 없음을 거절이
     * 아니라 축출로 바꾸면 이 픽스처를 쓰는 시험이 조용히 다른 것을 재게 된다.
     */
    private SecondWindowLimiter 꽉_찬_리미터() {
        SecondWindowLimiter 꽉_찬 = SecondWindowLimiter.withMaxKeys(1);
        꽉_찬.tryAcquire("다른-키", 1, 지금.getEpochSecond());
        assertThat(꽉_찬.tryAcquire("아무-새-키", 1_000, 지금.getEpochSecond()))
                .as("자리가 없으면 거절한다 — 이 전제가 깨지면 부르는 쪽이 무의미하다")
                .isFalse();
        return 꽉_찬;
    }

    /** 배수는 스냅샷 전체를 보고 나온 전역 값이라 쿠폰이 아니라 메타에 실린다. */
    private void 스냅샷을_심는다(CouponState state, double 배수) {
        holder.replace(new GatewaySnapshot(Map.of(COUPON, state),
                SnapshotMetas.overBudget(1, 1, 배수), 지금));
    }

    private MockServerWebExchange 조회한다(String path) {
        return 조회한다(filter, path);
    }

    /** 필터를 받는다. 안 그러면 자체 exchange 를 만드는 시험이 `다음으로_감` 을 못 본다. */
    private MockServerWebExchange 조회한다(QueueStatusFilter 대상, String path) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get(path).header("X-Member-Id", MEMBER));
        대상.filter(exchange, e -> {
            다음으로_감.set(true);
            return Mono.empty();
        }).block();
        return exchange;
    }

    private MockServerWebExchange 토큰으로_조회한다(String token) {
        return 토큰으로_조회한다(filter, token);
    }

    private MockServerWebExchange 토큰으로_조회한다(QueueStatusFilter 대상, String token) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/v1/coupons/" + COUPON + "/queue")
                        .header("X-Member-Id", MEMBER)
                        .header("Queue-Token", token));
        대상.filter(exchange, e -> {
            다음으로_감.set(true);
            return Mono.empty();
        }).block();
        return exchange;
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

    /**
     * <b>매진이면 줄을 안 칩니다</b> (R3 · 7.1.4).
     *
     * <p>재고가 없으면 답이 정해져 있습니다. 그런데도 물으러 가면, 매진 순간
     * 몰리는 폴링이 그대로 레디스 부하가 됩니다 — 정작 그때 줄을 정리해야 합니다.
     */
    @Test
    @DisplayName("매진이면_줄을_안_친다")
    void 매진이면_줄을_안_친다() {
        스냅샷을_심는다(CouponStates.closed(1_000));
        String 토큰 = tokens.issue(COUPON, MEMBER, 지금);

        MockServerWebExchange exchange =
                조회한다("/api/v1/coupons/" + COUPON + "/queue?queueToken=" + 토큰);

        // R3 은 "레디스·백엔드를 거치지 않고" 다. 둘 다 봐야 한다 — 응답을 쓴 뒤
        // 체인을 이어 붙여도 왕복만 보면 통과하고, 그러면 매진 순간의 폴링
        // 파도가 그대로 뒷단으로 간다.
        assertThat(줄.왕복()).as("레디스 왕복").isZero();
        assertThat(다음으로_감).as("뒷단 도달").isFalse();
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode data = 본문(exchange).get("data");
        assertThat(data.get("status").asText()).as("상태").isEqualTo("SOLD_OUT");
        assertThat(data.get("reason").asText()).as("사유").isEqualTo("STOCK_EXHAUSTED");
        // 태그가 안 잠기면 대시보드에서 매진 단락과 상한 거절이 뭉쳐도 안 문다.
        assertThat(meters.counter("waiting.queue.status", "outcome", "sold-out",
                "cause", "none").count()).as("매진 단락 계수").isEqualTo(1);
    }

    /**
     * <b>재고를 모르면 폴링을 안 끊습니다</b> (CY-702).
     *
     * <p>끊으면 그 줄의 생존 신호가 멎습니다. 갱신처가 폴링 하나뿐이라, 신호가
     * 멎은 사람들은 청소가 이탈자로 걷어 갑니다 — 재입장은 새 순번입니다.
     */
    @Test
    @DisplayName("재고를_모르면_폴링을_안_끊는다")
    void 재고를_모르면_폴링을_안_끊는다() {
        스냅샷을_심는다(CouponStates.stockUnknown(10, 1_000));
        String 토큰 = tokens.issue(COUPON, MEMBER, 지금);

        MockServerWebExchange exchange =
                조회한다("/api/v1/coupons/" + COUPON + "/queue?queueToken=" + 토큰);

        assertThat(본문(exchange).get("data").get("status").asText())
                .as("모른다고 종결하지 않는다").isNotEqualTo("SOLD_OUT");
        assertThat(줄.왕복()).as("줄을 쳐야 생존 신호가 갱신된다").isPositive();
    }

    /**
     * <b>스냅샷에 없는 쿠폰은 매진으로 안 봅니다.</b>
     *
     * <p>모른다는 것이 끝났다는 뜻은 아닙니다. 지금 이 분기를 뒤집으면 ETA 시험이
     * 대신 빨개져서, 매진 오판이 엉뚱한 이름으로 보고됩니다.
     */
    @Test
    @DisplayName("스냅샷에_없는_쿠폰은_매진으로_안_본다")
    void 스냅샷에_없는_쿠폰은_매진으로_안_본다() {
        holder.replace(new GatewaySnapshot(Map.of("남의-쿠폰", CouponStates.closed(1_000)),
                new SnapshotMeta(1, 1), 지금));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();
        String 토큰 = tokens.issue(COUPON, MEMBER, 지금);
        int 이전 = 줄.왕복();

        조회한다("/api/v1/coupons/" + COUPON + "/queue?queueToken=" + 토큰);

        assertThat(줄.왕복()).as("레디스 왕복").isGreaterThan(이전);
    }

    /**
     * <b>줄에 없는 것과 매진은 다른 사유입니다.</b>
     *
     * <p>이탈로 걷혔거나 큐가 정리돼 줄에 없는 사람에게 "다 팔렸다" 고 답하면,
     * 다시 설 수 있는데도 안 섭니다. 매진은 앞에서 이미 끝나므로 여기까지 오는
     * 것은 재고와 무관한 이유입니다.
     */
    @Test
    @DisplayName("줄에_없으면_매진과_다른_사유로_답한다")
    void 줄에_없으면_매진과_다른_사유로_답한다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        String 토큰 = tokens.issue(COUPON, MEMBER, 지금);

        MockServerWebExchange exchange =
                조회한다("/api/v1/coupons/" + COUPON + "/queue?queueToken=" + 토큰);

        JsonNode data = 본문(exchange).get("data");
        assertThat(data.get("status").asText()).as("상태").isEqualTo("CLOSED");
        assertThat(data.get("reason").asText()).as("사유").isEqualTo("NOT_IN_QUEUE");
    }

    /**
     * <b>다시 오라고 안 합니다</b> (7.1.5).
     *
     * <p>재고가 다시 생기지 않는데 재시도를 유도하면, 끝난 캠페인이 폴링을 계속
     * 만들어 냅니다.
     */
    @Test
    @DisplayName("매진에는_다시_올_시각을_안_준다")
    void 매진에는_다시_올_시각을_안_준다() {
        스냅샷을_심는다(CouponStates.closed(1_000));
        String 토큰 = tokens.issue(COUPON, MEMBER, 지금);

        MockServerWebExchange exchange =
                조회한다("/api/v1/coupons/" + COUPON + "/queue?queueToken=" + 토큰);

        assertThat(exchange.getResponse().getHeaders().getFirst("Retry-After"))
                .as("재시도 유도").isNull();
    }

    /**
     * <b>전역 폴링 배수를 그대로 지킵니다</b> (7.3.3).
     *
     * <p>제어 평면이 예산 초과를 알아내도 조회가 그 값을 안 보면, 배수는 아무
     * 데도 안 닿는 계산으로 남습니다 — 예산이 모자란 채로 간격만 그대로입니다.
     */
    @Test
    @DisplayName("전역_배수만큼_다음_폴링이_늦춰진다")
    void 전역_배수만큼_다음_폴링이_늦춰진다() {
        // **밴드가 1초가 아닌 자리에서 잰다.** 1초 밴드에서 재면 `밴드 × 배수` 와
        // 배수만 돌려주는 구현이 같은 값을 내서, 곱셈이 관측되지 않는다.
        // 상한 60초에도 안 닿아야 곱한 값이 그대로 보인다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100), 3.0);
        for (int i = 0; i < 60; i++) {
            줄.enqueue(COUPON, "앞사람" + i, NO_LIMIT, 지금).block();
        }
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        // 앞에 60명, 초당 10명이라 ETA 6초 — 3초 밴드다. 지터는 0.5 를 받아
        // 상쇄되므로 3 × 3 = 9 만 남는다.
        assertThat(exchange.getResponse().getHeaders().getFirst("Retry-After"))
                .as("밴드에 배수가 곱해진 다음 폴링").isEqualTo("9");
    }

    /**
     * <b>재료가 낡아도 배수는 안 되돌립니다.</b>
     *
     * <p>1.0 으로 되돌리면 제어 평면이 멎는 순간 전원의 간격이 한꺼번에
     * 짧아집니다 — 이미 흔들리는 노드에 폴링이 두 배로 몰립니다.
     */
    @Test
    @DisplayName("재료가_낡아도_배수를_안_되돌린다")
    void 재료가_낡아도_배수를_안_되돌린다() {
        holder.replace(new GatewaySnapshot(Map.of(COUPON, CouponStates.queueing(10, 1_000, 100)),
                SnapshotMetas.overBudget(1, 1, 1.5), 지금.minusSeconds(3_600)));
        // 맨 앞 사람은 ETA 가 정말 0 이라 배수 속도를 안 본다. 앞에 한 명을
        // 세워야 낡음이 ETA 로 흘러 가장 먼 밴드가 된다.
        줄.enqueue(COUPON, "앞사람", NO_LIMIT, 지금).block();
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        // 낡으면 배수 속도를 모르므로 ETA 는 가장 먼 밴드(30초)다. 30 × 1.5 = 45
        // — 천장 아래라 배수가 값으로 보인다. 1.0 으로 되돌리는 구현이면 30 이다.
        assertThat(exchange.getResponse().getHeaders().getFirst("Retry-After"))
                .as("낡아도 지키는 배수").isEqualTo("45");
    }

    /**
     * <b>거절에도 배수를 겁니다.</b>
     *
     * <p>이 갈래가 도는 조건이 곧 폴링이 이 노드의 상한을 넘었다는 것입니다.
     * 거절만 배수를 빼면 과부하일수록 거절 비중이 커져, 예산을 건다는 말이
     * 절반만 맞게 됩니다.
     */
    @Test
    @DisplayName("상한에_걸린_거절도_배수를_지킨다")
    void 상한에_걸린_거절도_배수를_지킨다() {
        // **천장 안쪽에서 잰다.** 배수 3 이면 90 이라 천장 50 에 잘리는데,
        // 그러면 배수 2 를 넘는 어떤 값을 넘겨도 같은 답이라 배선이 안 잠긴다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100), 1.5);
        String 토큰 = tokens.issue(COUPON, MEMBER, 지금);
        QueueStatusFilter 상한이_찬_필터 = QueueStatusFilter.of(
                holder, 줄, tokens, Clock.fixed(지금, ZoneOffset.UTC), meters, () -> 0.5,
                꽉_찬_리미터(), entryTokens);

        MockServerWebExchange exchange = 조회한다(
                상한이_찬_필터, "/api/v1/coupons/" + COUPON + "/queue?queueToken=" + 토큰);

        // ETA 를 모르는 갈래라 30초 밴드다. 30 × 1.5 = 45 — 천장 50 아래다.
        assertThat(exchange.getResponse().getHeaders().getFirst("Retry-After"))
                .as("거절에도 걸리는 배수").isEqualTo("45");
    }

    /**
     * <b>재료가 낡으면 매진으로 안 봅니다.</b>
     *
     * <p>모른다는 것이 끝났다는 뜻은 아닙니다. 여기서 잘못 말하면 기다리던 사람이
     * 줄을 잃고, 회복 뒤에는 맨 뒤로 갑니다 — 순번 역행이 됩니다.
     */
    @Test
    @DisplayName("재료가_낡으면_매진으로_안_본다")
    void 재료가_낡으면_매진으로_안_본다() {
        holder.replace(new GatewaySnapshot(Map.of(COUPON, CouponStates.closed(1_000)),
                new SnapshotMeta(1, 1), 지금.minusSeconds(3_600)));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();
        String 토큰 = tokens.issue(COUPON, MEMBER, 지금);
        int 이전 = 줄.왕복();

        조회한다("/api/v1/coupons/" + COUPON + "/queue?queueToken=" + 토큰);

        assertThat(줄.왕복()).as("낡은 재료로는 단락 안 한다").isGreaterThan(이전);
    }

    /**
     * <b>조회 상한이 찼어도 매진은 종결합니다.</b>
     *
     * <p>상한은 노드 전역 키 하나라 쿠폰별 격리가 없습니다. 매진 단락이 그 뒤에
     * 있으면 매진 폴러가 상한에 걸려 <b>재시도를 유도하는 503</b> 을 받고, 이
     * 변경이 없애려던 폴링 재생산이 그대로 돌아옵니다.
     */
    @Test
    @DisplayName("조회_상한이_찼어도_매진은_종결한다")
    void 조회_상한이_찼어도_매진은_종결한다() {
        스냅샷을_심는다(CouponStates.closed(1_000));
        String 토큰 = tokens.issue(COUPON, MEMBER, 지금);
        // 자리를 하나만 둔 리미터의 그 한 자리를 다른 키가 차지하면, 조회 키는
        // 상한과 무관하게 거절된다. 상한값을 시험이 알 필요가 없다.
        SecondWindowLimiter 꽉_찬 = SecondWindowLimiter.withMaxKeys(1);
        꽉_찬.tryAcquire("다른-키", 1, 지금.getEpochSecond());
        // **전제를 시험 안에서 확인한다** (TS-9). 리미터가 자리 없음을 거절이
        // 아니라 축출로 바꾸면 이 시험은 조용히 `매진이면_줄을_안_친다` 의
        // 사본이 되고, 그때 순서를 되돌려도 아무것도 안 문다.
        assertThat(꽉_찬.tryAcquire("아무-새-키", 1_000, 지금.getEpochSecond()))
                .as("자리가 없으면 거절한다 — 이 전제가 깨지면 아래가 무의미하다").isFalse();
        QueueStatusFilter 상한이_찬_필터 = QueueStatusFilter.of(
                holder, 줄, tokens, Clock.fixed(지금, ZoneOffset.UTC), meters, () -> 0.5,
                꽉_찬, entryTokens);

        MockServerWebExchange exchange = 조회한다(
                상한이_찬_필터, "/api/v1/coupons/" + COUPON + "/queue?queueToken=" + 토큰);

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(exchange.getResponse().getHeaders().getFirst("Retry-After"))
                .as("재시도 유도").isNull();
        assertThat(줄.왕복()).as("레디스 왕복").isZero();
        assertThat(다음으로_감).as("뒷단 도달").isFalse();
    }

    /**
     * <b>음성 대조 — 같은 상한에서 매진이 아니면 거절합니다.</b>
     *
     * <p>위 시험만 있으면 "상한이 원래 안 걸렸다" 로도 통과합니다. 같은 리미터로
     * 매진이 아닌 쿠폰을 물어 503 이 나오는 것을 봐야, 위에서 200 이 난 것이
     * <b>단락이 상한보다 앞이라서</b> 라고 말할 수 있습니다.
     */
    @Test
    @DisplayName("같은_상한에서_매진이_아니면_거절한다")
    void 같은_상한에서_매진이_아니면_거절한다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        String 토큰 = tokens.issue(COUPON, MEMBER, 지금);
        SecondWindowLimiter 꽉_찬 = SecondWindowLimiter.withMaxKeys(1);
        꽉_찬.tryAcquire("다른-키", 1, 지금.getEpochSecond());
        QueueStatusFilter 상한이_찬_필터 = QueueStatusFilter.of(
                holder, 줄, tokens, Clock.fixed(지금, ZoneOffset.UTC), meters, () -> 0.5,
                꽉_찬, entryTokens);

        MockServerWebExchange exchange = 조회한다(
                상한이_찬_필터, "/api/v1/coupons/" + COUPON + "/queue?queueToken=" + 토큰);

        assertThat(exchange.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        // 값까지 본다. 있기만 하면 되는 헤더가 아니라, 흔들림이 붙은 재시도
        // 간격이 실려야 같은 밴드가 한꺼번에 안 돌아온다.
        assertThat(exchange.getResponse().getHeaders().getFirst("Retry-After"))
                .as("다시 올 시각").isEqualTo("30");
        assertThat(줄.왕복()).as("레디스 왕복").isZero();
    }

    /**
     * <b>마지막 한 장은 아직 매진이 아닙니다.</b> 경계를 <code>&lt;=</code> 로 잘못
     * 쓰면 남은 재고를 두고 줄을 끊습니다 — 그 한 장이 영영 안 나갑니다.
     */
    @Test
    @DisplayName("재고가_한_장_남았으면_종결하지_않는다")
    void 재고가_한_장_남았으면_종결하지_않는다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1, 100));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();
        String 토큰 = tokens.issue(COUPON, MEMBER, 지금);
        int 이전 = 줄.왕복();

        조회한다("/api/v1/coupons/" + COUPON + "/queue?queueToken=" + 토큰);

        assertThat(줄.왕복()).as("레디스 왕복").isGreaterThan(이전);
    }

    /**
     * <b>줄이 비어도 매진은 매진입니다.</b>
     *
     * <p>큐를 정리해 대기자가 0 이 되면 상태 기계는 <code>CLOSED</code> 를 안
     * 만들고 <code>IDLE</code> 로 떨어뜨립니다. 그 자리를 매진이 아닌 것으로
     * 읽으면, 같은 쿠폰에 조회는 "다시 서라" 등록은 409 로 답합니다 — 그리고
     * 그 상태가 매진 쿠폰의 <b>정상 종착점</b>입니다.
     */
    @Test
    @DisplayName("줄이_비어도_재고가_0_이면_종결한다")
    void 줄이_비어도_재고가_0_이면_종결한다() {
        스냅샷을_심는다(CouponStates.idle(0));
        String 토큰 = tokens.issue(COUPON, MEMBER, 지금);

        MockServerWebExchange exchange =
                조회한다("/api/v1/coupons/" + COUPON + "/queue?queueToken=" + 토큰);

        assertThat(줄.왕복()).as("레디스 왕복").isZero();
        assertThat(본문(exchange).get("data").get("status").asText()).isEqualTo("SOLD_OUT");
    }

    /**
     * <b>배수 중이면 아직 매진이 아닙니다.</b> 경계가 열거값이 아니라 재고
     * 숫자이므로, 무는 자리는 <code>QUEUEING</code> 이 아니라 <code>DRAINING</code>
     * 입니다 — 둘 다 재고가 남은 상태인데 런타임만 다릅니다.
     */
    @Test
    @DisplayName("배수_중이면_종결하지_않는다")
    void 배수_중이면_종결하지_않는다() {
        스냅샷을_심는다(CouponStates.draining(200, 50, 100));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();
        String 토큰 = tokens.issue(COUPON, MEMBER, 지금);
        int 이전 = 줄.왕복();

        조회한다("/api/v1/coupons/" + COUPON + "/queue?queueToken=" + 토큰);

        assertThat(줄.왕복()).as("레디스 왕복").isGreaterThan(이전);
    }

    /**
     * <b>매진이어도 토큰이 먼저입니다.</b>
     *
     * <p>단락을 토큰 검증 위로 올리면, 인증 없이 아무 쿠폰의 매진 여부를 캐는
     * 오라클이 열립니다. 순서가 곧 정책인 자리라 값으로 못 박습니다.
     */
    @Test
    @DisplayName("매진이어도_토큰이_없으면_거절한다")
    void 매진이어도_토큰이_없으면_거절한다() {
        스냅샷을_심는다(CouponStates.closed(1_000));

        MockServerWebExchange exchange = 조회한다("/api/v1/coupons/" + COUPON + "/queue");

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(exchange.getResponse().getBodyAsString().block())
                .as("매진 여부를 흘리지 않는다").doesNotContain("SOLD_OUT");
    }

    /** 매진이 아니면 그대로 물으러 갑니다. 위 시험이 "늘 안 친다" 로도 통과하면 안 됩니다. */
    @Test
    @DisplayName("매진이_아니면_그대로_묻는다")
    void 매진이_아니면_그대로_묻는다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();
        String 토큰 = tokens.issue(COUPON, MEMBER, 지금);
        int 이전 = 줄.왕복();

        조회한다("/api/v1/coupons/" + COUPON + "/queue?queueToken=" + 토큰);

        assertThat(줄.왕복()).as("레디스 왕복").isGreaterThan(이전);
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

    /**
     * <b>도메인 시험만으로는 배선이 안 잠긴다.</b> 값 산출이 맞아도 응답에 실리는
     * 것이 다르면 사용자가 보는 것은 여전히 틀린다.
     */
    @Test
    @DisplayName("배수_속도를_모르면_가장_넓은_구간을_싣는다")
    void 배수_속도를_모르면_가장_넓은_구간을_싣는다() {
        // 스냅샷에 없는 쿠폰이라 배분 속도를 모른다.
        holder.replace(new GatewaySnapshot(Map.of(), new SnapshotMeta(1, 1), 지금));
        줄.enqueue(COUPON, "앞사람", NO_LIMIT, 지금).block();
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        // **값으로 읽는다.** 문자열 포함으로 재면 450 자리를 4500 이 통과한다.
        JsonNode data = 본문(exchange).get("data");
        assertThat(data.get("position").asLong()).isEqualTo(1);
        // 0 이면 순번이 남았는데 곧 입장이라고 말하는 셈이다.
        assertThat(data.get("etaSeconds").asLong()).isEqualTo(450);
    }

    @Test
    @DisplayName("기다리는_중이면_순번을_준다")
    void 기다리는_중이면_순번을_준다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, "앞사람", NO_LIMIT, 지금).block();
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode data = 본문(exchange).get("data");
        assertThat(data.get("status").asText()).isEqualTo("WAITING");
        assertThat(data.get("position").asLong()).isEqualTo(1);
        // **ETA 도 값으로 잰다.** 안 재면 credit 을 0 으로 만드는 변종이 산다 —
        // 아는 credit 으로 계산한 ETA 를 이 클래스에서 아무도 안 보게 된다.
        // credit 10 에 앞사람 하나면 이번 틱에 빠지므로 0 이다. credit 을 0 으로
        // 만드는 변종은 "모른다" 구간으로 넘어가 이 값이 달라진다.
        assertThat(data.get("etaSeconds").asLong()).as("남은 시각").isZero();
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

    /** 본문을 값으로 읽는다. 포함 검사로 재면 자릿수가 다른 값이 통과한다 (TS-11). */
    private JsonNode 본문(MockServerWebExchange exchange) {
        return JSON.readTree(exchange.getResponse().getBodyAsString().block());
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

    /**
     * <b>오류 경로의 폭이 실제로 넓다.</b>
     *
     * <p>상수 둘을 비교하는 것으로는 배선이 안 잠긴다. 난수를 양 끝으로 고정해
     * 실제로 나가는 값의 폭을 재야, 오류 경로를 정상 경로 정책으로 되돌린 경우가
     * 빨개진다. 이 티켓의 존재 이유가 바로 그 폭이다.
     */
    @Test
    @DisplayName("오류_경로의_폭이_정상_경로보다_넓다")
    void 오류_경로의_폭이_정상_경로보다_넓다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.터진다(new IllegalStateException("레디스가 죽었다"));

        int 아래 = Integer.parseInt(물러날_초(필터(() -> 0.0)));
        int 위 = Integer.parseInt(물러날_초(필터(() -> 1.0)));

        // 바닥 30초에 폭 0.5 면 [30, 45] 다. 정상 경로 폭 0.2 였다면 [30, 36] 이라
        // 이 차이가 못 나온다.
        assertThat(위 - 아래).as("오류 경로의 폭").isGreaterThanOrEqualTo(10);
    }

    private QueueStatusFilter 필터(DoubleSupplier 난수) {
        return QueueStatusFilter.of(holder, 줄, tokens, 흐름, meters, 난수, limiter, entryTokens);
    }

    /**
     * <b>장애가 이어지면 더 멀리 보낸다</b> (F7 · 8.2.5).
     *
     * <p>같은 간격으로 계속 두드리면 그 요청들이 회복하려는 뒷단의 자리를 계속
     * 차지한다. 단계는 요청 수가 아니라 실패가 이어진 시간으로 오른다 — 피크에서
     * 요청 수로 세면 밀리초 만에 상한에 닿아 백오프가 있으나 마나가 된다.
     */
    @Test
    @DisplayName("장애가_이어지면_더_멀리_보낸다")
    void 장애가_이어지면_더_멀리_보낸다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.터진다(new IllegalStateException("레디스가 죽었다"));
        QueueStatusFilter 흐르는_시계 = QueueStatusFilter.of(holder, 줄, tokens,
                흐름, meters, () -> 0.5, limiter, entryTokens);

        String 첫_실패 = 물러날_초(흐르는_시계);
        시계를_민다(Duration.ofSeconds(30));
        String 삼십초_뒤 = 물러날_초(흐르는_시계);

        assertThat(Integer.parseInt(삼십초_뒤))
                .as("이어진 시간만큼 멀어진다")
                .isGreaterThan(Integer.parseInt(첫_실패));
    }

    /**
     * <b>조용해지면 백오프가 풀린다.</b>
     *
     * <p>한 번 답한 것만으로는 안 푼다. 샤드 하나가 죽으면 일부만 실패하는데,
     * 성공마다 풀면 그 사이에 성공이 끼어 백오프가 영원히 안 걸린다.
     */
    @Test
    @DisplayName("조용해지면_백오프가_풀린다")
    void 조용해지면_백오프가_풀린다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        QueueStatusFilter 흐르는_시계 = QueueStatusFilter.of(holder, 줄, tokens,
                흐름, meters, () -> 0.5, limiter, entryTokens);
        줄.터진다(new IllegalStateException("레디스가 죽었다"));
        물러날_초(흐르는_시계);
        시계를_민다(Duration.ofSeconds(60));
        String 장애_중 = 물러날_초(흐르는_시계);

        줄.나았다();
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 흐르는_지금.get()).block();
        // **유예가 지나야 푼다.** 답한 직후에 풀면 부분 장애에서 안 걸린다.
        시계를_민다(ErrorBackoff.quiet());
        토큰으로_조회한다(흐르는_시계, tokens.issue(COUPON, MEMBER, 흐르는_지금.get()));
        줄.터진다(new IllegalStateException("다시 죽었다"));

        assertThat(Integer.parseInt(물러날_초(흐르는_시계)))
                .as("회복 뒤 첫 실패는 처음 단계다")
                .isLessThan(Integer.parseInt(장애_중));
    }

    @Test
    @DisplayName("조회가_실패해도_다시_오라고_한다")
    void 조회가_실패해도_다시_오라고_한다() {
        // 순번은 레디스에 남아 있다. 다시 물으면 되므로 줄에서 빼지 않는다.
        //
        // **배수를 실은 스냅샷에서 잰다.** 배수 1.0 인 스냅샷에서 재면 이 갈래의 배수를
        // 지워도 아무것도 안 문다 — 레디스가 흔들리는 구간이 곧 배수가 커져
        // 있는 구간이라, 하필 그때 거절받은 사람만 예산 밖으로 돌아간다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100), 1.5);
        줄.터진다(new IllegalStateException("레디스가 죽었다"));

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(exchange.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        // 바닥은 ETA 를 모르는 밴드(30초)에 배수 1.5 를 건 45 초다. 오류 경로는
        // 거기서 위로만 흔들므로 [45, 60] 안이다. 아래로 흔들면 하필 배수가
        // 커진 구간에만 예산이 안 걸린다.
        assertThat(Integer.parseInt(
                exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER)))
                .as("배수가 정한 바닥을 지킨다")
                .isBetween(45, (int) ErrorBackoff.MAX_SEC);
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

    /**
     * <b>토큰은 URL 에 안 남는다.</b> 앞단 프록시 액세스 로그에 쿼리스트링이
     * 그대로 남고, 그 한 줄이면 남의 차례를 통째로 가로챈다 — 페이로드에
     * {@code memberId} 가 평문이라 발급까지 이어진다.
     */
    @Test
    @DisplayName("헤더로_보낸_토큰이_받아들여진다")
    void 헤더로_보낸_토큰이_받아들여진다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();

        MockServerWebExchange exchange = 토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(exchange.getResponse().getBodyAsString().block()).contains("WAITING");
    }

    /**
     * <b>옛 자리도 한 릴리스는 받는다.</b> 같이 끊으면 이미 발급된 토큰을 든
     * 클라이언트가 배포 순간 전부 자기 순번을 잃는다.
     */
    @Test
    @DisplayName("쿼리로_보낸_토큰도_한_릴리스는_받는다")
    void 쿼리로_보낸_토큰도_한_릴리스는_받는다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();
        String 토큰 = tokens.issue(COUPON, MEMBER, 지금);

        MockServerWebExchange exchange =
                조회한다("/api/v1/coupons/" + COUPON + "/queue?queueToken=" + 토큰);

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    /**
     * <b>헤더가 먼저다.</b> 폴백이 이기면 옛 자리에 아무 값이나 붙여 헤더를 무르게
     * 만들 수 있고, 그러면 자리를 옮긴 의미가 없어진다.
     */
    @Test
    @DisplayName("헤더가_있으면_쿼리는_안_본다")
    void 헤더가_있으면_쿼리는_안_본다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();

        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest
                        .get("/api/v1/coupons/" + COUPON + "/queue?queueToken="
                                + tokens.issue(COUPON, MEMBER, 지금))
                        .header("X-Member-Id", MEMBER)
                        .header("Queue-Token", "망가진토큰"));
        filter.filter(exchange, e -> Mono.empty()).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    /**
     * <b>밖에서 온 이름을 라벨로 쓰지 않는다.</b> 여기 올라오는 예외의 메시지에는
     * 레디스가 실은 키(쿠폰 ID·회원 식별자)가 딸려 오고, 그것이 그대로 라벨이
     * 되면 지표 하나가 메모리를 밀어낸다.
     */
    @Test
    @DisplayName("실패_사유_라벨에_밖의_값이_안_들어간다")
    void 실패_사유_라벨에_밖의_값이_안_들어간다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();
        줄.터진다(new IllegalStateException(COUPON + " 의 " + MEMBER + " 가 죽었다"));

        토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(meters.getMeters())
                .filteredOn(m -> "unavailable".equals(m.getId().getTag("outcome")))
                .singleElement()
                .satisfies(m -> assertThat(m.getId().getTags())
                        .containsExactly(Tag.of("cause", "bad-state"),
                                Tag.of("outcome", "unavailable")));
    }

    /**
     * <b>우리가 안 던진 것은 전부 한 갈래다.</b> 라이브러리 예외를 종류별로
     * 나누면 그 라이브러리가 클래스 하나 바꿀 때마다 시계열이 는다.
     */
    @Test
    @DisplayName("모르는_실패는_한_갈래로_묶는다")
    void 모르는_실패는_한_갈래로_묶는다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();
        // 밖에서 온 예외를 흉내 낸다. 이름이 라벨로 새면 여기서 드러난다.
        줄.터진다(new UnsupportedOperationException(COUPON));

        토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        assertThat(meters.getMeters())
                .filteredOn(m -> "unavailable".equals(m.getId().getTag("outcome")))
                .singleElement()
                .satisfies(m -> assertThat(m.getId().getTags())
                        .containsExactly(Tag.of("cause", "io"),
                                Tag.of("outcome", "unavailable")));
    }

    /** 정상 판정도 같은 태그 키 집합을 쓴다. 안 그러면 프로메테우스가 거절한다. */
    @Test
    @DisplayName("정상_판정도_사유_태그를_싣는다")
    void 정상_판정도_사유_태그를_싣는다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.enqueue(COUPON, MEMBER, NO_LIMIT, 지금).block();

        토큰으로_조회한다(tokens.issue(COUPON, MEMBER, 지금));

        // **값까지 본다.** 있기만 보면 사유 자리에 아무 값이나 넣어도 통과한다.
        assertThat(meters.getMeters())
                .allSatisfy(m -> assertThat(m.getId().getTags())
                        .contains(Tag.of("cause", "none")));
    }
}
