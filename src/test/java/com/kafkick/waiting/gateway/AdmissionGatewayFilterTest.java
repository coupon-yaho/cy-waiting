package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import com.kafkick.waiting.domain.coupon.SnapshotMetas;
import com.kafkick.waiting.domain.coupon.Tunables;
import com.kafkick.waiting.domain.queue.EntryToken;
import com.kafkick.waiting.domain.queue.QueueToken;
import java.time.Clock;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

/**
 * 판정 재료를 <b>로컬 스냅샷에서만</b> 읽는다.
 *
 * <p>요청마다 레디스를 치면 제어 평면을 만든 이유가 사라진다. 그리고 스냅샷에
 * 없는 쿠폰을 그대로 흘리면 레디스 키가 무한히 생긴다.
 */
class AdmissionGatewayFilterTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String COUPON = "c1";

    private static final String MEMBER = "812934";

    /** 배수 속도를 아는 쿠폰의 크레딧. 상한을 여기서 유도한다. */
    private static final long CREDIT = 3;

    /** 스냅샷이 싣는 노드 예산. 심는 자리와 같은 값이어야 한다. */
    private static final SnapshotMeta META = new SnapshotMeta(1_000, 1);

    /**
     * 격벽을 재는 판. <b>2번 줄은 노드 예산을 상한으로 쓴다</b> (B-14) — 쿠폰 몫이
     * 아니다. 노드 예산을 작게 잡아야 상한이 시험 안에서 닿는다.
     */
    private static final SnapshotMeta 좁은_META = new SnapshotMeta(CREDIT, 1);

    /** 판정기에 주는 유휴 몫 비율. 운영 배선과 같은 값이다 (B-13). */
    private static final double IDLE_RATIO = 0.7;

    /** 한산 통과 상한. 숫자를 적지 않고 도메인에서 끌어온다. */
    private static final long IDLE_CAP = CouponStates.idle(1).idleCap(META, IDLE_RATIO);

    /** 고정 시계. 실제 시계를 쓰면 낡음 판정이 장비 속도에 걸린다 (TS-4). */
    private static final Instant 지금 = Instant.parse("2026-08-24T00:00:00Z");

    private final MeterRegistry meters = new SimpleMeterRegistry();

    /** 스냅샷을 아직 믿는 한계. 래치는 이만큼은 버텨야 한다 (불변식 4). */
    private static final Duration 홀더_유효_한계 = Duration.ofSeconds(10);

    private final SnapshotHolder holder = SnapshotHolder.of(
            Duration.ofSeconds(3), 홀더_유효_한계,
            Clock.fixed(지금, ZoneOffset.UTC));
    private final FakeQueuePort 줄 = FakeQueuePort.create();

    private final EntryToken entryTokens = EntryToken.of("not-a-real-secret-0123456789abcdef");

    private final QueueToken tokens = QueueToken.of("not-a-real-secret-0123456789abcdef");

    private final SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(10_000);

    /** 입장 토큰과 같은 비밀키다. 나누면 운영자가 하나만 넣은 채로 나간다. */
    private final IdempotencyKey 멱등키 =
            IdempotencyKey.of("not-a-real-secret-0123456789abcdef");

    /**
     * 흔들림을 고정한다. <b>안 고정하면 값을 못 잰다</b> (TS-4).
     *
     * <p>주입 안 하는 판을 쓰면 {@code ThreadLocalRandom} 이 들어가, 배수를 재는
     * 시험이 6 번에 한 번 다른 값을 받는다.
     */
    private static final DoubleSupplier 고정_난수 = () -> 0.5;

    private final AdmissionGatewayFilter filter = AdmissionGatewayFilter.withIsolatedSoldOutCache(
            holder, AdmissionDecider.of(limiter, IDLE_RATIO),
            Clock.fixed(지금, ZoneOffset.UTC), meters, 고정_난수,
            줄, tokens, limiter, entryTokens, 멱등키);

    private final AtomicReference<Boolean> 뒷단에_닿음 = new AtomicReference<>(false);

    /** 몇 번 닿았나. 한 번이라도 닿았는지만 보면 상한이 무너져도 안 걸린다. */
    private final AtomicInteger 뒷단_횟수 = new AtomicInteger();

    private MockServerWebExchange 요청(String couponId) {
        return 요청(couponId, MEMBER);
    }

    private MockServerWebExchange 요청(String couponId, String memberId) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                        "/api/v1/coupons/" + couponId + "/issue")
                        .header("X-Member-Id", memberId));
        exchange.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE,
                Map.of("couponId", couponId));
        return exchange;
    }

    private AdmissionDecision 태운다(AdmissionGatewayFilter f, String couponId) {
        MockServerWebExchange exchange = 요청(couponId);
        f.filter(exchange, e -> Mono.empty()).block();
        return exchange.getAttribute(AdmissionGatewayFilter.DECISION);
    }

    private MockServerWebExchange 태운다(String couponId) {
        return 태운다(couponId, MEMBER);
    }

    /**
     * <b>회원을 갈아 가며 태운다.</b> 같은 회원으로 반복하면 픽스처가 재등록으로
     * 보고 상한 검사를 건너뛴다 — 줄 길이가 1 에 멈춰 상한을 아무도 안 잰다.
     */
    private MockServerWebExchange 태운다(String couponId, String memberId) {
        MockServerWebExchange exchange = 요청(couponId, memberId);
        filter.filter(exchange, e -> {
            뒷단에_닿음.set(true);
            뒷단_횟수.incrementAndGet();
            return Mono.empty();
        }).block();
        return exchange;
    }

    /**
     * <b>재방문 여부를 응답에 싣습니다</b> (7.5.1).
     *
     * <p>클라이언트가 "내 줄이 사라졌다" 와 "내가 자리를 비웠다" 를 구분할 수
     * 있어야 합니다. 순번은 안 돌려주므로(D-11) 알려 주는 것이 전부입니다.
     */
    @Test
    @DisplayName("재방문_여부를_응답에_싣는다")
    void 재방문_여부를_응답에_싣는다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));
        줄.돌아온_사람으로_만든다(MEMBER);

        MockServerWebExchange exchange = 태운다(COUPON);

        assertThat(exchange.getResponse().getBodyAsString().block())
                .contains("\"rejoined\":true");
    }

    /** 처음 온 사람은 재방문이 아닙니다. 안 두면 "늘 참" 으로도 통과합니다. */
    @Test
    @DisplayName("처음_온_사람은_재방문이_아니다")
    void 처음_온_사람은_재방문이_아니다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100));

        MockServerWebExchange exchange = 태운다(COUPON);

        assertThat(exchange.getResponse().getBodyAsString().block())
                .contains("\"rejoined\":false");
    }

    /** 한산한 쿠폰 여럿을 심는다. 쿠폰별 상한에 먼저 걸리지 않고 노드 예산을 채우려면 필요하다. */
    private void 한산한_쿠폰_여럿을_심는다(int 수) {
        Map<String, CouponState> coupons = IntStream.range(0, 수).boxed()
                .collect(Collectors.toMap(i -> "한산한쿠폰" + i, i -> CouponStates.idle(1_000_000)));
        holder.replace(new GatewaySnapshot(coupons, META, 지금));
    }

    private void 스냅샷을_심는다(CouponState state) {
        스냅샷을_심는다(state, META);
    }

    /** 배수는 판 전체를 보고 나온 전역 값이라 쿠폰이 아니라 메타에 실린다. */
    private static SnapshotMeta 배수가_실린_메타(double 배수) {
        return SnapshotMetas.overBudget(META.globalCredit(), META.gatewayCount(), 배수);
    }

    /** 판 크기를 바꿔 심는다. 한산 통과 상한을 0 으로 만들어야 배분 전 등록을 잰다. */
    private void 스냅샷을_심는다(CouponState state, SnapshotMeta meta) {
        holder.replace(new GatewaySnapshot(
                state == null ? Map.of() : Map.of(COUPON, state),
                meta, 지금));
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
    @DisplayName("차례가_온_사람은_토큰으로_통과한다")
    void 차례가_온_사람은_토큰으로_통과한다() {
        // 토큰이 없으면 줄과 무관하게 판정되고, 기다린 사람과 안 기다린 사람이 같아진다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));

        MockServerWebExchange 토큰을_든_요청 = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                                "/api/v1/coupons/" + COUPON + "/issue")
                        .header("X-Member-Id", MEMBER)
                        .header("Entry-Token", entryTokens.issue(COUPON, MEMBER, 지금)));
        토큰을_든_요청.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE, Map.of("couponId", COUPON));
        filter.filter(토큰을_든_요청, e -> {
            뒷단에_닿음.set(true);
            return Mono.empty();
        }).block();

        assertThat(토큰을_든_요청.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.PASS_TOKEN);
        assertThat(뒷단에_닿음).hasValue(true);
        // 통과는 게이트웨이가 응답을 안 쓴다. 쓰면 뒷단 응답을 덮는다.
        assertThat(토큰을_든_요청.getResponse().getStatusCode()).isNull();
        // **검증에 레디스를 안 친다** (RD-4). 치면 발급마다 왕복이 통과 인원에 비례한다.
        assertThat(줄.왕복()).isZero();
    }

    @Test
    @DisplayName("남의_토큰으로는_안_통한다")
    void 남의_토큰으로는_안_통한다() {
        // 다른 쿠폰의 토큰이 통하면 한 번 받은 것으로 모든 줄을 건너뛴다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                                "/api/v1/coupons/" + COUPON + "/issue")
                        .header("X-Member-Id", MEMBER)
                        .header("Entry-Token", entryTokens.issue("다른쿠폰", MEMBER, 지금)));
        exchange.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE, Map.of("couponId", COUPON));

        filter.filter(exchange, e -> Mono.empty()).block();

        assertThat(exchange.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .matches(AdmissionDecision::isEnqueue, "대기 판정");
    }

    @Test
    @DisplayName("남이_받은_토큰으로는_안_통한다")
    void 남이_받은_토큰으로는_안_통한다() {
        // 토큰이 가리키는 사람을 안 보면 남의 것을 주워 와도 통하고, 발급은
        // 주워 온 사람 앞으로 나간다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                                "/api/v1/coupons/" + COUPON + "/issue")
                        .header("X-Member-Id", MEMBER)
                        .header("Entry-Token", entryTokens.issue(COUPON, "999999", 지금)));
        exchange.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE, Map.of("couponId", COUPON));

        filter.filter(exchange, e -> Mono.empty()).block();

        assertThat(exchange.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .matches(AdmissionDecision::isEnqueue, "대기 판정");
    }

    @Test
    @DisplayName("대기_판정은_뒷단에_안_간다")
    void 대기_판정은_뒷단에_안_간다() {
        // 줄에 세운 사람을 뒷단으로도 보내면, 줄을 선 채로 발급까지 받는다 —
        // 줄 선 사람을 자기가 추월하는 셈이다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));

        MockServerWebExchange exchange = 태운다(COUPON);

        assertThat(exchange.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .matches(AdmissionDecision::isEnqueue, "대기 판정");
        assertThat(뒷단에_닿음).hasValue(false);
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(줄.등록_횟수()).isEqualTo(1);
    }

    @Test
    @DisplayName("줄을_세운_뒤에는_신규_유입이_못_넘는다")
    void 줄을_세운_뒤에는_신규_유입이_못_넘는다() {
        // **스냅샷은 한 틱 늦다.** 줄에 세운 직후에도 여전히 한산하다고 말한다.
        // 그대로 두면 다음 창의 신규 유입이 방금 선 사람을 넘어간다 (불변식 4).
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        태운다(COUPON);

        // 배분이 줄을 비워 스냅샷이 한산해졌다. 그래도 방금 세운 줄은 남아 있다.
        스냅샷을_심는다(CouponStates.idle(1_000));
        MockServerWebExchange 뒤에_온_사람 = 태운다(COUPON);

        assertThat(뒤에_온_사람.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);
        assertThat(뒤에_온_사람.getResponse().getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    }

    @Test
    @DisplayName("래치는_다음_창까지_버틴다")
    void 래치는_다음_창까지_버틴다() {
        // **한 틱만 살면 그 다음 창이 뚫린다.** 초가 넘어가도 스냅샷은 아직
        // 안 따라잡았을 수 있다.
        MutableClock 시계 = MutableClock.at(지금);
        AdmissionGatewayFilter f = AdmissionGatewayFilter.withIsolatedSoldOutCache(
                holder, AdmissionDecider.of(limiter, IDLE_RATIO),
                시계, meters, () -> 0.5, 줄, tokens, limiter, entryTokens, 멱등키);
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        태운다(f, COUPON);

        스냅샷을_심는다(CouponStates.idle(1_000));
        시계.앞으로(Duration.ofSeconds(2));

        assertThat(태운다(f, COUPON)).isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);
    }

    /**
     * <b>래치는 스냅샷이 유효한 동안 살아 있어야 한다.</b> 먼저 풀리면 그 뒤로도
     * 유효한 스냅샷에 방금 세운 줄이 안 보여, 그 창으로 들어온 사람이 줄을 안
     * 서고 지나간다 (불변식 4).
     *
     * <p>홀더와 필터가 같은 시계를 써야 나이가 실제로 자란다. 안 묶으면 늘 0 이다.
     */
    @Test
    @DisplayName("래치는_스냅샷이_유효한_동안_버틴다")
    void 래치는_스냅샷이_유효한_동안_버틴다() {
        MutableClock 시계 = MutableClock.at(지금);
        SnapshotHolder 같은_시계_홀더 = SnapshotHolder.of(
                Duration.ofSeconds(3), 홀더_유효_한계, 시계);
        AdmissionGatewayFilter f = AdmissionGatewayFilter.withIsolatedSoldOutCache(
                같은_시계_홀더, AdmissionDecider.of(limiter, IDLE_RATIO),
                시계, meters, () -> 0.5, 줄, tokens, limiter, entryTokens, 멱등키);
        같은_시계_홀더.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.queueing(10, 1_000, 5_000)), META, 지금));
        태운다(f, COUPON);

        // 배분이 줄을 비운 것으로 보이는 스냅샷. 발행 시각은 그대로라 나이가 자란다.
        같은_시계_홀더.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.idle(1_000)), META, 지금));
        시계.앞으로(홀더_유효_한계);

        // 한계와 같은 나이는 아직 유효하다. 여유가 없으면 여기서 래치만 먼저 죽는다.
        assertThat(같은_시계_홀더.isDataStale(같은_시계_홀더.view())).as("아직 낡음 아님").isFalse();
        assertThat(태운다(f, COUPON)).isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);
    }

    /**
     * <b>한계가 정수 초가 아니어도 래치가 더 오래 산다.</b> 초로 내림하면 여유가
     * 절삭에 다 먹혀 실효 수명이 한계보다 짧아진다. 운영자가 넣을 수 있는 값이라
     * 잠재 결함이 아니라 계약의 구멍이다.
     */
    @Test
    @DisplayName("소수부_한계에서도_래치가_더_오래_산다")
    void 소수부_한계에서도_래치가_더_오래_산다() {
        Duration 한계 = Duration.ofMillis(5_500);
        MutableClock 시계 = MutableClock.at(지금);
        SnapshotHolder 소수_홀더 = SnapshotHolder.of(Duration.ofSeconds(3), 한계, 시계);
        AdmissionGatewayFilter f = AdmissionGatewayFilter.withIsolatedSoldOutCache(
                소수_홀더, AdmissionDecider.of(limiter, IDLE_RATIO),
                시계, meters, () -> 0.5, 줄, tokens, limiter, entryTokens, 멱등키);
        // **초 경계 한가운데서 줄을 세운다.** 래치는 초로 자른 시각을 재므로,
        // 초의 앞부분이 잘려 나간 만큼 실효 수명이 짧아진다. 경계에서 세우면
        // 그 손실이 0 이라 절삭을 못 잰다.
        시계.앞으로(Duration.ofMillis(900));
        소수_홀더.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.queueing(10, 1_000, 5_000)), META, 시계.instant()));
        태운다(f, COUPON);

        소수_홀더.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.idle(1_000)), META, 시계.instant()));
        시계.앞으로(한계);

        assertThat(소수_홀더.isDataStale(소수_홀더.view())).as("아직 낡음 아님").isFalse();
        assertThat(태운다(f, COUPON)).isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);
    }

    @Test
    @DisplayName("대기_응답에_순번과_토큰을_싣는다")
    void 대기_응답에_순번과_토큰을_싣는다() {
        // 토큰이 없으면 폴링할 수단이 없다. 순번이 없으면 얼마나 남았는지 모른다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));

        MockServerWebExchange exchange = 태운다(COUPON);
        String 본문 = exchange.getResponse().getBodyAsString().block();

        assertThat(본문)
                .contains("\"success\":true")
                .contains("\"admitted\":false")
                .contains("\"position\":0")
                .contains("\"queueMode\":\"ADAPTIVE\"");
        assertThat(tokens.verify(토큰(본문), COUPON, 지금)).contains(MEMBER);
        // 순번은 사람마다 다르다. 프록시가 캐시하면 뒤에 온 사람이 남의 순번을 받는다.
        assertThat(exchange.getResponse().getHeaders().getCacheControl()).isEqualTo("no-store");
    }

    @Test
    @DisplayName("통과_판정은_줄을_안_친다")
    void 통과_판정은_줄을_안_친다() {
        // **요청 경로에서 레디스를 치지 않는다** (RD-4). 판정 재료는 스냅샷에만 있다.
        스냅샷을_심는다(CouponStates.idle(1_000));

        태운다(COUPON);

        assertThat(뒷단에_닿음).hasValue(true);
        assertThat(줄.왕복()).isZero();
    }

    @Test
    @DisplayName("줄을_못_세우면_상한만큼_열어_준다")
    void 줄을_못_세우면_상한만큼_열어_준다() {
        // 전부 막으면 레디스 장애가 곧 전면 장애다. 전부 열면 뒷단이 무너진다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        줄.터진다(new IllegalStateException("레디스가 죽었다"));

        MockServerWebExchange exchange = 태운다(COUPON);

        assertThat(뒷단에_닿음).hasValue(true);
        assertThat(exchange.getResponse().getStatusCode()).isNull();
    }

    @Test
    @DisplayName("줄이_실제로_찼으면_거절한다")
    void 줄이_실제로_찼으면_거절한다() {
        // 판정은 자리가 있다고 봤지만 실제로는 없다. 스냅샷은 한 틱 늦다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        줄.가득_찼다();

        MockServerWebExchange exchange = 태운다(COUPON);

        assertThat(exchange.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(뒷단에_닿음).hasValue(false);
    }

    /**
     * <b>한산하던 쿠폰에 사람이 몰리기 시작하는 순간이다.</b> 그때 credit 은 0 인데
     * — 배분은 줄이 있어야 나가고 줄은 여기서 만들어진다 — 상한을 0 으로 넘기면
     * 줄이 한 번도 안 생기고 쿠폰이 영영 그 상태에 갇힌다.
     */
    @Test
    @DisplayName("배수를_아직_못_받아도_줄은_선다")
    void 배수를_아직_못_받아도_줄은_선다() {
        스냅샷을_심는다(CouponStates.always(1_000));

        MockServerWebExchange exchange = 태운다(COUPON);

        assertThat(exchange.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.ENQUEUE_ALWAYS);
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(뒷단에_닿음).hasValue(false);
        // **그 쿠폰의 줄에 섰는지 본다.** 등록 횟수만 세면 어느 줄에 들어갔든 초록이다.
        assertThat(줄.줄_길이(COUPON)).isEqualTo(1);
    }

    /**
     * <b>거절도 전역 배수를 지킨다.</b>
     *
     * <p>과부하가 심할수록 거절의 비중이 커진다. 거절만 배수를 빼면 예산을
     * 건다는 말이 절반만 맞게 된다 — 줄 선 사람은 60초, 못 선 사람은 30초다.
     */
    @Test
    @DisplayName("줄이_꽉_차_거절해도_배수를_지킨다")
    void 줄이_꽉_차_거절해도_배수를_지킨다() {
        // **천장 안쪽에서 잰다.** 배수 3 이면 90 이라 천장 50 에 잘리는데, 그러면
        // 배수 2 를 넘는 어떤 값을 넘겨도 같은 답이라 곱셈이 안 관측된다.
        assertThat(AdmissionGatewayFilter.retryAfterSec(
                AdmissionDecision.REJECT_QUEUE_FULL, () -> 0.5, 1.5))
                .as("ETA 를 모르는 밴드(30초)에 배수 1.5").isEqualTo(45);
    }

    /**
     * <b>차례가 온 사람은 배수에서 뺀다.</b>
     *
     * <p>이 사람은 이미 줄에서 빠졌고 손에 든 것은 수명이 있는 입장 토큰뿐이다.
     * 배수만큼 멀리 보내면 그 사이 몫이 남에게 가고, 토큰이 죽으면 줄 맨 뒤에
     * 새 순번으로 다시 선다 — 순번 역행이자 추월당함이다 (불변식 3·4).
     */
    // 예산 모델도 이 요청을 안 센다. expectedPollRps 는 조회 폴링만 세므로
    // 여기를 늘려도 예산은 안 줄고 토큰만 죽는다.
    @Test
    @DisplayName("차례가_온_사람의_재시도는_배수를_안_받는다")
    void 차례가_온_사람의_재시도는_배수를_안_받는다() {
        assertThat(AdmissionGatewayFilter.retryAfterSec(
                AdmissionDecision.RETRY_TOKEN, () -> 0.5, 3.0))
                .as("가장 가까운 밴드(1초). 배수 3 이 곱해지면 안 된다").isEqualTo(1);
        assertThat(AdmissionGatewayFilter.retryAfterSec(
                AdmissionDecision.RETRY_TOKEN, () -> 0.5, 50.0))
                .as("배수 50 이면 상한 60초 — 토큰 최소 수명 150초의 절반이 날아간다")
                .isEqualTo(1);
    }

    /**
     * <b>정적 함수의 산수만으로는 배선이 안 잠긴다.</b>
     *
     * <p>거절 갈래 넷이 각각 {@code meta.pollScale()} 을 넘기는데, 그것을 {@code 1.0}
     * 으로 바꿔도 기대값을 같은 함수로 만드는 시험은 전부 초록이다 — 동어반복이다.
     * 이 티켓이 발견한 결함이 정확히 그 모양이라, 넷 다 값으로 못 박는다.
     */
    // 30초 밴드에 배수 1.5 라 45 다. 천장(50) 아래여서 배수가 값으로 보인다.
    private static final String 배수가_걸린_거절 = "45";

    @Test
    @DisplayName("판정_거절이_전역_배수를_지킨다")
    void 판정_거절이_전역_배수를_지킨다() {
        스냅샷을_심는다(CouponStates.queueing(1, 1_000, 5_000), 배수가_실린_메타(1.5));

        MockServerWebExchange exchange = 요청(COUPON);
        filter.filter(exchange, e -> Mono.empty()).block();

        assertThat(exchange.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.REJECT_QUEUE_FULL);
        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .as("판정 거절에 걸리는 배수").isEqualTo(배수가_걸린_거절);
    }

    /**
     * <b>2차 방어의 거절도 지킨다.</b>
     *
     * <p>판정은 자리가 있다고 봤는데 실제로는 없던 갈래다. 재료가 낡을수록 이 갈래로
     * 새고, 낡음과 배수는 같이 커진다 — 빠지면 정확히 그때 예산 밖으로 돌아간다.
     */
    @Test
    @DisplayName("이차_거절이_전역_배수를_지킨다")
    void 이차_거절이_전역_배수를_지킨다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100), 배수가_실린_메타(1.5));
        줄.가득_찼다();

        MockServerWebExchange exchange = 요청(COUPON);
        filter.filter(exchange, e -> Mono.empty()).block();

        assertThat(exchange.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .as("2차 거절에 걸리는 배수").isEqualTo(배수가_걸린_거절);
    }

    /**
     * <b>장애 개방의 상한을 넘은 몫도 지킨다.</b>
     *
     * <p>레디스가 흔들려 줄을 못 세우는 구간이 곧 배수가 커져 있는 구간이다.
     * 여기만 빼면 하필 그때 되돌려 보낸 사람이 예산 밖에서 두드린다.
     */
    @Test
    @DisplayName("장애_개방_상한_초과가_전역_배수를_지킨다")
    void 장애_개방_상한_초과가_전역_배수를_지킨다() {
        // 전역 몫이 0 이라 fail-open 상한도 0 이다 — 첫 요청부터 되돌려 보낸다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100),
                SnapshotMetas.overBudget(0, 1, 1.5));
        줄.터진다(new IllegalStateException("레디스가 죽었다"));

        MockServerWebExchange exchange = 요청(COUPON);
        filter.filter(exchange, e -> Mono.empty()).block();

        assertThat(exchange.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .as("장애 개방 상한 초과에 걸리는 배수").isEqualTo(배수가_걸린_거절);
    }

    /**
     * <b>등록 응답도 전역 배수를 지킨다</b> (7.3.3).
     *
     * <p>조회에만 배수를 걸면 방금 줄에 선 사람이 예산 밖에서 두드린다. 매진
     * 파도에서 새로 서는 사람이 가장 많으므로, 빠지면 정확히 그때 샌다.
     */
    @Test
    @DisplayName("등록_응답이_전역_배수를_지킨다")
    void 등록_응답이_전역_배수를_지킨다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 100), 배수가_실린_메타(3.0));
        태운다(COUPON, "앞사람");

        MockServerWebExchange exchange = 태운다(COUPON, "뒷사람");

        // 앞에 한 명이라 ETA 는 0.1 초 — 가장 좁은 밴드(1초)에 배수만 걸린다.
        // 지터는 0.5 를 받아 상쇄된다.
        assertThat(exchange.getResponse().getHeaders().getFirst("Retry-After"))
                .as("배수가 걸린 다음 폴링").isEqualTo("3");
    }

    /**
     * <b>도메인 시험만으로는 배선이 안 잠긴다.</b> 배분 전이라 배수 속도를 모르는데
     * 0 이 나가면 앞에 사람이 남았는데 곧 입장이라고 말하는 셈이다.
     */
    @Test
    @DisplayName("배수를_모르면_가장_넓은_구간을_싣는다")
    void 배수를_모르면_가장_넓은_구간을_싣는다() {
        스냅샷을_심는다(CouponStates.always(1_000));
        // 앞사람이 있어야 ETA 가 0 이 아니다 — 맨 앞 사람은 정말 0 이 맞다.
        태운다(COUPON, "앞사람");

        MockServerWebExchange exchange = 태운다(COUPON, "뒷사람");

        // **값으로 읽는다** — 포함 검사는 450 자리를 4500 이 통과한다.
        assertThat(본문(exchange).get("data").get("position").asLong()).isEqualTo(1);
        assertThat(본문(exchange).get("data").get("etaSeconds").asLong()).isEqualTo(450);
    }

    private JsonNode 본문(MockServerWebExchange exchange) {
        return JSON.readTree(exchange.getResponse().getBodyAsString().block());
    }

    /**
     * 한산한 쿠폰이 노드 예산을 넘겨 몰릴 때다. 예산을 넘긴 첫 사람이 429 를
     * 받으면 줄이 한 번도 안 생기고, 그 쿠폰은 활성화되지 못해 배분을 영영
     * 못 받는다.
     */
    @Test
    @DisplayName("한산한_쿠폰의_초과분이_줄을_선다")
    void 한산한_쿠폰의_초과분이_줄을_선다() {
        스냅샷을_심는다(CouponStates.idle(1_000));

        // 한산 통과 상한은 전역 크레딧의 20% 다. 그 다음 한 명이 첫 대기자다.
        MockServerWebExchange 마지막_통과자 = null;
        for (long i = 0; i < IDLE_CAP; i++) {
            마지막_통과자 = 태운다(COUPON, "무대기" + i);
        }
        // 상한번째까지는 줄 없이 통과한다. 넘긴 쪽만 보면 상한이 1 로 무너져도 초록이다.
        assertThat(마지막_통과자.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.PASS_UNDER_CAP);
        MockServerWebExchange 첫_대기자 = 태운다(COUPON, "대기자");

        assertThat(첫_대기자.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.ENQUEUE_RATE_COUPON);
        assertThat(첫_대기자.getResponse().getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    }

    /**
     * <b>상한을 푸는 것과 없애는 것은 다르다.</b> 낡은 구간에는 대기 인원이 영영
     * 0 으로 보여 이 경로가 계속 불린다 — 없으면 장애 내내 줄이 자란다 (R5).
     * 값을 계산하는 것은 도메인 시험이 잰다. 여기서 재는 것은 <b>게이트웨이가
     * 그 값을 실제로 포트에 넘기는가</b> 다.
     */
    @Test
    @DisplayName("배분_전에도_상한은_유한하다")
    void 배분_전에도_상한은_유한하다() {
        // 배분 전 천장은 최소 배수 속도 × 최대 대기 시간이다. 판 크기와 무관하다.
        long CAP = AdmissionDecider.MIN_CREDIT * AdmissionGatewayFilter.MAX_ETA_SEC;
        // 한산 통과 상한이 0 이어야 첫 사람부터 줄로 간다. 노드 몫 자체가 0 인
        // 판을 쓴다 — 몫이 1 이면 절삭 보정이 걸려 상한이 1 이 된다 (C-10).
        스냅샷을_심는다(CouponStates.idle(1_000_000), new SnapshotMeta(0, 1));

        MockServerWebExchange 첫_사람 = 태운다(COUPON, "대기자0");
        assertThat(첫_사람.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.ENQUEUE_RATE_COUPON);
        for (long i = 1; i < CAP - 1; i++) {
            태운다(COUPON, "대기자" + i);
        }
        // 경계는 양쪽을 다 짚는다. 넘긴 쪽만 보면 상한이 1 로 무너져도 초록이다.
        MockServerWebExchange 마지막_자리 = 태운다(COUPON, "상한번째");
        MockServerWebExchange 넘긴_사람 = 태운다(COUPON, "상한다음");

        assertThat(마지막_자리.getResponse().getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(넘긴_사람.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        // 사다리 6번이 끊은 429 와 구별한다 — 여기서는 등록까지 갔다가 걸려야 한다.
        assertThat(줄.등록_횟수()).isEqualTo((int) CAP + 1);
        assertThat(뒷단에_닿음).hasValue(false);
    }

    /**
     * <b>상한 해제는 배분 전 구간에만 걸린다.</b> 배수 속도를 아는 쿠폰까지
     * 상한 없이 받으면 줄이 무한히 자라고, 뒤에 선 사람의 대기 시간이 어떤
     * 값도 아니게 된다.
     */
    @Test
    @DisplayName("배수를_아는_쿠폰은_상한을_지킨다")
    void 배수를_아는_쿠폰은_상한을_지킨다() {
        // 초당 CREDIT 명씩 최대 대기 시간만큼 뺄 수 있다 — 그것이 줄 길이 상한이다.
        long CAP = CREDIT * AdmissionGatewayFilter.MAX_ETA_SEC;
        스냅샷을_심는다(CouponStates.queueing(CREDIT, 1_000_000, CREDIT + 1));

        for (int i = 0; i < CAP - 1; i++) {
            태운다(COUPON, "대기자" + i);
        }
        // **경계는 양쪽을 다 짚는다.** 넘긴 쪽만 보면 상한이 1 로 무너져도
        // 시험이 초록으로 남는다.
        MockServerWebExchange 마지막_자리 = 태운다(COUPON, "상한번째");
        MockServerWebExchange 넘긴_사람 = 태운다(COUPON, "상한다음");

        assertThat(마지막_자리.getResponse().getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(넘긴_사람.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        // 사다리 6번이 끊은 429 와 구별한다 — 여기서는 등록까지 갔다가 걸려야 한다.
        assertThat(줄.등록_횟수()).isEqualTo((int) CAP + 1);
        assertThat(뒷단에_닿음).hasValue(false);
    }

    @Test
    @DisplayName("상한을_넘긴_몫은_되돌려_보낸다")
    void 상한을_넘긴_몫은_되돌려_보낸다() {
        // 전부 열면 뒷단이 그대로 무너진다. 노드 예산의 절반까지만 흘린다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        줄.터진다(new IllegalStateException("레디스가 죽었다"));

        // 노드 예산은 globalCredit(1,000) / 게이트웨이 수(1) 이고 그 절반이 500 이다.
        // **양쪽에서 못 박는다.** 끊기는 것만 보면 몫이 1 로 바뀌어도 통과한다.
        MockServerWebExchange 상한_직전 = null;
        for (int i = 0; i < 500; i++) {
            상한_직전 = 태운다(COUPON);
        }
        MockServerWebExchange 상한_직후 = 태운다(COUPON);

        assertThat(상한_직전.getResponse().getStatusCode()).isNull();
        assertThat(상한_직후.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    /**
     * 판정과 장애 개방이 각자 예산을 들면 한 초에 둘이 겹쳐 나간다. 리미터를
     * 하나로 두라는 규칙이 막으려던 버스트가 그대로 난다 (F4).
     */
    @Test
    @DisplayName("장애_개방이_판정_예산에서_가져간다")
    void 장애_개방이_판정_예산에서_가져간다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        줄.터진다(new IllegalStateException("레디스가 죽었다"));

        // **판정 경로로 채운다.** 리미터를 손으로 채우면 판정이 다른 리미터를
        // 쓰고 있어도 이 시험이 통과한다.
        한산한_쿠폰_여럿을_심는다(50);
        for (int i = 0; i < 1_000; i++) {
            // **통과했는지 본다.** 안 보면 예산이 절반만 차도 마지막 단언이 통과한다.
            assertThat(태운다("한산한쿠폰" + i % 50)
                    .<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                    .as("%d 번째", i)
                    .isEqualTo(AdmissionDecision.PASS_UNDER_CAP);
        }
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        뒷단에_닿음.set(false);

        // 예산을 따로 들었으면 여기서 500 명이 더 나간다.
        MockServerWebExchange exchange = 태운다(COUPON);

        assertThat(exchange.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(뒷단에_닿음).hasValue(false);
    }

    @Test
    @DisplayName("줄에_선_사람은_뒷단으로_안_보낸다")
    void 줄에_선_사람은_뒷단으로_안_보낸다() {
        // 등록은 됐는데 응답을 못 쓰는 구간이다. 여기서 열어 주면 자리를 쥔
        // 채로 재고까지 먹는다 — 자기가 자기를 추월한다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        MockServerWebExchange exchange = 요청(COUPON);
        exchange.getResponse().setComplete().block();

        filter.filter(exchange, e -> {
            뒷단에_닿음.set(true);
            return Mono.empty();
        }).block();

        assertThat(줄.등록_횟수()).isEqualTo(1);
        assertThat(뒷단에_닿음).hasValue(false);
    }

    @Test
    @DisplayName("회원_식별자가_없으면_줄을_안_친다")
    void 회원_식별자가_없으면_줄을_안_친다() {
        // 형식 검증이 앞에서 걸렀어야 한다. 여기 오면 배선이 틀린 것이다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                        "/api/v1/coupons/" + COUPON + "/issue"));
        exchange.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE, Map.of("couponId", COUPON));
        filter.filter(exchange, e -> Mono.empty()).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(줄.왕복()).isZero();
    }

    private String 토큰(String 본문) {
        int from = 본문.indexOf("\"queueToken\":\"") + "\"queueToken\":\"".length();
        return 본문.substring(from, 본문.indexOf('"', from));
    }

    @Test
    @DisplayName("거절_사유마다_다른_응답이다")
    void 거절_사유마다_다른_응답이다() {
        // **뭉치면 운영자가 엉뚱한 것을 조인다.** 매진은 끝난 것이고, 큐 만원은
        // 잠시 뒤 다시 오면 되고, 과부하는 노드를 늘려야 한다 — 셋이 다르다.
        assertThat(AdmissionGatewayFilter.codeOf(AdmissionDecision.REJECT_SOLD_OUT))
                .isEqualTo(ApiError.Code.SOLD_OUT);
        assertThat(AdmissionGatewayFilter.codeOf(AdmissionDecision.REJECT_QUEUE_FULL))
                .isEqualTo(ApiError.Code.QUEUE_FULL);
        assertThat(AdmissionGatewayFilter.codeOf(AdmissionDecision.REJECT_OVERLOAD))
                .isEqualTo(ApiError.Code.TEMPORARILY_UNAVAILABLE);
    }

    @Test
    @DisplayName("거절_판정마다_봉투가_있다")
    void 거절_판정마다_봉투가_있다() {
        // 새 거절 사유가 생겼는데 봉투를 안 정하면, 열거가 아닌 곳에서는
        // 조용히 매진으로 나간다 — 뒷단 코드를 남의 사유에 붙이는 셈이다.
        List<AdmissionDecision> 거절 = Arrays.stream(AdmissionDecision.values())
                .filter(AdmissionDecision::isReject)
                .toList();

        // 루프가 공회전해도 통과하지 않게 개수를 함께 본다.
        assertThat(거절).hasSize(4);
        // codeOf 는 switch 식이라 null 을 못 낸다 — 던지지 않는 것이 재려는 것이다.
        assertThat(거절).allSatisfy(decision ->
                assertThatCode(() -> AdmissionGatewayFilter.codeOf(decision))
                        .as("%s", decision).doesNotThrowAnyException());
    }

    @Test
    @DisplayName("통과_판정에_봉투를_물으면_던진다")
    void 통과_판정에_봉투를_물으면_던진다() {
        // 조용히 매진을 돌려주면 통과해야 할 사람이 끝난 것으로 처리된다.
        assertThatThrownBy(() -> AdmissionGatewayFilter.codeOf(AdmissionDecision.PASS_UNDER_CAP))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() ->
                AdmissionGatewayFilter.retryAfterSec(AdmissionDecision.ENQUEUE_ALWAYS, () -> 0.5, 1.0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("매진은_뒷단_카탈로그_그대로_낸다")
    void 매진은_뒷단_카탈로그_그대로_낸다() {
        // 코드나 문구가 다르면 그 하나로 게이트웨이가 끊었는지 뒷단까지 갔는지 갈린다.
        스냅샷을_심는다(CouponStates.closed(0));

        MockServerWebExchange exchange = 태운다(COUPON);

        assertThat(exchange.getResponse().getBodyAsString().block())
                .contains("\"code\":\"COUPON-306\"")
                .contains("\"message\":\"쿠폰 재고가 모두 소진되었습니다.\"");
    }

    @Test
    @DisplayName("매진에는_다시_올_때를_안_싣는다")
    void 매진에는_다시_올_때를_안_싣는다() {
        // 재고가 없는데 시각을 주면 소용없는 재시도를 부른다.
        스냅샷을_심는다(CouponStates.closed(0));

        MockServerWebExchange exchange = 태운다(COUPON);

        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .isNull();
    }

    @Test
    @DisplayName("큐가_찼으면_다시_올_때를_알려_준다")
    void 큐가_찼으면_다시_올_때를_알려_준다() {
        // 안 알려 주면 각자 마음대로 돌아온다. 그 파도가 다음 거절을 만든다.
        AdmissionGatewayFilter f = AdmissionGatewayFilter.withIsolatedSoldOutCache(
                holder, AdmissionDecider.of(limiter, IDLE_RATIO),
                Clock.fixed(지금, ZoneOffset.UTC), meters, () -> 0.5, 줄, tokens, limiter, entryTokens, 멱등키);
        스냅샷을_심는다(CouponStates.queueing(1, 1_000, 5_000));

        MockServerWebExchange exchange = 요청(COUPON);
        f.filter(exchange, e -> Mono.empty()).block();

        // **어느 판정에 닿았는지 못 박는다.** 429 는 둘이라 판정이 바뀌어도
        // 상태만 보면 통과한다.
        assertThat(exchange.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.REJECT_QUEUE_FULL);
        assertThat(exchange.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        // 난수를 고정했으니 값이 정해진다. 구간만 보면 정책이 통째로 바뀌어도 통과한다.
        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .isEqualTo("30");
    }

    @Test
    @DisplayName("다시_올_때를_흔들어_준다")
    void 다시_올_때를_흔들어_준다() {
        // 같은 값을 주면 거절당한 사람들이 다 같이 돌아온다. 되돌아오는
        // 파도가 다음 거절을 만들고, 그게 반복된다.
        //
        // 폭까지 본다. 다르기만 하면 흔들림이 얼마든 통과한다.
        assertThat(AdmissionGatewayFilter.retryAfterSec(
                AdmissionDecision.REJECT_QUEUE_FULL, () -> 0, 1.0)).isEqualTo(24);
        assertThat(AdmissionGatewayFilter.retryAfterSec(
                AdmissionDecision.REJECT_QUEUE_FULL, () -> 1, 1.0)).isEqualTo(36);
    }

    /**
     * 같은 값을 주면 거절당한 사람들이 다 같이 돌아온다. 그 파도가 다음 거절을
     * 만들고 반복된다 — 흔들림이 실제로 흩어 놓는지 폭으로 잰다 (F7).
     */
    @Test
    @DisplayName("재시도_안내가_충분히_흩어진다")
    void 재시도_안내가_충분히_흩어진다() {
        Random 난수 = new Random(20260825L);
        double[] 값 = new double[10_000];
        for (int i = 0; i < 값.length; i++) {
            값[i] = AdmissionGatewayFilter.retryAfterSec(
                    AdmissionDecision.REJECT_QUEUE_FULL, 난수::nextDouble, 1.0);
        }

        double 평균 = Arrays.stream(값).average().orElseThrow();
        double 분산 = Arrays.stream(값).map(v -> (v - 평균) * (v - 평균)).average().orElseThrow();

        assertThat(Math.sqrt(분산))
                .as("표준편차가 작으면 다 같이 돌아온다")
                .isGreaterThanOrEqualTo(0.5);
    }

    @Test
    @DisplayName("차례가_온_사람은_멀리_안_보낸다")
    void 차례가_온_사람은_멀리_안_보낸다() {
        // 상한에 걸렸을 뿐 차례는 왔다. 큐 만원인 사람과 같이 두면 그 사이
        // 자기 몫이 남에게 간다.
        //
        // 가장 가까운 밴드라 흔들림이 반올림에 흡수된다 — 그것도 못 박는다.
        assertThat(AdmissionGatewayFilter.retryAfterSec(AdmissionDecision.RETRY_TOKEN, () -> 0, 1.0))
                .isEqualTo(1);
        assertThat(AdmissionGatewayFilter.retryAfterSec(AdmissionDecision.RETRY_TOKEN, () -> 1, 1.0))
                .isEqualTo(1);
    }

    @Test
    @DisplayName("매진에는_안내를_안_싣는다")
    void 매진에는_안내를_안_싣는다() {
        assertThat(AdmissionGatewayFilter.retryAfterSec(AdmissionDecision.REJECT_SOLD_OUT,
                () -> 0.5, 1.0)).isEqualTo(ApiError.NO_RETRY);
    }

    @Test
    @DisplayName("차례가_온_사람은_큐로_안_돌린다")
    void 차례가_온_사람은_큐로_안_돌린다() {
        // 토큰을 들고 왔는데 노드 상한을 넘은 경우다. 어느 술어에도 안 걸려서
        // 그냥 두면 조용히 통과한다 — 상한을 넘겼는데 지나가는 것이다.
        assertThat(AdmissionGatewayFilter.codeOf(AdmissionDecision.RETRY_TOKEN))
                .isEqualTo(ApiError.Code.RETRY_TOKEN);
    }

    @Test
    @DisplayName("모든_판정값에_대응이_있다")
    void 모든_판정값에_대응이_있다() {
        // 필터는 통과·대기·거절 셋으로만 가른다. 어디에도 안 걸리는 값이 생기면
        // 거절 경로로 떨어져 봉투를 못 찾고 500 이 나간다.
        // 개수를 정확히 못 박는다. 넘기만 하면 통과하면 값이 줄어도 안 걸린다.
        assertThat(AdmissionDecision.values()).hasSize(14);
        assertThat(AdmissionDecision.values()).allSatisfy(d -> {
            int 해당 = (d.isPass() ? 1 : 0) + (d.isEnqueue() ? 1 : 0) + (d.isReject() ? 1 : 0);
            assertThat(해당).as("판정 %s", d).isEqualTo(1);
        });
    }

    /** 아무 문자열이나 흘려보내면 그것마다 큐 키가 하나씩 생긴다. */
    /**
     * <b>모른다는 것이 무제한의 사유가 아니다.</b> 사다리 4번은 같은 무지에서
     * 노드 몫 안에서만 여는데, 이 경로만 리미터를 아예 안 탔다. 낡음이 지속되면
     * 아무 문자열 쿠폰이나 초당 무한히 뒷단에 꽂힌다.
     */
    @Test
    @DisplayName("미지_쿠폰도_낡으면_상한_안에서만_흘린다")
    void 미지_쿠폰도_낡으면_상한_안에서만_흘린다() {
        // 스냅샷이 낡으면 없는 쿠폰을 404 로 끝내지 않고 이연한다.
        holder.replace(new GatewaySnapshot(Map.of(COUPON, CouponStates.idle(1_000)),
                META, 지금.minusSeconds(60)));
        long CAP = (long) (AdmissionDecider.globalCap(META) * 0.5);

        // **예산 안은 전부 통과해야 한다.** 한 번이라도 닿았는지만 보면 앞쪽이
        // 이미 막혀도 초록이다.
        for (long i = 0; i < CAP; i++) {
            MockServerWebExchange 통과자 = 태운다("없는쿠폰" + i, "회원" + i);
            assertThat(통과자.getResponse().getStatusCode())
                    .as("예산 안 %d 번째", i)
                    .isNotEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        }
        MockServerWebExchange 넘긴_사람 = 태운다("없는쿠폰넘침", "회원넘침");

        assertThat(뒷단_횟수).hasValue((int) CAP);
        assertThat(넘긴_사람.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    /**
     * <b>판정을 못 거친 갈래도 배수를 들고 가야 한다.</b>
     *
     * <p>안 실으면 폴백이 배수를 못 찾아 1.0 으로 답한다. 같은 요청·같은 장애에
     * 보호 차단은 배수를 걸고 폴백은 안 거는 상태가 되고, 낡음이 곧 배수가 커진
     * 구간이라 하필 그때 갈린다.
     */
    @Test
    @DisplayName("모르는_쿠폰도_판의_배수를_들고_간다")
    void 모르는_쿠폰도_판의_배수를_들고_간다() {
        holder.replace(new GatewaySnapshot(Map.of(COUPON, CouponStates.idle(1_000)),
                SnapshotMetas.overBudget(META.globalCredit(), 1, 1.5), 지금.minusSeconds(60)));

        MockServerWebExchange exchange = 태운다("없는쿠폰", "회원");

        assertThat(exchange.<Double>getAttribute(AdmissionGatewayFilter.POLL_SCALE))
                .as("낡아 이연된 갈래도 자기 판의 배수를 남긴다").isEqualTo(1.5);
    }

    /**
     * <b>첫 틱 전도 마찬가지다.</b> 값은 1.0 이지만 "안 실렸다" 와 "1.0 이다" 는
     * 다른 상태다 — 폴백이 둘을 구분 못 하면 다음에 기본값이 바뀔 때 조용히 갈린다.
     */
    @Test
    @DisplayName("첫_틱_전_통과도_배수를_들고_간다")
    void 첫_틱_전_통과도_배수를_들고_간다() {
        // 스냅샷을 안 심는다. 홀더가 첫 틱 전이다.
        MockServerWebExchange exchange = 태운다("없는쿠폰", "회원");

        assertThat(exchange.<Double>getAttribute(AdmissionGatewayFilter.POLL_SCALE))
                .as("재료가 없어도 자리는 채운다").isEqualTo(1.0);
    }

    @Test
    @DisplayName("미지_쿠폰을_만_번_불러도_줄을_안_만든다")
    void 미지_쿠폰을_만_번_불러도_줄을_안_만든다() {
        스냅샷을_심는다(CouponStates.idle(1_000));

        for (int i = 0; i < 10_000; i++) {
            태운다("없는쿠폰" + i);
        }

        assertThat(줄.왕복()).isZero();
        assertThat(뒷단에_닿음).hasValue(false);
    }

    /**
     * 래치가 안 풀리면 한 번 붐빈 쿠폰이 영영 무대기 통과를 못 준다. 대기 인원이
     * 0 이 되는 것을 한 번도 못 봐도 시간만으로 풀려야 한다.
     */
    @Test
    @DisplayName("래치가_풀리면_무대기_통과가_되살아난다")
    void 래치가_풀리면_무대기_통과가_되살아난다() {
        MutableClock 시계 = MutableClock.at(지금);
        AdmissionGatewayFilter f = AdmissionGatewayFilter.withIsolatedSoldOutCache(
                holder, AdmissionDecider.of(limiter, IDLE_RATIO), 시계, meters, () -> 0.5,
                줄, tokens, limiter, entryTokens, 멱등키);
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        // 래치가 실제로 걸렸는지부터 본다. 안 걸렸으면 뒤의 통과가 아무 뜻이 없다.
        assertThat(태운다(f, COUPON)).matches(AdmissionDecision::isEnqueue, "대기 판정");

        // 스냅샷은 한산해졌지만 대기 인원이 0 이 되는 것은 여기서 한 번도 안 본다.
        스냅샷을_심는다(CouponStates.idle(1_000));
        시계.앞으로(홀더_유효_한계.minusSeconds(1));
        assertThat(태운다(f, COUPON))
                .as("아직 안 풀렸다")
                .isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);

        // 한계를 넘기면 풀린다. **여기도 한계에서 끌어온다** — 숫자를 손으로
        // 적으면 수명을 바꿀 때 이 시험이 조용히 다른 것을 재게 된다.
        // 바로 위 대기 판정이 래치를 다시 찍었으므로 그 시점부터 센다.
        시계.앞으로(홀더_유효_한계.plusSeconds(2));

        assertThat(태운다(f, COUPON)).isEqualTo(AdmissionDecision.PASS_UNDER_CAP);
    }

    /**
     * <b>래치가 스스로 갱신되면 안 된다.</b> 대기 판정이 다시 표식을 찍으면,
     * 트래픽이 이어지는 쿠폰에서 래치가 영영 안 풀린다 — 줄이 다 빠지고 스냅샷이
     * 계속 한산해도 그 노드는 무대기 통과를 못 준다 (R1).
     */
    @Test
    @DisplayName("트래픽이_이어져도_래치가_풀린다")
    void 트래픽이_이어져도_래치가_풀린다() {
        MutableClock 시계 = MutableClock.at(지금);
        AdmissionGatewayFilter f = AdmissionGatewayFilter.withIsolatedSoldOutCache(
                holder, AdmissionDecider.of(limiter, IDLE_RATIO),
                시계, meters, () -> 0.5, 줄, tokens, limiter, entryTokens, 멱등키);
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        태운다(f, COUPON);

        // 줄이 다 빠졌다. 그 뒤로도 초당 한 명씩 계속 들어온다.
        스냅샷을_심는다(CouponStates.idle(1_000));
        for (int 초 = 0; 초 < 홀더_유효_한계.plusSeconds(5).toSeconds(); 초++) {
            시계.앞으로(Duration.ofSeconds(1));
            태운다(f, COUPON);
        }

        assertThat(태운다(f, COUPON)).isEqualTo(AdmissionDecision.PASS_UNDER_CAP);
    }

    /**
     * <b>줄이 보여도 표식은 찍는다.</b> 그 스냅샷은 방금 넣은 이 사람을 아직
     * 모른다 — 다음 판에 줄이 다 빠져 한산으로 뒤집히면 그 사람이 통째로
     * 추월당한다. 계획서가 "줄이 보이면 바로 풀어도 된다" 고 적은 것은 그 한
     * 명을 안 센 것이다.
     */
    @Test
    @DisplayName("줄이_보여도_표식은_찍는다")
    void 줄이_보여도_표식은_찍는다() {
        MutableClock 시계 = MutableClock.at(지금);
        AdmissionGatewayFilter f = AdmissionGatewayFilter.withIsolatedSoldOutCache(
                holder, AdmissionDecider.of(limiter, IDLE_RATIO),
                시계, meters, () -> 0.5, 줄, tokens, limiter, entryTokens, 멱등키);
        // 스냅샷이 이미 줄을 보고 있는 상태에서 한 명 더 넣는다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        태운다(f, COUPON);

        // 배분이 줄을 비웠다. 방금 넣은 사람은 이 스냅샷에 없다.
        스냅샷을_심는다(CouponStates.idle(1_000));

        assertThat(태운다(f, COUPON)).isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);
    }

    /**
     * <b>거절도 줄이 있다는 관측이다.</b> 상한에 걸렸다는 것은 그 줄이 가득
     * 찼다는 뜻인데, 그때 래치를 안 찍으면 만료 뒤 사다리 4번이 켜져 낡음
     * 구간에서 fail-open 으로 뒤집힌다 — 방금 줄 선 사람들을 그 뒤 전원이
     * 추월한다 (불변식 4).
     */
    @Test
    @DisplayName("줄이_찼어도_래치는_찍힌다")
    void 줄이_찼어도_래치는_찍힌다() {
        // 한산 통과 상한이 0 이어야 첫 사람부터 줄로 간다. 노드 몫 자체가 0 인
        // 판을 쓴다 — 몫이 1 이면 절삭 보정이 걸려 상한이 1 이 된다 (C-10).
        스냅샷을_심는다(CouponStates.idle(1_000_000), new SnapshotMeta(0, 1));
        줄.가득_찼다();

        MockServerWebExchange 거절된_사람 = 태운다(COUPON, "대기자0");
        assertThat(거절된_사람.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);

        // 래치가 안 찍혔으면 이 노드가 줄을 세운 적 없는 것처럼 판정된다.
        assertThat(태운다(COUPON, "대기자1")
                .<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);
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

        assertThat(meters.counter("waiting.admission",
                "outcome", "unknown-coupon", "cause", "none").count()).isEqualTo(1);
        assertThat(meters.counter("waiting.admission",
                "outcome", "PASS_UNDER_CAP", "cause", "none").count()).isEqualTo(1);
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
        //
        // `cause` 는 늘 실린다 — 같은 이름에 태그 키 집합이 둘이면 프로메테우스
        // 레지스트리가 등록을 거절한다.
        // 격벽 게이지는 판정과 무관하게 늘 있다. 판정 지표만 골라서 본다.
        assertThat(meters.getMeters())
                .filteredOn(m -> m.getId().getName().equals("waiting.admission"))
                .singleElement()
                .satisfies(m -> assertThat(m.getId().getTags())
                        .containsExactly(Tag.of("cause", "none"),
                                Tag.of("outcome", "PASS_UNDER_CAP")));
        // **좁혀 본 만큼 넓게도 본다.** 위에서 판정 지표만 골라 보면, 이 필터가
        // 거는 다른 계량기에 식별자가 실려도 안 걸린다 — 실제로 격벽 게이지가
        // 붙으면서 그 구멍이 생겼다.
        assertThat(meters.getMeters())
                .allSatisfy(m -> assertThat(m.getId().getTags())
                        .as("%s 의 라벨", m.getId().getName())
                        .noneMatch(tag -> tag.getValue().equals(COUPON)));
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
        AdmissionGatewayFilter 시계를_쓰는_필터 = AdmissionGatewayFilter.withIsolatedSoldOutCache(
                holder, AdmissionDecider.of(limiter, IDLE_RATIO), 시계, meters, 고정_난수,
                줄, tokens, limiter, entryTokens, 멱등키);
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

    /**
     * 판정 품질은 <b>요청마다 한 번만</b> 세어집니다 (O-7).
     *
     * <p>운영 카운터를 여럿 더해 만들면 한 요청이 여러 사유를 지날 때 여러 번
     * 세어져 실패율이 100% 를 넘습니다.
     */
    @Test
    @DisplayName("요청마다_판정_품질을_한_번만_센다")
    void 요청마다_판정_품질을_한_번만_센다() {
        스냅샷을_심는다(CouponStates.idle(100));

        태운다(COUPON);

        assertThat(품질_합계()).as("요청 하나에 한 건").isEqualTo(1.0);
    }

    /** 재료가 낡으면 그 사실이 품질에 남습니다. 안 남기면 그 구간이 성공으로 잡힙니다. */
    @Test
    @DisplayName("낡은_재료로_판정하면_품질이_떨어진_것으로_센다")
    void 낡은_재료로_판정하면_품질이_떨어진_것으로_센다() {
        Instant 낡은_발행 = 지금.minusSeconds(3_600);
        holder.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.idle(1_000_000)),
                new SnapshotMeta(1, 1), 낡은_발행));

        태운다(COUPON);

        assertThat(품질("degraded")).as("재료 없이 판정한 건").isEqualTo(1.0);
        assertThat(품질("fresh")).as("재료를 갖고 판정한 건").isZero();
    }

    /** 재료가 신선하면 신선한 것으로 셉니다. 위 시험이 "늘 degraded" 로도 통과하면 안 됩니다. */
    @Test
    @DisplayName("신선한_재료로_판정하면_그대로_센다")
    void 신선한_재료로_판정하면_그대로_센다() {
        스냅샷을_심는다(CouponStates.idle(100));

        태운다(COUPON);

        assertThat(품질("fresh")).as("재료를 갖고 판정한 건").isEqualTo(1.0);
        assertThat(품질("degraded")).as("재료 없이 판정한 건").isZero();
    }

    private double 품질(String 라벨) {
        var counter = meters.find(AdmissionGatewayFilter.JUDGEMENT)
                .tag("quality", 라벨).counter();
        return counter == null ? 0 : counter.count();
    }

    private double 품질_합계() {
        return meters.find(AdmissionGatewayFilter.JUDGEMENT).counters().stream()
                .mapToDouble(c -> c.count()).sum();
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

    /**
     * <b>게이트웨이가 끊어도 뒷단은 처리했을 수 있다.</b> 타임아웃은 응답을 안
     * 기다리겠다는 뜻이지 뒷단이 안 했다는 뜻이 아니다. 사용자가 다시 시도하면
     * 같은 사람이 두 번 발급된다 — 재사용 방지는 발급 계층이 지되(A-10) 그
     * 멱등성이 작동할 근거는 우리가 줘야 한다.
     */
    @Test
    @DisplayName("통과에는_멱등_키를_실어_보낸다")
    void 통과에는_멱등_키를_실어_보낸다() {
        스냅샷을_심는다(CouponStates.idle(1_000), META);
        AtomicReference<String> 실린_키 = new AtomicReference<>();
        MockServerWebExchange exchange = 요청(COUPON);

        filter.filter(exchange, e -> {
            실린_키.set(e.getRequest().getHeaders().getFirst(IdempotencyKey.HEADER));
            return Mono.empty();
        }).block();

        assertThat(실린_키.get()).isNotBlank();
    }

    /**
     * <b>같은 시도면 같은 키다.</b> 다르면 뒷단이 두 건으로 보고 두 번 발급한다 —
     * 멱등 키를 실은 의미가 사라진다.
     */
    @Test
    @DisplayName("같은_시도를_다시_보내면_같은_키다")
    void 같은_시도를_다시_보내면_같은_키다() {
        스냅샷을_심는다(CouponStates.idle(1_000), META);

        assertThat(실린_멱등_키()).isEqualTo(실린_멱등_키());
    }

    /** 사람이 다르면 다른 시도다. 같은 키를 주면 뒤에 온 사람이 조용히 버려진다. */
    @Test
    @DisplayName("사람이_다르면_다른_키다")
    void 사람이_다르면_다른_키다() {
        스냅샷을_심는다(CouponStates.idle(1_000), META);

        assertThat(실린_멱등_키(MEMBER)).isNotEqualTo(실린_멱등_키("다른사람"));
    }

    /**
     * <b>클라이언트가 준 값을 안 믿는다.</b> 그대로 쓰면 매 요청 다른 값을 넣어
     * 멱등성을 우회하고, 끊긴 발급을 두 번 받아 갈 수 있다.
     */
    @Test
    @DisplayName("클라이언트가_준_멱등_키를_덮는다")
    void 클라이언트가_준_멱등_키를_덮는다() {
        스냅샷을_심는다(CouponStates.idle(1_000), META);
        AtomicReference<String> 실린_키 = new AtomicReference<>();
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                                "/api/v1/coupons/" + COUPON + "/issue")
                        .header("X-Member-Id", MEMBER)
                        .header(IdempotencyKey.HEADER, "내가-정한-값"));
        exchange.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE,
                Map.of("couponId", COUPON));

        filter.filter(exchange, e -> {
            실린_키.set(e.getRequest().getHeaders().getFirst(IdempotencyKey.HEADER));
            return Mono.empty();
        }).block();

        assertThat(실린_키.get()).isNotEqualTo("내가-정한-값");
    }

    private String 실린_멱등_키() {
        return 실린_멱등_키(MEMBER);
    }

    private String 실린_멱등_키(String memberId) {
        AtomicReference<String> 키 = new AtomicReference<>();
        filter.filter(요청(COUPON, memberId), e -> {
            키.set(e.getRequest().getHeaders().getFirst(IdempotencyKey.HEADER));
            return Mono.empty();
        }).block();
        return 키.get();
    }

    /**
     * <b>통과하는 모든 길이 키를 덮어야 한다.</b> 한 갈래만 덮으면 나머지에서
     * 클라이언트가 준 값이 그대로 뒷단에 닿고, 거기로 멱등성을 우회한다.
     *
     * <p>fail-open 은 레디스가 흔들리는 구간이다 — 뒷단 지연이 가장 크고 타임아웃이
     * 실제로 나는 그 구간에서 안 막히면 이 장치가 있으나 마나다.
     */
    @Test
    @DisplayName("fail_open_통과에도_멱등_키를_덮는다")
    void fail_open_통과에도_멱등_키를_덮는다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        줄.터진다(new IllegalStateException("레디스가 죽었다"));

        assertThat(실린_키(요청_with_키("내가-정한-값"))).isNotEqualTo("내가-정한-값");
    }

    /**
     * <b>첫 틱 전 통과도 마찬가지다.</b> 기동 직후라 판정 재료가 없을 뿐,
     * 뒷단으로 가는 것은 같다.
     */
    @Test
    @DisplayName("첫_틱_전_통과에도_멱등_키를_덮는다")
    void 첫_틱_전_통과에도_멱등_키를_덮는다() {
        // 스냅샷을 안 심는다. 홀더가 첫 틱 전이다.
        assertThat(실린_키(요청_with_키("내가-정한-값"))).isNotEqualTo("내가-정한-값");
    }

    /**
     * <b>클라이언트가 준 값이 시도를 가른다.</b> 게이트웨이는 무엇이 한 번의
     * 시도인지 모른다 — 발급 정책은 뒷단 것이다.
     */
    @Test
    @DisplayName("클라이언트_값이_다르면_다른_키가_나간다")
    void 클라이언트_값이_다르면_다른_키가_나간다() {
        스냅샷을_심는다(CouponStates.idle(1_000), META);

        assertThat(실린_키(요청_with_키("시도-1")))
                .isNotEqualTo(실린_키(요청_with_키("시도-2")));
    }

    /** 같은 값을 다시 주면 같은 키다. 아니면 끊긴 발급의 재시도가 새 시도가 된다. */
    @Test
    @DisplayName("같은_클라이언트_값은_같은_키가_나간다")
    void 같은_클라이언트_값은_같은_키가_나간다() {
        스냅샷을_심는다(CouponStates.idle(1_000), META);

        assertThat(실린_키(요청_with_키("시도-1")))
                .isEqualTo(실린_키(요청_with_키("시도-1")));
    }

    private MockServerWebExchange 요청_with_키(String 클라이언트_값) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                                "/api/v1/coupons/" + COUPON + "/issue")
                        .header("X-Member-Id", MEMBER)
                        .header(IdempotencyKey.HEADER, 클라이언트_값));
        exchange.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE,
                Map.of("couponId", COUPON));
        return exchange;
    }

    private String 실린_키(MockServerWebExchange exchange) {
        AtomicReference<String> 키 = new AtomicReference<>();
        filter.filter(exchange, e -> {
            키.set(e.getRequest().getHeaders().getFirst(IdempotencyKey.HEADER));
            return Mono.empty();
        }).block();
        return 키.get();
    }

    /** 차례가 온 사람의 요청. 격벽 상한이 배분된 몫에서 나오는지는 이 경로로 잰다. */
    private MockServerWebExchange 토큰_요청(String memberId) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                        "/api/v1/coupons/" + COUPON + "/issue")
                        .header("X-Member-Id", memberId)
                        .header("Entry-Token", entryTokens.issue(COUPON, memberId, 지금)));
        exchange.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE,
                Map.of("couponId", COUPON));
        return exchange;
    }

    /** 안 끝나는 요청이 쥐고 있는 자리. 시험 끝에 {@link #풀어_준다} 로 돌려준다. */
    private final List<Sinks.Empty<Void>> 붙잡은_자리 = new ArrayList<>();

    /**
     * 격벽을 재는 시계.
     *
     * <p><b>초를 넘겨야 격벽에 닿는다.</b> 사다리 2번 줄이 초당 노드 예산만큼만
     * 통과시키고 격벽은 그 세 배(3초 치)라, 한 초에 몰아 보내면 늘 리미터가 먼저
     * 막는다. 격벽은 걸린 요청이 여러 초에 걸쳐 쌓일 때 비로소 상한이 된다.
     */
    private final MutableClock 격벽_시계 = MutableClock.at(지금);

    private final AdmissionGatewayFilter 격벽_필터 = AdmissionGatewayFilter.withIsolatedSoldOutCache(
            holder, AdmissionDecider.of(limiter, IDLE_RATIO),
            격벽_시계, meters, 고정_난수, 줄, tokens, limiter, entryTokens, 멱등키);

    /**
     * 자리를 놓게 하는 시한. <b>구현과 같은 값이어야 한다</b> — 여기 손으로 적은
     * 수를 두면 상수가 움직여도 시험은 옛 값만 잰다.
     */
    private static final Duration 격벽_시한 = AdmissionGatewayFilter.MAX_IN_FLIGHT;

    /** 한 초에 사다리를 지나갈 수 있는 건수. 좁은 판의 노드 예산 그대로다. */
    private static final int 초당_통과 = (int) CREDIT;

    /** 안 끝나는 요청을 태워 격벽을 채운다. 반환값은 그때 태운 요청들이다. */
    private List<MockServerWebExchange> 붙잡아_채운다(int 건수) {
        List<MockServerWebExchange> 태운_것 = new ArrayList<>();
        for (int i = 0; i < 건수; i++) {
            if (i % 초당_통과 == 0) {
                격벽_시계.앞으로(Duration.ofSeconds(1));
            }
            Sinks.Empty<Void> 안_끝남 = Sinks.empty();
            붙잡은_자리.add(안_끝남);
            MockServerWebExchange exchange = 토큰_요청("사람" + i);
            태운_것.add(exchange);
            격벽_필터.filter(exchange, e -> 안_끝남.asMono()).subscribe();
        }
        return 태운_것;
    }

    /** 다음 초의 요청 하나. 리미터가 아니라 격벽에 걸리는지를 본다. */
    private MockServerWebExchange 다음_초에_한_건(String memberId,
            Function<ServerWebExchange, Mono<Void>> 뒷단) {
        격벽_시계.앞으로(Duration.ofSeconds(1));
        MockServerWebExchange exchange = 토큰_요청(memberId);
        격벽_필터.filter(exchange, e -> 뒷단.apply(e)).block();
        return exchange;
    }

    private void 풀어_준다() {
        붙잡은_자리.forEach(Sinks.Empty::tryEmitEmpty);
    }

    /**
     * <b>초당 건수로는 못 막는 것이 있습니다.</b> 상한은 이 통과가 실제로 차감한
     * 예산에 한 건이 걸려 있을 수 있는 시간을 곱한 값입니다 — 3 × 3초 = 동시 9건.
     */
    @Test
    @DisplayName("동시_건수가_배분된_몫의_세_배를_넘으면_막는다")
    void 동시_건수가_배분된_몫의_세_배를_넘으면_막는다() {
        스냅샷을_심는다(CouponStates.queueing(CREDIT, 1_000_000, 10), 좁은_META);
        List<MockServerWebExchange> 태운_것 = 붙잡아_채운다(초당_통과 * 3);

        MockServerWebExchange 한_건_더 = 다음_초에_한_건("사람" + 초당_통과 * 3, e -> Mono.empty());

        // **상한 직전까지는 통과해야 한다.** 넘긴 것만 보면 늘 막아도 통과한다.
        assertThat(태운_것.getLast().getResponse().getStatusCode()).isNull();
        assertThat(한_건_더.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        // 리미터가 아니라 격벽이 막았는지 못 박는다. 둘 다 거절이라 상태만
        // 보면 사다리가 막아도 이 시험은 통과한다.
        assertThat(한_건_더.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.REJECT_OVERLOAD);
        풀어_준다();
    }

    /**
     * <b>한산한 쿠폰은 credit 이 0 입니다</b> (I1). 그 값으로 상한을 재면 한산한
     * 쿠폰일수록 조여져 R1 이 뒤집힙니다 — 이전 구현의 핵심 버그입니다.
     */
    @Test
    @DisplayName("한산한_쿠폰은_배분_전_폴백으로_안_조여진다")
    void 한산한_쿠폰은_배분_전_폴백으로_안_조여진다() {
        스냅샷을_심는다(CouponStates.idle(1_000_000), META);
        // 최소 배수 속도(1)로 잰 상한은 3 이다. 한산 몫으로 재야 그 위가 열린다.
        for (int i = 0; i < 20; i++) {
            Sinks.Empty<Void> 안_끝남 = Sinks.empty();
            붙잡은_자리.add(안_끝남);
            filter.filter(요청(COUPON, "사람" + i), e -> 안_끝남.asMono()).subscribe();
        }

        MockServerWebExchange 스물한번째 = 요청(COUPON, "사람20");
        filter.filter(스물한번째, e -> Mono.empty()).block();

        assertThat(스물한번째.getResponse().getStatusCode()).isNull();
        풀어_준다();
    }

    /**
     * <b>곱이 넘치면 음수가 되고, 음수 상한은 전면 차단입니다.</b> 예산은 밖에서
     * 오는 값이라, 판이 커지는 방향으로 잘못 실리면 격벽이 정반대로 동작합니다.
     */
    @Test
    @DisplayName("예산이_아무리_커도_격벽이_안_뒤집힌다")
    void 예산이_아무리_커도_격벽이_안_뒤집힌다() {
        // 세 배가 정확히 음수로 넘어가는 판. 큰 값이면 아무거나 되는 것이 아니다.
        스냅샷을_심는다(CouponStates.off(1_000_000), new SnapshotMeta(1L << 62, 1));

        MockServerWebExchange exchange = 요청(COUPON, "사람0");
        filter.filter(exchange, e -> Mono.empty()).block();

        // **어느 판정에 닿았는지 못 박는다.** PASS_UNDER_CAP 으로 바뀌면 밑변이
        // 한산 몫이라 곱이 안 넘치고, 그때도 상태는 null 이다 — 오버플로 방어가
        // 사라진 것을 아무도 모른 채 이 시험만 초록으로 남는다.
        assertThat(exchange.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.PASS_BYPASS);
        assertThat(exchange.getResponse().getStatusCode()).isNull();
    }

    /**
     * <b>연 예산이 곧 격벽의 밑변입니다.</b> 여기서 최소 배수 속도로 떨어지면
     * 상한을 두고 연 몫의 대부분이 격벽에서 다시 막힙니다 — 레디스가 흔들리는
     * 구간에 그게 곧 전면 차단입니다.
     */
    @Test
    @DisplayName("장애_개방은_연_예산만큼_격벽도_연다")
    void 장애_개방은_연_예산만큼_격벽도_연다() {
        스냅샷을_심는다(CouponStates.queueing(10, 1_000_000, 5_000), META);
        줄.터진다(new IllegalStateException("레디스가 죽었다"));
        for (int i = 0; i < 10; i++) {
            Sinks.Empty<Void> 안_끝남 = Sinks.empty();
            붙잡은_자리.add(안_끝남);
            filter.filter(요청(COUPON, "사람" + i), e -> 안_끝남.asMono()).subscribe();
        }

        MockServerWebExchange 열한번째 = 요청(COUPON, "사람10");
        filter.filter(열한번째, e -> Mono.empty()).block();

        assertThat(열한번째.getResponse().getStatusCode()).isNull();
        풀어_준다();
    }

    /**
     * <b>끝나면 자리가 돌아와야 합니다.</b> 안 돌려주면 격벽이 한 번 차고 나서
     * 영영 안 열리고, 그 쿠폰은 뒷단이 멀쩡해져도 계속 막힙니다.
     */
    @Test
    @DisplayName("끝나면_자리가_돌아온다")
    void 끝나면_자리가_돌아온다() {
        스냅샷을_심는다(CouponStates.queueing(CREDIT, 1_000_000, 10), 좁은_META);
        붙잡아_채운다(초당_통과 * 3);
        풀어_준다();

        assertThat(다음_초에_한_건("사람" + 초당_통과 * 3, e -> Mono.empty()).getResponse().getStatusCode())
                .isNull();
    }

    /**
     * <b>안 끝나는 요청은 자리를 영영 쥡니다.</b> {@code doFinally} 는 끝나는
     * 것만 돌려주지 끝나지 않는 것을 끝내지 못합니다. 멈춘 뒷단 한 대가 그
     * 쿠폰의 격벽을 영구히 닫는 것을 상한이 막아야 합니다.
     */
    @Test
    @DisplayName("안_끝나는_요청도_자리를_내놓는다")
    void 안_끝나는_요청도_자리를_내놓는다() {
        스냅샷을_심는다(CouponStates.queueing(CREDIT, 1_000_000, 10), 좁은_META);
        List<MockServerWebExchange> 태운_것 = new ArrayList<>();
        StepVerifier.withVirtualTime(() -> {
            for (int i = 0; i < 초당_통과 * 3; i++) {
                if (i % 초당_통과 == 0) {
                    격벽_시계.앞으로(Duration.ofSeconds(1));
                }
                MockServerWebExchange 멈춘_것 = 토큰_요청("사람" + i);
                태운_것.add(멈춘_것);
                격벽_필터.filter(멈춘_것, e -> Mono.never()).subscribe();
            }
            return Mono.empty();
        }).thenAwait(Duration.ofSeconds(20)).verifyComplete();

        // 상한이 실제로 끊었는지부터 본다. 안 끊고 자리만 돌려주면 격벽이
        // 세는 것이고, 그때는 동시 건수가 상한을 넘어도 아무도 안 막는다.
        assertThat(태운_것).allSatisfy(끊긴_것 ->
                assertThat(끊긴_것.getResponse().getStatusCode())
                        .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE));
        // 건수를 못 박는다. 하나만 보면 일부만 끊기고 나머지가 자리를 쥔 채
        // 남는 경우를 못 잡는다.
        assertThat(meters.counter("waiting.admission",
                "outcome", "bulkhead-timeout", "cause", "timeout").count())
                .isEqualTo(초당_통과 * 3);

        assertThat(다음_초에_한_건("사람" + 초당_통과 * 3, e -> Mono.empty()).getResponse().getStatusCode())
                .isNull();
    }

    /**
     * <b>차례가 온 사람을 멀리 보내면 그의 자리가 사라집니다.</b> 그는 이미 줄에서
     * 빠졌고 손에 든 것은 수명 180초짜리 입장 토큰뿐입니다. 30초 뒤로 보내면 그
     * 사이 그의 몫이 남에게 가고, 세 번째 시도에서 토큰이 죽어 줄 맨 뒤로 다시
     * 섭니다 — 장애 중에 공정성이 깨지는 자리입니다 (불변식 4).
     */
    @Test
    @DisplayName("격벽이_끊어도_차례가_온_사람은_가까이_부른다")
    void 격벽이_끊어도_차례가_온_사람은_가까이_부른다() {
        스냅샷을_심는다(CouponStates.queueing(CREDIT, 1_000_000, 10), 좁은_META);
        붙잡아_채운다(초당_통과 * 3);

        MockServerWebExchange 차례가_온_사람 = 다음_초에_한_건(
                "사람" + 초당_통과 * 3, e -> Mono.empty());

        assertThat(차례가_온_사람.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        // 폴백이 같은 장애에 쓰는 갈래와 같은 값이어야 한다. 밴드만 보면
        // 정책이 통째로 바뀌어도 통과하므로 값으로 못 박는다.
        assertThat(차례가_온_사람.getResponse().getHeaders().getFirst("Retry-After"))
                .isEqualTo(String.valueOf(
                        AdmissionGatewayFilter.retryAfterSec(
                                AdmissionDecision.RETRY_TOKEN, 고정_난수, 1.0)));
        풀어_준다();
    }

    /**
     * <b>줄에 안 선 쪽은 배수를 지킨다.</b>
     *
     * <p>보호 차단이 도는 순간이 곧 예산이 빠듯한 순간이다. 여기만 빼면 과부하가
     * 심할수록 거절 비중이 커져 예산을 건다는 말이 절반만 맞게 된다.
     */
    @Test
    @DisplayName("보호_차단의_거절이_전역_배수를_지킨다")
    void 보호_차단의_거절이_전역_배수를_지킨다() {
        스냅샷을_심는다(CouponStates.off(1_000), SnapshotMetas.overBudget(1, 1, 1.5));
        for (int i = 0; i < 3; i++) {
            Sinks.Empty<Void> 안_끝남 = Sinks.empty();
            붙잡은_자리.add(안_끝남);
            격벽_필터.filter(요청(COUPON, "사람" + i), e -> 안_끝남.asMono()).subscribe();
        }

        MockServerWebExchange 막힌_것 = 요청(COUPON, "사람9");
        격벽_필터.filter(막힌_것, e -> Mono.empty()).block();

        assertThat(막힌_것.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(막힌_것.getResponse().getHeaders().getFirst("Retry-After"))
                .as("30초 밴드에 배수 1.5").isEqualTo(배수가_걸린_거절);
        풀어_준다();
    }

    /**
     * <b>배수가 걸려 있어도 마찬가지다.</b>
     *
     * <p>보호 차단이 도는 순간이 곧 배수가 큰 순간이라, 여기에 배수를 곱하면
     * 정확히 장애 중에 토큰 보유자가 가장 멀리 밀린다. 배수 50 이면 상한 60초를
     * 두 번 받는 사이 토큰 최소 수명 150초의 대부분이 날아간다.
     *
     * <p>예산 모델이 이 요청을 안 세므로 늘려도 얻는 것이 없다.
     */
    @Test
    @DisplayName("배수가_걸려도_차례가_온_사람은_가까이_부른다")
    void 배수가_걸려도_차례가_온_사람은_가까이_부른다() {
        스냅샷을_심는다(CouponStates.queueing(CREDIT, 1_000_000, 10),
                SnapshotMetas.overBudget(CREDIT, 1, 50.0));
        붙잡아_채운다(초당_통과 * 3);

        MockServerWebExchange 차례가_온_사람 = 다음_초에_한_건(
                "사람" + 초당_통과 * 3, e -> Mono.empty());

        assertThat(차례가_온_사람.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(차례가_온_사람.getResponse().getHeaders().getFirst("Retry-After"))
                .as("배수 50 이 곱해지면 상한 60 이 된다").isEqualTo("1");
        풀어_준다();
    }

    /**
     * <b>줄에 안 선 사람은 멀리 보냅니다.</b> 둘을 같은 밴드로 부르면, 차례를
     * 기다린 사람과 방금 온 사람이 같은 간격으로 다시 옵니다 — 기다린 쪽이
     * 손해입니다.
     */
    @Test
    @DisplayName("차례가_없으면_먼_밴드로_부른다")
    void 차례가_없으면_먼_밴드로_부른다() {
        스냅샷을_심는다(CouponStates.off(1_000), new SnapshotMeta(1, 1));
        List<Sinks.Empty<Void>> 잡은_것 = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Sinks.Empty<Void> 안_끝남 = Sinks.empty();
            잡은_것.add(안_끝남);
            붙잡은_자리.add(안_끝남);
            격벽_필터.filter(요청(COUPON, "사람" + i), e -> 안_끝남.asMono()).subscribe();
        }

        MockServerWebExchange 막힌_것 = 요청(COUPON, "사람9");
        격벽_필터.filter(막힌_것, e -> Mono.empty()).block();

        assertThat(막힌_것.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(막힌_것.<AdmissionDecision>getAttribute(AdmissionGatewayFilter.DECISION))
                .isEqualTo(AdmissionDecision.REJECT_OVERLOAD);
        // **기대값을 같은 함수로 만들지 않는다.** 그러면 호출부가 배수를 안
        // 넘겨도 좌우가 같이 움직여 초록이다.
        assertThat(막힌_것.getResponse().getHeaders().getFirst("Retry-After"))
                .as("ETA 를 모르는 밴드(30초). 배수가 안 실린 판이라 그대로 30")
                .isEqualTo("30");
        풀어_준다();
    }

    /**
     * <b>시한의 양쪽을 잽니다.</b> 위쪽만 재면 값이 커지는 방향으로는 아무 값이나
     * 통과하고, 아래쪽만 재면 정상적으로 느린 요청까지 끊는 것을 못 잡습니다 —
     * 그때 격벽은 하려던 것과 정반대로 뒷단이 멀쩡한데도 응답을 버립니다.
     */
    @Test
    @DisplayName("시한_직전은_끝까지_가고_직후는_끊는다")
    void 시한_직전은_끝까지_가고_직후는_끊는다() {
        스냅샷을_심는다(CouponStates.queueing(CREDIT, 1_000_000, 10), 좁은_META);
        MockServerWebExchange 직전 = 토큰_요청("사람0");
        StepVerifier.withVirtualTime(() -> 격벽_필터.filter(직전,
                        e -> Mono.delay(격벽_시한.minusSeconds(1)).then()))
                .thenAwait(격벽_시한)
                .verifyComplete();

        MockServerWebExchange 직후 = 토큰_요청("사람1");
        StepVerifier.withVirtualTime(() -> 격벽_필터.filter(직후,
                        e -> Mono.delay(격벽_시한.plusSeconds(1)).then()))
                .thenAwait(격벽_시한.plusSeconds(2))
                .verifyComplete();

        assertThat(직전.getResponse().getStatusCode()).as("시한 직전").isNull();
        assertThat(직후.getResponse().getStatusCode()).as("시한 직후")
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    /**
     * <b>실패로 끝나도 자리가 돌아와야 합니다.</b> 뒷단이 터지는 구간이 곧 격벽이
     * 가장 필요한 구간인데, 거기서 자리가 새면 그때부터 아무도 못 지나갑니다.
     */
    @Test
    @DisplayName("터져서_끝나도_자리가_돌아온다")
    void 터져서_끝나도_자리가_돌아온다() {
        스냅샷을_심는다(CouponStates.queueing(CREDIT, 1_000_000, 10), 좁은_META);
        for (int i = 0; i < 초당_통과 * 3; i++) {
            if (i % 초당_통과 == 0) {
                격벽_시계.앞으로(Duration.ofSeconds(1));
            }
            격벽_필터.filter(토큰_요청("사람" + i),
                    e -> Mono.error(new IllegalStateException("뒷단이 터졌다")))
                    .onErrorResume(e -> Mono.empty()).block();
        }

        assertThat(다음_초에_한_건("사람" + 초당_통과 * 3, e -> Mono.empty()).getResponse().getStatusCode())
                .isNull();
    }

    /**
     * <b>핫 쿠폰이 콜드 쿠폰의 통로를 막으면 안 됩니다.</b> 하나로 세면 몰리는
     * 쿠폰이 자리를 다 쓰고 한산한 쿠폰이 그 뒤에 밀립니다 — R1 이 뒤집힙니다.
     */
    @Test
    @DisplayName("핫_쿠폰이_차도_콜드_쿠폰은_지나간다")
    void 핫_쿠폰이_차도_콜드_쿠폰은_지나간다() {
        holder.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.queueing(CREDIT, 1_000_000, 10),
                        "c2", CouponStates.idle(1_000_000)),
                좁은_META, 지금));
        붙잡아_채운다(초당_통과 * 3);

        // 핫 쿠폰이 실제로 찼는지 먼저 본다. 안 찼으면 아래 단언은 아무것도 못 말한다.
        MockServerWebExchange 핫 = 다음_초에_한_건("사람" + 초당_통과 * 3, e -> Mono.empty());
        MockServerWebExchange 콜드 = 요청("c2", "사람9");
        격벽_필터.filter(콜드, e -> Mono.empty()).block();

        assertThat(핫.getResponse().getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(콜드.getResponse().getStatusCode()).isNull();
        풀어_준다();
    }

    /**
     * <b>배포 없이 되돌릴 수 있어야 롤백이 성립합니다</b> (P-1). 실려 온 값이 이깁니다.
     */
    @Test
    @DisplayName("실려_온_걸림_시간이_격벽_상한을_바꾼다")
    void 실려_온_걸림_시간이_격벽_상한을_바꾼다() {
        List<MockServerWebExchange> 태운_것;
        // 걸림 시간을 2초로 줄이면 상한이 세 배에서 두 배로 조여진다.
        holder.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.queueing(CREDIT, 1_000_000, 10)),
                SnapshotMeta.withoutPollScale(CREDIT, 1, new Tunables(0.7, 2)), 지금));
        태운_것 = 붙잡아_채운다(초당_통과 * 2);

        MockServerWebExchange 한_건_더 = 다음_초에_한_건("사람" + 초당_통과, e -> Mono.empty());

        assertThat(한_건_더.getResponse().getStatusCode())
                .as("걸림 시간 2초면 상한도 2배다")
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        풀어_준다();
    }

    /**
     * <b>안 실려 오면 기동값입니다.</b> 기본값으로 채우면 각 노드의 설정이 덮입니다.
     */
    @Test
    @DisplayName("안_실려_오면_기동값으로_돈다")
    void 안_실려_오면_기동값으로_돈다() {
        스냅샷을_심는다(CouponStates.queueing(CREDIT, 1_000_000, 10), 좁은_META);
        붙잡아_채운다(초당_통과 * 2);

        // 기동값은 3초라 상한이 세 배다. 두 배 치를 채운 것으로는 안 막힌다.
        MockServerWebExchange 한_건_더 = 다음_초에_한_건("사람" + 초당_통과, e -> Mono.empty());

        assertThat(한_건_더.getResponse().getStatusCode()).isNull();
        풀어_준다();
    }
}
