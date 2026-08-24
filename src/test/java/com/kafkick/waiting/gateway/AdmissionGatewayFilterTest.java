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
import com.kafkick.waiting.domain.queue.QueueToken;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
import reactor.core.publisher.Mono;

/**
 * 판정 재료를 <b>로컬 스냅샷에서만</b> 읽는다.
 *
 * <p>요청마다 레디스를 치면 제어 평면을 만든 이유가 사라진다. 그리고 스냅샷에
 * 없는 쿠폰을 그대로 흘리면 레디스 키가 무한히 생긴다.
 */
class AdmissionGatewayFilterTest {

    private static final String COUPON = "c1";

    private static final String MEMBER = "812934";

    /** 고정 시계. 실제 시계를 쓰면 낡음 판정이 장비 속도에 걸린다 (TS-4). */
    private static final Instant 지금 = Instant.parse("2026-08-24T00:00:00Z");

    private final MeterRegistry meters = new SimpleMeterRegistry();

    private final SnapshotHolder holder = SnapshotHolder.of(
            Duration.ofSeconds(3), Duration.ofSeconds(10),
            Clock.fixed(지금, ZoneOffset.UTC));
    private final FakeQueuePort 줄 = FakeQueuePort.create();

    private final QueueToken tokens = QueueToken.of("not-a-real-secret-0123456789abcdef");

    private final AdmissionGatewayFilter filter = AdmissionGatewayFilter.of(
            holder, AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(10_000), 0.2),
            Clock.fixed(지금, ZoneOffset.UTC), meters, 줄, tokens);

    private final AtomicReference<Boolean> 뒷단에_닿음 = new AtomicReference<>(false);

    private MockServerWebExchange 요청(String couponId) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                        "/api/v1/coupons/" + couponId + "/issue")
                        .header("X-Member-Id", MEMBER));
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
        MockServerWebExchange exchange = 요청(couponId);
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
        AdmissionGatewayFilter f = AdmissionGatewayFilter.of(
                holder, AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(10_000), 0.2),
                시계, meters, () -> 0.5, 줄, tokens);
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        태운다(f, COUPON);

        스냅샷을_심는다(CouponStates.idle(1_000));
        시계.앞으로(Duration.ofSeconds(2));

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

    @Test
    @DisplayName("상한을_넘긴_몫은_되돌려_보낸다")
    void 상한을_넘긴_몫은_되돌려_보낸다() {
        // 전부 열면 뒷단이 그대로 무너진다. 초당 상한을 넘긴 몫은 끊는다.
        스냅샷을_심는다(CouponStates.queueing(10, 1_000, 5_000));
        줄.터진다(new IllegalStateException("레디스가 죽었다"));

        // **양쪽에서 못 박는다.** 끊기는 것만 보면 상한이 1 로 바뀌어도 통과한다.
        MockServerWebExchange 상한_직전 = null;
        for (int i = 0; i < 200; i++) {
            상한_직전 = 태운다(COUPON);
        }
        MockServerWebExchange 상한_직후 = 태운다(COUPON);

        assertThat(상한_직전.getResponse().getStatusCode()).isNull();
        assertThat(상한_직후.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
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
                AdmissionGatewayFilter.retryAfterSec(AdmissionDecision.ENQUEUE_ALWAYS, () -> 0.5))
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
        AdmissionGatewayFilter f = AdmissionGatewayFilter.of(
                holder, AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(10_000), 0.2),
                Clock.fixed(지금, ZoneOffset.UTC), meters, () -> 0.5, 줄, tokens);
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
                AdmissionDecision.REJECT_QUEUE_FULL, () -> 0)).isEqualTo(24);
        assertThat(AdmissionGatewayFilter.retryAfterSec(
                AdmissionDecision.REJECT_QUEUE_FULL, () -> 1)).isEqualTo(36);
    }

    @Test
    @DisplayName("차례가_온_사람은_멀리_안_보낸다")
    void 차례가_온_사람은_멀리_안_보낸다() {
        // 상한에 걸렸을 뿐 차례는 왔다. 큐 만원인 사람과 같이 두면 그 사이
        // 자기 몫이 남에게 간다.
        //
        // 가장 가까운 밴드라 흔들림이 반올림에 흡수된다 — 그것도 못 박는다.
        assertThat(AdmissionGatewayFilter.retryAfterSec(AdmissionDecision.RETRY_TOKEN, () -> 0))
                .isEqualTo(1);
        assertThat(AdmissionGatewayFilter.retryAfterSec(AdmissionDecision.RETRY_TOKEN, () -> 1))
                .isEqualTo(1);
    }

    @Test
    @DisplayName("매진에는_안내를_안_싣는다")
    void 매진에는_안내를_안_싣는다() {
        assertThat(AdmissionGatewayFilter.retryAfterSec(AdmissionDecision.REJECT_SOLD_OUT,
                () -> 0.5)).isEqualTo(ApiError.NO_RETRY);
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
                holder, AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(10), 0.2), 시계,
                meters, 줄, tokens);
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
