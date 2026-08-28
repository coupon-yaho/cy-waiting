package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.Map;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 뒷단의 매진 응답을 <b>관찰만</b> 한다 (7.2.2 · B-10).
 *
 * <p>응답을 바꾸지 않습니다. 게이트웨이가 매진 응답을 스스로 만들면 그 순간
 * 게이트웨이의 존재가 드러나므로, 뒷단이 낸 것을 그대로 흘려보내면서 사실만
 * 기억합니다.
 */
class SoldOutObserverTest {

    private static final Instant 지금 = Instant.parse("2026-08-28T00:00:00Z");
    private static final String COUPON = "c1";
    private static final String PATH = "/api/v1/coupons/" + COUPON + "/issue";

    private final SoldOutCache 캐시 = SoldOutCache.of(Duration.ofSeconds(10), 100);
    private final SoldOutObserver observer =
            SoldOutObserver.of(캐시, Clock.fixed(지금, ZoneOffset.UTC));

    /**
     * 라우트를 탄 요청.
     *
     * <p><b>둘 다 라우팅 필터가 심는 값</b>이다. 쿠폰 이름은 판정과 같은 출처라야
     * 담는 키와 읽는 키가 안 갈리고, 뒷단 응답 표시는 게이트웨이 자신이 낸
     * 매진을 되먹이지 않게 하는 자리다.
     */
    private MockServerWebExchange 라우트를_탄다(String path) {
        MockServerWebExchange exchange =
                MockServerWebExchange.from(MockServerHttpRequest.post(path));
        exchange.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE,
                Map.of("couponId", COUPON));
        exchange.getAttributes().put(ServerWebExchangeUtils.CLIENT_RESPONSE_ATTR, "뒷단 응답");
        return exchange;
    }

    /** 뒷단이 낸 응답을 그대로 흘려보내면서 기록한다. */
    private MockServerWebExchange 뒷단이_답한다(HttpStatus status, String body) {
        MockServerWebExchange exchange = 라우트를_탄다(PATH);
        observer.filter(exchange, e -> {
            e.getResponse().setStatusCode(status);
            DataBuffer buffer = e.getResponse().bufferFactory()
                    .wrap(body.getBytes(StandardCharsets.UTF_8));
            return e.getResponse().writeWith(Flux.just(buffer));
        }).block();
        return exchange;
    }

    private static String 매진봉투() {
        return """
                {"success":false,"error":{"status":409,"code":"COUPON-306",\
                "message":"쿠폰 재고가 모두 소진되었습니다."}}""";
    }

    /**
     * <b>사유 코드까지 봅니다.</b> 409 만 보면 중복 발급·상태 충돌 같은 다른
     * 409 가 그 쿠폰을 통째로 매진으로 만듭니다.
     */
    @Test
    @DisplayName("매진_봉투를_보면_기록한다")
    void 매진_봉투를_보면_기록한다() {
        뒷단이_답한다(HttpStatus.CONFLICT, 매진봉투());

        assertThat(캐시.soldOut(COUPON)).isTrue();
    }

    @Test
    @DisplayName("다른_사유의_409_는_기록하지_않는다")
    void 다른_사유의_409_는_기록하지_않는다() {
        뒷단이_답한다(HttpStatus.CONFLICT, """
                {"success":false,"error":{"status":409,"code":"COUPON-307",\
                "message":"이미 발급받았습니다."}}""");

        assertThat(캐시.soldOut(COUPON)).isFalse();
    }

    /**
     * <b>상태 코드도 봅니다.</b> 본문만 보면 매진을 설명하는 200 응답이
     * 그 쿠폰을 끊습니다.
     */
    @Test
    @DisplayName("성공_응답은_본문에_코드가_있어도_기록하지_않는다")
    void 성공_응답은_본문에_코드가_있어도_기록하지_않는다() {
        뒷단이_답한다(HttpStatus.OK, """
                {"success":true,"data":{"note":"COUPON-306 은 매진 코드다"}}""");

        assertThat(캐시.soldOut(COUPON)).isFalse();
    }

    /** 응답을 바꾸지 않습니다. 삼켰다가 다시 쓰면 그 자리가 사고의 원인이 됩니다. */
    @Test
    @DisplayName("응답을_그대로_흘려보낸다")
    void 응답을_그대로_흘려보낸다() {
        MockServerWebExchange exchange = 뒷단이_답한다(HttpStatus.CONFLICT, 매진봉투());

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(exchange.getResponse().getBodyAsString().block()).isEqualTo(매진봉투());
    }

    /**
     * <b>본문이 없는 409 도 있습니다.</b> 그때 코드를 못 봤다고 매진으로 접으면,
     * 뒷단이 본문 없이 튕기는 모든 충돌이 그 쿠폰을 끊습니다.
     */
    @Test
    @DisplayName("본문_없는_409_는_기록하지_않는다")
    void 본문_없는_409_는_기록하지_않는다() {
        MockServerWebExchange exchange = 라우트를_탄다(PATH);
        observer.filter(exchange, e -> {
            e.getResponse().setStatusCode(HttpStatus.CONFLICT);
            return Mono.empty();
        }).block();

        assertThat(캐시.soldOut(COUPON)).isFalse();
    }

    /**
     * <b>봉투가 쪼개져 와도 봅니다.</b>
     *
     * <p>뒷단이 본문을 잘게 보내면 코드가 첫 조각에 안 들어올 수 있습니다.
     * 첫 조각만 보면 그때 관찰을 놓치고, 매진 순간의 부하가 그대로 뒷단으로
     * 갑니다 — 이 기능이 있는 이유가 통째로 사라집니다.
     */
    @Test
    @DisplayName("본문이_쪼개져_와도_관찰한다")
    void 본문이_쪼개져_와도_관찰한다() {
        String 봉투 = 매진봉투();
        // **코드 한가운데를 자른다.** 아무 데나 자르면 코드가 한쪽에 온전히
        // 들어가 "첫 조각만 본다" 로도 통과한다.
        int 반 = 봉투.indexOf("COUPON-306") + 4;
        MockServerWebExchange exchange = 라우트를_탄다(PATH);
        observer.filter(exchange, e -> {
            e.getResponse().setStatusCode(HttpStatus.CONFLICT);
            return e.getResponse().writeWith(Flux.just(
                    조각(e, 봉투.substring(0, 반)), 조각(e, 봉투.substring(반))));
        }).block();

        assertThat(캐시.soldOut(COUPON)).isTrue();
        assertThat(exchange.getResponse().getBodyAsString().block()).isEqualTo(봉투);
    }

    /**
     * <b>앞부분까지만 봅니다.</b>
     *
     * <p>상한이 없으면 뒷단이 큰 본문을 409 로 낼 때 그 전부를 들고 있게 됩니다.
     * 못 보는 쪽은 안전한 방향입니다 — 못 막을 뿐 잘못 막지는 않습니다.
     */
    @Test
    @DisplayName("앞부분_밖의_코드는_안_본다")
    void 앞부분_밖의_코드는_안_본다() {
        String 패딩 = "x".repeat(600);
        MockServerWebExchange exchange = 라우트를_탄다(PATH);
        observer.filter(exchange, e -> {
            e.getResponse().setStatusCode(HttpStatus.CONFLICT);
            return e.getResponse().writeWith(Flux.just(
                    조각(e, "{\"pad\":\"" + 패딩 + "\",\"code\":\"COUPON-306\"}")));
        }).block();

        assertThat(캐시.soldOut(COUPON)).isFalse();
    }

    private DataBuffer 조각(org.springframework.web.server.ServerWebExchange e, String text) {
        return e.getResponse().bufferFactory().wrap(text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * <b>게이트웨이 자신이 낸 매진은 안 봅니다.</b>
     *
     * <p>이 필터는 쓰기 필터보다 바깥이라 판정·서킷보다도 바깥입니다. 안 가르면
     * 자기 급전 루프가 됩니다 — 캐시가 끊는다 → 게이트웨이가 같은 코드를 낸다 →
     * 관찰자가 그걸 다시 기록한다 → 뒷단이 살아나도 영영 안 풀립니다.
     *
     * <p>가르는 값은 라우팅 필터만 심고, 서킷 폴백은 재디스패치 전에 지웁니다.
     */
    @Test
    @DisplayName("뒷단에_안_닿은_응답은_기록하지_않는다")
    void 뒷단에_안_닿은_응답은_기록하지_않는다() {
        MockServerWebExchange exchange = 라우트를_탄다(PATH);
        // 라우팅 표시만 걷는다. 판정이 스스로 거절했거나 서킷이 폴백을 낸 자리다.
        exchange.getAttributes().remove(ServerWebExchangeUtils.CLIENT_RESPONSE_ATTR);

        observer.filter(exchange, e -> {
            e.getResponse().setStatusCode(HttpStatus.CONFLICT);
            return e.getResponse().writeWith(Flux.just(조각(e, 매진봉투())));
        }).block();

        assertThat(캐시.size()).isZero();
    }

    /**
     * <b>스트리밍 응답도 봅니다.</b>
     *
     * <p>쓰기 필터는 미디어 타입이 스트리밍이면 <code>writeAndFlushWith</code> 로
     * 갑니다. 한쪽만 덮으면 그 응답의 관찰이 통째로 지나갑니다.
     */
    @Test
    @DisplayName("스트리밍으로_와도_관찰한다")
    void 스트리밍으로_와도_관찰한다() {
        MockServerWebExchange exchange = 라우트를_탄다(PATH);

        observer.filter(exchange, e -> {
            e.getResponse().setStatusCode(HttpStatus.CONFLICT);
            return e.getResponse().writeAndFlushWith(
                    Flux.just(Flux.just(조각(e, 매진봉투()))));
        }).block();

        assertThat(캐시.soldOut(COUPON)).isTrue();
    }

    /**
     * <b>라우트를 안 탄 요청은 아무것도 안 합니다.</b> 못 뽑은 것을 한 자리에
     * 몰아 담으면 그 이름이 매진으로 굳고, 다음 판정이 그것을 읽습니다.
     */
    @Test
    @DisplayName("쿠폰을_못_뽑으면_기록하지_않는다")
    void 쿠폰을_못_뽑으면_기록하지_않는다() {
        MockServerWebExchange exchange =
                MockServerWebExchange.from(MockServerHttpRequest.post("/actuator/health"));
        observer.filter(exchange, e -> {
            e.getResponse().setStatusCode(HttpStatus.CONFLICT);
            DataBuffer buffer = e.getResponse().bufferFactory()
                    .wrap(매진봉투().getBytes(StandardCharsets.UTF_8));
            return e.getResponse().writeWith(Flux.just(buffer));
        }).block();

        assertThat(캐시.size()).isZero();
    }
}
