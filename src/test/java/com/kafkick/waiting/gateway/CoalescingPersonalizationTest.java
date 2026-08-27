package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.MutableClock;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * <b>이 기능의 안전장치입니다</b> (6.10.5).
 *
 * <p>지금 조회 응답에 개인화는 없습니다. 하지만 "내가 발급받았는지" 필드가 하나
 * 붙는 순간 남의 응답을 받게 되고, <b>사람 리뷰로는 그 한 줄을 못 막습니다.</b>
 *
 * <p>뒷단이 {@code Vary} 를 달면 필터가 가려 냅니다. 안 달면 아무도 못 막으므로,
 * 여기서 "응답이 회원마다 갈리는가" 를 직접 잽니다.
 */
class CoalescingPersonalizationTest {

    private static final String PATH = "/api/v1/coupons";

    private final MutableClock 시계 = MutableClock.at(Instant.parse("2026-08-27T00:00:00Z"));

    private final QueryCoalescingFilter filter = QueryCoalescingFilter.of(
            new CoalescingProperties(true, 1024, 1 << 20, 100,
                    List.of(new CoalescingProperties.Route(PATH, Duration.ofMillis(200)))),
            시계, new SimpleMeterRegistry());

    private MockServerWebExchange 조회(String memberId) {
        return MockServerWebExchange.from(MockServerHttpRequest.method(HttpMethod.GET, PATH)
                .header("X-Member-Id", memberId)
                .header("X-Member-Grade", "GOLD"));
    }

    /** 뒷단이 <b>공유해도 된다고 말한</b> 응답. 계약을 지킨 쪽이다. */
    private static Mono<Void> 답한다(org.springframework.web.server.ServerWebExchange e,
            String body) {
        e.getResponse().getHeaders().setCacheControl("public");
        return 그냥_답한다(e, body);
    }

    /** 아무 말도 안 한 응답. 개인화됐는지 게이트웨이는 알 방법이 없다. */
    private static Mono<Void> 그냥_답한다(org.springframework.web.server.ServerWebExchange e,
            String body) {
        e.getResponse().setStatusCode(org.springframework.http.HttpStatus.OK);
        return e.getResponse().writeWith(Mono.just(
                e.getResponse().bufferFactory().wrap(body.getBytes())));
    }

    /**
     * <b>계약을 어기면 남의 응답이 나갑니다.</b> 공유해도 된다고 말해 놓고 회원마다
     * 다르게 답하는 뒷단은 게이트웨이가 못 막습니다 — 이 시험이 그 사실을 적어 둡니다.
     *
     * <p>막을 수 있는 것은 <b>말 안 한 응답</b>까지입니다. 그 위는 계약입니다 (6.10.5).
     */
    @Test
    @DisplayName("공유해도_된다고_해_놓고_개인화하면_남의_응답이_나간다")
    void 공유해도_된다고_해_놓고_개인화하면_남의_응답이_나간다() {
        Set<String> 받은_것 = ConcurrentHashMap.newKeySet();

        List<MockServerWebExchange> 사람들 = IntStream.range(0, 5)
                .mapToObj(i -> 조회("사람" + i))
                .toList();
        // Vary 를 안 단 채 회원별로 다르게 답하는 뒷단.
        사람들.forEach(e -> filter.filter(e, ex -> 답한다(ex,
                "이력:" + ex.getRequest().getHeaders().getFirst("X-Member-Id"))).block());
        사람들.forEach(e -> 받은_것.add(e.getResponse().getBodyAsString().block()));

        // **이것이 지금의 사실입니다.** 다섯이 한 가지를 받습니다 — 넷은 남의
        // 것입니다. 안전장치가 아니라, 안전이 어디에 달려 있는지의 기록입니다.
        assertThat(받은_것)
                .as("화이트리스트에 올린 경로가 개인화되면 이렇게 된다")
                .hasSize(1);
    }

    /**
     * <b>말 안 하면 안 모읍니다.</b> 게이트웨이는 응답이 개인화됐는지 알 방법이
     * 없습니다. 아는 것은 뒷단뿐이라, <b>뒷단이 말한 것만</b> 나눠 줍니다.
     */
    @Test
    @DisplayName("공유해도_된다고_안_하면_안_모은다")
    void 공유해도_된다고_안_하면_안_모은다() {
        AtomicInteger 뒷단 = new AtomicInteger();
        Set<String> 받은_것 = ConcurrentHashMap.newKeySet();

        List<MockServerWebExchange> 사람들 = IntStream.range(0, 5)
                .mapToObj(i -> 조회("사람" + i))
                .toList();
        사람들.forEach(e -> filter.filter(e, ex -> {
            뒷단.incrementAndGet();
            return 그냥_답한다(ex, "이력:" + ex.getRequest().getHeaders().getFirst("X-Member-Id"));
        }).block());
        사람들.forEach(e -> 받은_것.add(e.getResponse().getBodyAsString().block()));

        assertThat(뒷단).as("말 안 한 응답은 안 나눠 준다").hasValue(5);
        assertThat(받은_것).as("각자 자기 것을 받는다").hasSize(5);
    }

    /**
     * <b>쿠키를 심는 응답은 안 나눠 줍니다.</b>
     *
     * <p>{@code Cache-Control: public} 은 "공유 캐시가 저장해도 된다" 이지 "이 응답에
     * 개인 자격 증명이 없다" 가 아닙니다. 뒷단 프레임워크가 세션을 부트스트랩하며
     * 쿠키를 붙이면 그 사이로 지나갑니다 — 받는 브라우저는 남의 세션을 자기 것으로
     * 저장합니다.
     */
    @Test
    @DisplayName("쿠키를_심는_응답은_안_나눠_준다")
    void 쿠키를_심는_응답은_안_나눠_준다() {
        AtomicInteger 뒷단 = new AtomicInteger();
        Set<String> 받은_쿠키 = ConcurrentHashMap.newKeySet();

        List<MockServerWebExchange> 사람들 = IntStream.range(0, 5)
                .mapToObj(i -> 조회("사람" + i))
                .toList();
        사람들.forEach(e -> filter.filter(e, ex -> {
            String 회원 = ex.getRequest().getHeaders().getFirst("X-Member-Id");
            뒷단.incrementAndGet();
            ex.getResponse().getHeaders().add("Set-Cookie", "SESSION=" + 회원 + "; Path=/");
            return 답한다(ex, "목록");
        }).block());
        사람들.forEach(e -> 받은_쿠키.addAll(
                e.getResponse().getHeaders().getOrEmpty("Set-Cookie")));

        assertThat(뒷단).as("쿠키가 실린 응답은 안 나눠 준다").hasValue(5);
        assertThat(받은_쿠키).as("각자 자기 쿠키를 받는다").hasSize(5);
    }

    /**
     * <b>{@code public} 은 토큰이어야 합니다.</b>
     *
     * <p>부분 문자열로 보면 {@code no-public} 이라고 <b>거절한</b> 뒷단이 허락한
     * 것으로 읽힙니다. 확장 지시어에 그 여섯 글자가 들어가는 것도 마찬가지입니다.
     */
    @Test
    @DisplayName("public이_토큰이_아니면_안_모은다")
    void public이_토큰이_아니면_안_모은다() {
        List.of("max-age=300, no-public", "community=\"public-catalog\"").forEach(지시어 -> {
            AtomicInteger 뒷단 = new AtomicInteger();
            IntStream.range(0, 3).forEach(i ->
                    filter.filter(조회("사람" + i), ex -> {
                        뒷단.incrementAndGet();
                        ex.getResponse().getHeaders().setCacheControl(지시어);
                        return 그냥_답한다(ex, "목록");
                    }).block());

            assertThat(뒷단).as("%s 는 허락이 아니다", 지시어).hasValue(3);
        });
    }

    /** 진짜 허락은 다른 지시어와 같이 와도 알아본다. */
    @Test
    @DisplayName("다른_지시어와_같이_와도_알아본다")
    void 다른_지시어와_같이_와도_알아본다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        IntStream.range(0, 3).forEach(i ->
                filter.filter(조회("사람" + i), ex -> {
                    뒷단.incrementAndGet();
                    ex.getResponse().getHeaders().setCacheControl("PUBLIC, max-age=60");
                    return 그냥_답한다(ex, "목록");
                }).block());

        assertThat(뒷단).as("대소문자와 나머지 지시어는 상관없다").hasValue(1);
    }

    /**
     * <b>같은 것을 답하면 모여야 합니다.</b> 위 시험이 "늘 안 모인다" 로도 통과하면
     * 안전장치가 아니라 기능 정지 확인이 됩니다.
     */
    @Test
    @DisplayName("뒷단이_같게_답하면_모인다")
    void 뒷단이_같게_답하면_모인다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        IntStream.range(0, 5).forEach(i ->
                filter.filter(조회("사람" + i), ex -> {
                    뒷단.incrementAndGet();
                    return 답한다(ex, "모두 같은 목록");
                }).block());

        assertThat(뒷단).as("뒷단 호출").hasValue(1);
    }
}
