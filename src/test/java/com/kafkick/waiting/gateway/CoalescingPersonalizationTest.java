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
            new CoalescingProperties(true, 1024, 100,
                    List.of(new CoalescingProperties.Route(PATH, Duration.ofMillis(200)))),
            시계, new SimpleMeterRegistry());

    private MockServerWebExchange 조회(String memberId) {
        return MockServerWebExchange.from(MockServerHttpRequest.method(HttpMethod.GET, PATH)
                .header("X-Member-Id", memberId)
                .header("X-Member-Grade", "GOLD"));
    }

    private static Mono<Void> 답한다(org.springframework.web.server.ServerWebExchange e,
            String body) {
        e.getResponse().setStatusCode(org.springframework.http.HttpStatus.OK);
        return e.getResponse().writeWith(Mono.just(
                e.getResponse().bufferFactory().wrap(body.getBytes())));
    }

    /**
     * <b>화이트리스트는 "이 경로는 누구에게나 같다" 는 선언입니다.</b> 거짓이면
     * 남의 응답이 나갑니다 — 이 시험이 그 사실을 숨기지 않고 적어 둡니다.
     *
     * <p>발급 계층이 조회 응답에 회원별 필드를 붙이는 날, 이 경로를 목록에서
     * 빼야 합니다 (6.10.5).
     */
    @Test
    @DisplayName("화이트리스트가_거짓이면_남의_응답이_나간다")
    void 화이트리스트가_거짓이면_남의_응답이_나간다() {
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
