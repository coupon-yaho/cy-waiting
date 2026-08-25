package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 한 사람이 노드 예산을 다 먹는 것을 막는다.
 *
 * <p>판정의 상한은 쿠폰별과 노드 전역뿐이라 <b>사용자 단위 상한이 없다.</b> 큐가
 * 결국 막긴 하지만, 정상 사용자를 전부 큐로 미는 것 자체가 공격 성공이다.
 */
class AbuseLimitFilterTest {

    private static final Instant 지금 = Instant.parse("2026-08-25T00:00:00Z");
    private static final String ISSUE = "/api/v1/coupons/c1/issue";
    private static final String POLL = "/api/v1/coupons/c1/queue";

    private final MeterRegistry meters = new SimpleMeterRegistry();
    private final AtomicInteger 다음으로_감 = new AtomicInteger();

    private final AbuseLimitFilter filter = AbuseLimitFilter.of(
            Clock.fixed(지금, ZoneOffset.UTC), meters, () -> 0.5);

    private MockServerWebExchange 태운다(String path, String member, String ip) {
        MockServerHttpRequest.BaseBuilder<?> 요청 = MockServerHttpRequest
                .method(HttpMethod.POST, path)
                .header("X-Member-Id", member);
        if (ip != null) {
            요청.header("X-Forwarded-For", ip);
        }
        MockServerWebExchange exchange = MockServerWebExchange.from(요청);
        filter.filter(exchange, e -> {
            다음으로_감.incrementAndGet();
            return Mono.empty();
        }).block();
        return exchange;
    }

    @Test
    @DisplayName("정상_속도는_안_막는다")
    void 정상_속도는_안_막는다() {
        assertThat(태운다(ISSUE, "1", "10.0.0.1").getResponse().getStatusCode()).isNull();
        assertThat(다음으로_감).hasValue(1);
    }

    @Test
    @DisplayName("한_사람이_너무_빨리_두드리면_막는다")
    void 한_사람이_너무_빨리_두드리면_막는다() {
        // 주소를 매번 바꾼다. 그래야 회원 상한만으로 막히는지 보인다.
        for (int i = 0; i < 5; i++) {
            태운다(ISSUE, "1", "10.0.0." + i);
        }

        MockServerWebExchange 넘긴_것 = 태운다(ISSUE, "1", "10.0.0.99");

        assertThat(넘긴_것.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        // **큐에 안 넣는다.** 넣으면 공격자가 자리를 차지하고 그 자리는 남의 것이다.
        assertThat(다음으로_감).hasValue(5);
    }

    /** 로그인이 없어 식별자를 바꾸는 비용이 0 이다. 그것만으로는 우회된다. */
    @Test
    @DisplayName("식별자를_바꿔도_주소로_막는다")
    void 식별자를_바꿔도_주소로_막는다() {
        for (int i = 0; i < 200; i++) {
            태운다(ISSUE, "member" + i, "10.0.0.1");
        }

        assertThat(태운다(ISSUE, "member999", "10.0.0.1").getResponse().getStatusCode())
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    /**
     * NAT 나 회사 프록시 뒤에서는 수백 명이 한 주소를 쓴다. 주소 상한을 사람당
     * 값과 같게 잡으면 그들이 통째로 막힌다 — 막으려던 것이 아니라 정상 사용자다.
     */
    @Test
    @DisplayName("한_주소_뒤의_여럿은_안_막는다")
    void 한_주소_뒤의_여럿은_안_막는다() {
        for (int i = 0; i < 50; i++) {
            assertThat(태운다(ISSUE, "member" + i, "10.0.0.1").getResponse().getStatusCode())
                    .as("%d 번째 사람", i)
                    .isNull();
        }
    }

    @Test
    @DisplayName("다른_사람은_안_막힌다")
    void 다른_사람은_안_막힌다() {
        for (int i = 0; i < 5; i++) {
            태운다(ISSUE, "1", "10.0.0.1");
        }

        assertThat(태운다(ISSUE, "2", "10.0.0.2").getResponse().getStatusCode()).isNull();
    }

    /**
     * 앞쪽은 클라이언트가 채워 넣을 수 있다. 그걸 믿으면 남의 주소로 위장해
     * 남의 몫을 태우고, 정작 자기는 안 걸린다.
     */
    @Test
    @DisplayName("앞에_끼워_넣은_주소를_안_믿는다")
    void 앞에_끼워_넣은_주소를_안_믿는다() {
        for (int i = 0; i < 200; i++) {
            태운다(ISSUE, "m" + i, "1.2.3.4, 10.0.0.9");
        }

        // 프록시가 넣은 것은 맨 끝이다. 앞을 믿었다면 매번 다른 키가 되어 안 막힌다.
        assertThat(태운다(ISSUE, "m99", "9.9.9.9, 10.0.0.9").getResponse().getStatusCode())
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    /**
     * 게이트웨이가 물으라고 해 놓고 그 폴링을 막으면 정상 대기자가 끊긴다.
     * 탭이 여럿이면 지시를 지켜도 발급 상한은 쉽게 넘는다.
     */
    @Test
    @DisplayName("폴링은_발급보다_느슨하다")
    void 폴링은_발급보다_느슨하다() {
        for (int i = 0; i < 8; i++) {
            assertThat(태운다(POLL, "1", "10.0.0.1").getResponse().getStatusCode())
                    .as("%d 번째 폴링", i)
                    .isNull();
        }
    }

    @Test
    @DisplayName("막을_때_다시_올_때를_알려_준다")
    void 막을_때_다시_올_때를_알려_준다() {
        for (int i = 0; i < 5; i++) {
            태운다(ISSUE, "1", "10.0.0.1");
        }

        MockServerWebExchange 넘긴_것 = 태운다(ISSUE, "1", "10.0.0.1");

        // 안 알려 주면 막힌 사람들이 곧바로, 그것도 다 같이 되돌아온다.
        assertThat(넘긴_것.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .isEqualTo("30");
    }

    @Test
    @DisplayName("어느_키로_막았는지_센다")
    void 어느_키로_막았는지_센다() {
        for (int i = 0; i < 6; i++) {
            태운다(ISSUE, "1", "10.0.0.1");
        }

        // 정상 사용자가 걸리는지 보려면 사유가 갈려 있어야 한다.
        assertThat(meters.getMeters()).singleElement().satisfies(m ->
                assertThat(m.getId().getTag("key")).isEqualTo("member"));
    }

    @Test
    @DisplayName("남의_경로는_그대로_흘려보낸다")
    void 남의_경로는_그대로_흘려보낸다() {
        for (int i = 0; i < 20; i++) {
            태운다("/actuator/health", "1", "10.0.0.1");
        }

        assertThat(다음으로_감).hasValue(20);
    }
}
