package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.kafkick.waiting.MutableClock;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 식별자 축을 접은 구간의 진입과 해제 (LG-2).
 *
 * <p><b>접힌 채로도 이미 자리를 잡은 회원은 통과한다.</b> 그 통과를 해제로 읽으면 진입과 해제가 요청마다
 * 번갈아 나가, 통제가 꺼져 있던 구간이 로그에서 사라진다.
 */
@Tag("unit")
class AbuseFoldLogTest {

    private static final Instant 지금 = Instant.parse("2026-08-25T00:00:00Z");
    private static final String ISSUE = "/api/v1/coupons/c1/issue";

    private final MeterRegistry meters = new SimpleMeterRegistry();
    private final MutableClock 시계 = MutableClock.at(지금);
    private final AtomicInteger 통과 = new AtomicInteger();

    private ListAppender<ILoggingEvent> 로그;

    private Logger 로거() {
        return ((LoggerContext) LoggerFactory.getILoggerFactory())
                .getLogger(AbuseLimitFilter.class);
    }

    @BeforeEach
    void 로그를_받는다() {
        로그 = new ListAppender<>();
        로그.start();
        로거().addAppender(로그);
    }

    @AfterEach
    void 로그를_뗀다() {
        로거().detachAppender(로그);
    }

    private long 줄수(Level 수준) {
        return 로그.list.stream().filter(e -> e.getLevel() == 수준).count();
    }

    private MockServerWebExchange 태운다(AbuseLimitFilter 필터, String member, String ip) {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest
                .method(HttpMethod.POST, ISSUE)
                .remoteAddress(new InetSocketAddress("127.0.0.1", 12345))
                .header("X-Member-Id", member)
                .header("X-Forwarded-For", ip));
        필터.filter(exchange, e -> {
            통과.incrementAndGet();
            return Mono.empty();
        }).block();
        return exchange;
    }

    private AbuseLimitFilter 필터(SecondWindowLimiter 리미터) {
        return AbuseLimitFilter.withLimiter(
                시계, meters, () -> 0.5, TrustedProxies.of(List.of("127.0.0.1")), 리미터);
    }

    private void 축을_채운다(AbuseLimitFilter 필터) {
        for (int i = 0; i < 4; i++) {
            태운다(필터, "flood" + i + "-" + 시계.instant().getEpochSecond(), "10.0.0.1");
        }
    }

    /**
     * <b>창이 비는 것은 해제가 아니다.</b> 리미터는 매 초 두 축을 통째로 비우므로 새 초의 첫 요청은 반드시
     * 통과한다. 그것을 해제로 읽으면 공격이 이어지는 내내 진입과 해제가 초당 한 쌍씩 나가고, 그 구간이
     * 로그를 덮으면서 정작 "얼마나 오래 접혀 있었나" 는 못 남는다.
     */
    @Test
    @DisplayName("초가_바뀌어도_접힘은_안_풀린다")
    void 초가_바뀌어도_접힘은_안_풀린다() {
        SecondWindowLimiter 좁은_것 = SecondWindowLimiter.withMaxKeys(4, 2);
        AbuseLimitFilter 좁은_필터 = 필터(좁은_것);

        // 세 초 내리 같은 공격이 이어진다. 초마다 창이 비고 첫 요청은 통과한다.
        for (int 초 = 0; 초 < 3; 초++) {
            축을_채운다(좁은_필터);
            시계.앞으로(Duration.ofSeconds(1));
        }

        assertThat(줄수(Level.WARN)).as("진입은 한 번").isEqualTo(1);
        assertThat(줄수(Level.INFO)).as("안 풀렸으니 해제는 없다").isZero();
    }

    /** 공격이 걷히면 해제가 한 줄 나간다. 안 나가면 언제까지 접혀 있었는지를 못 읽는다. */
    @Test
    @DisplayName("공격이_걷히면_해제가_남는다")
    void 공격이_걷히면_해제가_남는다() {
        SecondWindowLimiter 좁은_것 = SecondWindowLimiter.withMaxKeys(4, 2);
        AbuseLimitFilter 좁은_필터 = 필터(좁은_것);
        축을_채운다(좁은_필터);
        assertThat(줄수(Level.WARN)).as("전제 — 접혔다").isEqualTo(1);

        시계.앞으로(Duration.ofSeconds(10));
        태운다(좁은_필터, "after", "10.0.0.1");

        assertThat(줄수(Level.INFO)).as("해제가 한 줄").isEqualTo(1);
        assertThat(로그.list.stream().anyMatch(e -> e.getFormattedMessage().contains("10초")))
                .as("모드가 실제로 살아 있던 시간을 낸다").isTrue();
    }

    /**
     * <b>두 축이 다 마른 것은 접힘이 아니다.</b> 접을 곳이 없어 거절로 끝나는데, 접힘과 같은 이름으로
     * 세면 운영자가 훨씬 심각한 쪽을 못 본다.
     */
    @Test
    @DisplayName("두_축이_다_차면_접힘과_다른_이름으로_센다")
    void 두_축이_다_차면_접힘과_다른_이름으로_센다() {
        SecondWindowLimiter 좁은_것 = SecondWindowLimiter.withMaxKeys(2, 2);
        AbuseLimitFilter 좁은_필터 = 필터(좁은_것);
        태운다(좁은_필터, "m0", "10.0.0.1");
        태운다(좁은_필터, "m1", "10.0.0.2");

        MockServerWebExchange 막힌_것 = 태운다(좁은_필터, "m9", "10.0.0.9");

        assertThat(막힌_것.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(meters.get("waiting.abuse").tag("key", "keyspace").counter().count())
                .as("두 축이 다 말랐다").isEqualTo(1);
    }
}
