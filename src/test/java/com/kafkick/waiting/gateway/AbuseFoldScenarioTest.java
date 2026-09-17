package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.MutableClock;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 식별자 축 접힘의 진입·유지·회복 (CY-941).
 *
 * <p>접기는 메모리 상한을 지키는 마지막 받침대다. 이 분기가 운영에서 처음 도는 순간이 사고 한복판이라, 세
 * 단계가 각각 무엇을 보장하는지 여기서 못 박는다.
 */
@Tag("chaos")
class AbuseFoldScenarioTest {

    private static final String ISSUE = "/api/v1/coupons/c1/issue";

    /** 공격자의 주소. 식별자를 바꿔가며 축을 채우는 쪽이다. */
    private static final String 공격 = "10.0.0.1";

    /** 정상 대기자의 주소. 접힌 구간에도 들어와야 한다. */
    private static final String 정상 = "10.0.0.2";

    /** 접힘이 풀리기까지 기다리는 시간. 필터가 잡아 두는 여유와 같다. */
    private static final Duration 접힘_여유 = Duration.ofSeconds(4);

    private final MeterRegistry meters = new SimpleMeterRegistry();

    private final MutableClock 시계 = MutableClock.at(Instant.parse("2026-08-25T00:00:00Z"));

    /** 축마다 자리를 좁게 준다. 실제 상한(20만)을 채우려면 회차가 통째로 그 루프가 된다. */
    private final SecondWindowLimiter 좁은_것 = SecondWindowLimiter.withMaxKeys(400, 2);

    private final AbuseLimitFilter filter = AbuseLimitFilter.withLimiter(
            시계, meters, () -> 0.5, TrustedProxies.of(List.of("127.0.0.1")), 좁은_것);

    private final AtomicInteger 통과 = new AtomicInteger();

    private MockServerWebExchange 태운다(String member, String ip) {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest
                .method(HttpMethod.POST, ISSUE)
                .remoteAddress(new InetSocketAddress("127.0.0.1", 12345))
                .header("X-Member-Id", member)
                .header("X-Forwarded-For", ip));
        filter.filter(exchange, e -> {
            통과.incrementAndGet();
            return Mono.empty();
        }).block();
        return exchange;
    }

    private double 접힌_통과() {
        return meters.find("waiting.abuse.folded").counter().count();
    }

    private double 접힘_게이지() {
        return meters.find("waiting.abuse.folding").gauge().value();
    }

    /** 식별자를 바꿔가며 축을 채운다. 자리를 못 얻은 요청부터 접힘이 시작된다. */
    private void 축을_채운다(int 건수, String 접두어) {
        for (int i = 0; i < 건수; i++) {
            태운다(접두어 + i, 공격);
        }
    }

    /**
     * <b>진입 — 축이 차는 순간 새 대기자가 통과한다.</b>
     *
     * <p>거절로 두면 채운 쪽이 아니라 새 주소로 오는 정상 대기자가 막힌다.
     */
    @Test
    @DisplayName("진입_축이_차면_접고_새_대기자는_통과한다")
    void 진입_축이_차면_접고_새_대기자는_통과한다() {
        축을_채운다(5, "flood");

        int 앞서_통과 = 통과.get();
        MockServerWebExchange 새_대기자 = 태운다("new-member", 정상);

        assertThat(새_대기자.getResponse().getStatusCode()).as("접힌 구간에도 들어온다").isNull();
        assertThat(통과.get()).isEqualTo(앞서_통과 + 1);
        assertThat(접힌_통과()).as("접고 통과한 것을 센다").isPositive();
        assertThat(접힘_게이지()).as("지금 접혀 있다").isEqualTo(1);
    }

    /**
     * <b>유지 — 접힌 채로 둬도 주소 축은 유계다.</b>
     *
     * <p>접는 것이 여는 것이 되면 그 자체가 통로다. 주소 상한은 그대로 걸려야 한다.
     */
    @Test
    @DisplayName("유지_접혀_있어도_주소_상한은_그대로다")
    void 유지_접혀_있어도_주소_상한은_그대로다() {
        축을_채운다(5, "flood");
        // 채우는 데 쓴 주소 예산은 그 초의 것이다. 다음 초부터 재야 상한이 온전히 보인다.
        시계.앞으로(Duration.ofSeconds(1));
        MockServerWebExchange 마지막 = null;

        // 열 초 동안 같은 주소로 계속 때린다. 초가 바뀌어도 주소 예산은 초당 상한을 못 넘는다.
        for (int 초 = 0; 초 < 10; 초++) {
            통과.set(0);
            for (int i = 0; i < 300; i++) {
                마지막 = 태운다("m" + 초 + "-" + i, 공격);
            }
            assertThat(통과.get()).as("한 초에 주소 상한만큼만 지나간다").isEqualTo(200);
            시계.앞으로(Duration.ofSeconds(1));
        }

        assertThat(마지막.getResponse().getStatusCode())
                .as("상한을 넘기면 막는다").isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(접힘_게이지()).as("유지 구간 내내 접혀 있다").isEqualTo(1);
    }

    /**
     * <b>회복 — 공격이 멎으면 사람당 상한이 다시 선다.</b>
     *
     * <p>게이지가 0 으로 안 돌아오면 통제가 꺼진 채로 남은 것과 구분이 안 된다.
     */
    @Test
    @DisplayName("회복_공격이_멎으면_사람당_상한이_다시_선다")
    void 회복_공격이_멎으면_사람당_상한이_다시_선다() {
        축을_채운다(5, "flood");
        assertThat(접힘_게이지()).as("전제 — 접혀 있다").isEqualTo(1);

        시계.앞으로(접힘_여유);
        태운다("quiet", 정상);

        assertThat(접힘_게이지()).as("접힘이 풀렸다").isZero();

        통과.set(0);
        MockServerWebExchange 마지막 = null;
        for (int i = 0; i < 8; i++) {
            마지막 = 태운다("one-person", 정상);
        }

        assertThat(마지막.getResponse().getStatusCode())
                .as("사람당 상한이 다시 걸린다").isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(통과.get()).as("사람당 상한만큼만 지나간다").isEqualTo(5);
    }
}
