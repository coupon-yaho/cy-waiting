package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.kafkick.waiting.MutableClock;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.time.Instant;
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

    /** 초가 바뀌면 예산이 풀린다. 고정 시계로는 그 계약이 한 번도 안 돈다 (TS-4). */
    private final MutableClock 시계 = MutableClock.at(지금);

    /** 하네스는 신뢰 홉을 지나온 것처럼 군다. 안 그러면 전달 헤더가 무시된다. */
    private final AbuseLimitFilter filter = AbuseLimitFilter.of(
            시계, meters, () -> 0.5, TrustedProxies.of(List.of("127.0.0.1")));

    private MockServerWebExchange 태운다(String path, String member, String ip) {
        MockServerHttpRequest.BaseBuilder<?> 요청 = MockServerHttpRequest
                .method(HttpMethod.POST, path)
                // 신뢰 홉을 지나온 연결. 없으면 전달 헤더를 아예 안 본다.
                .remoteAddress(new InetSocketAddress("127.0.0.1", 12345))
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

    /** 초가 바뀌면 다시 통과해야 한다. 안 그러면 한 번 걸린 사람이 영영 막힌다. */
    @Test
    @DisplayName("초가_바뀌면_다시_통과한다")
    void 초가_바뀌면_다시_통과한다() {
        for (int i = 0; i < 6; i++) {
            태운다(ISSUE, "1", "10.0.0.1");
        }
        assertThat(태운다(ISSUE, "1", "10.0.0.1").getResponse().getStatusCode())
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);

        시계.앞으로(Duration.ofSeconds(1));

        assertThat(태운다(ISSUE, "1", "10.0.0.1").getResponse().getStatusCode()).isNull();
    }

    /**
     * 신뢰하지 않는 홉의 전달 헤더를 믿으면, 매 요청 다른 값을 넣어 상한을 넘고
     * 키를 무한히 만들어 상한에 닿게 한다 — 그때부터 정상 사용자가 막힌다.
     */
    @Test
    @DisplayName("신뢰하지_않는_홉의_헤더는_안_믿는다")
    void 신뢰하지_않는_홉의_헤더는_안_믿는다() {
        AbuseLimitFilter 안_믿는_것 = AbuseLimitFilter.of(
                시계, new SimpleMeterRegistry(), () -> 0.5, TrustedProxies.of(List.of()));

        // 주소를 매번 바꿔도 소켓 주소가 같으므로 한 키로 모인다.
        MockServerWebExchange 마지막 = null;
        for (int i = 0; i < 201; i++) {
            MockServerWebExchange exchange = MockServerWebExchange.from(
                    MockServerHttpRequest.method(HttpMethod.POST, ISSUE)
                            .remoteAddress(new InetSocketAddress("127.0.0.1", 12345))
                            .header("X-Member-Id", "m" + i)
                            .header("X-Forwarded-For", "9.9.9." + i));
            안_믿는_것.filter(exchange, e -> Mono.empty()).block();
            마지막 = exchange;
        }

        assertThat(마지막.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    /** 뒤에서 거부됐을 때 앞의 몫이 이미 깎이면, 통과가 하나도 없는데 예산이 빈다. */
    @Test
    @DisplayName("주소로_막힌_요청은_회원_예산을_안_깎는다")
    void 주소로_막힌_요청은_회원_예산을_안_깎는다() {
        // 주소 예산을 다 쓴다. 회원은 매번 다르므로 회원 예산은 안 닿는다.
        for (int i = 0; i < 200; i++) {
            태운다(ISSUE, "other" + i, "10.0.0.1");
        }
        // 이 사람은 주소 때문에 막힌다. 그때 회원 몫이 깎이면 안 된다.
        assertThat(태운다(ISSUE, "victim", "10.0.0.1").getResponse().getStatusCode())
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);

        // **같은 초 안에서 본다.** 시계를 넘기면 예산이 전부 풀려서 새는 것이 안 보인다.
        // 다른 주소에서 오면 자기 몫을 그대로 써야 한다.
        for (int i = 0; i < 5; i++) {
            assertThat(태운다(ISSUE, "victim", "10.0.0.2").getResponse().getStatusCode())
                    .as("%d 번째", i)
                    .isNull();
        }
    }

    /** 열어 주면 그 상태를 만드는 것이 곧 우회 통로가 된다. */
    @Test
    @DisplayName("주소를_못_읽으면_막는다")
    void 주소를_못_읽으면_막는다() {
        MockServerWebExchange 주소_없음 = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST, ISSUE).header("X-Member-Id", "1"));
        filter.filter(주소_없음, e -> {
            다음으로_감.incrementAndGet();
            return Mono.empty();
        }).block();

        assertThat(주소_없음.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(다음으로_감).hasValue(0);
    }

    /** 빈 값을 프록시 주소로 바꾸면, 빈 값을 보내는 것만으로 남을 막을 수 있다. */
    @Test
    @DisplayName("빈_전달_값은_막는다")
    void 빈_전달_값은_막는다() {
        MockServerWebExchange 빈_값 = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST, ISSUE)
                        .remoteAddress(new InetSocketAddress("127.0.0.1", 12345))
                        .header("X-Member-Id", "1")
                        .header("X-Forwarded-For", ""));
        filter.filter(빈_값, e -> {
            다음으로_감.incrementAndGet();
            return Mono.empty();
        }).block();

        assertThat(빈_값.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(다음으로_감).hasValue(0);
    }

    /**
     * 값을 바꿔가며 키를 무한히 만들면 리미터가 포화하고, 그때부터 정상 요청도
     * 막힌다. 신뢰 홉이 넘겨도 주소로 안 읽히면 버린다.
     */
    @Test
    @DisplayName("주소가_아닌_전달_값은_막는다")
    void 주소가_아닌_전달_값은_막는다() {
        MockServerWebExchange 이상한_값 = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST, ISSUE)
                        .remoteAddress(new InetSocketAddress("127.0.0.1", 12345))
                        .header("X-Member-Id", "1")
                        .header("X-Forwarded-For", "invalid-address"));
        filter.filter(이상한_값, e -> {
            다음으로_감.incrementAndGet();
            return Mono.empty();
        }).block();

        assertThat(이상한_값.getResponse().getStatusCode())
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(다음으로_감).hasValue(0);
    }

    /**
     * 식별자를 바꾸는 비용이 0 이다. 상한이 없으면 그것으로 메모리를 밀어낸다.
     */
    @Test
    @DisplayName("식별자를_만_개로_바꿔도_맵이_유계다")
    void 식별자를_만_개로_바꿔도_맵이_유계다() {
        SecondWindowLimiter 좁은_것 = SecondWindowLimiter.withMaxKeys(100);

        for (int i = 0; i < 10_000; i++) {
            좁은_것.tryAcquire("abuse:m:member" + i, 5, 지금.getEpochSecond());
        }

        assertThat(좁은_것.size()).isLessThanOrEqualTo(100);
    }

    /**
     * <b>식별자 축이 차면 그 축을 접는다</b> (CY-925). 식별자에 서명이 없어 값을 바꿔가며 자리를 채울 수
     * 있는데, 거절로 두면 채운 쪽이 아니라 새로 온 대기자가 막힌다.
     */
    @Test
    @DisplayName("식별자_자리가_차도_새_대기자가_들어온다")
    void 식별자_자리가_차도_새_대기자가_들어온다() {
        SecondWindowLimiter 좁은_것 = SecondWindowLimiter.withMaxKeys(4, 2);
        AbuseLimitFilter 좁은_필터 = AbuseLimitFilter.withLimiter(
                시계, meters, () -> 0.5, TrustedProxies.of(List.of("127.0.0.1")), 좁은_것);
        AtomicInteger 통과 = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            태운다(좁은_필터, "flood" + i, "10.0.0.1", 통과);
        }

        assertThat(좁은_것.saturated(SecondWindowLimiter.Axis.SECONDARY, 지금.getEpochSecond()))
                .as("전제 — 식별자 축이 찼다").isTrue();

        int 앞서_통과 = 통과.get();
        MockServerWebExchange 새_대기자 = 태운다(좁은_필터, "new-member", "10.0.0.2", 통과);

        assertThat(통과.get()).as("주소 축으로 판정해 통과한다").isEqualTo(앞서_통과 + 1);
        assertThat(새_대기자.getResponse().getStatusCode()).isNull();
    }

    /** 접어도 주소 축은 그대로 판정한다. 접는 것이 여는 것이 되면 그 자체가 통로다. */
    @Test
    @DisplayName("접어도_주소_상한은_그대로다")
    void 접어도_주소_상한은_그대로다() {
        SecondWindowLimiter 좁은_것 = SecondWindowLimiter.withMaxKeys(4, 2);
        AbuseLimitFilter 좁은_필터 = AbuseLimitFilter.withLimiter(
                시계, meters, () -> 0.5, TrustedProxies.of(List.of("127.0.0.1")), 좁은_것);
        AtomicInteger 통과 = new AtomicInteger();
        for (int i = 0; i < 3; i++) {
            태운다(좁은_필터, "flood" + i, "10.0.0.1", 통과);
        }

        MockServerWebExchange 마지막 = null;
        for (int i = 0; i < 300; i++) {
            마지막 = 태운다(좁은_필터, "m" + i, "10.0.0.1", 통과);
        }

        assertThat(마지막.getResponse().getStatusCode())
                .as("주소 상한을 넘기면 막힌다").isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        // **받는 쪽도 센다.** 막는 것만 보면 경계가 한 칸 밀려도 통과한다.
        assertThat(통과.get()).as("주소 상한만큼만 지나간다").isEqualTo(200);
    }

    private MockServerWebExchange 태운다(AbuseLimitFilter 필터, String member, String ip,
            AtomicInteger 통과) {
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

    /**
     * <b>같은 주소는 같은 키다</b> (CY-925). v6 는 한 주소를 여러 모양으로 적을 수 있어, 원문을 키로 쓰면
     * 표기만 바꿔 상한을 통째로 우회한다 — 접힌 구간에서는 이 축이 유일한 문이다.
     */
    @Test
    @DisplayName("같은_v6_주소는_표기가_달라도_한_몫이다")
    void 같은_v6_주소는_표기가_달라도_한_몫이다() {
        // **회원을 매번 다르게 한다.** 같은 회원이면 사람당 상한이 먼저 걸려 주소 몫이 안 깎인다.
        for (int i = 0; i < 200; i++) {
            태운다(ISSUE, String.valueOf(1_000 + i), i % 2 == 0 ? "::1" : "0:0:0:0:0:0:0:1");
        }

        MockServerWebExchange 넘긴_것 = 태운다(ISSUE, "9999", "::0001");

        assertThat(넘긴_것.getResponse().getStatusCode())
                .as("표기를 바꿔도 같은 주소의 몫을 쓴다").isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    /**
     * <b>접혀도 이미 아는 사람은 상한을 받는다</b> (CY-925). 자리를 못 얻은 사람만 접히고, 추적 중인
     * 회원은 그대로 제 상한에 걸려야 한다 — 축을 통째로 건너뛰면 한 사람이 주소 상한까지 쏠 수 있다.
     */
    @Test
    @DisplayName("접혀도_아는_회원은_제_상한을_받는다")
    void 접혀도_아는_회원은_제_상한을_받는다() {
        SecondWindowLimiter 좁은_것 = SecondWindowLimiter.withMaxKeys(4, 2);
        AbuseLimitFilter 좁은_필터 = AbuseLimitFilter.withLimiter(
                시계, meters, () -> 0.5, TrustedProxies.of(List.of("127.0.0.1")), 좁은_것);
        AtomicInteger 통과 = new AtomicInteger();
        // 먼저 자리를 잡은 회원. 그 뒤 다른 식별자로 축을 채운다.
        태운다(좁은_필터, "known", "10.0.0.1", 통과);
        for (int i = 0; i < 4; i++) {
            태운다(좁은_필터, "flood" + i, "10.0.0.1", 통과);
        }
        assertThat(좁은_것.saturated(SecondWindowLimiter.Axis.SECONDARY, 지금.getEpochSecond()))
                .as("전제 — 식별자 축이 찼다").isTrue();

        MockServerWebExchange 마지막 = null;
        for (int i = 0; i < 10; i++) {
            마지막 = 태운다(좁은_필터, "known", "10.0.0.1", 통과);
        }

        assertThat(마지막.getResponse().getStatusCode())
                .as("아는 회원은 사람당 상한에 걸린다").isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(meters.get("waiting.abuse").tag("key", "member").counter().count())
                .as("사람당 상한으로 막았다고 센다").isGreaterThan(0);
    }

    @Test
    @DisplayName("접고_통과한_것을_센다")
    void 접고_통과한_것을_센다() {
        SecondWindowLimiter 좁은_것 = SecondWindowLimiter.withMaxKeys(4, 2);
        AbuseLimitFilter 좁은_필터 = AbuseLimitFilter.withLimiter(
                시계, meters, () -> 0.5, TrustedProxies.of(List.of("127.0.0.1")), 좁은_것);
        AtomicInteger 통과 = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            태운다(좁은_필터, "flood" + i, "10.0.0.1", 통과);
        }

        태운다(좁은_필터, "new-member", "10.0.0.2", 통과);

        assertThat(meters.get("waiting.abuse.folded").counter().count())
                .as("접고 통과한 수").isGreaterThan(0);
        assertThat(meters.find("waiting.abuse").tag("key", "folded").counter())
                .as("거절 지표에 섞지 않는다").isNull();
    }

    /**
     * <b>주소 축이 차면 접을 곳이 없다</b>. 같은 결과값으로 오지만 처분이 다르다 — 접힌 것과 한데 묶으면
     * 운영자가 훨씬 심각한 쪽을 못 본다.
     */
    @Test
    @DisplayName("주소_자리가_차면_접지_않고_막는다")
    void 주소_자리가_차면_접지_않고_막는다() {
        SecondWindowLimiter 좁은_것 = SecondWindowLimiter.withMaxKeys(2, 400);
        AbuseLimitFilter 좁은_필터 = AbuseLimitFilter.withLimiter(
                시계, meters, () -> 0.5, TrustedProxies.of(List.of("127.0.0.1")), 좁은_것);
        AtomicInteger 통과 = new AtomicInteger();
        for (int i = 0; i < 2; i++) {
            태운다(좁은_필터, "m" + i, "10.0.0." + (i + 1), 통과);
        }

        MockServerWebExchange 막힌_것 = 태운다(좁은_필터, "m9", "10.0.0.9", 통과);

        assertThat(막힌_것.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(meters.get("waiting.abuse").tag("key", "keyspace").counter().count())
                .as("접힘과 다른 이름으로 센다").isGreaterThan(0);
    }

    /**
     * <b>두 축을 다 밟아도 유계다</b>. 축별 상한만 보는 시험은 필터가 실제로 쓰는 배치를 안 밟아,
     * 합이 늘어나도 초록으로 남는다.
     */
    @Test
    @DisplayName("두_축을_함께_채워도_맵이_유계다")
    void 두_축을_함께_채워도_맵이_유계다() {
        SecondWindowLimiter 좁은_것 = SecondWindowLimiter.withMaxKeys(50, 100);
        AbuseLimitFilter 좁은_필터 = AbuseLimitFilter.withLimiter(
                시계, meters, () -> 0.5, TrustedProxies.of(List.of("127.0.0.1")), 좁은_것);
        AtomicInteger 통과 = new AtomicInteger();
        for (int i = 0; i < 3_000; i++) {
            태운다(좁은_필터, "m" + i, "10." + (i % 251) + "." + (i % 253) + ".1", 통과);
        }

        assertThat(좁은_것.size()).isLessThanOrEqualTo(좁은_것.totalMaxKeys());
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
