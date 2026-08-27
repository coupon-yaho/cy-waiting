package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.MutableClock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * 조회를 모아서 <b>뒷단 한 번</b>으로 보냅니다.
 *
 * <p>발급은 판정이 막아 주는데 조회는 그대로 통과합니다. 그리고 이건 R2 까지
 * 갉아먹습니다 — 인스턴스가 조회로 포화되면 낮은 가용량을 보고하고, 발급 유입까지
 * 같이 조여집니다.
 */
class QueryCoalescingFilterTest {

    private static final String PATH = "/api/v1/coupons";

    private static final Instant 지금 = Instant.parse("2026-08-27T00:00:00Z");

    private final MutableClock 시계 = MutableClock.at(지금);

    private final MeterRegistry meters = new SimpleMeterRegistry();

    private final CoalescingProperties 설정 = new CoalescingProperties(
            true, 1024, 100, List.of(new CoalescingProperties.Route(PATH,
            Duration.ofMillis(200))));

    private final QueryCoalescingFilter filter =
            QueryCoalescingFilter.of(설정, 시계, meters);

    /**
     * 신원 필터를 지난 평범한 조회.
     *
     * <p><b>회원 헤더가 늘 있습니다.</b> 없는 요청은 프로덕션에 도달할 수 없고
     * (신원 필터가 막습니다), 그런 픽스처는 "회원 헤더가 늘 있다" 때문에 난
     * 결함에 구조적으로 눈이 없습니다 (TS-3).
     */
    private MockServerWebExchange 조회(String uri) {
        return MockServerWebExchange.from(MockServerHttpRequest.method(HttpMethod.GET, uri)
                .header("X-Member-Id", "812934")
                .header("X-Member-Grade", "GOLD"));
    }

    /** 브라우저가 보내는 조회. 분석 쿠키 하나만 있어도 매 요청에 실린다. */
    private MockServerWebExchange 브라우저_조회(String uri) {
        return MockServerWebExchange.from(MockServerHttpRequest.method(HttpMethod.GET, uri)
                .header("X-Member-Id", "812934")
                .header("X-Member-Grade", "GOLD")
                .header("Cookie", "_ga=GA1.1.1; SESSION=abc"));
    }

    /** 뒷단이 답하는 척한다. 몇 번 불렸는지가 이 시험의 값이다. */
    private static Mono<Void> 답한다(org.springframework.web.server.ServerWebExchange e,
            String body) {
        e.getResponse().setStatusCode(HttpStatus.OK);
        return e.getResponse().writeWith(Mono.just(
                e.getResponse().bufferFactory().wrap(body.getBytes(StandardCharsets.UTF_8))));
    }

    private static String 본문(MockServerWebExchange e) {
        return e.getResponse().getBodyAsString().block();
    }

    /**
     * <b>이 기능의 전부입니다.</b> 동시에 온 같은 조회가 뒷단에 그대로 나가면
     * 모은 것이 아닙니다.
     */
    @Test
    @DisplayName("동시에_온_같은_조회는_뒷단을_한_번만_부른다")
    void 동시에_온_같은_조회는_뒷단을_한_번만_부른다() {
        AtomicInteger 뒷단 = new AtomicInteger();
        Sinks.Empty<Void> 아직 = Sinks.empty();

        List<MockServerWebExchange> 요청들 = IntStream.range(0, 1_000)
                .mapToObj(i -> 조회(PATH)).toList();
        요청들.forEach(e -> filter.filter(e, ex -> {
            뒷단.incrementAndGet();
            return 답한다(ex, "쿠폰 목록").then(아직.asMono());
        }).subscribe());
        아직.tryEmitEmpty();

        assertThat(뒷단).as("뒷단 호출").hasValue(1);
        assertThat(요청들).allSatisfy(e ->
                assertThat(본문(e)).isEqualTo("쿠폰 목록"));
    }

    /**
     * <b>코얼레싱만으로는 부족합니다.</b> 1ms 어긋난 요청은 각각 나가므로, 짧은
     * 수명을 얹어야 연속 도착까지 흡수됩니다.
     */
    @Test
    @DisplayName("수명_안의_연속_조회는_뒷단에_안_간다")
    void 수명_안의_연속_조회는_뒷단에_안_간다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        for (int i = 0; i < 5; i++) {
            MockServerWebExchange e = 조회(PATH);
            filter.filter(e, ex -> {
                뒷단.incrementAndGet();
                return 답한다(ex, "쿠폰 목록");
            }).block();
            assertThat(본문(e)).isEqualTo("쿠폰 목록");
            시계.앞으로(Duration.ofMillis(30));
        }

        assertThat(뒷단).hasValue(1);
    }

    /** 수명이 지나면 다시 물어야 한다. 안 그러면 매진된 쿠폰을 남아 있다고 답한다. */
    @Test
    @DisplayName("수명이_지나면_다시_묻는다")
    void 수명이_지나면_다시_묻는다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        filter.filter(조회(PATH), ex -> {
            뒷단.incrementAndGet();
            return 답한다(ex, "첫 응답");
        }).block();
        시계.앞으로(Duration.ofMillis(200));
        MockServerWebExchange 둘째 = 조회(PATH);
        filter.filter(둘째, ex -> {
            뒷단.incrementAndGet();
            return 답한다(ex, "새 응답");
        }).block();

        assertThat(뒷단).hasValue(2);
        assertThat(본문(둘째)).isEqualTo("새 응답");
    }

    /** 쿼리가 다르면 다른 응답이다. 하나로 묶으면 남의 응답을 받는다. */
    @Test
    @DisplayName("쿼리가_다르면_따로_묻는다")
    void 쿼리가_다르면_따로_묻는다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        MockServerWebExchange 하나 = 조회(PATH + "?page=1");
        filter.filter(하나, ex -> 답한다(ex, "페이지" + 뒷단.incrementAndGet())).block();
        MockServerWebExchange 둘 = 조회(PATH + "?page=2");
        filter.filter(둘, ex -> 답한다(ex, "페이지" + 뒷단.incrementAndGet())).block();

        // **뒤바뀜을 잡는다.** 다르기만 보면 하나가 둘의 응답을 받아도 통과한다 —
        // 키가 뒤섞이는 것이 정확히 이 기능의 사고 모양이다.
        assertThat(본문(하나)).isEqualTo("페이지1");
        assertThat(본문(둘)).isEqualTo("페이지2");
        assertThat(뒷단).hasValue(2);
    }

    /**
     * <b>목록에 없는 경로는 그대로 흘립니다.</b> 기본이 켜짐이면 개인화된 응답이
     * 붙는 순간 남의 응답을 받습니다.
     */
    @Test
    @DisplayName("목록에_없는_경로는_안_모은다")
    void 목록에_없는_경로는_안_모은다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        for (int i = 0; i < 3; i++) {
            filter.filter(조회("/api/v1/coupons/c1"),
                    ex -> 답한다(ex, "상세" + 뒷단.incrementAndGet())).block();
        }

        assertThat(뒷단).hasValue(3);
    }

    /** 발급은 모으면 안 된다. 같은 응답을 여럿이 받으면 그게 곧 초과 발급이다. */
    @Test
    @DisplayName("조회가_아니면_안_모은다")
    void 조회가_아니면_안_모은다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        for (int i = 0; i < 3; i++) {
            MockServerWebExchange e = MockServerWebExchange.from(
                    MockServerHttpRequest.method(HttpMethod.POST, PATH));
            filter.filter(e, ex -> 답한다(ex, "발급" + 뒷단.incrementAndGet())).block();
        }

        assertThat(뒷단).hasValue(3);
    }

    /** 자격 증명이 실려 오면 안 모은다. 하나로 모으면 남의 응답을 받는다. */
    @Test
    @DisplayName("자격_증명이_실려_오면_안_모은다")
    void 자격_증명이_실려_오면_안_모은다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        for (int i = 0; i < 2; i++) {
            MockServerWebExchange e = MockServerWebExchange.from(
                    MockServerHttpRequest.method(HttpMethod.GET, PATH)
                            .header("X-Member-Id", "812934")
                            .header("Authorization", "Bearer 토큰"));
            filter.filter(e, ex -> 답한다(ex, "개인" + 뒷단.incrementAndGet())).block();
        }

        assertThat(뒷단).as("토큰을 든 요청은 각자 간다").hasValue(2);
    }

    /**
     * <b>회원 헤더는 있다는 것만으로 못 거릅니다.</b> 신원 필터가 모든 조회에
     * 요구하는 값이라, 그것으로 걸러 내면 이 기능이 한 번도 안 돕니다 — 실제로
     * 그렇게 만들어 놓고 부하를 돌려서야 알았습니다.
     */
    @Test
    @DisplayName("회원_헤더가_있어도_모은다")
    void 회원_헤더가_있어도_모은다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        for (int i = 0; i < 3; i++) {
            MockServerWebExchange e = MockServerWebExchange.from(
                    MockServerHttpRequest.method(HttpMethod.GET, PATH)
                            .header("X-Member-Id", "81293" + i)
                            .header("X-Member-Grade", "GOLD"));
            filter.filter(e, ex -> 답한다(ex, "목록" + 뒷단.incrementAndGet())).block();
        }

        assertThat(뒷단).as("뒷단 호출").hasValue(1);
    }

    /**
     * <b>이 기능의 안전장치입니다.</b> 지금 조회 응답에 개인화는 없습니다. 그런데
     * "내가 발급받았는지" 필드가 하나 붙는 순간 남의 응답을 받게 되고, 사람
     * 리뷰로는 그 한 줄을 못 막습니다. 뒷단이 그 사실을 말하는 자리가 Vary 입니다.
     */
    @Test
    @DisplayName("Vary_가_지목한_헤더가_다르면_따로_묻는다")
    void Vary_가_지목한_헤더가_다르면_따로_묻는다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        for (int i = 0; i < 3; i++) {
            MockServerWebExchange e = MockServerWebExchange.from(
                    MockServerHttpRequest.method(HttpMethod.GET, PATH)
                            .header("X-Member-Id", "사람" + i));
            filter.filter(e, ex -> {
                ex.getResponse().getHeaders().set("Vary", "X-Member-Id");
                return 답한다(ex, "개인" + 뒷단.incrementAndGet());
            }).block();
        }

        assertThat(뒷단).as("갈리는 값이 다르면 각자 물어야 한다").hasValue(3);
    }

    /**
     * <b>배우기 전의 첫 무리가 가장 위험합니다.</b> 그때는 갈림 헤더를 몰라 회원이
     * 서로 다른 요청들이 한 키에 붙어 있습니다. 응답이 "이 헤더로 갈린다" 고
     * 말하는 순간, 그 무리는 남의 응답을 받게 됩니다.
     *
     * <p>순차로 재면 이 자리가 안 보입니다 — 첫 요청이 이미 배운 뒤라 키가
     * 갈립니다. <b>동시일 때만 납니다. 그리고 이 기능이 쓰이는 순간이 그때입니다.</b>
     */
    @Test
    @DisplayName("배우기_전에_모인_무리는_남의_응답을_안_받는다")
    void 배우기_전에_모인_무리는_남의_응답을_안_받는다() {
        Sinks.Empty<Void> 아직 = Sinks.empty();
        List<MockServerWebExchange> 사람들 = IntStream.range(0, 5)
                .mapToObj(i -> MockServerWebExchange.from(
                        MockServerHttpRequest.method(HttpMethod.GET, PATH)
                                .header("X-Member-Id", "사람" + i)))
                .toList();

        사람들.forEach(e -> filter.filter(e, ex -> {
            ex.getResponse().getHeaders().set("Vary", "X-Member-Id");
            String 회원 = ex.getRequest().getHeaders().getFirst("X-Member-Id");
            return 답한다(ex, "발급이력:" + 회원).then(아직.asMono());
        }).subscribe());
        아직.tryEmitEmpty();

        // 각자 자기 것을 받아야 한다. 하나로 모이면 넷이 남의 발급 이력을 본다.
        assertThat(사람들).allSatisfy(e -> {
            String 회원 = e.getRequest().getHeaders().getFirst("X-Member-Id");
            assertThat(본문(e)).as("%s 가 받은 것", 회원).isEqualTo("발급이력:" + 회원);
        });
    }

    /**
     * <b>같은 값이면 모읍니다.</b> CORS 필터가 모든 응답에 {@code Vary: Origin} 을
     * 다는데, 그것까지 거부하면 이 기능이 한 번도 안 돕니다 — 실제로 그랬습니다.
     */
    @Test
    @DisplayName("Vary_가_있어도_값이_같으면_모은다")
    void Vary_가_있어도_값이_같으면_모은다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        for (int i = 0; i < 3; i++) {
            MockServerWebExchange e = 조회(PATH);
            filter.filter(e, ex -> {
                ex.getResponse().getHeaders().set("Vary", "Origin");
                return 답한다(ex, "목록" + 뒷단.incrementAndGet());
            }).block();
        }

        // **한 번이다.** 첫 응답을 담을 때 방금 배운 것으로 키를 다시 만들기
        // 때문에, 두 번째 요청이 같은 키로 찾아 담아 둔 것을 받는다. 담는 것과
        // 나눠 주는 것을 같은 판단으로 묶었을 때는 여기가 2 였다 — 경로마다
        // 뒷단을 한 번씩 더 부르고, 그 한 번이 오픈 순간의 버스트다.
        assertThat(뒷단).as("뒷단 호출").hasValue(1);
    }

    /** 전부 갈린다는 뜻은 키로 못 만든다. 그때는 나눠 주지도 담지도 않는다. */
    @Test
    @DisplayName("Vary_별표면_안_모은다")
    void Vary_별표면_안_모은다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        for (int i = 0; i < 3; i++) {
            MockServerWebExchange e = 조회(PATH);
            filter.filter(e, ex -> {
                ex.getResponse().getHeaders().set("Vary", "*");
                return 답한다(ex, "전부 갈림" + 뒷단.incrementAndGet());
            }).block();
        }

        assertThat(뒷단).hasValue(3);
    }

    /**
     * <b>장애 응답을 담으면 안 됩니다.</b> 담으면 그 수명 동안 장애가 고정되고,
     * 뒷단이 멀쩡해져도 계속 실패를 돌려줍니다.
     */
    @Test
    @DisplayName("장애_응답은_안_담는다")
    void 장애_응답은_안_담는다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        MockServerWebExchange 첫째 = 조회(PATH);
        filter.filter(첫째, ex -> {
            뒷단.incrementAndGet();
            ex.getResponse().setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return ex.getResponse().setComplete();
        }).block();
        MockServerWebExchange 둘째 = 조회(PATH);
        filter.filter(둘째, ex -> 답한다(ex, "회복됨" + 뒷단.incrementAndGet())).block();

        assertThat(뒷단).hasValue(2);
        assertThat(본문(둘째)).isEqualTo("회복됨2");
    }

    /** 뒷단이 담지 말라면 안 담는다. 우리 판단으로 덮어쓰지 않는다. */
    @Test
    @DisplayName("뒷단이_no_store_면_안_담는다")
    void 뒷단이_no_store_면_안_담는다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        for (int i = 0; i < 2; i++) {
            MockServerWebExchange e = 조회(PATH);
            filter.filter(e, ex -> {
                ex.getResponse().getHeaders().setCacheControl("no-store");
                return 답한다(ex, "안 담김" + 뒷단.incrementAndGet());
            }).block();
        }

        assertThat(뒷단).hasValue(2);
    }

    /**
     * <b>보호 장치가 메모리 사고의 원인이 되면 안 됩니다.</b> 상한을 넘긴 응답은
     * 담지도 나눠 주지도 않습니다.
     */
    @Test
    @DisplayName("상한을_넘는_응답은_안_담는다")
    void 상한을_넘는_응답은_안_담는다() {
        AtomicInteger 뒷단 = new AtomicInteger();
        String 큰_것 = "가".repeat(2_000);

        MockServerWebExchange 첫째 = 조회(PATH);
        filter.filter(첫째, ex -> {
            뒷단.incrementAndGet();
            return 답한다(ex, 큰_것);
        }).block();
        MockServerWebExchange 둘째 = 조회(PATH);
        filter.filter(둘째, ex -> {
            뒷단.incrementAndGet();
            return 답한다(ex, 큰_것);
        }).block();

        // 담지 않았으므로 두 번 다 뒷단까지 간다. 그리고 각자 온전한 응답을 받는다.
        assertThat(뒷단).hasValue(2);
        assertThat(본문(첫째)).isEqualTo(큰_것);
        assertThat(본문(둘째)).isEqualTo(큰_것);
    }

    /** 통째로 끄는 스위치. 장애 중에 되돌릴 수단이다. */
    @Test
    @DisplayName("꺼_두면_아무것도_안_모은다")
    void 꺼_두면_아무것도_안_모은다() {
        QueryCoalescingFilter 꺼진_것 = QueryCoalescingFilter.of(
                new CoalescingProperties(false, 1024, 100, List.of()), 시계, meters);
        AtomicInteger 뒷단 = new AtomicInteger();

        for (int i = 0; i < 3; i++) {
            꺼진_것.filter(조회(PATH), ex -> 답한다(ex, "목록" + 뒷단.incrementAndGet())).block();
        }

        assertThat(뒷단).hasValue(3);
    }

    /**
     * <b>브라우저는 쿠키를 보냅니다.</b> 분석 쿠키 하나만 있어도 매 요청에 실리므로,
     * 있다고 거르면 이 기능이 브라우저에서 한 번도 안 돕니다 — 회원 헤더로 거르다
     * 통째로 죽였던 것과 같은 실수입니다.
     */
    @Test
    @DisplayName("쿠키가_실려_와도_모은다")
    void 쿠키가_실려_와도_모은다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        for (int i = 0; i < 3; i++) {
            MockServerWebExchange e = 브라우저_조회(PATH);
            filter.filter(e, ex -> 답한다(ex, "목록" + 뒷단.incrementAndGet())).block();
        }

        assertThat(뒷단).as("뒷단 호출").hasValue(1);
    }
}
