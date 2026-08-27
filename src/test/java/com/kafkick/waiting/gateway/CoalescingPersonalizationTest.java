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
 * <p>뒷단이 <b>공유해도 된다고 말한 응답만</b> 나눠 줍니다. 말이 없으면 각자 자기
 * 것을 받으므로, 필드 하나가 붙는 것만으로는 남의 응답이 안 나갑니다.
 */
// 막지 못하는 것은 계약을 어긴 응답이다 — 공유해도 된다고 해 놓고 회원마다 다르게
// 답하는 뒷단. 그 사실도 여기서 값으로 적어 둔다.

class CoalescingPersonalizationTest {

    private static final String PATH = "/api/v1/coupons";

    private final MutableClock 시계 = MutableClock.at(Instant.parse("2026-08-27T00:00:00Z"));

    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    private final QueryCoalescingFilter filter = 새_필터();

    /**
     * 상태를 안 물려받는 필터.
     *
     * <p><b>안 붙기는 경로에 남는다.</b> 한 판에서 선언 없는 응답을 한 번 보면 그
     * 뒤로는 무엇을 보내든 안 붙으므로, 값마다 새 필터로 재지 않으면 첫 값 하나만
     * 재고 나머지는 항진명제가 된다.
     */
    private QueryCoalescingFilter 새_필터() {
        return QueryCoalescingFilter.of(
                new CoalescingProperties(true, 1024, 1 << 20, 100,
                        List.of(new CoalescingProperties.Route(PATH, Duration.ofMillis(200)))),
                시계, meters);
    }

    /** 왜 안 모았는지. <b>사유가 갈려야 계약이 안 선 것을 다른 거절과 구분한다.</b> */
    private double 센다(String 결과, String 사유) {
        var counter = meters.find("waiting.coalescing")
                .tags("outcome", 결과, "cause", 사유).counter();
        return counter == null ? 0 : counter.count();
    }

    private double 건너뛴(String 사유) {
        return 센다("skipped", 사유);
    }

    private double 거절한(String 사유) {
        return 센다("refused", 사유);
    }

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
        // 공유해도 된다고 말해 놓고 회원별로 다르게 답하는 뒷단.
        사람들.forEach(e -> filter.filter(e, ex -> 답한다(ex,
                "이력:" + ex.getRequest().getHeaders().getFirst("X-Member-Id"))).block());
        사람들.forEach(e -> 받은_것.add(e.getResponse().getBodyAsString().block()));

        // **이것이 지금의 사실입니다.** 다섯이 한 가지를 받습니다 — 넷은 남의
        // 것입니다. 안전장치가 아니라, 안전이 어디에 달려 있는지의 기록입니다.
        // **누구 것을 받는지까지 본다.** "같은 것을 받는다" 만 보면 이 사고의
        // 모양이 안 드러난다 — 먼저 온 사람의 것이 나머지에게 간다.
        assertThat(받은_것)
                .as("계약을 어기면 이렇게 된다")
                .containsExactly("이력:사람0");
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
        List.of("max-age=300, no-public", "community=\"public-catalog\"",
                // 따옴표 안의 콤마로 자르면 여기서 public 조각이 나온다.
                "community=\"catalog, public, personalized\"").forEach(지시어 -> {
            QueryCoalescingFilter 새것 = 새_필터();
            AtomicInteger 뒷단 = new AtomicInteger();
            // **첫 두 건으로 판정한다.** 세 번째부터는 안 붙기가 이미 배워져 있어
            // 무엇을 보내든 뒷단이 불린다 — 그 상태로 세면 항진명제다.
            IntStream.range(0, 2).forEach(i ->
                    새것.filter(조회("사람" + i), ex -> {
                        뒷단.incrementAndGet();
                        ex.getResponse().getHeaders().setCacheControl(지시어);
                        return 그냥_답한다(ex, "목록");
                    }).block());

            assertThat(뒷단).as("%s 는 허락이 아니다", 지시어).hasValue(2);
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
     * <b>선언을 안 하는 뒷단에는 붙지 않습니다.</b>
     *
     * <p>붙으면 리더의 왕복이 끝난 뒤 각자 다시 부릅니다 — 뒷단 부하는 필터가 없을
     * 때와 같고 지연만 두 배가 됩니다. 없느니만 못한 상태입니다.
     */
    @Test
    @DisplayName("선언을_안_하면_다음부터는_안_붙는다")
    void 선언을_안_하면_다음부터는_안_붙는다() {
        // 첫 판이 가르친다. 뒷단 호출 수로는 못 가른다 — 붙든 안 붙든 요청
        // 하나면 뒷단은 한 번 불린다. 왜 건너뛰었는지를 사유로 본다.
        filter.filter(조회("먼저"), ex -> 그냥_답한다(ex, "목록")).block();
        assertThat(건너뛴("no-shared-marker")).as("가르치는 판은 아직 붙는다").isZero();

        filter.filter(조회("나중"), ex -> 그냥_답한다(ex, "목록")).block();

        assertThat(건너뛴("no-shared-marker")).as("배운 뒤로는 안 붙는다").isEqualTo(1);
    }

    /**
     * <b>실패 응답으로는 안 배웁니다.</b>
     *
     * <p>장애 구간의 5xx 에 헤더가 없다고 계약이 깨진 것으로 읽으면, 뒷단이 살아난
     * 뒤에도 안 모읍니다 — 장애 하나가 성능 저하로 굳습니다.
     */
    @Test
    @DisplayName("실패_응답으로는_계약을_안_배운다")
    void 실패_응답으로는_계약을_안_배운다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        filter.filter(조회("장애"), ex -> {
            ex.getResponse().setStatusCode(org.springframework.http.HttpStatus.BAD_GATEWAY);
            return ex.getResponse().writeWith(Mono.just(
                    ex.getResponse().bufferFactory().wrap("에러".getBytes())));
        }).block();
        시계.앞으로(Duration.ofSeconds(1));

        IntStream.range(0, 3).forEach(i ->
                filter.filter(조회("나중" + i), ex -> {
                    뒷단.incrementAndGet();
                    return 답한다(ex, "목록");
                }).block());

        assertThat(건너뛴("no-shared-marker")).as("5xx 로는 계약을 안 배운다").isZero();
        assertThat(뒷단).as("그대로 모인다").hasValue(1);
    }

    /** 쿠키·선언 없음·명시 거절이 <b>다른 사유</b>로 남는다. 한 라벨이면 원인을 못 가린다. */
    @Test
    @DisplayName("거절_사유를_갈라_남긴다")
    void 거절_사유를_갈라_남긴다() {
        List.of(조회("A"), 조회("B")).forEach(e -> filter.filter(e, ex -> {
            ex.getResponse().getHeaders().add("Set-Cookie", "SESSION=x");
            return 답한다(ex, "목록");
        }).block());
        시계.앞으로(Duration.ofSeconds(1));
        List.of(조회("C"), 조회("D")).forEach(e -> filter.filter(e, ex -> {
            ex.getResponse().getHeaders().setCacheControl("public, no-store");
            return 그냥_답한다(ex, "목록");
        }).block());

        assertThat(거절한("set-cookie")).as("쿠키").isPositive();
        assertThat(거절한("not-shareable")).as("명시 거절").isPositive();
    }

    /** 선언이 돌아오면 곧바로 다시 모읍니다. 안 그러면 한 번의 누락이 영구가 됩니다. */
    @Test
    @DisplayName("선언이_돌아오면_다시_모은다")
    void 선언이_돌아오면_다시_모은다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        filter.filter(조회("먼저"), ex -> 그냥_답한다(ex, "목록")).block();
        시계.앞으로(Duration.ofSeconds(1));
        // **한 건으로는 안 되돌린다.** 롤링 배포 구간에서 켜졌다 꺼졌다 하면
        // 로그가 요청마다 나가고 병합 배수도 무의미해진다.
        filter.filter(조회("한 건"), ex -> 답한다(ex, "목록")).block();
        assertThat(건너뛴("no-shared-marker")).as("한 건으로는 안 돌아온다").isEqualTo(1);
        IntStream.range(0, 30).forEach(i -> {
            시계.앞으로(Duration.ofSeconds(1));
            filter.filter(조회("가르침" + i), ex -> 답한다(ex, "목록")).block();
        });
        시계.앞으로(Duration.ofSeconds(1));

        IntStream.range(0, 3).forEach(i ->
                filter.filter(조회("나중" + i), ex -> {
                    뒷단.incrementAndGet();
                    return 답한다(ex, "목록");
                }).block());

        assertThat(뒷단).as("다시 모인다").hasValue(1);
    }

    /**
     * <b>같은 것을 답하면 모여야 합니다.</b> 위 시험이 "늘 안 모인다" 로도 통과하면
     * 안전장치가 아니라 기능 정지 확인이 됩니다.
     */
    @Test
    @DisplayName("뒷단이_같게_답하면_모인다")
    void 뒷단이_같게_답하면_모인다() {
        AtomicInteger 뒷단 = new AtomicInteger();
        List<MockServerWebExchange> 사람들 = IntStream.range(0, 5)
                .mapToObj(i -> 조회("사람" + i))
                .toList();

        사람들.forEach(e -> filter.filter(e, ex -> {
            뒷단.incrementAndGet();
            return 답한다(ex, "모두 같은 목록");
        }).block());

        assertThat(뒷단).as("뒷단 호출").hasValue(1);
        // **본문까지 본다.** 호출 수만 세면 "한 번 부르고 빈 것을 준다" 가 통과한다.
        assertThat(사람들).allSatisfy(e ->
                assertThat(e.getResponse().getBodyAsString().block())
                        .isEqualTo("모두 같은 목록"));
    }

    /**
     * <b>뒷단이 쓴 캐시 지시어가 클라이언트까지 그대로 나갑니다.</b>
     *
     * <p>같은 헤더가 두 청중에게 쓰입니다 — 게이트웨이에게는 "모아도 된다", 브라우저
     * 에게는 "담아도 된다". 뒷단이 {@code max-age} 를 얹으면 브라우저가 쿠폰 목록을
     * 그만큼 담고, <b>매진을 게이트웨이가 종결한다</b>(R3)가 클라이언트에서 낡습니다.
     */
    // 지금은 그대로 흘리는 것이 사실이다. 고쳐야 할지는 발급 계층과 정할 일이라
    // 여기서는 상태를 못 박아 두기만 한다 (CY-684).
    @Test
    @DisplayName("뒷단의_캐시_지시어가_클라이언트까지_간다")
    void 뒷단의_캐시_지시어가_클라이언트까지_간다() {
        MockServerWebExchange e = 조회("사람1");

        filter.filter(e, ex -> {
            ex.getResponse().getHeaders().setCacheControl("public, max-age=60");
            return 그냥_답한다(ex, "목록");
        }).block();

        assertThat(e.getResponse().getHeaders().getCacheControl())
                .as("게이트웨이가 다시 쓰지 않는다")
                .isEqualTo("public, max-age=60");
    }

    /**
     * <b>{@code max-age} 만으로는 허락이 아닙니다.</b>
     *
     * <p>표준으로는 공유 캐시가 담아도 되는 값이지만, 여기서 필요한 것은 "이 응답에
     * 회원별 값이 없다" 는 선언입니다. 발급 계층이 표준대로 보내고 조용히 안 모이는
     * 경로가 생기므로 값으로 못 박습니다.
     */
    @Test
    @DisplayName("max_age만으로는_허락이_아니다")
    void max_age만으로는_허락이_아니다() {
        AtomicInteger 뒷단 = new AtomicInteger();

        IntStream.range(0, 3).forEach(i ->
                filter.filter(조회("사람" + i), ex -> {
                    뒷단.incrementAndGet();
                    ex.getResponse().getHeaders().setCacheControl("max-age=60, s-maxage=30");
                    return 그냥_답한다(ex, "목록");
                }).block());

        assertThat(뒷단).as("담아도 된다와 나눠도 된다는 다른 말이다").hasValue(3);
    }
}
