package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 회원 식별자의 <b>형식만</b> 본다.
 *
 * <p>로그인이 없어 이 헤더는 위조가 자유롭다. 서명 없는 값을 검사하는 것은
 * 방어가 아니라 장식이다 — 여기서 막는 것은 깨진 값이 뒷단까지 흘러가는 것뿐이고,
 * 남의 자리를 건드리는 것은 게이트웨이가 서명한 토큰이 막는다.
 */
class MemberIdentityFilterTest {

    private static final String ID = "X-Member-Id";
    private static final String GRADE = "X-Member-Grade";

    private final MemberIdentityFilter filter = MemberIdentityFilter.create();

    private final AtomicReference<ServerWebExchange> 뒷단이_본_것 = new AtomicReference<>();

    private MockServerWebExchange 통과시킨다(MockServerHttpRequest.BaseBuilder<?> 요청) {
        MockServerWebExchange exchange = MockServerWebExchange.from(요청);
        filter.filter(exchange, e -> {
            뒷단이_본_것.set(e);
            return Mono.empty();
        }).block();
        return exchange;
    }

    private MockServerHttpRequest.BaseBuilder<?> 발급_요청() {
        return MockServerHttpRequest.method(HttpMethod.POST, "/api/v1/coupons/c1/issue");
    }

    @Test
    @DisplayName("제대로_된_헤더는_뒷단까지_그대로_간다")
    void 제대로_된_헤더는_뒷단까지_그대로_간다() {
        // **지우지도 넣지도 않는다.** 인증 계층이 없어 넣을 검증된 신원이 없고,
        // 지우면 회원 식별자가 통째로 사라져 뒷단이 누구인지 모른다.
        통과시킨다(발급_요청().header(ID, "12345").header(GRADE, "GOLD"));

        HttpHeaders 받은_것 = 뒷단이_본_것.get().getRequest().getHeaders();
        assertThat(받은_것.getFirst(ID)).isEqualTo("12345");
        assertThat(받은_것.getFirst(GRADE)).isEqualTo("GOLD");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "abc", "12.3", "-1", "0", "1e5", "١٢٣"})
    @DisplayName("식별자가_수치가_아니면_막는다")
    void 식별자가_수치가_아니면_막는다(String 이상한_값) {
        // 깨진 값이 뒷단까지 가면 거기서 터진다. 회원 식별자는 양의 정수다.
        MockServerWebExchange exchange =
                통과시킨다(발급_요청().header(ID, 이상한_값).header(GRADE, "GOLD"));

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(뒷단이_본_것.get()).as("막았으면 뒷단에 안 닿는다").isNull();
    }

    @Test
    @DisplayName("식별자가_없으면_막는다")
    void 식별자가_없으면_막는다() {
        MockServerWebExchange exchange = 통과시킨다(발급_요청().header(GRADE, "GOLD"));

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(뒷단이_본_것.get()).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "gold", "PLATINUM", "GOLD "})
    @DisplayName("등급이_아는_값이_아니면_막는다")
    void 등급이_아는_값이_아니면_막는다(String 이상한_값) {
        // 게이트웨이는 등급으로 판정하지 않는다. 자격 대조는 발급 계층 몫이고,
        // 여기서 보는 것은 깨진 값을 뒷단에 안 흘리는 것뿐이다.
        MockServerWebExchange exchange =
                통과시킨다(발급_요청().header(ID, "1").header(GRADE, 이상한_값));

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(뒷단이_본_것.get()).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"WELCOME", "SILVER", "GOLD", "VIP"})
    @DisplayName("계약에_적힌_등급은_전부_통과한다")
    void 계약에_적힌_등급은_전부_통과한다(String 등급) {
        // 좁히다 실제 등급을 막으면 그 등급 사용자가 통째로 못 쓴다.
        MockServerWebExchange exchange = 통과시킨다(발급_요청().header(ID, "1").header(GRADE, 등급));

        assertThat(뒷단이_본_것.get()).as("등급 %s 가 뒷단에 닿는다", 등급)
                .extracting(e -> e.getRequest().getHeaders().getFirst(GRADE)).isEqualTo(등급);
        assertThat(exchange.getResponse().getStatusCode()).isNull();
    }

    @Test
    @DisplayName("관리_경로는_안_본다")
    void 관리_경로는_안_본다() {
        // 헬스체크에 회원 헤더를 붙일 리 없다. 막으면 프로브가 통째로 죽는다.
        MockServerWebExchange exchange =
                통과시킨다(MockServerHttpRequest.method(HttpMethod.GET, "/actuator/health"));

        assertThat(뒷단이_본_것.get())
                .extracting(e -> e.getRequest().getPath().value()).isEqualTo("/actuator/health");
        assertThat(exchange.getResponse().getStatusCode()).isNull();
    }

    @Test
    @DisplayName("순번_조회에도_걸린다")
    void 순번_조회에도_걸린다() {
        // **그 경로가 이 필터의 존재 이유다.** 게이트웨이 라우트를 안 타므로
        // 라우트 필터로만 검사를 붙이면 여기만 통째로 뚫린다.
        MockServerWebExchange exchange = 통과시킨다(
                MockServerHttpRequest.method(HttpMethod.GET, "/api/v1/coupons/c1/queue"));

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(뒷단이_본_것.get()).isNull();
    }

    @Test
    @DisplayName("막을_때_발급_계층과_같은_봉투를_쓴다")
    void 막을_때_발급_계층과_같은_봉투를_쓴다() {
        // 봉투가 다르면 클라이언트가 게이트웨이 응답과 뒷단 응답을 다르게 다뤄야 한다.
        MockServerWebExchange exchange = 통과시킨다(발급_요청());

        assertThat(exchange.getResponse().getHeaders().getContentType())
                .hasToString("application/json");
        String 본문 = exchange.getResponse().getBodyAsString().block();
        assertThat(본문)
                .contains("\"success\":false")
                .contains("\"data\":null")
                .contains("\"code\":\"COMMON-001\"")
                .contains("\"status\":400");
    }

    @Test
    @DisplayName("막을_때_이유를_안_나눈다")
    void 막을_때_이유를_안_나눈다() {
        // 어느 헤더가 왜 틀렸는지 알려 주면 형식을 맞추는 데 쓰인다.
        List<String> 본문들 = List.of(
                통과시킨다(발급_요청()).getResponse().getBodyAsString().block(),
                통과시킨다(발급_요청().header(ID, "abc").header(GRADE, "GOLD"))
                        .getResponse().getBodyAsString().block(),
                통과시킨다(발급_요청().header(ID, "1").header(GRADE, "PLATINUM"))
                        .getResponse().getBodyAsString().block());

        assertThat(본문들).allSatisfy(본문 ->
                assertThat(본문).doesNotContain("X-Member-Id", "X-Member-Grade", "PLATINUM"));
    }
}
