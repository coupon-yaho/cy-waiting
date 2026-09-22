package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.gateway.RouteRules.Kind;
import com.kafkick.waiting.gateway.RouteRules.Rule;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 규칙을 <b>기동 때</b> 막는가.
 *
 * <p>여기서 안 막으면 전부 조용하다 — 기동은 성공하고 그 경로만 아무 데도 안 잡히거나,
 * 판정 없이 뒷단으로 새거나, 같은 이름의 서킷이 서로를 덮는다.
 */
class RouteRulesTest {

    private static Rule 발급(String id, String path, String uri) {
        return new Rule(id, Kind.ENTRY, "POST", List.of(path), uri);
    }

    @Test
    @DisplayName("안 적으면 지금 쓰는 규칙 셋이 선다")
    void 기본값() {
        List<Rule> 규칙 = new RouteRules(null).rules();

        assertThat(규칙).extracting(Rule::id)
                .as("기본값이 사라지면 아무 경로도 안 잡힌다")
                .containsExactly("issue", "coupons", "queue");
        assertThat(규칙).extracting(Rule::kind)
                .containsExactly(Kind.ENTRY, Kind.QUERY, Kind.QUEUE);
    }

    @Test
    @DisplayName("이름이 겹치면 막는다")
    void 이름_중복() {
        assertThatThrownBy(() -> new RouteRules(List.of(
                발급("issue", "/a/{couponId}/x", "http://a:8080"),
                발급("issue", "/b/{couponId}/x", "http://b:8080"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("issue");
    }

    @Test
    @DisplayName("뒷단으로 보내는 규칙의 경로에 식별자가 없으면 막는다")
    void 식별자_없는_경로() {
        assertThatThrownBy(() -> new RouteRules(List.of(
                발급("issue", "/api/v1/coupons/issue", "http://a:8080"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("couponId");
    }

    @Test
    @DisplayName("조회는 식별자가 없어도 된다 — 목록 경로가 그렇다")
    void 조회는_식별자_선택() {
        assertThatCode(() -> new RouteRules(List.of(
                new Rule("coupons", Kind.QUERY, "GET", List.of("/api/v1/coupons"), null))))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("주소가 뒷단 형식이 아니면 막는다")
    void 잘못된_주소() {
        assertThatThrownBy(() -> new RouteRules(List.of(
                발급("issue", "/a/{couponId}/x", "http://a:8080/api"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("규칙이 비면 막는다 — 빈 목록은 라우트가 하나도 없다는 뜻이다")
    void 빈_목록() {
        assertThatThrownBy(() -> new RouteRules(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("메서드 이름이 틀리면 막는다")
    void 잘못된_메서드() {
        assertThatThrownBy(() -> new RouteRules(List.of(
                new Rule("issue", Kind.ENTRY, "POSTT", List.of("/a/{couponId}/x"), null))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("줄 조회 규칙은 뒷단 주소를 받지 않는다 — 게이트웨이가 종결한다")
    void 줄_조회는_주소_없음() {
        assertThatThrownBy(() -> new RouteRules(List.of(
                new Rule("queue", Kind.QUEUE, "GET", List.of(RouteRules.QUEUE_PATH),
                        "http://a:8080"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("줄 조회 경로를 바꾸려 들면 막는다 — 필터가 안 따라온다")
    void 줄_조회_경로는_고정() {
        assertThatThrownBy(() -> new RouteRules(List.of(
                new Rule("queue", Kind.QUEUE, "GET", List.of("/q/{couponId}"), null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(RouteRules.QUEUE_PATH);
    }

    @Test
    @DisplayName("성질과 메서드가 어긋나면 막는다 — 판정이 통째로 빠진다")
    void 성질과_메서드() {
        assertThatThrownBy(() -> new RouteRules(List.of(
                new Rule("issue", Kind.QUERY, "POST",
                        List.of("/api/v1/coupons/{couponId}/issue"), null))))
                .as("조회로 적으면 판정도 매진 관찰도 안 붙는다")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RouteRules(List.of(
                new Rule("coupons", Kind.ENTRY, "GET",
                        List.of("/api/v1/coupons/{couponId}"), null))))
                .as("진입으로 적으면 한산한 조회가 줄로 간다")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("제약을 직접 적어 넓히려 들면 막는다")
    void 정규식_우회() {
        assertThatThrownBy(() -> new RouteRules(List.of(
                new Rule("issue", Kind.ENTRY, "POST", List.of(
                        "/api/v1/coupons/{couponId}/issue",
                        "/api/v1/coupons/{couponId:.*}/issue"), null))))
                .as("한 경로만 자리를 맞춰 두고 다른 경로로 넓히는 길이 있었다")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RouteRules(List.of(
                new Rule("coupons", Kind.QUERY, "GET",
                        List.of("/api/v1/coupons/{couponId:.*}"), null))))
                .as("조회는 자리 검사를 건너뛰어 한 줄이면 됐다")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("진입 규칙이 서로 다른 뒷단을 보면 막는다 — 서킷이 하나다")
    void 진입_뒷단_둘() {
        assertThatThrownBy(() -> new RouteRules(List.of(
                발급("issue", "/api/v1/x/{couponId}/issue", "http://a:8080"),
                발급("issue-v2", "/api/v2/x/{couponId}/issue", "http://b:9090"))))
                .as("한 뒷단의 장애가 다른 규칙의 대기자를 같이 민다")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("같은 경로를 두 규칙이 적으면 막는다 — 뒤가 조용히 죽는다")
    void 경로_중복() {
        assertThatThrownBy(() -> new RouteRules(List.of(
                new Rule("a", Kind.QUERY, "GET", List.of("/api/v1/coupons"), "http://a:8080"),
                new Rule("b", Kind.QUERY, "GET", List.of("/api/v1/coupons"), "http://b:9090"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("식별자 자리에 제약이 박힌다 — 설정이 그 정규식을 못 넓힌다")
    void 식별자_제약() {
        Rule 규칙 = new RouteRules(List.of(
                발급("issue", "/api/v1/coupons/{couponId}/issue", null))).rules().getFirst();

        assertThat(규칙.expandedPaths())
                .as("설정이 적는 것은 자리뿐이고 제약은 코드가 준다")
                .containsExactly("/api/v1/coupons/{couponId:[A-Za-z0-9_-]{1,64}}/issue");
    }
}
