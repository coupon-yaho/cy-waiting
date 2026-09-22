package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.gateway.RouteRules.Kind;
import com.kafkick.waiting.gateway.RouteRules.Rule;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

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

    private static Rule 발급(String id, List<String> paths) {
        return new Rule(id, Kind.ENTRY, "POST", paths, null);
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
                발급("issue", "/api/a/{couponId}/x", "http://a:8080"),
                발급("issue", "/api/b/{couponId}/x", "http://b:8080"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("이름이 겹친다");
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
                발급("issue", "/api/a/{couponId}/x", "http://a:8080/api"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("뒷단 형식이 아니다");
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
                new Rule("issue", Kind.ENTRY, "POSTT", List.of("/api/a/{couponId}/x"), null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("메서드를 모른다");
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
                new Rule("queue", Kind.QUEUE, "GET", List.of("/api/q/{couponId}"), null))))
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
    @DisplayName("경로가 /api/ 밖이면 막는다 — 신원 검사·남용 제한·교차 출처가 거기에만 걸린다")
    void api_밖() {
        for (String 경로 : new String[] {"/v2/x/{couponId}/issue", "/api", "/apix/{couponId}",
                "/API/v1/x/{couponId}/issue"}) {
            assertThatThrownBy(() -> new RouteRules(List.of(발급("issue", 경로, null))))
                    .as("'%s'", 경로).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("/api/ 밖").hasMessageContaining(경로);
        }
        assertThatThrownBy(() -> new Rule("coupons", Kind.QUERY, "GET", List.of("/v2/coupons"), null))
                .as("조회 규칙도 같다").hasMessageContaining("/api/ 밖");
        assertThatThrownBy(() -> 발급("issue", List.of("/api/v1/x/{couponId}/issue",
                "/v2/x/{couponId}/issue")))
                .as("경로 하나만 밖이어도 막는다 — 첫 경로만 보면 미끼가 된다")
                .hasMessageContaining("/v2/x/{couponId}/issue");
    }

    @Test
    @DisplayName("설정에서 묶을 때도 /api/ 밖 경로는 기동을 막는다")
    void 설정_바인딩() {
        Binder 바인더 = new Binder(new MapConfigurationPropertySource(Map.of(
                "waiting.routes.rules[0].id", "x",
                "waiting.routes.rules[0].kind", "ENTRY",
                "waiting.routes.rules[0].method", "POST",
                "waiting.routes.rules[0].paths[0]", "/v2/x/{couponId}/issue")));

        assertThatThrownBy(() -> 바인더.bind("waiting.routes", RouteRules.class))
                .isInstanceOf(BindException.class)
                .rootCause().hasMessageContaining("/api/ 밖");
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
