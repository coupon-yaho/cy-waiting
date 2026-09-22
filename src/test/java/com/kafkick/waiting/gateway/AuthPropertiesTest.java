package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.gateway.AuthProperties.Jwt;
import com.kafkick.waiting.gateway.AuthProperties.Mode;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * 인증 설정을 <b>기동 때</b> 막는가 (CY-980).
 *
 * <p>여기서 안 막으면 기동은 성공하고 첫 요청부터 전원이 401 이거나, 더 나쁘게는 약한 키로 서명된
 * 토큰이 통과한다. 조건마다 메시지로 어느 검사에 걸렸는지 고정한다.
 */
class AuthPropertiesTest {

    private static final String 비밀 = "0123456789abcdef0123456789abcdef";

    private static final String 공개키 = "-----BEGIN PUBLIC KEY-----";

    private static final String 대상 = "waiting";

    private static Jwt hs(String secret) {
        return new Jwt("HS256", secret, null, null, null, 대상, null, null, null, false);
    }

    private static Jwt jwks(String uri, boolean allowHttp) {
        return new Jwt("RS256", null, null, uri, "https://idp", 대상, null, null, null, allowHttp);
    }

    @Test
    @DisplayName("기본은 지금과 같다 — 인증 없이 헤더를 형식만 본다")
    void 기본값() {
        assertThat(new AuthProperties(null, null).mode()).isEqualTo(Mode.NONE);
    }

    @Test
    @DisplayName("JWT 모드인데 설정이 없으면 막는다")
    void 설정_없음() {
        assertThatThrownBy(() -> new AuthProperties(Mode.JWT, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("waiting.auth.jwt 가 없다");
    }

    @ParameterizedTest(name = "[{index}] {0} 은 {1} 바이트부터")
    @CsvSource({"HS256, 32", "HS384, 48", "HS512, 64"})
    @DisplayName("HMAC 비밀은 해시 출력 길이 이상이어야 한다 (RFC 7518 3.2)")
    void 비밀_길이(String alg, int min) {
        String 딱 = "k".repeat(min);
        assertThatCode(() -> new Jwt(alg, 딱, null, null, null, 대상, null, null, null, false))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> new Jwt(alg, 딱.substring(1), null, null, null, 대상, null, null,
                null, false)).hasMessageContaining(min + " 바이트보다 짧다");
    }

    @Test
    @DisplayName("HMAC 은 비밀 말고 다른 키 공급을 같이 받지 않는다")
    void hmac_공급_하나() {
        assertThatThrownBy(() -> new Jwt("HS256", null, null, null, null, 대상, null, null, null,
                false)).hasMessageContaining("secret 하나로만");
        assertThatThrownBy(() -> new Jwt("HS256", 비밀, null, "https://idp/jwks", null, 대상, null,
                null, null, false)).hasMessageContaining("secret 하나로만");
        assertThatThrownBy(() -> new Jwt("HS256", 비밀, 공개키, null, null, 대상, null, null, null,
                false)).hasMessageContaining("secret 하나로만");
    }

    @Test
    @DisplayName("RSA·EC 는 비밀을 안 받고, 공개키와 JWKS 중 하나만 받는다")
    void 비대칭_공급() {
        assertThatThrownBy(() -> new Jwt("RS256", 비밀, 공개키, null, null, 대상, null, null, null,
                false)).hasMessageContaining("secret 을 안 받는다");
        assertThatThrownBy(() -> new Jwt("RS256", null, null, null, null, 대상, null, null, null,
                false)).hasMessageContaining("중 하나로");
        assertThatThrownBy(() -> new Jwt("RS256", null, 공개키, "https://idp/jwks", "https://idp",
                대상, null, null, null, false)).hasMessageContaining("중 하나로");
        assertThatCode(() -> new Jwt("ES256", null, 공개키, null, null, 대상, null, null, null, false))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("모르는 알고리즘과 none 은 막는다")
    void 알고리즘() {
        for (String 나쁜 : new String[] {"none", "HS1", "RS1024", ""}) {
            assertThatThrownBy(() -> new Jwt(나쁜, 비밀, null, null, null, 대상, null, null, null,
                    false)).as("'%s'", 나쁜).hasMessageContaining("algorithm 을 모른다");
        }
    }

    @Test
    @DisplayName("알고리즘 이름의 공백과 대소문자는 가리지 않는다")
    void 알고리즘_표기() {
        assertThat(new Jwt(" hs256 ", 비밀, null, null, null, 대상, null, null, null, false)
                .algorithm()).isEqualTo("HS256");
    }

    @Test
    @DisplayName("JWKS 주소는 https 만 받는다 — http 는 중간에서 키를 바꿔 끼운다")
    void jwks_주소() {
        assertThatCode(() -> jwks("https://idp/jwks", false)).doesNotThrowAnyException();
        assertThatCode(() -> jwks("HTTPS://idp/jwks", false))
                .as("스킴은 대소문자를 안 가린다 (RFC 3986 3.1)").doesNotThrowAnyException();
        assertThatThrownBy(() -> jwks("http://idp/jwks", false)).hasMessageContaining("https 여야");
        assertThatThrownBy(() -> jwks("file:///etc/keys", true)).hasMessageContaining("https 여야");
        assertThatCode(() -> jwks("http://idp/jwks", true))
                .as("시험 환경은 명시적으로 연다").doesNotThrowAnyException();
        for (String 주소 : new String[] {"https:/jwks", "https:jwks", "/jwks", "https:///jwks"}) {
            assertThatThrownBy(() -> jwks(주소, false)).as("'%s'", 주소)
                    .hasMessageContaining("jwks-uri");
        }
    }

    @Test
    @DisplayName("JWKS 를 쓰면 발급자가 필수다")
    void jwks_발급자() {
        assertThatThrownBy(() -> new Jwt("RS256", null, null, "https://idp/jwks", null, 대상, null,
                null, null, false)).hasMessageContaining("issuer 를 적는다");
    }

    @Test
    @DisplayName("대상은 필수다 — 같은 발급자의 다른 서비스용 토큰이 통과하면 안 된다")
    void 대상_필수() {
        assertThatThrownBy(() -> new Jwt("HS256", 비밀, null, null, null, " ", null, null, null,
                false)).hasMessageContaining("audience 가 없다");
    }

    @Test
    @DisplayName("클레임 이름을 안 적으면 sub 와 grade 다")
    void 클레임_기본값() {
        assertThat(hs(비밀).memberClaim()).isEqualTo("sub");
        assertThat(hs(비밀).gradeClaim()).isEqualTo("grade");
    }

    @Test
    @DisplayName("문자열로 찍어도 비밀이 안 나온다")
    void 비밀_가림() {
        assertThat(hs(비밀).toString()).doesNotContain(비밀).contains("****");
    }

    @Test
    @DisplayName("인증을 켰는데 입장 토큰 헤더가 회원 헤더와 같으면 막는다 — 클라이언트 값이 검증된 신원을 덮는다")
    void 입장_토큰_이름_충돌() {
        AuthProperties 켬 = new AuthProperties(Mode.JWT, hs(비밀));
        for (String 이름 : new String[] {"X-Member-Id", "x-member-grade"}) {
            assertThatThrownBy(() -> 켬.checkAgainst(
                    new EntryTokenDelivery(EntryTokenDelivery.Where.BODY, null, 이름)))
                    .as("뒷단 이름 '%s'", 이름).hasMessageContaining("입장 토큰 헤더로 못 쓴다");
            assertThatThrownBy(() -> 켬.checkAgainst(
                    new EntryTokenDelivery(EntryTokenDelivery.Where.BODY, 이름, null)))
                    .as("받는 이름 '%s'", 이름).hasMessageContaining("입장 토큰 헤더로 못 쓴다");
        }
        assertThatCode(() -> 켬.checkAgainst(new EntryTokenDelivery(null, null, null)))
                .doesNotThrowAnyException();
        assertThatCode(() -> new AuthProperties(Mode.NONE, null).checkAgainst(
                new EntryTokenDelivery(EntryTokenDelivery.Where.BODY, null, "X-Member-Id")))
                .as("인증을 끄면 막을 검증된 신원이 없다").doesNotThrowAnyException();
    }

    @Test
    @DisplayName("인증을 켰는데 라우트가 /api/ 밖이면 막는다 — 그 경로는 토큰 없이 남의 이름을 쓴다")
    void 라우트_범위() {
        AuthProperties 켬 = new AuthProperties(Mode.JWT, hs(비밀));
        RouteRules 밖 = new RouteRules(List.of(new RouteRules.Rule("v2", RouteRules.Kind.ENTRY,
                "POST", List.of("/v2/coupons/{couponId}/issue"), null)));

        assertThatThrownBy(() -> 켬.checkCovers(밖)).hasMessageContaining("/api/ 밖이다");
        assertThatCode(() -> 켬.checkCovers(new RouteRules(null))).doesNotThrowAnyException();
        assertThatCode(() -> new AuthProperties(Mode.NONE, null).checkCovers(밖))
                .doesNotThrowAnyException();
    }
}
