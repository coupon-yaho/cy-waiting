package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.gateway.AuthProperties.Jwt;
import com.kafkick.waiting.gateway.AuthProperties.Mode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 인증 설정을 <b>기동 때</b> 막는가 (CY-980).
 *
 * <p>여기서 안 막으면 기동은 성공하고 첫 요청부터 전원이 401 이거나, 더 나쁘게는 약한 키로 서명된
 * 토큰이 통과한다.
 */
class AuthPropertiesTest {

    private static final String 비밀 = "0123456789abcdef0123456789abcdef";

    private static Jwt hs(String secret) {
        return new Jwt("HS256", secret, null, null, null, null, null, null, null);
    }

    @Test
    @DisplayName("기본은 지금과 같다 — 인증 없이 헤더를 형식만 본다")
    void 기본값() {
        assertThat(new AuthProperties(null, null).mode()).isEqualTo(Mode.NONE);
    }

    @Test
    @DisplayName("끈 모드는 JWT 설정을 안 본다")
    void 끈_모드() {
        assertThatCode(() -> new AuthProperties(Mode.NONE, null)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("JWT 모드인데 설정이 없으면 막는다")
    void 설정_없음() {
        assertThatThrownBy(() -> new AuthProperties(Mode.JWT, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("HMAC 비밀은 256비트 이상이어야 한다 — 짧으면 무차별 대입에 뚫린다")
    void 짧은_비밀() {
        assertThatThrownBy(() -> hs("short-secret"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> hs(비밀)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("알고리즘과 키 공급이 어긋나면 막는다")
    void 어긋난_키() {
        assertThatThrownBy(() -> new Jwt("HS256", null, null, "https://idp/jwks", null, null, null,
                null, null))
                .as("HMAC 은 비밀로만 검증한다").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Jwt("RS256", 비밀, null, null, null, null, null, null, null))
                .as("RSA 는 공개키나 JWKS 로 검증한다").isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("공개키 공급은 하나만 받는다")
    void 공급_둘() {
        assertThatThrownBy(() -> new Jwt("RS256", null, "-----BEGIN PUBLIC KEY-----",
                "https://idp/jwks", null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("모르는 알고리즘과 none 은 막는다")
    void 알고리즘() {
        for (String 나쁜 : new String[] {"none", "HS1", "RS1024", ""}) {
            assertThatThrownBy(() -> new Jwt(나쁜, 비밀, null, null, null, null, null, null, null))
                    .as("'%s'", 나쁜).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    @DisplayName("JWKS 주소는 http 나 https 여야 한다")
    void jwks_주소() {
        assertThatThrownBy(() -> new Jwt("RS256", null, null, "file:///etc/keys", null, null, null,
                null, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("식별자 클레임을 안 적으면 sub 다")
    void 식별자_클레임() {
        assertThat(hs(비밀).memberClaim()).isEqualTo("sub");
    }

    @Test
    @DisplayName("인증을 켰는데 입장 토큰 헤더가 회원 헤더와 같으면 막는다 — 클라이언트 값이 검증된 신원을 덮는다")
    void 입장_토큰_이름_충돌() {
        AuthProperties 켬 = new AuthProperties(Mode.JWT, hs(비밀));
        for (String 이름 : new String[] {"X-Member-Id", "x-member-grade", "Authorization"}) {
            assertThatThrownBy(() -> 켬.checkAgainst(
                    new EntryTokenDelivery(EntryTokenDelivery.Where.BODY, null, 이름)))
                    .as("뒷단 이름 '%s'", 이름).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> 켬.checkAgainst(
                    new EntryTokenDelivery(EntryTokenDelivery.Where.BODY, 이름, null)))
                    .as("받는 이름 '%s'", 이름).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatCode(() -> 켬.checkAgainst(new EntryTokenDelivery(null, null, null)))
                .doesNotThrowAnyException();
        assertThatCode(() -> new AuthProperties(Mode.NONE, null).checkAgainst(
                new EntryTokenDelivery(EntryTokenDelivery.Where.BODY, null, "X-Member-Id")))
                .as("인증을 끄면 막을 검증된 신원이 없다").doesNotThrowAnyException();
    }
}
