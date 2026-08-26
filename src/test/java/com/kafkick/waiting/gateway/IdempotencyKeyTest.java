package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 끊긴 발급이 두 번 나가지 않게 하는 키.
 *
 * <p><b>게이트웨이가 끊어도 뒷단은 처리했을 수 있다.</b> 사용자가 다시 시도하면
 * 같은 사람이 두 번 발급된다 (불변식 2). 재사용 방지는 발급 계층의 멱등성이
 * 지고(A-10), 게이트웨이는 같은 시도에 같은 키를 실어 그 근거를 준다.
 */
class IdempotencyKeyTest {

    private final IdempotencyKey keys = IdempotencyKey.of("not-a-real-secret-0123456789abcdef");

    /** 같은 시도면 같은 키다. 다르면 뒷단이 두 건으로 보고 두 번 발급한다. */
    @Test
    @DisplayName("같은_시도는_같은_키를_낸다")
    void 같은_시도는_같은_키를_낸다() {
        assertThat(keys.of("c1", "m1", "token-a"))
                .isEqualTo(keys.of("c1", "m1", "token-a"));
    }

    /**
     * <b>토큰이 다르면 다른 시도다.</b> 한 사람이 두 번 줄을 서서 두 번 차례가
     * 오면 두 번 받는 것이 맞다 — 같은 키를 주면 두 번째가 조용히 버려진다.
     */
    @Test
    @DisplayName("토큰이_다르면_다른_키다")
    void 토큰이_다르면_다른_키다() {
        assertThat(keys.of("c1", "m1", "token-a"))
                .isNotEqualTo(keys.of("c1", "m1", "token-b"));
    }

    /** 쿠폰과 회원이 다르면 당연히 다른 시도다. */
    @Test
    @DisplayName("쿠폰이나_회원이_다르면_다른_키다")
    void 쿠폰이나_회원이_다르면_다른_키다() {
        assertThat(keys.of("c1", "m1", "t")).isNotEqualTo(keys.of("c2", "m1", "t"));
        assertThat(keys.of("c1", "m1", "t")).isNotEqualTo(keys.of("c1", "m2", "t"));
    }

    /**
     * <b>토큰을 그대로 실어 보내지 않는다.</b> 키는 뒷단 로그와 저장소에 남는데,
     * 거기에 서명된 토큰이 그대로 있으면 그 로그를 보는 사람이 남의 차례로 발급을
     * 시도할 수 있다.
     */
    @Test
    @DisplayName("키에_토큰이_그대로_안_들어간다")
    void 키에_토큰이_그대로_안_들어간다() {
        String 토큰 = "eyJhbGciOiJIUzI1NiJ9.signed-entry-token";

        assertThat(keys.of("c1", "m1", 토큰)).doesNotContain(토큰);
    }

    /**
     * <b>헤더로 나갈 값이다.</b> 제어 문자나 공백이 섞이면 프록시가 요청을 통째로
     * 거절하거나, 헤더를 쪼개 다른 헤더를 만들어 낸다.
     */
    @Test
    @DisplayName("헤더에_실을_수_있는_글자만_낸다")
    void 헤더에_실을_수_있는_글자만_낸다() {
        assertThat(keys.of("c1", "m1", "t")).matches("[A-Za-z0-9_-]{16,128}");
    }

    /** 비밀키가 다르면 다른 키다. 배포마다 갈리면 안 되지만 남이 못 만들어야 한다. */
    @Test
    @DisplayName("비밀키가_다르면_다른_키다")
    void 비밀키가_다르면_다른_키다() {
        IdempotencyKey 남 = IdempotencyKey.of("another-secret-0123456789abcdefgh");

        assertThat(keys.of("c1", "m1", "t")).isNotEqualTo(남.of("c1", "m1", "t"));
    }
}
