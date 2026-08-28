package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    @DisplayName("같은_재료는_같은_키를_낸다")
    void 같은_재료는_같은_키를_낸다() {
        assertThat(keys.of("c1", "m1", "attempt-1"))
                .isEqualTo(keys.of("c1", "m1", "attempt-1"));
    }

    /**
     * <b>클라이언트가 시도를 가른다.</b> 게이트웨이는 무엇이 한 번의 시도인지
     * 모른다 — 발급 정책은 뒷단 것이다. 값이 다르면 다른 시도로 본다.
     */
    @Test
    @DisplayName("클라이언트_값이_다르면_다른_키다")
    void 클라이언트_값이_다르면_다른_키다() {
        assertThat(keys.of("c1", "m1", "attempt-1"))
                .isNotEqualTo(keys.of("c1", "m1", "attempt-2"));
    }

    /**
     * <b>회원에 묶는다.</b> 안 묶으면 뒷단 로그에서 주운 키를 그대로 실어 남의
     * 진짜 시도를 재생으로 버리게 만들 수 있다.
     */
    @Test
    @DisplayName("같은_값이라도_사람이_다르면_다른_키다")
    void 같은_값이라도_사람이_다르면_다른_키다() {
        assertThat(keys.of("c1", "m1", "attempt-1"))
                .isNotEqualTo(keys.of("c1", "m2", "attempt-1"));
        assertThat(keys.of("c1", "m1", "attempt-1"))
                .isNotEqualTo(keys.of("c2", "m1", "attempt-1"));
    }

    /**
     * <b>값을 안 준 것과 빈 값을 준 것을 가른다.</b> 둘을 같게 두면 클라이언트가
     * 빈 헤더 하나로 남의 자동 생성 키에 부딪칠 수 있다.
     */
    @Test
    @DisplayName("값을_안_주면_빈_값과_다른_키다")
    void 값을_안_주면_빈_값과_다른_키다() {
        assertThat(keys.of("c1", "m1", null)).isNotEqualTo(keys.of("c1", "m1", ""));
    }

    /**
     * <b>값을 안 줘도 같은 사람에게는 같은 키다.</b> 매번 다른 값을 만들면 끊긴
     * 발급의 재시도가 새 시도로 처리된다 — 이 클래스가 존재하는 이유가 사라진다.
     */
    @Test
    @DisplayName("값을_안_줘도_같은_사람은_같은_키다")
    void 값을_안_줘도_같은_사람은_같은_키다() {
        assertThat(keys.of("c1", "m1", null)).isEqualTo(keys.of("c1", "m1", null));
    }

    /**
     * <b>클라이언트 값을 그대로 실어 보내지 않는다.</b> 그대로 쓰면 남의 키를
     * 주워 와 그 사람 앞으로 태울 수 있다.
     */
    @Test
    @DisplayName("클라이언트_값이_키에_그대로_안_들어간다")
    void 클라이언트_값이_키에_그대로_안_들어간다() {
        String 준_값 = "client-chosen-nonce-9f2a";

        assertThat(keys.of("c1", "m1", 준_값)).doesNotContain(준_값);
    }

    /**
     * <b>헤더로 나갈 값이다.</b> 제어 문자나 공백이 섞이면 프록시가 요청을 통째로
     * 거절하거나, 헤더를 쪼개 다른 헤더를 만들어 낸다.
     */
    @Test
    @DisplayName("헤더에_실을_수_있는_글자만_낸다")
    void 헤더에_실을_수_있는_글자만_낸다() {
        assertThat(keys.of("c1", "m1", "t\r\nX-Evil: 1")).matches("[A-Za-z0-9_-]{16,128}");
    }

    /** 비밀키가 다르면 다른 키다. 남이 우리 키를 못 만들어야 한다. */
    @Test
    @DisplayName("비밀키가_다르면_다른_키다")
    void 비밀키가_다르면_다른_키다() {
        IdempotencyKey 남 = IdempotencyKey.of("another-secret-0123456789abcdefgh");

        assertThat(keys.of("c1", "m1", "t")).isNotEqualTo(남.of("c1", "m1", "t"));
    }

    /** 짧은 비밀키는 거부한다. 서명이 있다는 사실만 남고 뜻은 사라진다. */
    @Test
    @DisplayName("짧은_비밀키로는_안_만들어진다")
    void 짧은_비밀키로는_안_만들어진다() {
        assertThatThrownBy(() -> IdempotencyKey.of("short"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
