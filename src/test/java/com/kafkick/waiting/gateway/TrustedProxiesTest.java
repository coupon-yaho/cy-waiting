package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 전달 헤더를 믿어도 되는 홉.
 *
 * <p>아무나 채워 넣게 두면 키를 무한히 만들어 리미터를 포화시키고, 그때부터
 * 정상 사용자가 막힌다.
 */
class TrustedProxiesTest {

    @Test
    @DisplayName("설정이_비면_아무도_안_믿는다")
    void 설정이_비면_아무도_안_믿는다() {
        // 기본은 안 믿는 것이다. 앞단이 헤더를 덮어쓴다는 보장이 있을 때만 연다.
        assertThat(new TrustedProxies(List.of()).isTrusted("10.0.0.1")).isFalse();
        assertThat(new TrustedProxies(null).isTrusted("10.0.0.1")).isFalse();
    }

    /** 접두 문자열로 보면 이 표기가 아무 주소도 안 잡는다 — 어디에도 안 붙는다. */
    @Test
    @DisplayName("대역_표기를_비트로_읽는다")
    void 대역_표기를_비트로_읽는다() {
        TrustedProxies 신뢰 = new TrustedProxies(List.of("10.0.0.0/8"));

        assertThat(신뢰.isTrusted("10.1.2.3")).isTrue();
        assertThat(신뢰.isTrusted("10.255.255.255")).isTrue();
        assertThat(신뢰.isTrusted("11.0.0.1")).isFalse();
    }

    @Test
    @DisplayName("바이트_경계가_아닌_대역도_읽는다")
    void 바이트_경계가_아닌_대역도_읽는다() {
        TrustedProxies 신뢰 = new TrustedProxies(List.of("192.168.16.0/20"));

        assertThat(신뢰.isTrusted("192.168.16.1")).isTrue();
        assertThat(신뢰.isTrusted("192.168.31.255")).isTrue();
        assertThat(신뢰.isTrusted("192.168.32.1")).isFalse();
    }

    @Test
    @DisplayName("대역_없는_표기는_그_주소만이다")
    void 대역_없는_표기는_그_주소만이다() {
        TrustedProxies 신뢰 = new TrustedProxies(List.of("10.0.0.1"));

        assertThat(신뢰.isTrusted("10.0.0.1")).isTrue();
        assertThat(신뢰.isTrusted("10.0.0.2")).isFalse();
    }

    /** 오타 하나가 전 대역을 여는 것보다 아무도 안 믿는 쪽이 낫다. */
    @Test
    @DisplayName("못_읽는_표기는_안_믿는다")
    void 못_읽는_표기는_안_믿는다() {
        assertThat(new TrustedProxies(List.of("10.0.0.0/aa")).isTrusted("10.0.0.1")).isFalse();
        assertThat(new TrustedProxies(List.of("10.0.0.0/99")).isTrusted("10.0.0.1")).isFalse();
        assertThat(new TrustedProxies(List.of("헛소리")).isTrusted("10.0.0.1")).isFalse();
        assertThat(new TrustedProxies(List.of("10.0.0.0/8")).isTrusted("헛소리")).isFalse();
    }

    /** 이름을 찾으면 그 조회가 요청 경로에 붙는다. */
    @Test
    @DisplayName("이름은_안_찾는다")
    void 이름은_안_찾는다() {
        assertThat(new TrustedProxies(List.of("localhost")).isTrusted("127.0.0.1")).isFalse();
    }
}
