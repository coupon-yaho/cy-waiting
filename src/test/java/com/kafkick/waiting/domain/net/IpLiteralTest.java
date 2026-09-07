package com.kafkick.waiting.domain.net;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 숫자 표기의 IP 만 바이트로 푼다.
 *
 * <p><b>이름을 안 푼다.</b> 푸는 순간 이름 조회가 붙고, 그 결과는 검사한 순간과
 * 쓰는 순간이 다를 수 있다 — 검사를 통과한 이름이 다른 곳을 가리키게 된다.
 */
@Tag("unit")
class IpLiteralTest {

    @Test
    @DisplayName("숫자_표기를_푼다")
    void 숫자_표기를_푼다() {
        assertThat(IpLiteral.parse("10.0.1.7")).containsExactly(10, 0, 1, 7);
    }

    /**
     * <b>이름 조회로 넘어가는 갈래를 다 막는다.</b> 실측으로 {@code beef} 는 150ms,
     * {@code dead} 는 42ms 짜리 DNS 조회였다 — 그 대기가 요청 경로와 배분 틱에
     * 그대로 얹히고, 풀린 값이 대역을 통과하면 검사와 연결이 갈린다.
     */
    @Test
    @DisplayName("이름은_안_푼다")
    void 이름은_안_푼다() {
        assertThat(IpLiteral.parse("dead.beef")).isNull();
        assertThat(IpLiteral.parse("coupon-be.internal")).isNull();
        assertThat(IpLiteral.parse("beef")).isNull();
        assertThat(IpLiteral.parse("dead")).isNull();
    }

    /**
     * <b>점 넷이 아니면 IPv4 가 아니다.</b> {@code InetAddress} 는 {@code 1234} 를
     * {@code 0.0.4.210} 으로 읽는다 — 그대로 두면 한 낱말이 주소가 된다.
     */
    @Test
    @DisplayName("점_넷이_아니면_주소가_아니다")
    void 점_넷이_아니면_주소가_아니다() {
        assertThat(IpLiteral.parse("1234")).isNull();
        assertThat(IpLiteral.parse("10.0.1")).isNull();
    }

    /** 콜론이 든 값은 리터럴로만 읽는다. 못 읽으면 조회 없이 바로 거절이다. */
    @Test
    @DisplayName("콜론이_들면_리터럴로만_읽는다")
    void 콜론이_들면_리터럴로만_읽는다() {
        assertThat(IpLiteral.parse("fd00::1")).hasSize(16);
        assertThat(IpLiteral.parse("zz::qq")).isNull();
    }

    @Test
    @DisplayName("없는_값은_null_이다")
    void 없는_값은_null_이다() {
        assertThat(IpLiteral.parse(null)).isNull();
    }

    /** 모양은 숫자인데 범위를 벗어나면 못 읽는다. 그 값을 대역으로 쓰면 안 된다. */
    @Test
    @DisplayName("범위를_벗어난_숫자는_못_읽는다")
    void 범위를_벗어난_숫자는_못_읽는다() {
        assertThat(IpLiteral.parse("999.0.0.1")).isNull();
    }
}
