package com.kafkick.waiting.domain.net;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

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

    /**
     * <b>모양만 맞고 못 푸는 v6 도 조회로 안 샌다.</b> 이 보장은 JDK 구현에 얹혀
     * 있어 계약이 아니다 — 새면 폴링 스레드가 이름 조회에서 블로킹된다.
     */
    @Test
    @DisplayName("못_푸는_v6_는_조회_없이_null_이다")
    void 못_푸는_v6_는_조회_없이_null_이다() {
        // **한 번을 재면 잡음이 판정을 뒤집는다.** 여러 번의 합으로 본다 — 한 건이
        // 조회를 타면 수십 밀리초라, 상한이 넉넉해도 그 합은 못 넘긴다.
        int 횟수 = 200;
        long 시작 = System.nanoTime();
        for (int i = 0; i < 횟수; i++) {
            assertThat(IpLiteral.parse("1:2:3")).isNull();
            assertThat(IpLiteral.parse("1:2:3:4:5:6:7:8:9")).isNull();
        }

        assertThat(Duration.ofNanos(System.nanoTime() - 시작))
                .as("조회가 돌면 건당 수십 밀리초라 합이 수십 초가 된다")
                .isLessThan(Duration.ofSeconds(3));
    }

    /**
     * <b>v4 매핑 주소는 네 바이트로 접힌다.</b> 그래야 v4 대역과 견줄 수 있다.
     * 안 접히면 길이가 갈려 그 대가 조용히 후보에서 빠지고 크레딧만 남는다.
     */
    @Test
    @DisplayName("v4_매핑_주소는_네_바이트다")
    void v4_매핑_주소는_네_바이트다() {
        assertThat(IpLiteral.parse("::ffff:10.0.1.7"))
                .isEqualTo(IpLiteral.parse("10.0.1.7"));
    }

    /**
     * <b>한 대를 가리키는 주소만 목적지다.</b> 뒷단이 보고한 값이 그대로 연결 대상이
     * 되므로, 어느 한 대도 아닌 주소가 여기를 지나면 게이트웨이가 엉뚱한 데로 보낸다.
     */
    @Test
    @DisplayName("한_대를_가리키는_주소만_목적지다")
    void 한_대를_가리키는_주소만_목적지다() {
        assertThat(IpLiteral.routable(IpLiteral.parse("10.0.1.7"))).isTrue();
        // 루프백은 같은 호스트의 뒷단이라 목적지다. 자기 자신은 포트가 가른다.
        assertThat(IpLiteral.routable(IpLiteral.parse("127.0.0.1"))).isTrue();
        assertThat(IpLiteral.routable(IpLiteral.parse("fd00::5"))).isTrue();
        assertThat(IpLiteral.routable(IpLiteral.parse("::1"))).as("v6 루프백도 같다").isTrue();
        // fe 로 시작해도 다음 두 비트가 다르면 링크 로컬이 아니다.
        assertThat(IpLiteral.routable(IpLiteral.parse("fec0::1"))).isTrue();
        // 169 로 시작해도 둘째 바이트가 다르면 링크 로컬이 아니다.
        assertThat(IpLiteral.routable(IpLiteral.parse("169.1.1.1"))).isTrue();
        // 앞 두 바이트가 20 01 이어도 그다음이 0 이 아니면 Teredo 가 아니다.
        assertThat(IpLiteral.routable(IpLiteral.parse("2001:db8::1"))).isTrue();
        // 20 으로 시작해도 둘째가 02 가 아니면 6to4 가 아니다.
        assertThat(IpLiteral.routable(IpLiteral.parse("2003::1"))).isTrue();
        // 192 로 시작해도 둘째가 88 이 아니면 릴레이 애니캐스트가 아니다.
        assertThat(IpLiteral.routable(IpLiteral.parse("192.168.0.5"))).isTrue();
        // 192.88 로 시작해도 셋째가 99 가 아니면 릴레이 애니캐스트가 아니다.
        assertThat(IpLiteral.routable(IpLiteral.parse("192.88.1.1"))).isTrue();
        // 20 01 로 시작해도 넷째가 0 이 아니면 Teredo 가 아니다.
        assertThat(IpLiteral.routable(IpLiteral.parse("2001:1::1"))).isTrue();
        // 00 64 로 시작해도 셋째·넷째가 ff 9b 가 아니면 NAT64 가 아니다.
        assertThat(IpLiteral.routable(IpLiteral.parse("64:1::1"))).isTrue();
        assertThat(IpLiteral.routable(IpLiteral.parse("64:ff00::1"))).isTrue();
    }

    @Test
    @DisplayName("한_대가_아닌_주소는_목적지가_아니다")
    void 한_대가_아닌_주소는_목적지가_아니다() {
        assertThat(IpLiteral.routable(null)).as("못 읽은 것은 목적지가 아니다").isFalse();
        assertThat(IpLiteral.routable(IpLiteral.parse("0.0.0.0"))).isFalse();
        assertThat(IpLiteral.routable(IpLiteral.parse("::"))).isFalse();
        assertThat(IpLiteral.routable(IpLiteral.parse("169.254.1.1"))).isFalse();
        assertThat(IpLiteral.routable(IpLiteral.parse("fe80::1"))).isFalse();
        assertThat(IpLiteral.routable(IpLiteral.parse("224.0.0.1"))).isFalse();
        assertThat(IpLiteral.routable(IpLiteral.parse("255.255.255.255"))).isFalse();
        assertThat(IpLiteral.routable(IpLiteral.parse("ff02::1"))).isFalse();
    }

    /**
     * <b>v4 를 v6 표기 안에 실어 나르는 것은 목적지가 아니다.</b> 번역되는 순간
     * 어디로 가는지는 안에 실린 v4 가 정하는데, 그 v4 는 여기 판정을 안 거친다.
     */
    @Test
    @DisplayName("v4_를_실어_나르는_v6_표기는_거절한다")
    void v4_를_실어_나르는_v6_표기는_거절한다() {
        assertThat(IpLiteral.routable(IpLiteral.parse("::127.0.0.1"))).isFalse();
        assertThat(IpLiteral.routable(IpLiteral.parse("::10.0.0.5"))).isFalse();
        assertThat(IpLiteral.routable(IpLiteral.parse("::2"))).as("::1 만 빼고 거절한다")
                .isFalse();
        assertThat(IpLiteral.routable(IpLiteral.parse("64:ff9b::a00:105")))
                .as("NAT64").isFalse();
        assertThat(IpLiteral.routable(IpLiteral.parse("2002:0a00:0005::1")))
                .as("6to4").isFalse();
        assertThat(IpLiteral.routable(IpLiteral.parse("2001:0:0a00:5::1")))
                .as("Teredo").isFalse();
        assertThat(IpLiteral.routable(IpLiteral.parse("192.88.99.1")))
                .as("6to4 릴레이 애니캐스트").isFalse();
    }
}
