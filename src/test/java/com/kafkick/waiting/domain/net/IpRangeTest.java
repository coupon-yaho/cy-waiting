package com.kafkick.waiting.domain.net;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * CIDR 대역 하나.
 *
 * <p><b>사본을 두지 않는 자리다.</b> 신뢰하는 홉과 연결해도 되는 목적지가 각자
 * 같은 산술을 들면, 한쪽만 고쳤을 때 두 판정이 갈린다.
 */
@Tag("unit")
class IpRangeTest {

    private static byte[] 주소(String raw) {
        return IpLiteral.parse(raw);
    }

    @Test
    @DisplayName("대역_안팎을_가른다")
    void 대역_안팎을_가른다() {
        IpRange 대역 = IpRange.parse("10.0.1.0/24").orElseThrow();

        assertThat(대역.contains(주소("10.0.1.7"))).isTrue();
        assertThat(대역.contains(주소("10.0.2.7"))).isFalse();
    }

    /** 프리픽스를 안 적으면 그 한 대다. 대역으로 읽으면 뜻이 통째로 달라진다. */
    @Test
    @DisplayName("프리픽스가_없으면_한_대다")
    void 프리픽스가_없으면_한_대다() {
        IpRange 하나 = IpRange.parse("10.0.1.5").orElseThrow();

        assertThat(하나.contains(주소("10.0.1.5"))).isTrue();
        assertThat(하나.contains(주소("10.0.1.6"))).isFalse();
    }

    @Test
    @DisplayName("못_읽는_표기는_비어_있다")
    void 못_읽는_표기는_비어_있다() {
        assertThat(IpRange.parse(null)).isEmpty();
        assertThat(IpRange.parse("not-an-ip/24")).isEmpty();
        assertThat(IpRange.parse("10.0.1.0/x")).isEmpty();
        assertThat(IpRange.parse("10.0.1.0/33")).isEmpty();
        assertThat(IpRange.parse("10.0.1.0/-1")).isEmpty();
    }

    /**
     * 길이가 다르면 안 품는다. <b>실제로 나는 조합이다</b> — 듀얼스택에서 v6 피어가
     * 붙고 신뢰하는 대역이 v4 뿐이면 홉 판정이 정확히 이 짝을 만난다.
     */
    @Test
    @DisplayName("길이가_다르면_안_품는다")
    void 길이가_다르면_안_품는다() {
        IpRange 대역 = IpRange.parse("10.0.1.0/24").orElseThrow();

        assertThat(대역.contains(주소("fd00::1"))).isFalse();
    }

    /** 비트가 바이트 경계에 안 맞으면 마지막 바이트를 마스크로 본다. */
    @Test
    @DisplayName("경계에_안_맞는_대역도_본다")
    void 경계에_안_맞는_대역도_본다() {
        IpRange 대역 = IpRange.parse("10.0.1.0/25").orElseThrow();

        assertThat(대역.contains(주소("10.0.1.127"))).as("경계 안").isTrue();
        assertThat(대역.contains(주소("10.0.1.128"))).as("경계 밖").isFalse();
    }

    /**
     * <b>정규 생성자도 막는다.</b> 팩토리만 검증하면 {@code new} 로 만들 수 없는
     * 대역이 생기고, 그건 아무 주소도 안 잡거나 배열 밖을 읽는다.
     */
    @Test
    @DisplayName("정규_생성자도_막는다")
    void 정규_생성자도_막는다() {
        byte[] 넷 = 주소("10.0.1.0");

        assertThatThrownBy(() -> new IpRange(넷, 33))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IpRange(넷, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IpRange(null, 8))
                .isInstanceOf(NullPointerException.class);
    }

    /** record 기본 동등성은 배열에 참조를 쓴다. 같은 대역 둘이 안 같아진다. */
    @Test
    @DisplayName("같은_대역은_같다")
    void 같은_대역은_같다() {
        IpRange 하나 = IpRange.parse("10.0.1.0/24").orElseThrow();
        IpRange 둘 = IpRange.parse("10.0.1.0/24").orElseThrow();

        assertThat(하나).isEqualTo(둘).hasSameHashCodeAs(둘);
        assertThat(하나).isEqualTo(하나);
        assertThat(하나).isNotEqualTo(IpRange.parse("10.0.1.0/25").orElseThrow());
        assertThat(하나).isNotEqualTo(IpRange.parse("10.0.2.0/24").orElseThrow());
        assertThat(하나).isNotEqualTo("10.0.1.0/24");
    }

    /** <b>사본을 준다.</b> 살아 있는 배열을 넘기면 값 타입이 밖에서 바뀐다. */
    @Test
    @DisplayName("주소를_사본으로_준다")
    void 주소를_사본으로_준다() {
        IpRange 대역 = IpRange.parse("10.0.1.0/24").orElseThrow();

        대역.address()[0] = 99;

        assertThat(대역.contains(주소("10.0.1.7"))).isTrue();
    }

    /**
     * <b>프리픽스의 두 끝을 다 못 박는다.</b> 한쪽만 재면 부등호를 옮겨도 초록이고,
     * 그러면 주소 길이를 넘는 대역이 서서 배열 밖을 읽는다.
     */
    @Test
    @DisplayName("프리픽스는_주소_길이까지만_받는다")
    void 프리픽스는_주소_길이까지만_받는다() {
        assertThat(IpRange.parse("10.0.1.0/32")).as("v4 의 끝은 32 다").isPresent();
        assertThat(IpRange.parse("10.0.1.0/33")).as("한 칸 넘으면 못 읽는다").isEmpty();
        assertThat(IpRange.parse("10.0.1.0/0")).as("0 도 표기로는 성립한다").isPresent();
        assertThat(IpRange.parse("10.0.1.0/-1")).isEmpty();
        assertThat(IpRange.parse("fd00::/128")).as("v6 의 끝은 128 이다").isPresent();
        assertThat(IpRange.parse("fd00::/129")).isEmpty();
    }

    /**
     * <b>정규 생성자도 같은 두 끝을 본다.</b> 팩토리만 막으면 {@code new} 로 배열
     * 밖을 읽는 대역을 만들 수 있다.
     */
    @Test
    @DisplayName("생성자도_프리픽스의_두_끝을_본다")
    void 생성자도_프리픽스의_두_끝을_본다() {
        byte[] 주소 = {10, 0, 1, 0};

        assertThat(new IpRange(주소, 32).prefixBits()).isEqualTo(32);
        assertThat(new IpRange(주소, 0).prefixBits()).isZero();
        assertThatThrownBy(() -> new IpRange(주소, 33))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IpRange(주소, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>같은 대역은 같은 값이고 다른 대역은 다른 값이다.</b> 뒤엣것을 안 재면
     * 늘 같은 수를 내는 구현도 통과하고, 그러면 집합에 넣는 순간 전부 한 통에 쌓인다.
     */
    @Test
    @DisplayName("같은_대역은_같은_해시다")
    void 같은_대역은_같은_해시다() {
        IpRange 하나 = IpRange.parse("10.0.1.0/24").orElseThrow();
        IpRange 같은_것 = IpRange.parse("10.0.1.0/24").orElseThrow();
        IpRange 폭이_다른_것 = IpRange.parse("10.0.1.0/25").orElseThrow();
        IpRange 주소가_다른_것 = IpRange.parse("10.0.2.0/24").orElseThrow();

        assertThat(하나).isEqualTo(같은_것).hasSameHashCodeAs(같은_것);
        assertThat(하나.hashCode()).isNotEqualTo(폭이_다른_것.hashCode());
        assertThat(하나.hashCode()).isNotEqualTo(주소가_다른_것.hashCode());
    }
}
