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
     * <b>주소 길이와 같은 프리픽스를 받는다.</b> 한 칸 밀리면 `/32` 와 `/128` 이 못
     * 읽는 표기가 되는데, 신뢰 홉 목록은 못 읽는 표기를 조용히 버린다 — 한 대를
     * 지목한 홉이 목록에서 사라지고 리미터 키가 프록시 하나로 뭉친다.
     */
    @Test
    @DisplayName("프리픽스는_주소_길이까지만_받는다")
    void 프리픽스는_주소_길이까지만_받는다() {
        assertThat(IpRange.parse("10.0.1.0/32")).as("v4 의 끝은 32 다").isPresent();
        assertThat(IpRange.parse("fd00::/128")).as("v6 의 끝은 128 이다").isPresent();
        assertThat(IpRange.parse("fd00::/129")).isEmpty();
        // **여기서는 0 도 표기로 성립한다.** 넓은 대역을 막는 것은 부르는 쪽의 일이고,
        // 신뢰 홉 목록에는 그 하한이 아직 없다 (CY-917).
        assertThat(IpRange.parse("10.0.1.0/0")).isPresent();
    }

    /** 거절은 옆 시험이 든다. 여기는 <b>받아야 하는 두 끝</b>만 본다. */
    @Test
    @DisplayName("생성자도_주소_길이와_같은_폭을_받는다")
    void 생성자도_주소_길이와_같은_폭을_받는다() {
        byte[] 주소 = {10, 0, 1, 0};

        assertThat(new IpRange(주소, 32).prefixBits()).isEqualTo(32);
        assertThat(new IpRange(주소, 0).prefixBits()).isZero();
    }

    /**
     * <b>비트 수가 주소 수가 아니다.</b> 같은 폭이 v6 에서는 비교가 안 되게 넓어
     * 패밀리마다 하한이 다르고, 사설 대역은 정본 표기 자신이 하한이다.
     */
    @Test
    @DisplayName("패밀리마다_하한이_다르다")
    void 패밀리마다_하한이_다르다() {
        assertThat(IpRange.parse("10.0.0.0/8").orElseThrow().minimumPrefixBits()).isEqualTo(8);
        assertThat(IpRange.parse("2001:db8::/32").orElseThrow().minimumPrefixBits())
                .isEqualTo(32);
        assertThat(IpRange.parse("fc00::/7").orElseThrow().minimumPrefixBits())
                .as("정본 표기 그 자신은 받는다").isEqualTo(7);
        assertThat(IpRange.parse("fd00::/8").orElseThrow().minimumPrefixBits()).isEqualTo(7);
    }

    /** 하한을 한 비트라도 못 미치면 대역 하나로 안 본다. */
    @Test
    @DisplayName("하한에_한_비트_모자라면_너무_넓다")
    void 하한에_한_비트_모자라면_너무_넓다() {
        assertThat(IpRange.parse("10.0.0.0/8").orElseThrow().tooWide()).isFalse();
        assertThat(IpRange.parse("10.0.0.0/7").orElseThrow().tooWide()).isTrue();
        assertThat(IpRange.parse("2001:db8::/32").orElseThrow().tooWide()).isFalse();
        assertThat(IpRange.parse("2001:db8::/31").orElseThrow().tooWide()).isTrue();
        assertThat(IpRange.parse("fc00::/7").orElseThrow().tooWide()).isFalse();
        assertThat(IpRange.parse("fc00::/6").orElseThrow().tooWide()).isTrue();
    }
}
