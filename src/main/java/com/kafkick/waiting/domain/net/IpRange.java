package com.kafkick.waiting.domain.net;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * CIDR 대역 하나. 주소와 유효 비트 수다.
 *
 * <p><b>사본을 두지 않는다.</b> 신뢰하는 홉과 연결해도 되는 목적지가 각자 같은
 * 산술을 들면, 한쪽만 고쳤을 때 두 판정이 갈린다.
 */
public record IpRange(byte[] address, int prefixBits) {

    private static final int BITS_PER_BYTE = 8;

    private static final int V4_BYTES = 4;

    private static final int V6_BYTES = 16;

    /**
     * v4 대역 프리픽스의 하한. 내부망이 쓰는 가장 넓은 폭이 `10.0.0.0/8` 이고
     * 그보다 넓은 것은 내부망일 수 없다.
     */
    private static final int MIN_V4_PREFIX_BITS = 8;

    /**
     * v6 대역 프리픽스의 하한. <b>비트 수가 주소 수가 아니다</b> — 같은 `/8` 이
     * v6 에서는 v4 전체의 2의 88승 배다. 그래서 훨씬 좁게 받는다.
     */
    private static final int MIN_V6_PREFIX_BITS = 32;

    /**
     * 사설 대역(ULA)의 하한. 정본 표기가 `fc00::/7` 이라 위 하한으로는 못 적는데,
     * 그 대역은 정의상 내부망이라 그 자신은 받는다.
     */
    private static final int MIN_ULA_PREFIX_BITS = 7;

    /**
     * <b>정규 생성자도 막는다.</b> 팩토리만 검증하면 {@code new} 로 만들 수 없는
     * 대역이 생기고, 그건 아무 주소도 안 잡거나 배열 밖을 읽는다.
     */
    public IpRange {
        Objects.requireNonNull(address, "address 는 필수다");
        // **길이가 둘뿐이다.** 안 막으면 빈 배열로도 만들어지고, 그 대역에 하한을
        // 물으면 첫 바이트를 읽다 터진다.
        if (address.length != V4_BYTES && address.length != V6_BYTES) {
            throw new IllegalArgumentException(
                    "주소는 4 바이트나 16 바이트여야 한다: " + address.length);
        }
        if (prefixBits < 0 || prefixBits > address.length * BITS_PER_BYTE) {
            throw new IllegalArgumentException(
                    "프리픽스는 0..%d 여야 한다: %d".formatted(
                            address.length * BITS_PER_BYTE, prefixBits));
        }
        address = address.clone();
    }

    /** @return 읽은 대역. 표기가 어긋나면 비어 있다 */
    public static Optional<IpRange> parse(String cidr) {
        if (cidr == null) {
            return Optional.empty();
        }
        int mark = cidr.indexOf('/');
        byte[] address = IpLiteral.parse(mark < 0 ? cidr : cidr.substring(0, mark));
        if (address == null) {
            return Optional.empty();
        }
        int full = address.length * BITS_PER_BYTE;
        // 프리픽스를 안 적었으면 그 한 대를 뜻한다.
        if (mark < 0) {
            return Optional.of(new IpRange(address, full));
        }
        int bits;
        try {
            bits = Integer.parseInt(cidr.substring(mark + 1));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
        return bits < 0 || bits > full ? Optional.empty()
                : Optional.of(new IpRange(address, bits));
    }

    /** <b>사본을 준다.</b> 살아 있는 배열을 넘기면 값 타입이 밖에서 바뀐다. */
    @Override
    public byte[] address() {
        return address.clone();
    }

    /** record 기본 동등성은 배열에 참조를 쓴다. 같은 대역 둘이 안 같아진다. */
    @Override
    public boolean equals(Object other) {
        return other instanceof IpRange that
                && prefixBits == that.prefixBits
                && Arrays.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(address) * 31 + prefixBits;
    }

    @Override
    public String toString() {
        return "IpRange[/" + prefixBits + "]";
    }

    /**
     * 이 대역이 그 주소를 품는가. 길이가 다르면 안 품는다 — 신뢰 홉 판정에서
     * v6 피어가 v4 대역을 만나는 조합이 실제로 난다.
     */
    public boolean contains(byte[] target) {
        Objects.requireNonNull(target, "target 은 필수다");
        if (address.length != target.length) {
            return false;
        }
        int whole = prefixBits / BITS_PER_BYTE;
        for (int i = 0; i < whole; i++) {
            if (address[i] != target[i]) {
                return false;
            }
        }
        int rest = prefixBits % BITS_PER_BYTE;
        if (rest == 0) {
            return true;
        }
        // 부호 확장은 양변에 똑같이 걸려 결과를 안 뒤집는다.
        int mask = ~((1 << (BITS_PER_BYTE - rest)) - 1);
        return (address[whole] & mask) == (target[whole] & mask);
    }

    /** v4 대역인가. 표기와 실제가 갈리는지 보는 자리가 이걸 묻는다. */
    public boolean isV4() {
        return address.length == V4_BYTES;
    }

    /**
     * 이 대역이 지켜야 하는 프리픽스 하한. <b>패밀리마다 다르다</b> — 비트 수가
     * 주소 수가 아니라, 같은 폭이 v6 에서는 비교가 안 되게 넓다.
     */
    public int minimumPrefixBits() {
        if (isV4()) {
            return MIN_V4_PREFIX_BITS;
        }
        // fc00::/7. 정의상 내부망이라 정본 표기 그 자신은 받는다.
        return (address[0] & 0xfe) == 0xfc ? MIN_ULA_PREFIX_BITS : MIN_V6_PREFIX_BITS;
    }

    /**
     * 대역 하나로 보기엔 너무 넓은가. <b>v6 에는 눈에 띄는 신호가 없다</b> —
     * `2000::/3` 하나로 공인 유니캐스트 전부가 열리는데 표기는 대역 하나처럼 읽힌다.
     */
    public boolean tooWide() {
        return prefixBits < minimumPrefixBits();
    }
}
