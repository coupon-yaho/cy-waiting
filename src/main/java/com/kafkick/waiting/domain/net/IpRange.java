package com.kafkick.waiting.domain.net;

import java.util.Optional;

/**
 * CIDR 대역 하나. 주소와 유효 비트 수다.
 *
 * <p><b>사본을 두지 않는다.</b> 신뢰하는 홉과 연결해도 되는 목적지가 각자 같은
 * 산술을 들면, 한쪽만 고쳤을 때 두 판정이 갈린다.
 */
public record IpRange(byte[] address, int prefixBits) {

    private static final int BITS_PER_BYTE = 8;

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

    /** 이 대역이 그 주소를 품는가. 길이가 다르면 안 품는다 — v4 대역에 v6 주소다. */
    public boolean contains(byte[] target) {
        if (target == null || address.length != target.length) {
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
}
