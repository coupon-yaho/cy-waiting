package com.kafkick.waiting.gateway;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * 전달 헤더를 믿어도 되는 홉.
 *
 * <p><b>아무나 채워 넣게 두면 상한이 무의미해진다.</b> 매 요청 다른 값을 넣어
 * 키를 무한히 만들면 리미터가 포화하고, 그때부터 정상 사용자가 막힌다.
 *
 * @param cidrs 신뢰하는 대역. 비어 있으면 아무 헤더도 안 믿는다
 */
public final class TrustedProxies {

    private static final int BITS_PER_BYTE = 8;

    /**
     * 숫자 표기만 받는 형태.
     *
     * <p>이름이 섞이면 그 조회가 요청 경로에 붙는다. {@code dead.beef} 처럼
     * 주소처럼 생긴 이름도 여기서 걸러야 한다.
     */
    private static final Pattern NUMERIC = Pattern.compile(
            "^(\\d{1,3}(\\.\\d{1,3}){3}|[0-9a-fA-F:]+)$");

    /**
     * 미리 푼 대역. <b>요청마다 다시 풀지 않는다</b> — 그 파싱이 요청 경로에 붙고,
     * 이름이 섞이면 이름 조회까지 거기서 일어난다.
     */
    private final List<Network> networks;

    private TrustedProxies(List<String> cidrs) {
        this.networks = (cidrs == null ? List.<String>of() : cidrs).stream()
                .map(Network::parse)
                .filter(Objects::nonNull)
                .toList();
    }

    /** 설정에서 만든다. 못 읽는 표기는 여기서 버려 요청 경로에 안 남긴다. */
    public static TrustedProxies of(List<String> cidrs) {
        return new TrustedProxies(cidrs);
    }

    /** 대역 하나. 주소와 유효 비트 수다. */
    private record Network(byte[] address, int prefixBits) {

        /** 못 읽는 표기는 버린다. 오타 하나가 전 대역을 여는 것보다 낫다. */
        static Network parse(String cidr) {
            int mark = cidr.indexOf('/');
            byte[] address = literal(mark < 0 ? cidr : cidr.substring(0, mark));
            if (address == null) {
                return null;
            }
            int bits = address.length * BITS_PER_BYTE;
            if (mark < 0) {
                return new Network(address, bits);
            }
            try {
                int prefix = Integer.parseInt(cidr.substring(mark + 1));
                return prefix < 0 || prefix > bits ? null : new Network(address, prefix);
            } catch (NumberFormatException e) {
                return null;
            }
        }
    }

    /**
     * 이 주소가 신뢰하는 홉인가.
     *
     * <p><b>기본은 안 믿는 것이다.</b> 헤더를 믿는 것은 앞단이 그 헤더를
     * 덮어쓴다는 보장이 있을 때만이다.
     */
    public boolean isTrusted(String address) {
        Objects.requireNonNull(address, "address 는 필수다");
        byte[] target = literal(address);
        return target != null && networks.stream().anyMatch(net -> contains(net, target));
    }

    /**
     * <b>대역을 비트로 자른다.</b> 접두 문자열로 보면 {@code 10.0.0.0/8} 이
     * 아무 주소도 안 잡는다 — 그 표기가 문자열로는 어디에도 안 붙기 때문이다.
     */
    private boolean contains(Network net, byte[] target) {
        if (net.address().length != target.length) {
            return false;
        }
        int whole = net.prefixBits() / BITS_PER_BYTE;
        for (int i = 0; i < whole; i++) {
            if (net.address()[i] != target[i]) {
                return false;
            }
        }
        int rest = net.prefixBits() % BITS_PER_BYTE;
        if (rest == 0) {
            return true;
        }
        int mask = ~((1 << (BITS_PER_BYTE - rest)) - 1);
        return (net.address()[whole] & mask) == (target[whole] & mask);
    }


    /**
     * 숫자 표기만 읽는다. <b>이름을 안 찾는다</b> — 조회가 일어나면 그 대기가
     * 요청 경로에 붙는다.
     */
    static byte[] literal(String address) {
        // 숫자 표기만 통과시킨 뒤 푼다. 이름이 오면 여기서 조회가 일어난다.
        if (address == null || !NUMERIC.matcher(address).matches()) {
            return null;
        }
        try {
            return InetAddress.getByName(address).getAddress();
        } catch (UnknownHostException e) {
            return null;
        }
    }
}
