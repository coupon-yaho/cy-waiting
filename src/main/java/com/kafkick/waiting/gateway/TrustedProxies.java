package com.kafkick.waiting.gateway;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 전달 헤더를 믿어도 되는 홉.
 *
 * <p><b>아무나 채워 넣게 두면 상한이 무의미해진다.</b> 매 요청 다른 값을 넣어
 * 키를 무한히 만들면 리미터가 포화하고, 그때부터 정상 사용자가 막힌다.
 *
 * @param cidrs 신뢰하는 대역. 비어 있으면 아무 헤더도 안 믿는다
 */
@ConfigurationProperties("waiting.proxy")
public record TrustedProxies(List<String> cidrs) {

    private static final int BITS_PER_BYTE = 8;

    /** 숫자 표기만 받는다. 이름이 들어오면 조회가 요청 경로에 붙는다. */
    private static final Pattern LITERAL = Pattern.compile("^[0-9.:a-fA-F\\[\\]]+$");

    public TrustedProxies {
        cidrs = cidrs == null ? List.of() : List.copyOf(cidrs);
    }

    /**
     * 이 주소가 신뢰하는 홉인가.
     *
     * <p><b>기본은 안 믿는 것이다.</b> 헤더를 믿는 것은 앞단이 그 헤더를
     * 덮어쓴다는 보장이 있을 때만이다.
     */
    public boolean isTrusted(String address) {
        Objects.requireNonNull(address, "address 는 필수다");
        byte[] target = parse(address);
        return target != null && cidrs.stream().anyMatch(cidr -> contains(cidr, target));
    }

    /**
     * <b>대역을 비트로 자른다.</b> 접두 문자열로 보면 {@code 10.0.0.0/8} 이
     * 아무 주소도 안 잡는다 — 그 표기가 문자열로는 어디에도 안 붙기 때문이다.
     */
    private boolean contains(String cidr, byte[] target) {
        int mark = cidr.indexOf('/');
        byte[] network = parse(mark < 0 ? cidr : cidr.substring(0, mark));
        if (network == null || network.length != target.length) {
            return false;
        }
        int prefix = mark < 0 ? network.length * BITS_PER_BYTE : prefixLength(cidr, mark, network);
        if (prefix < 0) {
            return false;
        }
        for (int i = 0; i < prefix / BITS_PER_BYTE; i++) {
            if (network[i] != target[i]) {
                return false;
            }
        }
        int rest = prefix % BITS_PER_BYTE;
        if (rest == 0) {
            return true;
        }
        int mask = ~((1 << (BITS_PER_BYTE - rest)) - 1);
        return (network[prefix / BITS_PER_BYTE] & mask) == (target[prefix / BITS_PER_BYTE] & mask);
    }

    /** 못 읽는 표기는 신뢰하지 않는다. 오타 하나가 전 대역을 여는 것보다 낫다. */
    private int prefixLength(String cidr, int mark, byte[] network) {
        try {
            int bits = Integer.parseInt(cidr.substring(mark + 1));
            return bits < 0 || bits > network.length * BITS_PER_BYTE ? -1 : bits;
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /** 이름을 안 찾는다. 설정에 호스트명이 들어오면 그 조회가 요청 경로에 붙는다. */
    private byte[] parse(String address) {
        // 숫자 표기만 받는다. 이름이 오면 조회가 일어나므로 그 전에 막는다.
        if (address.isEmpty() || !LITERAL.matcher(address).matches()) {
            return null;
        }
        try {
            return InetAddress.getByName(address).getAddress();
        } catch (UnknownHostException e) {
            return null;
        }
    }
}
