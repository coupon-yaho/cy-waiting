package com.kafkick.waiting.gateway;

import java.util.List;
import java.util.Objects;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 전달 헤더를 믿어도 되는 홉.
 *
 * <p><b>아무나 채워 넣게 두면 상한이 무의미해진다.</b> 매 요청 다른 값을 넣어
 * 상한을 넘고, 더 나쁘게는 키를 무한히 만들어 상한에 닿게 한다 — 그때부터
 * 정상 사용자가 막힌다. 남용 방지가 서비스 거부 수단이 된다.
 *
 * @param cidrs 신뢰하는 대역. 비어 있으면 아무 헤더도 안 믿는다
 */
@ConfigurationProperties("waiting.proxy")
public record TrustedProxies(List<String> cidrs) {

    public TrustedProxies {
        cidrs = cidrs == null ? List.of() : List.copyOf(cidrs);
    }

    /**
     * 이 주소가 신뢰하는 홉인가.
     *
     * <p><b>기본은 안 믿는 것이다.</b> 설정이 비면 소켓 주소만 쓴다 — 헤더를
     * 믿는 것은 앞단이 그 헤더를 덮어쓴다는 보장이 있을 때만이다.
     */
    public boolean isTrusted(String address) {
        Objects.requireNonNull(address, "address 는 필수다");
        return cidrs.stream().anyMatch(cidr -> matches(cidr, address));
    }

    /**
     * 접두로 본다. 대역을 비트로 자르는 것은 여기서 필요 없다 — 신뢰 홉은 몇
     * 대뿐이고, 그 목록은 배포가 정한다.
     */
    private boolean matches(String cidr, String address) {
        String prefix = cidr.endsWith(".") ? cidr : cidr + ".";
        return address.equals(cidr) || address.startsWith(prefix);
    }
}
