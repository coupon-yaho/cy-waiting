package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.net.IpLiteral;
import com.kafkick.waiting.domain.net.IpRange;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 전달 헤더를 믿어도 되는 홉. <b>아무나 채워 넣게 두면 상한이 무의미해진다</b> — 매 요청
 * 다른 값으로 키를 무한히 만들면 리미터가 포화하고, 그때부터 정상 사용자가 막힌다.
 *
 * @param cidrs 신뢰하는 대역. 비어 있으면 아무 헤더도 안 믿는다
 */
public final class TrustedProxies {

    /**
     * 미리 푼 대역. <b>요청마다 다시 풀지 않는다</b> — 그 파싱이 요청 경로에 붙고,
     * 이름이 섞이면 이름 조회까지 거기서 일어난다.
     */
    private final List<IpRange> networks;

    private TrustedProxies(List<String> cidrs) {
        // 못 읽는 표기는 버린다. 오타 하나가 전 대역을 여는 것보다 낫다.
        this.networks = (cidrs == null ? List.<String>of() : cidrs).stream()
                .map(IpRange::parse)
                .flatMap(Optional::stream)
                .toList();
    }

    /** 설정에서 만든다. 못 읽는 표기는 여기서 버려 요청 경로에 안 남긴다. */
    public static TrustedProxies of(List<String> cidrs) {
        return new TrustedProxies(cidrs);
    }

    /**
     * 이 주소가 신뢰하는 홉인가. <b>기본은 안 믿는 것이다</b> — 헤더를 믿는 것은
     * 앞단이 그 헤더를 덮어쓴다는 보장이 있을 때만이다.
     */
    public boolean isTrusted(String address) {
        Objects.requireNonNull(address, "address 는 필수다");
        byte[] target = IpLiteral.parse(address);
        return target != null && networks.stream().anyMatch(net -> net.contains(target));
    }
}
