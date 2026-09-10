package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.net.IpLiteral;
import com.kafkick.waiting.domain.net.IpRange;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 전달 헤더를 믿어도 되는 홉. <b>아무나 채워 넣게 두면 상한이 무의미해진다</b> — 매 요청
 * 다른 값으로 키를 무한히 만들면 리미터가 포화하고, 그때부터 정상 사용자가 막힌다.
 *
 * <p><b>오타와 진술을 가른다.</b> 못 읽는 표기는 버리면 좁아지므로 버리고, 너무 넓거나
 * 홉일 수 없는 대역은 오타가 아니라 의도라 조용히 뒤집지 않고 던진다.
 */
public final class TrustedProxies {

    private static final Logger log = LoggerFactory.getLogger(TrustedProxies.class);

    /** 로그와 예외가 가리킬 설정 키. 어느 값을 고칠지가 안 적히면 로그가 반쪽이다. */
    private static final String KEY = "waiting.proxy.cidrs";

    /**
     * 미리 푼 대역. <b>요청마다 다시 풀지 않는다</b> — 그 파싱이 요청 경로에 붙고,
     * 이름이 섞이면 이름 조회까지 거기서 일어난다.
     */
    private final List<IpRange> networks;

    private TrustedProxies(List<String> cidrs) {
        List<IpRange> parsed = new ArrayList<>();
        for (String raw : cidrs == null ? List.<String>of() : cidrs) {
            // **양끝을 턴다.** 환경변수에 쉼표로 여럿을 적으면 둘째부터 공백이 붙어
            // 오고, 안 털면 파싱이 실패해 신뢰 범위가 조용히 좁아진다.
            String entry = raw == null ? "" : raw.strip();
            // 빈 줄은 안 적은 것과 같다. 기본 배포가 빈 값을 한 줄로 주는 자리다.
            if (entry.isEmpty()) {
                continue;
            }
            Optional<IpRange> range = IpRange.parse(entry);
            if (range.isEmpty()) {
                // 원문을 싣는다. 대역만 찍으면 어느 줄이 틀렸는지 못 찾는다. 줄바꿈은
                // 수집된 로그에서 줄을 위조하므로 지운다.
                log.error("{} 의 신뢰 홉 표기를 못 읽어 버린다 — 그만큼 전달 헤더를 "
                        + "안 믿는다: \"{}\"", KEY, entry.replaceAll("\\R", " "));
                continue;
            }
            parsed.add(usableAsHop(range.orElseThrow(), entry));
        }
        this.networks = List.copyOf(parsed);
    }

    /**
     * 홉으로 쓸 수 있는 대역인가. <b>전 대역 한 줄이면 모든 홉이 신뢰 홉이 되고</b>,
     * v4 를 실어 나르는 v6 대역은 번역된 인터넷 전부를 한 줄로 연다.
     */
    private IpRange usableAsHop(IpRange range, String entry) {
        // **v6 표기가 v4 규칙으로 서는 것을 막는다.** 매핑을 풀면 네 바이트라 v4
        // 하한이 걸려, 적은 표기와 실제로 믿는 범위가 갈린다.
        if (entry.indexOf(':') >= 0 && range.isV4()) {
            throw new IllegalArgumentException(
                    "%s 에 v4 매핑 표기를 쓸 수 없다: %s".formatted(KEY, entry));
        }
        if (range.tooWide()) {
            throw new IllegalArgumentException(
                    "%s 의 신뢰 홉 대역이 너무 넓다 — 프리픽스가 %d 비트 이상이어야 한다: %s"
                            .formatted(KEY, range.minimumPrefixBits(), entry));
        }
        if (!IpLiteral.routable(range.address())) {
            throw new IllegalArgumentException(
                    ("%s 에 신뢰 홉으로 쓸 수 없는 대역이 있다 — v4 를 실어 나르거나 "
                            + "한 대를 안 가리킨다: %s").formatted(KEY, entry));
        }
        return range;
    }

    /**
     * 설정에서 만든다. 못 읽는 표기는 여기서 버려 요청 경로에 안 남기고, 너무 넓거나
     * 홉일 수 없는 대역은 <b>던져 기동을 멈춘다</b>.
     */
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
