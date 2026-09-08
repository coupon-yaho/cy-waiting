package com.kafkick.waiting.domain.routing;

import com.kafkick.waiting.domain.net.IpLiteral;
import com.kafkick.waiting.domain.net.IpRange;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 뒷단이 보고한 주소를 연결해도 되는가. <b>{@link InstanceAddress} 와 다른 질문이다</b> —
 * 거기는 읽을 수 있는 주소인지를 보고, 여기는 그것이 우리 뒷단의 자리인지를 본다.
 * 보고에 쓸 수 있는 쪽이 게이트웨이를 임의 주소로 향하게 하는 것을 막는 자리다.
 */
public final class AllowedDestinations {

    private static final int MAX_PORT = 65535;

    /** 주소를 쓰다 만 모양. 점으로 끊긴 열 진수인데 넷이 아니다. */
    private static final Pattern PARTIAL_ADDRESS =
            Pattern.compile("\\d{1,3}(\\.\\d{1,3}){0,2}");

    private final List<HostSuffix> suffixes;

    private final List<IpRange> ranges;

    /**
     * 연결해도 되는 포트. <b>호스트만 보면 반쪽이다</b> — 허용한 망 안의 아무 포트나
     * 고를 수 있으면 같은 망의 다른 서비스가 그대로 대상이 된다.
     */
    private final Set<Integer> ports;

    /** 제한이 없으면 참. 라우팅이 꺼진 배포의 상태다. */
    private final boolean unrestricted;

    private AllowedDestinations(List<HostSuffix> suffixes, List<IpRange> ranges,
            Set<Integer> ports, boolean unrestricted) {
        this.suffixes = suffixes;
        this.ranges = ranges;
        this.ports = ports;
        this.unrestricted = unrestricted;
    }

    /**
     * <b>아무 데나 보내도 되는 상태를 이름으로 남긴다.</b> 인자를 빠뜨려 조용히
     * 무제한이 되는 것과, 그렇게 부르겠다고 적는 것은 다르다.
     */
    public static AllowedDestinations unrestricted() {
        return new AllowedDestinations(List.of(), List.of(), Set.of(), true);
    }

    /**
     * 이름 접미사({@code .internal}), 대역({@code 10.0.1.0/24}·{@code fd00::/8}), 맨
     * 주소를 섞어 받는다. <b>여기에는 대괄호를 안 쓴다</b> — 보고 표기와 반대다.
     *
     * <p><b>빈 목록을 안 받는다.</b> 안 적은 것이 전부 허용이 되면 설정을 빠뜨린
     * 배포가 그대로 통로가 되고, 그 사실은 아무 데도 안 남는다. 포트도 같다 —
     * 호스트만 보면 허용한 망 안의 아무 서비스나 대상이 된다.
     *
     * @throws IllegalArgumentException 비었거나 못 읽는 항목이 있을 때
     */
    public static AllowedDestinations of(List<String> entries, Collection<Integer> ports) {
        if (entries == null || entries.isEmpty()) {
            throw new IllegalArgumentException("허용 목적지가 비면 라우팅을 켤 수 없다");
        }
        if (ports == null || ports.isEmpty()) {
            throw new IllegalArgumentException("허용 포트가 비면 라우팅을 켤 수 없다");
        }
        for (Integer port : ports) {
            if (port == null || port < 1 || port > MAX_PORT) {
                throw new IllegalArgumentException("허용 포트가 범위 밖이다: " + port);
            }
        }
        List<HostSuffix> suffixes = new ArrayList<>();
        List<IpRange> ranges = new ArrayList<>();
        for (String entry : entries) {
            if (entry == null || entry.isBlank()) {
                throw new IllegalArgumentException("허용 목적지에 빈 항목이 있다");
            }
            String trimmed = entry.trim();
            // **주소를 쓰다 만 것을 이름으로 받지 않는다.** `10.0.1` 은 라벨이 다
            // 유효해 접미사로 들어가고, 그 접미사는 영영 아무것도 안 맞는다.
            if (PARTIAL_ADDRESS.matcher(trimmed).matches()) {
                throw new IllegalArgumentException("허용 목적지의 주소가 덜 적혔다: " + entry);
            }
            // 주소나 대역이면 대역으로, 아니면 이름으로 본다. 맨 주소를 이름으로
            // 넣으면 이름끼리만 견주므로 그 주소를 보고한 뒷단이 도리어 거절된다.
            if (trimmed.indexOf('/') >= 0 || IpLiteral.parse(trimmed) != null) {
                IpRange range = IpRange.parse(trimmed).orElseThrow(() ->
                        new IllegalArgumentException("허용 목적지의 대역을 못 읽는다: " + entry));
                // **전면 개방을 이름 없이 만들지 않는다.** 빈 목록은 막으면서 `/0` 을
                // 받으면 같은 결과가 표기 하나로 조용히 선다.
                if (range.prefixBits() == 0) {
                    throw new IllegalArgumentException(
                            "허용 목적지에 전 대역을 적을 수 없다: " + entry);
                }
                ranges.add(range);
            } else {
                suffixes.add(HostSuffix.parse(trimmed));
            }
        }
        return new AllowedDestinations(List.copyOf(suffixes), List.copyOf(ranges),
                Set.copyOf(ports), false);
    }

    /** 항목 하나라도 맞으면 받는다. 다 안 맞으면 그 인스턴스는 라우팅 후보가 아니다. */
    public boolean permits(InstanceAddress address) {
        if (unrestricted) {
            return true;
        }
        // **포트를 먼저 본다.** 허용한 망 안의 아무 포트나 고를 수 있으면 같은 망의
        // 다른 서비스가 그대로 연결 대상이 된다.
        if (!ports.contains(address.port())) {
            return false;
        }
        String host = address.host().toLowerCase(Locale.ROOT);
        byte[] literal = IpLiteral.parse(host);
        // **이름을 대역으로 안 풀어 준다.** 풀려면 이름 조회가 필요하고, 그 결과는
        // 검사한 순간과 연결하는 순간이 다를 수 있다.
        if (literal != null) {
            return ranges.stream().anyMatch(range -> range.contains(literal));
        }
        return suffixes.stream().anyMatch(suffix -> suffix.matches(host));
    }
}
