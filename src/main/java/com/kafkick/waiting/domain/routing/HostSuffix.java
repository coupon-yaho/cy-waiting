package com.kafkick.waiting.domain.routing;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * 허용 목적지의 이름 항목. {@code .internal} 은 그 망의 이름 전부를,
 * {@code coupon-be} 는 그 이름 하나를 뜻한다.
 */
public record HostSuffix(String value) {

    /**
     * <b>{@link InstanceAddress} 와 같은 규칙이라야 한다.</b> 여기가 넓으면 영영
     * 안 맞을 항목이 조용히 들어가고, 증상은 "라우팅 후보 0" 이다.
     */
    private static final Pattern LABEL =
            Pattern.compile("[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?");

    /**
     * 기동에서 검증한다. 못 맞을 항목을 조용히 받으면 전면 거절이 설정 오타로
     * 일어나고 아무 데도 안 남는다.
     *
     * @throws IllegalArgumentException 라벨 규칙을 못 지킬 때
     */
    public static HostSuffix parse(String raw) {
        String host = raw.toLowerCase(Locale.ROOT);
        String bare = host.startsWith(".") ? host.substring(1) : host;
        if (bare.isEmpty() || bare.startsWith(".") || bare.endsWith(".")) {
            throw new IllegalArgumentException("허용 목적지의 이름을 못 읽는다: " + raw);
        }
        for (String label : bare.split("\\.", -1)) {
            if (!LABEL.matcher(label).matches()) {
                throw new IllegalArgumentException("허용 목적지의 이름을 못 읽는다: " + raw);
            }
        }
        return new HostSuffix(host);
    }

    /**
     * <b>라벨 경계에서 끊는다.</b> 문자열 끝만 보면 {@code evil-internal} 이
     * {@code .internal} 로 통과한다. 접미사와 같은 이름은 그 망 자체라 받는다.
     */
    public boolean matches(String host) {
        if (value.startsWith(".")) {
            return host.endsWith(value) || host.equals(value.substring(1));
        }
        return host.equals(value);
    }
}
