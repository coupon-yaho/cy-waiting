package com.kafkick.waiting.domain.routing;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * 뒷단 인스턴스가 보고에 실어 올린 자기 주소 (D-C1 · A-11).
 *
 * <p><b>밖에서 오는 값이다.</b> 뒷단이 쓰고 게이트웨이가 읽어 그리로 연결하므로,
 * 그대로 믿으면 게이트웨이가 아무 데나 요청을 보내는 통로가 된다.
 */
// 스킴·경로·자격 증명을 안 받는다. 받는 순간 이 값이 주소가 아니라 URL 이 되고,
// 그 URL 이 어디를 가리킬지는 뒷단을 쥔 쪽이 정한다.
public record InstanceAddress(String host, int port) {

    /** 로그·지표로 그대로 흘러 들어가므로 길이를 자른다. */
    private static final int MAX_LENGTH = 255;

    private static final Pattern HOST = Pattern.compile("[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?");

    /** @return 모양을 지킨 주소. 아니면 비어 있다 — 그 인스턴스는 라우팅 후보가 아니다 */
    public static Optional<InstanceAddress> parse(String raw) {
        if (raw == null || raw.isEmpty() || raw.length() > MAX_LENGTH) {
            return Optional.empty();
        }
        int colon = raw.lastIndexOf(':');
        if (colon <= 0 || colon == raw.length() - 1) {
            return Optional.empty();
        }
        String host = raw.substring(0, colon);
        if (!HOST.matcher(host).matches()) {
            return Optional.empty();
        }
        int port;
        try {
            port = Integer.parseInt(raw.substring(colon + 1));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
        if (port < 1 || port > 65535) {
            return Optional.empty();
        }
        return Optional.of(new InstanceAddress(host, port));
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
