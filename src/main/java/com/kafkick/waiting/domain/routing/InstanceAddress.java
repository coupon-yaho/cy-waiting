package com.kafkick.waiting.domain.routing;

import com.kafkick.waiting.domain.net.IpLiteral;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * 뒷단 인스턴스가 보고에 실어 올린 자기 주소. <b>밖에서 오는 값이라</b>
 * 그대로 믿으면 게이트웨이가 아무 데나 요청을 보내는 통로가 된다. 스킴·경로·자격
 * 증명을 안 받는 것은, 받는 순간 주소가 아니라 뒷단이 정하는 URL 이 되기 때문이다.
 */
public record InstanceAddress(String host, int port) {

    /** 로그·지표로 그대로 흘러 들어가므로 길이를 자른다. */
    private static final int MAX_LENGTH = 255;

    private static final int MAX_PORT = 65535;

    /**
     * 라벨 하나. <b>점으로 가른 조각마다 본다</b> — 전체를 한 덩어리로 보면
     * {@code a..b} 나 {@code a.-b} 처럼 못 푸는 이름이 양 끝만 맞아 통과한다.
     * 라벨 길이 상한 63 도 조각 단위라야 걸린다.
     */
    private static final Pattern LABEL =
            Pattern.compile("[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?");

    /**
     * <b>정규 생성자에서 막는다.</b> 목적지 판정이 호스트를 그대로 리터럴이나 이름으로
     * 읽는데, {@code new} 가 무검증이면 그 전제가 관례일 뿐이다.
     */
    public InstanceAddress {
        if (host == null || host.isEmpty() || host.length() > MAX_LENGTH) {
            throw new IllegalArgumentException("호스트가 없거나 너무 길다: " + host);
        }
        // **대괄호는 표기이지 값이 아니다.** 벗긴 모양만 든다 — 목적지 판정과
        // 고르개가 리터럴을 그대로 읽으므로, 씌운 채 담으면 둘 다 못 읽는다.
        if (host.indexOf(':') >= 0) {
            if (IpLiteral.parse(host) == null) {
                throw new IllegalArgumentException("콜론 든 호스트가 v6 리터럴이 아니다: " + host);
            }
        } else {
            // **끝점을 안 받는다.** `a.b.` 처럼 점으로 끝나면 마지막 조각이 비고,
            // 빈 라벨은 못 푸는 이름이다. 앞도 같다.
            if (host.startsWith(".") || host.endsWith(".")) {
                throw new IllegalArgumentException("호스트가 점으로 끝난다: " + host);
            }
            for (String label : host.split("\\.", -1)) {
                if (!LABEL.matcher(label).matches()) {
                    throw new IllegalArgumentException("호스트의 라벨을 못 읽는다: " + host);
                }
            }
        }
        if (port < 1 || port > MAX_PORT) {
            throw new IllegalArgumentException("포트는 1.." + MAX_PORT + " 여야 한다: " + port);
        }
    }

    /**
     * <b>v6 는 대괄호 표기로만 받는다.</b> 안 씌우면 {@code ::1:80} 이 호스트
     * {@code ::1} 에 포트 80 으로도, 포트 없는 호스트로도 읽힌다 — 어느 쪽인지
     * 정할 근거가 값 안에 없다.
     *
     * @return 모양을 지킨 주소. 아니면 비어 있다 — 그 인스턴스는 라우팅 후보가 아니다
     */
    public static Optional<InstanceAddress> parse(String raw) {
        if (raw == null || raw.isEmpty() || raw.length() > MAX_LENGTH) {
            return Optional.empty();
        }
        String host;
        String port;
        if (raw.charAt(0) == '[') {
            int close = raw.indexOf(']');
            if (close < 0 || close + 1 >= raw.length() || raw.charAt(close + 1) != ':') {
                return Optional.empty();
            }
            host = raw.substring(1, close);
            // **대괄호는 v6 에만 쓴다.** 이름을 싸서 넣는 길을 내면 콜론 검사를
            // 우회하는 통로가 되고, 그 이름은 목적지 판정에서 리터럴로도 안 읽힌다.
            if (host.indexOf(':') < 0) {
                return Optional.empty();
            }
            port = raw.substring(close + 2);
        } else {
            int colon = raw.indexOf(':');
            if (colon <= 0 || colon != raw.lastIndexOf(':') || colon == raw.length() - 1) {
                return Optional.empty();
            }
            host = raw.substring(0, colon);
            port = raw.substring(colon + 1);
        }
        try {
            return Optional.of(new InstanceAddress(host, Integer.parseInt(port)));
        } catch (IllegalArgumentException e) {
            // 모양이 어긋난 것은 정상 실패다. 부르는 쪽이 그 인스턴스를 후보에서 뺀다.
            return Optional.empty();
        }
    }

    /** <b>다시 읽힐 표기로 낸다.</b> 스냅샷이 이 문자열로 실려 나가고 받는 쪽이 푼다. */
    @Override
    public String toString() {
        return host.indexOf(':') < 0 ? host + ":" + port : "[" + host + "]:" + port;
    }
}
