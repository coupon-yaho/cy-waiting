package com.kafkick.waiting.domain.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

/**
 * 숫자 표기의 IP 만 바이트로 푼다. <b>이름은 안 푼다</b> — 푸는 순간 이름 조회가
 * 붙고, 그 결과는 검사한 순간과 연결하는 순간이 다를 수 있다. 조회 자체도 블로킹이라
 * 요청 경로와 배분 틱에 그대로 얹힌다.
 */
public final class IpLiteral {

    private static final int V4_OCTETS = 4;

    private static final int MAX_OCTET = 255;

    /**
     * 점 넷으로 끊긴 열 진수만. <b>{@code InetAddress} 에 안 맡긴다</b> — 그쪽은
     * {@code 1234} 를 {@code 0.0.4.210} 으로 읽고, 못 읽으면 이름으로 넘겨 조회한다.
     *
     * <p><b>앞자리 0 을 거절한다.</b> {@code 010} 을 8진수로 읽는 파서가 있어,
     * 받아 주면 검사한 값과 연결하는 값이 갈린다.
     */
    private static final Pattern V4 =
            Pattern.compile("(0|[1-9]\\d{0,2})(\\.(0|[1-9]\\d{0,2})){3}");

    /**
     * v6 리터럴의 모양. <b>콜론 유무만 보면 안 된다</b> — {@code zz::qq} 는 콜론이
     * 있어도 리터럴 판정을 건너뛰고 이름 조회로 간다(실측 9.4ms). 첫 글자까지
     * 묶어야 그 갈래가 닫힌다.
     */
    private static final Pattern V6_SHAPE = Pattern.compile("[0-9A-Fa-f:][0-9A-Fa-f:.]*");

    private IpLiteral() {
    }

    /**
     * <b>{@code Optional} 을 안 쓴다.</b> 요청 경로와 배분 틱에서 인스턴스마다 도는
     * 자리라 할당을 안 늘린다.
     *
     * @return 푼 바이트. 숫자 표기가 아니거나 못 읽으면 {@code null}
     */
    public static byte[] parse(String raw) {
        if (raw == null) {
            return null;
        }
        if (V4.matcher(raw).matches()) {
            String[] parts = raw.split("\\.");
            byte[] out = new byte[V4_OCTETS];
            for (int i = 0; i < V4_OCTETS; i++) {
                int octet = Integer.parseInt(parts[i]);
                if (octet > MAX_OCTET) {
                    return null;
                }
                out[i] = (byte) octet;
            }
            return out;
        }
        // **콜론과 글자 집합을 함께 본다.** 이름에 못 쓰는 콜론이 있고 v6 가 쓰는
        // 글자만 있으면 리터럴로만 읽히고, 못 읽으면 조회 없이 바로 거절이다.
        if (raw.indexOf(':') < 0 || !V6_SHAPE.matcher(raw).matches()) {
            return null;
        }
        try {
            return InetAddress.getByName(raw).getAddress();
        } catch (UnknownHostException e) {
            return null;
        }
    }
}
