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

    /**
     * 이 주소로 요청을 보내도 되는가. <b>한 대를 가리키는 주소만 참이다.</b>
     *
     * <p>미지정은 "아무 대나" 라 목적지가 아니고, 링크 로컬은 어느 링크인지가 빠져
     * 있으며, 멀티캐스트와 브로드캐스트는 애초에 한 대가 아니다.
     *
     * <p><b>루프백은 참이다</b> — 같은 호스트의 뒷단이 실제 배치 모양이다.
     */
    public static boolean routable(byte[] address) {
        if (address == null) {
            return false;
        }
        int first = address[0] & 0xff;
        if (address.length == V4_OCTETS) {
            // 0.0.0.0/8 · 169.254.0.0/16 · 224.0.0.0/4 · 255.255.255.255
            if (first == 0 || first >= 224) {
                return false;
            }
            if (first == 169 && (address[1] & 0xff) == 254) {
                return false;
            }
            // 192.88.99.0/24 — 6to4 릴레이 애니캐스트라 한 대가 아니다.
            return !(first == 192 && (address[1] & 0xff) == 88 && (address[2] & 0xff) == 99);
        }
        if (first == 0xff) {
            return false;
        }
        // fe80::/10. 다음 바이트의 위 두 비트까지 봐야 fec0::/10 과 안 섞인다.
        // **fec0::/10 은 참으로 둔다** — 폐기된 대역이지만 한 대를 가리키기는 한다.
        if (first == 0xfe && (address[1] & 0xc0) == 0x80) {
            return false;
        }
        // **v4 를 v6 표기 안에 실어 나르는 것들.** 앞 열두 바이트가 0 이면 v4 호환
        // 표기라 뒤의 넉 자를 v4 로 쓰고, 그러면 여기 판정이 v4 규칙을 안 거친다 —
        // `::127.0.0.1` 이 목적지가 된다. `::1` 만 남기고 거절한다.
        // 미지정(`::`)도 여기서 걸린다 — 앞 열두 바이트가 0 이고 뒤가 `::1` 이 아니다.
        return !embedsV4(address);
    }

    /**
     * v4 주소를 실어 나르는 v6 표기인가. <b>한 대를 가리키는지가 v6 규칙으로 안
     * 갈린다</b> — 번역되는 순간 어디로 가는지는 안에 실린 v4 가 정한다.
     *
     * <p>RULE-EXCEPTION(JS-13): 주소를 푸는 유틸리티라 인스턴스가 없다.
     */
    private static boolean embedsV4(byte[] address) {
        // 6to4 `2002::/16` · Teredo `2001::/32`
        if ((address[0] & 0xff) == 0x20 && (address[1] & 0xff) == 0x02) {
            return true;
        }
        if ((address[0] & 0xff) == 0x20 && (address[1] & 0xff) == 0x01
                && address[2] == 0 && address[3] == 0) {
            return true;
        }
        // NAT64 `64:ff9b::/96`
        if ((address[0] & 0xff) == 0x00 && (address[1] & 0xff) == 0x64
                && (address[2] & 0xff) == 0xff && (address[3] & 0xff) == 0x9b) {
            return true;
        }
        // v4 호환 `::a.b.c.d`. 앞 열두 바이트가 0 인 자리다 — `::1` 은 빼고 본다.
        for (int i = 0; i < 12; i++) {
            if (address[i] != 0) {
                return false;
            }
        }
        // 앞이 다 0 이면 남는 것은 뒤 넉 자다. 한 수로 접어 `::1` 하나만 뺀다.
        int tail = ((address[12] & 0xff) << 24) | ((address[13] & 0xff) << 16)
                | ((address[14] & 0xff) << 8) | (address[15] & 0xff);
        return tail != 1;
    }
}
