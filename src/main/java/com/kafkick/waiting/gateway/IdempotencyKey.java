package com.kafkick.waiting.gateway;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 게이트웨이가 끊어도 뒷단은 처리했을 수 있다. 재사용 방지는 발급 계층의 멱등성이
 * 지고, 게이트웨이는 같은 시도에 같은 키를 실어 준다. 뒷단 계약이 <b>UUID v4</b> 라
 * 값을 바꾸지 않고 그대로 넘긴다.
 */
public final class IdempotencyKey {

    public static final String HEADER = "Idempotency-Key";

    /** 값을 안 줬을 때 떨어질 자리를 가르는 이름공간. 다른 용도와 안 겹치게 한다. */
    private static final UUID NAMESPACE =
            UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

    /**
     * 받아 주는 표기. <b>버전 자리와 변종 자리까지 본다</b> — 모양만 보면 v1·v3 을
     * 그대로 넘겼다가 뒷단이 거절해 사용자가 발급을 못 받는다.
     */
    private static final Pattern UUID_V4 = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}"
                    + "-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$");

    private IdempotencyKey() {
    }

    public static IdempotencyKey passThrough() {
        return new IdempotencyKey();
    }

    /**
     * 이 시도의 키. <b>시도는 클라이언트가 가른다</b> — 게이트웨이는 무엇이 한 번의
     * 시도인지 모른다. 도용 방어는 뒷단이 회원과 키의 쌍으로 저장해서 진다.
     *
     * @param clientKey 클라이언트가 준 값. UUID v4 가 아니면 안 준 것으로 본다
     */
    public String of(String couponId, String memberId, String clientKey) {
        Objects.requireNonNull(couponId, "couponId 는 필수다");
        Objects.requireNonNull(memberId, "memberId 는 필수다");
        if (clientKey != null && UUID_V4.matcher(clientKey.trim()).matches()) {
            // 표기를 맞춘다. 같은 값을 대소문자만 다르게 재시도하면 뒷단이 두
            // 건으로 본다.
            return clientKey.trim().toLowerCase(Locale.ROOT);
        }
        return fallback(couponId, memberId);
    }

    /**
     * 값을 안 줬을 때 떨어지는 자리. 두 번 줄 서서 두 번 차례가 온 사람의 두 번째를
     * 잃을 수 있지만, 재고보다 많이 발급하는 것은 타협할 수 없고 잃는 쪽은 아니라
     * 안전한 방향으로 치우친다.
     */
    private String fallback(String couponId, String memberId) {
        // 길이를 같이 넣어야 ("a|b", "c") 와 ("a", "b|c") 가 안 겹친다. 비밀키를 쓰면
        // 회전할 때 진행 중이던 재시도의 키가 바뀌어 이중 발급이 난다. 로케일을 박는
        // 것은 %d 가 노드마다 다르게 찍히면 같은 재시도가 두 키로 갈라져서다.
        String material = String.format(Locale.ROOT, "%s:%d:%s:%d:%s", NAMESPACE,
                couponId.length(), couponId, memberId.length(), memberId);
        // nameUUIDFromBytes 는 v3 을 내는데 뒷단 계약이 v4 라 거절당한다. 해시는
        // 그대로 쓰고 버전·변종 자리만 세운다 — 같은 재료에 같은 값이 나온다.
        byte[] hash = sha256(material);
        hash[6] = (byte) ((hash[6] & 0x0f) | 0x40);
        hash[8] = (byte) ((hash[8] & 0x3f) | 0x80);
        long high = 0;
        long low = 0;
        for (int i = 0; i < 8; i++) {
            high = (high << 8) | (hash[i] & 0xffL);
            low = (low << 8) | (hash[i + 8] & 0xffL);
        }
        return new UUID(high, low).toString();
    }

    private byte[] sha256(String material) {
        try {
            return MessageDigest.getInstance("SHA-256")
                    .digest(material.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            // JDK 가 늘 갖고 있다. 여기 오면 우리가 모르는 상태다.
            throw new IllegalStateException("멱등 키를 만들 수 없다", e);
        }
    }
}
