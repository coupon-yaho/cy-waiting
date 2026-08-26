package com.kafkick.waiting.gateway;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Objects;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * 끊긴 발급이 두 번 나가지 않게 하는 키.
 *
 * <p><b>게이트웨이가 끊어도 뒷단은 처리했을 수 있다.</b> 재사용 방지는 발급 계층의
 * 멱등성이 지고(A-10), 게이트웨이는 <b>같은 시도에 같은 키</b>를 실어 그 멱등성이
 * 작동할 근거를 준다.
 */
public final class IdempotencyKey {

    /** 뒷단이 읽는 헤더. 브라우저가 붙일 수 있게 CORS 허용 목록에도 있다. */
    public static final String HEADER = "Idempotency-Key";

    private static final String ALGORITHM = "HmacSHA256";

    private static final int MIN_SECRET_LENGTH = 16;

    /** 쓰임을 가르는 접두. <b>서명에 들어간다</b> — 안 그러면 바꿔 끼울 수 있다. */
    private static final String PREFIX = "ik_";

    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

    private final byte[] secret;

    private IdempotencyKey(String secret) {
        if (secret == null || secret.length() < MIN_SECRET_LENGTH) {
            throw new IllegalArgumentException(
                    "멱등 키 비밀키는 %d자 이상이어야 한다".formatted(MIN_SECRET_LENGTH));
        }
        this.secret = secret.getBytes(StandardCharsets.UTF_8);
    }

    public static IdempotencyKey of(String secret) {
        return new IdempotencyKey(secret);
    }

    /**
     * 이 시도의 키.
     *
     * <p><b>입장 토큰이 시도를 가른다.</b> 회원과 쿠폰만으로 만들면 두 번 줄 서서
     * 두 번 차례가 온 사람의 두 번째가 조용히 버려진다. 토큰을 그대로 싣지도
     * 않는다 — 키가 남는 뒷단 로그에서 남의 차례를 주울 수 있다.
     */
    public String of(String couponId, String memberId, String entryToken) {
        Objects.requireNonNull(couponId, "couponId 는 필수다");
        Objects.requireNonNull(memberId, "memberId 는 필수다");
        Objects.requireNonNull(entryToken, "entryToken 은 필수다");
        // 길이를 같이 넣는다. 구분자만 쓰면 ("a|b", "c") 와 ("a", "b|c") 가 같은
        // 바이트가 되어 서로 다른 시도가 같은 키를 받는다.
        String material = "%d:%s:%d:%s:%d:%s"
                .formatted(couponId.length(), couponId,
                        memberId.length(), memberId,
                        entryToken.length(), entryToken);
        return PREFIX + ENCODER.encodeToString(sign(material));
    }

    private byte[] sign(String material) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret, ALGORITHM));
            return mac.doFinal(material.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            // 알고리즘은 JDK 가 늘 갖고 있고 키는 생성자가 봤다. 여기 오면
            // 우리가 모르는 상태이므로 조용히 넘기지 않는다.
            throw new IllegalStateException("멱등 키를 만들 수 없다", e);
        }
    }
}
