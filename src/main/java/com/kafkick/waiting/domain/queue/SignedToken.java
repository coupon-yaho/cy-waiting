package com.kafkick.waiting.domain.queue;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * 서명한 토큰. <b>요청 경로에서 레디스를 안 친다</b> — 검증에 조회가 필요하면 요청마다
 * 왕복이 생기고, 그 왕복이 곧 사람 수에 비례한다.
 */
public final class SignedToken {

    private static final String ALGORITHM = "HmacSHA256";

    /** 128비트. 이보다 짧으면 서명이 있다는 사실이 무의미해진다. */
    private static final int MIN_SECRET_LENGTH = 16;

    private static final char SEPARATOR = '.';

    /**
     * 필드 구분자. 값에 못 들어가는 글자여야 한다 — 쿠폰 이름에 섞이면 경계가
     * 옮겨져 한 필드가 둘로 쪼개진다. 단위 구분자는 식별자에 쓸 수 없다.
     */
    private static final char FIELD = (char) 0x1f;

    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    private final String prefix;
    private final long ttlSec;
    private final long windowSec;
    private final byte[] secret;

    /**
     * 검증에서만 받아 주는 옛 키들. <b>발급에는 안 쓴다</b> — 옛 키로 내면 배포가
     * 끝나고 그 키를 뺀 뒤에 방금 낸 토큰이 죽는다.
     */
    private final List<byte[]> alsoAccept;

    private SignedToken(String prefix, long ttlSec, long windowSec, byte[] secret,
            List<byte[]> alsoAccept) {
        this.prefix = prefix;
        this.ttlSec = ttlSec;
        this.windowSec = windowSec;
        this.secret = secret;
        this.alsoAccept = alsoAccept;
    }

    /**
     * <b>약한 키로 조용히 돌지 않는다.</b> 기동을 막는 것이 목적이다 — 서명이
     * 있다는 사실만 믿고 지나가면 그 믿음이 틀린 채로 운영에 나간다.
     */
    public static SignedToken of(String prefix, long ttlSec, long windowSec, String secret) {
        return of(prefix, ttlSec, windowSec, secret, List.of());
    }

    /**
     * <b>옛 키를 검증에서만 받는다</b> (CY-902). 키가 하나뿐이면 롤링 배포 중
     * 새 키 파드가 낸 토큰을 옛 키 파드가 거절하고, 그 사람은 큐에 새로 선다.
     */
    public static SignedToken of(String prefix, long ttlSec, long windowSec, String secret,
            List<String> alsoAccept) {
        if (secret == null || secret.length() < MIN_SECRET_LENGTH) {
            throw new IllegalArgumentException(
                    "토큰 비밀키는 %d자 이상이어야 한다".formatted(MIN_SECRET_LENGTH));
        }
        // **받아 주는 키도 같은 규칙이다.** 여기만 느슨하면 짧은 옛 키를 남겨 두는
        // 것으로 서명이 뜻을 잃는다.
        for (String old : alsoAccept) {
            if (old == null || old.length() < MIN_SECRET_LENGTH) {
                throw new IllegalArgumentException(
                        "받아 주는 키도 %d자 이상이어야 한다".formatted(MIN_SECRET_LENGTH));
            }
        }
        // 창이 0 이면 발급이 0 으로 나누고, 수명이 0 이면 받자마자 만료된다.
        if (windowSec < 1 || ttlSec < 1) {
            throw new IllegalArgumentException(
                    "수명과 창은 양수여야 한다: ttl=%d window=%d".formatted(ttlSec, windowSec));
        }
        // 창이 수명보다 길면 최소 수명이 음수가 되어 방금 받은 토큰이 이미 만료다.
        if (windowSec > ttlSec) {
            throw new IllegalArgumentException(
                    "창은 수명보다 짧아야 한다: ttl=%d window=%d".formatted(ttlSec, windowSec));
        }
        return new SignedToken(prefix, ttlSec, windowSec,
                secret.getBytes(StandardCharsets.UTF_8),
                alsoAccept.stream().map(v -> v.getBytes(StandardCharsets.UTF_8)).toList());
    }

    /**
     * 발급한다. <b>같은 사람에게 늘 같은 값을 준다</b> — 매번 갈리면 앞서 받은
     * 토큰이 조용히 죽고, 그 사람은 자기 차례를 못 쓴다.
     */
    public String issue(String couponId, String memberId, Instant now) {
        String payload = ENCODER.encodeToString(
                claims(couponId, memberId, expiry(now)).getBytes(StandardCharsets.UTF_8));
        return prefix + payload + SEPARATOR + ENCODER.encodeToString(sign(payload));
    }

    /**
     * 이 쿠폰의 유효한 토큰인가. <b>서명을 먼저 본다</b> — 검증 전의 페이로드로
     * 파서를 흔들 수 있다. 사유도 안 나눈다. 알려 주면 맞추는 데 쓰인다.
     *
     * @return 회원 식별자. 하나라도 어긋나면 빈 값
     */
    public Optional<String> verify(String token, String couponId, Instant now) {
        if (token == null || !token.startsWith(prefix)) {
            return Optional.empty();
        }
        int mark = token.indexOf(SEPARATOR);
        if (mark < 0) {
            return Optional.empty();
        }
        String payload = token.substring(prefix.length(), mark);
        byte[] presented = decode(token.substring(mark + 1));
        if (presented == null || !signedByUs(payload, presented)) {
            return Optional.empty();
        }
        // 서명이 맞으므로 여기서부터는 우리가 만든 문자열이다.
        String[] parts = new String(DECODER.decode(payload), StandardCharsets.UTF_8)
                .split(String.valueOf(FIELD), -1);
        // **칸 수를 본다.** 쿠폰 이름에 구분자가 섞이면 한 필드가 둘로 쪼개져
        // 만료 자리에 남의 값이 온다.
        if (parts.length != 3 || !parts[0].equals(couponId)) {
            return Optional.empty();
        }
        return Long.parseLong(parts[2]) <= now.getEpochSecond()
                ? Optional.empty()
                : Optional.of(parts[1]);
    }

    /**
     * 우리가 낸 서명인가. <b>맞은 뒤에도 나머지를 다 본다</b> — 첫 키에서 빠져나가면
     * 걸린 시간이 어느 키였는지를 알려 준다.
     */
    private boolean signedByUs(String payload, byte[] presented) {
        boolean matched = MessageDigest.isEqual(sign(secret, payload), presented);
        for (byte[] old : alsoAccept) {
            matched |= MessageDigest.isEqual(sign(old, payload), presented);
        }
        return matched;
    }

    /**
     * <b>만료가 아니라 발급 시각을 끊는다.</b> 만료를 끊으면 창 끝에 받은 사람의
     * 토큰이 몇 초만 살고, 지금 시각을 그대로 담으면 매번 다른 값이 나온다.
     */
    private long expiry(Instant now) {
        return now.getEpochSecond() / windowSec * windowSec + ttlSec;
    }

    private String claims(String couponId, String memberId, long expiry) {
        return couponId + FIELD + memberId + FIELD + expiry;
    }

    /**
     * <b>접두를 서명에 넣는다.</b> 안 넣으면 접두만 바꿔 끼우는 것으로 다른 쓰임의
     * 토큰이 되고, 순번 토큰 하나로 줄을 통째로 건너뛴다. 인스턴스를 공유하지
     * 않는다 — {@link Mac} 은 스레드 안전하지 않다.
     */
    private byte[] sign(String payload) {
        return sign(secret, payload);
    }

    private byte[] sign(byte[] key, String payload) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(key, ALGORITHM));
            return mac.doFinal((prefix + payload).getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("토큰 서명 실패", e);
        }
    }

    private byte[] decode(String value) {
        try {
            return DECODER.decode(value);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
