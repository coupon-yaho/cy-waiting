package com.kafkick.waiting.domain.queue;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * 순번 조회의 신원 수단.
 *
 * <p>로그인이 없어 {@code X-Member-Id} 는 위조 가능하다. 헤더로 대상을 특정하면
 * <b>헤더 하나로 남의 순번을 본다.</b> 게이트웨이가 서명한 것만 믿는다.
 *
 * <p><b>레디스를 안 친다</b> (RD-4). 검증에 조회가 필요하면 폴링마다 왕복이 생기고,
 * 그 왕복이 곧 대기 인원에 비례한다.
 */
public final class QueueToken {

    /** 토큰 수명의 상한. 대기 자체가 그보다 길면 다시 받아야 한다. */
    public static final long TTL_SEC = 3_600;

    /**
     * 발급 값을 끊는 단위. 짧을수록 새로고침 연타에 토큰이 자주 갈리고, 길수록
     * 수명 편차가 커진다. 최소 수명은 {@code TTL_SEC - WINDOW_SEC} 이다.
     */
    private static final long WINDOW_SEC = 600;

    private static final String PREFIX = "qt_";

    private static final String ALGORITHM = "HmacSHA256";

    /** 128비트. 이보다 짧으면 서명이 있다는 사실이 무의미해진다. */
    private static final int MIN_SECRET_LENGTH = 16;

    private static final char SEPARATOR = '.';

    /**
     * 필드 구분자.
     *
     * <p>값에 못 들어가는 글자여야 한다. 쿠폰 이름에 섞이면 경계가 옮겨져
     * 한 필드가 둘로 쪼개진다 — 단위 구분자는 식별자에 쓸 수 없는 제어문자다.
     */
    private static final char FIELD = (char) 0x1f;

    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    private final byte[] secret;

    private QueueToken(byte[] secret) {
        this.secret = secret;
    }

    /**
     * <b>약한 키로 조용히 돌지 않는다.</b> 기동을 막는 것이 목적이다 — 서명이
     * 있다는 사실만 믿고 지나가면 그 믿음이 틀린 채로 운영에 나간다.
     */
    public static QueueToken of(String secret) {
        if (secret == null || secret.length() < MIN_SECRET_LENGTH) {
            throw new IllegalArgumentException(
                    "토큰 비밀키는 %d자 이상이어야 한다".formatted(MIN_SECRET_LENGTH));
        }
        return new QueueToken(secret.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 발급한다. <b>같은 사람에게 늘 같은 값을 준다</b> — 새로고침 연타로 갈리면
     * 앞서 받은 토큰이 조용히 죽는다.
     */
    public String issue(String couponId, String memberId, Instant now) {
        String payload = ENCODER.encodeToString(
                claims(couponId, memberId, expiry(now)).getBytes(StandardCharsets.UTF_8));
        return PREFIX + payload + SEPARATOR + ENCODER.encodeToString(sign(payload));
    }

    /**
     * 이 쿠폰의 유효한 토큰인가.
     *
     * <p><b>서명을 먼저 본다.</b> 검증 전의 페이로드는 공격자가 고른 문자열이라
     * 파싱부터 하면 그 문자열로 파서를 흔들 수 있다. <b>사유도 나누지 않는다</b> —
     * 어디가 틀렸는지 알려 주면 맞추는 데 쓰인다.
     *
     * @return 회원 식별자. 하나라도 어긋나면 빈 값
     */
    public Optional<String> verify(String token, String couponId, Instant now) {
        if (token == null || !token.startsWith(PREFIX)) {
            return Optional.empty();
        }
        int mark = token.indexOf(SEPARATOR);
        if (mark < 0) {
            return Optional.empty();
        }
        String payload = token.substring(PREFIX.length(), mark);
        byte[] presented = decode(token.substring(mark + 1));
        if (presented == null || !MessageDigest.isEqual(sign(payload), presented)) {
            return Optional.empty();
        }
        // 서명이 맞으므로 여기서부터는 우리가 만든 문자열이다 — 인코더가 낸
        // 값이라 반드시 디코딩된다.
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
     * <b>만료가 아니라 발급 시각을 끊는다.</b> 만료를 끊으면 창 끝에 받은 사람의
     * 토큰이 몇 초만 살고, 지금 시각을 그대로 담으면 초마다 다른 토큰이 나와
     * 새로고침 연타가 앞서 받은 토큰을 죽인다.
     *
     * <p>수명은 {@link #TTL_SEC} 를 안 넘고 창 하나 이상 짧아지지도 않는다.
     */
    private long expiry(Instant now) {
        return now.getEpochSecond() / WINDOW_SEC * WINDOW_SEC + TTL_SEC;
    }

    private String claims(String couponId, String memberId, long expiry) {
        return couponId + FIELD + memberId + FIELD + expiry;
    }

    /** <b>인스턴스를 공유하지 않는다.</b> {@link Mac} 은 스레드 안전하지 않다. */
    private byte[] sign(String payload) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret, ALGORITHM));
            return mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
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
