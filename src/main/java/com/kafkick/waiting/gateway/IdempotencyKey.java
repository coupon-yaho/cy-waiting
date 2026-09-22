package com.kafkick.waiting.gateway;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * 게이트웨이가 끊어도 뒷단은 처리했을 수 있다. 재사용 방지는 발급 계층의 멱등성이
 * 지고, 게이트웨이는 같은 시도에 같은 키를 실어 준다.
 *
 * <p>뒷단 계약은 곳마다 다르다. 무엇을 실을지를 {@link Mode} 로 고른다.
 */
public final class IdempotencyKey {

    public static final String HEADER = "Idempotency-Key";

    /** 뒷단마다 키 계약이 달라서 고르게 한다. 우리가 정할 수 있는 것이 아니다. */
    public enum Mode {
        /** 뒷단이 UUID 를 요구한다. 아니면 만들어 넣는다. */
        UUID,
        /** 뒷단이 제 형식을 쓴다. 모양을 맞추면 우리 표기가 계약으로 오해된다. */
        RAW,
        /** 뒷단이 멱등을 제 계약으로 진다. 게이트웨이가 값을 안 만든다. */
        OFF
    }

    /**
     * 원문 모드에서 받아 주는 값. <b>그 값이 뒷단 키가 된다</b> — 헤더를 가르는
     * 문자나 긴 값을 그대로 넘기면 뒷단의 저장 키가 우리 손을 떠난다.
     */
    private static final int MAX_RAW = 128;

    private static final Pattern RAW_OK =
            Pattern.compile("^[A-Za-z0-9_.:@=+/-]{1," + MAX_RAW + "}$");

    /** 값을 안 줬을 때 떨어질 자리를 가르는 이름공간. 다른 용도와 안 겹치게 한다. */
    private static final String NAMESPACE = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

    /**
     * 받아 주는 표기. <b>버전 자리와 변종 자리까지 본다</b> — 모양만 보면 v1·v3 을
     * 그대로 넘겼다가 뒷단이 거절해 사용자가 발급을 못 받는다.
     */
    private static final Pattern UUID_V4 = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}"
                    + "-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$");

    private static final String FALLBACK_METRIC = "waiting.idempotency.fallback";

    /** 값을 안 줬다. 빈 값도 여기다 — 헤더만 붙이고 비워 보내는 클라이언트가 있다. */
    private final Counter missing;

    /** 값을 줬는데 UUID v4 가 아니다. 클라이언트가 계약을 틀리게 안다는 신호다. */
    private final Counter malformed;

    private final Mode mode;

    private IdempotencyKey(Mode mode, MeterRegistry meters) {
        this.mode = Objects.requireNonNull(mode, "mode 는 필수다");
        Objects.requireNonNull(meters, "meters 는 필수다");
        String name = mode.name().toLowerCase(Locale.ROOT);
        this.missing = meters.counter(FALLBACK_METRIC, "reason", "missing", "mode", name);
        this.malformed = meters.counter(FALLBACK_METRIC, "reason", "malformed", "mode", name);
    }

    public static IdempotencyKey of(Mode mode, MeterRegistry meters) {
        return new IdempotencyKey(mode, meters);
    }

    public Mode mode() {
        return mode;
    }

    /**
     * 떨어진 수를 사유별로 센다. <b>떨어지면 응답으로는 안 드러난다</b> — 같은 회원의
     * 다른 시도가 한 키로 합쳐져 두 번째 발급을 잃어도 운영이 모른다.
     */
    public static IdempotencyKey passThrough(MeterRegistry meters) {
        return new IdempotencyKey(Mode.UUID, meters);
    }

    /** 계측 없이 만든다. <b>시험 편의다</b> — 운영은 위 팩토리를 쓴다. */
    public static IdempotencyKey passThrough() {
        return new IdempotencyKey(Mode.UUID, new SimpleMeterRegistry());
    }

    /**
     * 이 시도의 키. <b>시도는 클라이언트가 가른다</b> — 게이트웨이는 무엇이 한 번의
     * 시도인지 모른다. 도용 방어는 뒷단이 회원과 키의 쌍으로 저장해서 진다.
     *
     * @param clientKey 클라이언트가 준 값. 모드가 받아 주는 모양이 아니면 안 준 것으로 본다
     * @return 실을 값. 끈 모드면 {@code null} 이고 그때는 헤더를 안 건드린다
     */
    public String of(String couponId, String memberId, String clientKey) {
        Objects.requireNonNull(couponId, "couponId 는 필수다");
        Objects.requireNonNull(memberId, "memberId 는 필수다");
        if (mode == Mode.OFF) {
            return null;
        }
        // **원문 모드는 안 깎는다.** 깎으면 `" k "` 와 `"k"` 가 뒷단에서 한 키가
        // 되어, 서로 다른 두 시도의 두 번째가 재생으로 버려진다. UUID 모드는
        // 표기를 맞추는 것이 목적이라 깎는다.
        String given = clientKey == null ? null
                : mode == Mode.UUID ? clientKey.trim() : clientKey;
        if (given != null && accepts(given)) {
            // 표기를 맞춘다. 같은 값을 대소문자만 다르게 재시도하면 뒷단이 두
            // 건으로 본다. **원문 모드는 안 맞춘다** — 뒷단이 대소문자를 가르는
            // 곳이면 우리가 바꾸는 순간 그 키가 다른 것이 된다.
            return mode == Mode.UUID ? given.toLowerCase(Locale.ROOT) : given;
        }
        (given == null || given.isBlank() ? missing : malformed).increment();
        return fallback(couponId, memberId);
    }

    /**
     * 끈 모드에서 <b>그대로 보낼 수 있는 값인가</b>. 안 건드리는 것과 아무거나
     * 통과시키는 것은 다르다 — 신원 헤더가 이미 같은 원칙으로 줄이 둘이면 막는다.
     *
     * @param values 클라이언트가 실어 온 줄 전부
     */
    public boolean accepts(List<String> values) {
        if (values == null || values.isEmpty()) {
            return true;
        }
        if (values.size() != 1) {
            return false;
        }
        // 길이와 허용 문자는 원문 모드와 같은 잣대다. 헤더 줄이 되는 값이라
        // 모드가 달라도 안전한 범위는 같다.
        String only = values.getFirst();
        return only != null && RAW_OK.matcher(only).matches();
    }

    private boolean accepts(String given) {
        return mode == Mode.UUID
                ? UUID_V4.matcher(given).matches()
                : RAW_OK.matcher(given).matches();
    }

    /**
     * 값을 안 줬을 때 떨어지는 자리. 두 번 줄 서서 두 번 차례가 온 사람의 두 번째를
     * 잃을 수 있지만, 재고보다 많이 발급하는 것은 타협할 수 없고 잃는 쪽은 아니라
     * 안전한 방향으로 치우친다.
     */
    private String fallback(String couponId, String memberId) {
        // **UUID 를 안 쓰기로 한 곳에 UUID 를 만들어 보내지 않는다.** 모양을 맞추면
        // 그 뒷단은 우리가 고른 표기를 계약으로 오해한다.
        if (mode == Mode.RAW) {
            return "w-" + HexFormat.of().formatHex(sha256(material(couponId, memberId)), 0, 16);
        }
        // 길이를 같이 넣어야 ("a|b", "c") 와 ("a", "b|c") 가 안 겹친다. 비밀키를 쓰면
        // 회전할 때 진행 중이던 재시도의 키가 바뀌어 이중 발급이 난다. 로케일을 박는
        // 것은 %d 가 노드마다 다르게 찍히면 같은 재시도가 두 키로 갈라져서다.
        // nameUUIDFromBytes 는 v3 을 내는데 뒷단 계약이 v4 라 거절당한다. 해시는
        // 그대로 쓰고 버전·변종 자리만 세운다 — 같은 재료에 같은 값이 나온다.
        byte[] hash = sha256(material(couponId, memberId));
        hash[6] = (byte) ((hash[6] & 0x0f) | 0x40);
        hash[8] = (byte) ((hash[8] & 0x3f) | 0x80);
        String hex = HexFormat.of().formatHex(hash, 0, 16);
        return hex.substring(0, 8) + "-" + hex.substring(8, 12) + "-" + hex.substring(12, 16)
                + "-" + hex.substring(16, 20) + "-" + hex.substring(20, 32);
    }

    private String material(String couponId, String memberId) {
        return String.format(Locale.ROOT, "%s:%d:%s:%d:%s", NAMESPACE,
                couponId.length(), couponId, memberId.length(), memberId);
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
