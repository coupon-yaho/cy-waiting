package com.kafkick.waiting.gateway;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 끊긴 발급이 두 번 나가지 않게 하는 키.
 *
 * <p><b>게이트웨이가 끊어도 뒷단은 처리했을 수 있다.</b> 재사용 방지는 발급
 * 계층의 멱등성이 지고(A-10), 게이트웨이는 같은 시도에 같은 키를 실어 준다.
 */
// **뒷단은 UUID 만 받는다.** 그래서 값을 바꾸지 않고 그대로 넘긴다 (CY-830).
// 앞 판은 회원에 묶어 다시 서명했는데, 그 값은 UUID 가 아니라 뒷단이 형식으로
// 거절한다. 도용 방어는 뒷단이 회원과 키의 쌍으로 저장해서 진다 — 남의 키를
// 실어도 자기 회원 앞으로만 쓰이므로 남의 시도를 못 지운다.
public final class IdempotencyKey {

    public static final String HEADER = "Idempotency-Key";

    /** 값을 안 줬을 때 떨어질 자리를 가르는 이름공간. 다른 용도와 안 겹치게 한다. */
    private static final UUID NAMESPACE =
            UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

    private static final Pattern UUID_FORM = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}"
                    + "-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    private IdempotencyKey() {
    }

    public static IdempotencyKey passThrough() {
        return new IdempotencyKey();
    }

    /**
     * 이 시도의 키.
     *
     * <p><b>클라이언트가 시도를 가른다</b> (C-11). 게이트웨이는 무엇이 한 번의
     * 시도인지 모른다 — 발급 정책은 뒷단 것이다.
     *
     * @param clientKey 클라이언트가 준 값. UUID 가 아니면 안 준 것으로 본다
     */
    public String of(String couponId, String memberId, String clientKey) {
        Objects.requireNonNull(couponId, "couponId 는 필수다");
        Objects.requireNonNull(memberId, "memberId 는 필수다");
        if (clientKey != null && UUID_FORM.matcher(clientKey.trim()).matches()) {
            // **표기를 맞춘다.** 같은 값을 대소문자만 다르게 재시도하면 뒷단이
            // 두 건으로 본다.
            return clientKey.trim().toLowerCase(Locale.ROOT);
        }
        return fallback(couponId, memberId);
    }

    /**
     * 값을 안 줬을 때 떨어지는 자리 (C-11).
     *
     * <p>두 번 줄 서서 두 번 차례가 온 사람의 두 번째를 잃을 수 있다. 초과
     * 발급은 타협 불가이고(불변식 2) 잃는 쪽은 아니라, 안전한 방향으로 치우친다.
     */
    // **비밀키를 안 쓴다.** 쓰면 회전할 때 진행 중이던 재시도의 키가 통째로
    // 바뀌어 이중 발급이 난다. 추측 가능해도 뒷단이 회원으로 묶으므로 남의
    // 시도를 못 건드린다.
    private String fallback(String couponId, String memberId) {
        // 길이를 같이 넣는다. 구분자만 쓰면 ("a|b", "c") 와 ("a", "b|c") 가 같은
        // 바이트가 되어 서로 다른 시도가 같은 키를 받는다.
        String material = "%s:%d:%s:%d:%s".formatted(NAMESPACE,
                couponId.length(), couponId, memberId.length(), memberId);
        return UUID.nameUUIDFromBytes(material.getBytes(StandardCharsets.UTF_8)).toString();
    }
}
