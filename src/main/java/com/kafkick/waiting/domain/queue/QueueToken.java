package com.kafkick.waiting.domain.queue;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * 순번 조회의 신원 수단. 로그인이 없어 {@code X-Member-Id} 는 위조 가능하고,
 * 헤더로 대상을 특정하면 <b>헤더 하나로 남의 순번을 본다.</b> 서명한 것만 믿는다.
 */
public final class QueueToken {

    /** 토큰 수명의 상한. */
    public static final long TTL_SEC = 3_600;

    /** 발급 값을 끊는 단위. 최소 수명은 {@code TTL_SEC - WINDOW_SEC} 이다. */
    private static final long WINDOW_SEC = 600;

    /** 쓰임을 가르는 접두. <b>서명에 들어간다</b> — 안 그러면 바꿔 끼울 수 있다. */
    private static final String PREFIX = "qt_";

    private final SignedToken signer;

    private QueueToken(SignedToken signer) {
        this.signer = signer;
    }

    public static QueueToken of(String secret) {
        return of(secret, List.of());
    }

    /**
     * 옛 키를 검증에서만 받는다 (CY-902). <b>여기가 더 아프다</b> — 줄 토큰을
     * 거절당하면 자리를 잃고 처음부터 다시 선다. 수명도 한 시간이라 창이 넓다.
     */
    public static QueueToken of(String secret, List<String> alsoAccept) {
        return new QueueToken(SignedToken.of(PREFIX, TTL_SEC, WINDOW_SEC, secret, alsoAccept));
    }

    public String issue(String couponId, String memberId, Instant now) {
        return signer.issue(couponId, memberId, now);
    }

    /** @return 회원 식별자. 하나라도 어긋나면 빈 값 */
    public Optional<String> verify(String token, String couponId, Instant now) {
        return signer.verify(token, couponId, now);
    }
}
