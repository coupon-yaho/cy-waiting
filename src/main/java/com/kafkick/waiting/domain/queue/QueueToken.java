package com.kafkick.waiting.domain.queue;

import java.time.Instant;
import java.util.Optional;

/**
 * 순번 조회의 신원 수단.
 *
 * <p>로그인이 없어 {@code X-Member-Id} 는 위조 가능하다. 헤더로 대상을 특정하면
 * <b>헤더 하나로 남의 순번을 본다.</b> 게이트웨이가 서명한 것만 믿는다.
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
        return new QueueToken(SignedToken.of(PREFIX, TTL_SEC, WINDOW_SEC, secret));
    }

    public String issue(String couponId, String memberId, Instant now) {
        return signer.issue(couponId, memberId, now);
    }

    /** @return 회원 식별자. 하나라도 어긋나면 빈 값 */
    public Optional<String> verify(String token, String couponId, Instant now) {
        return signer.verify(token, couponId, now);
    }
}
