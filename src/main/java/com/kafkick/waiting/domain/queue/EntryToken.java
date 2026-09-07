package com.kafkick.waiting.domain.queue;

import java.time.Instant;
import java.util.Optional;

/**
 * 차례가 왔다는 증거. 이게 없으면 발급이 줄과 무관해져 줄 선 사람이 추월당한다.
 *
 * <p>수명이 짧아야 한다. 길면 받아만 두고 나중에 몰려와 그 순간 상한을 넘긴다.
 */
public final class EntryToken {

    /** 토큰 수명의 상한. */
    public static final long TTL_SEC = 180;

    /** 발급 값을 끊는 단위. 최소 수명은 {@code TTL_SEC - WINDOW_SEC} 이다. */
    private static final long WINDOW_SEC = 30;

    /** 쓰임을 가르는 접두. <b>서명에 들어간다</b> — 안 그러면 바꿔 끼울 수 있다. */
    private static final String PREFIX = "et_";

    private final SignedToken signer;

    private EntryToken(SignedToken signer) {
        this.signer = signer;
    }

    public static EntryToken of(String secret) {
        return new EntryToken(SignedToken.of(PREFIX, TTL_SEC, WINDOW_SEC, secret));
    }

    public String issue(String couponId, String memberId, Instant now) {
        return signer.issue(couponId, memberId, now);
    }

    /** @return 회원 식별자. 하나라도 어긋나면 빈 값 */
    public Optional<String> verify(String token, String couponId, Instant now) {
        return signer.verify(token, couponId, now);
    }
}
