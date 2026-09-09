package com.kafkick.waiting.domain.queue;

import java.time.Instant;
import java.util.List;
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

    /** 옛 키를 받는 기간(초). 그 키로 낸 마지막 토큰의 수명이다. */
    public static final long ACCEPT_WINDOW_SEC = SignedToken.acceptWindowSec(TTL_SEC, WINDOW_SEC);

    public static EntryToken of(String secret) {
        return of(secret, List.of(), null);
    }

    /** 옛 키를 검증에서만 받는다 — 롤링 배포 창을 여는 자리다 (CY-902). */
    public static EntryToken of(String secret, List<String> alsoAccept, Instant rolloutEndsAt) {
        return new EntryToken(SignedToken.of(PREFIX, TTL_SEC, WINDOW_SEC, secret, alsoAccept, rolloutEndsAt));
    }

    /** 옛 키로 맞은 횟수. 누적이라 더 안 오르는 때가 창을 닫아도 되는 때다. */
    public long acceptedByPrevious() {
        return signer.acceptedByPrevious();
    }

    public String issue(String couponId, String memberId, Instant now) {
        return signer.issue(couponId, memberId, now);
    }

    /** @return 회원 식별자. 하나라도 어긋나면 빈 값 */
    public Optional<String> verify(String token, String couponId, Instant now) {
        return signer.verify(token, couponId, now);
    }
}
