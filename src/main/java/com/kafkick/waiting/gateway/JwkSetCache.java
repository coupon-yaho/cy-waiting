package com.kafkick.waiting.gateway;

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jwt.SignedJWT;
import java.text.ParseException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 받아 둔 키 집합. <b>받으러 가는 것은 {@link #MIN_INTERVAL} 에 한 번이다</b> — 모르는 kid 를 단 위조 토큰
 * 하나가 발급자 호출 하나가 되면, 인증 없는 요청으로 발급자를 두드릴 수 있다.
 */
final class JwkSetCache implements Function<SignedJWT, Flux<JWK>> {

    private static final Logger log = LoggerFactory.getLogger(JwkSetCache.class);

    /** 이만큼 지나면 다시 받는다. 발급자가 뺀 키를 이 안에 놓는다. */
    static final Duration TTL = Duration.ofMinutes(5);

    /** 받으러 가는 최소 간격. 실패한 시도도 이 동안은 그 결과를 돌려준다. */
    static final Duration MIN_INTERVAL = Duration.ofSeconds(10);

    static final Duration TIMEOUT = Duration.ofSeconds(3);

    /** 못 받는 동안 받아 둔 키로 버티는 한도. 끝이 없으면 발급자를 끊어 뺀 키를 살려 둔다. */
    static final Duration STALE_LIMIT = Duration.ofHours(1);

    private final Mono<String> fetch;

    private final Clock clock;

    private final AtomicReference<Snapshot> current = new AtomicReference<>();

    private final AtomicReference<Attempt> last = new AtomicReference<>();

    JwkSetCache(Mono<String> fetch, Clock clock) {
        this.fetch = fetch;
        this.clock = clock;
    }

    @Override
    public Flux<JWK> apply(SignedJWT jwt) {
        Snapshot snap = current.get();
        Instant now = clock.instant();
        if (snap != null && !snap.stale(now, jwt.getHeader().getKeyID())) {
            return Flux.fromIterable(snap.keys().getKeys());
        }
        // 못 받으면 가진 것으로 버틴다. 발급자 장애가 곧 전원 거절이 되지 않게.
        return refresh(now).flatMapMany(set -> Flux.fromIterable(set.getKeys()))
                .onErrorResume(e -> snap == null || !now.isBefore(snap.at().plus(STALE_LIMIT))
                        ? Flux.error(e) : Flux.fromIterable(snap.keys().getKeys()));
    }

    private Mono<JWKSet> refresh(Instant now) {
        Attempt prev = last.get();
        if (prev != null && now.isBefore(prev.at().plus(MIN_INTERVAL))) {
            return prev.result();
        }
        Mono<JWKSet> result = fetch.timeout(TIMEOUT)
                .map(this::parse)
                .doOnNext(set -> current.set(new Snapshot(set, clock.instant())))
                .doOnError(e -> log.warn("키 집합을 못 받았다 — {}", e.toString()))
                .cache();
        return last.compareAndSet(prev, new Attempt(now, result)) ? result : last.get().result();
    }

    private JWKSet parse(String body) {
        try {
            return JWKSet.parse(body);
        } catch (ParseException e) {
            throw new IllegalStateException("키 집합을 못 읽었다", e);
        }
    }

    private record Snapshot(JWKSet keys, Instant at) {

        /** 오래됐거나, 모르는 kid 가 왔을 때 다시 받는다. 간격은 {@link #refresh} 가 지킨다. */
        boolean stale(Instant now, String kid) {
            return !now.isBefore(at.plus(TTL)) || (kid != null && keys.getKeyByKeyId(kid) == null);
        }
    }

    private record Attempt(Instant at, Mono<JWKSet> result) {
    }
}
