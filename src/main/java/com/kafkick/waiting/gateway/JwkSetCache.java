package com.kafkick.waiting.gateway;

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.SignedJWT;
import java.text.ParseException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

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

    /** 실패가 이어진 첫 시각. 회복 로그가 지속 시간을 적는다. */
    private final AtomicReference<Instant> failingSince = new AtomicReference<>();

    private final AtomicInteger failures = new AtomicInteger();

    private final AtomicBoolean exhausted = new AtomicBoolean();

    private JwkSetCache(Mono<String> fetch, Clock clock) {
        this.fetch = fetch;
        this.clock = clock;
    }

    static JwkSetCache of(Mono<String> fetch, Clock clock) {
        return new JwkSetCache(fetch, clock);
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
                .onErrorResume(e -> snap != null && snap.usable(now)
                        ? Flux.fromIterable(snap.keys().getKeys()) : Flux.error(e));
    }

    private Mono<JWKSet> refresh(Instant now) {
        Attempt prev = last.get();
        if (prev != null && now.isBefore(prev.at().plus(MIN_INTERVAL))) {
            return prev.result();
        }
        Mono<JWKSet> result = fetch.timeout(TIMEOUT)
                .handle(this::parse)
                .doOnNext(this::stored)
                .doOnError(this::failed)
                .cache();
        return last.compareAndSet(prev, new Attempt(now, result)) ? result : last.get().result();
    }

    /** 짧은 RSA 키는 버린다. 공개키 설정에 거는 하한과 같다. */
    private void parse(String body, SynchronousSink<JWKSet> sink) {
        try {
            List<JWK> all = JWKSet.parse(body).getKeys();
            List<JWK> kept = all.stream()
                    .filter(k -> !(k instanceof RSAKey rsa) || rsa.size() >= JwtDecoders.MIN_RSA_BITS)
                    .toList();
            if (kept.size() < all.size()) {
                log.warn("키 집합에서 {} 비트보다 짧은 RSA 키 {}개를 버렸다 — 발급자의 키 길이를 본다",
                        JwtDecoders.MIN_RSA_BITS, all.size() - kept.size());
            }
            sink.next(new JWKSet(kept));
        } catch (ParseException e) {
            sink.error(new IllegalStateException("키 집합을 못 읽었다", e));
        }
    }

    private void stored(JWKSet set) {
        current.set(new Snapshot(set, clock.instant()));
        exhausted.set(false);
        Instant since = failingSince.getAndSet(null);
        if (since != null) {
            log.info("키 집합을 다시 받았다 — {}초 동안 {}번 실패",
                    Duration.between(since, clock.instant()).toSeconds(), failures.getAndSet(0));
        }
    }

    /** 상태가 바뀔 때만 남긴다. 그 사이 실패는 세기만 하고 회복 로그가 횟수를 싣는다. */
    private void failed(Throwable e) {
        Instant now = clock.instant();
        failures.incrementAndGet();
        Snapshot snap = current.get();
        boolean usable = snap != null && snap.usable(now);
        if (failingSince.compareAndSet(null, now) && usable) {
            log.warn("키 집합을 못 받아 {} 에 받은 것으로 버틴다 — jwks-uri 와 발급자 상태를 본다: {}",
                    snap.at(), e.toString());
        }
        if (!usable && exhausted.compareAndSet(false, true)) {
            log.error("쓸 키 집합이 없어 인증 요청이 모두 503 이다 — jwks-uri 와 발급자 상태를 본다: {}",
                    e.toString());
        }
    }

    private record Snapshot(JWKSet keys, Instant at) {

        /** 오래됐거나, 모르는 kid 가 왔을 때 다시 받는다. 간격은 {@link #refresh} 가 지킨다. */
        boolean stale(Instant now, String kid) {
            return !now.isBefore(at.plus(TTL)) || (kid != null && keys.getKeyByKeyId(kid) == null);
        }

        /** 못 받는 동안 버틸 수 있는가. */
        boolean usable(Instant now) {
            return now.isBefore(at.plus(STALE_LIMIT));
        }
    }

    private record Attempt(Instant at, Mono<JWKSet> result) {
    }
}
