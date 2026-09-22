package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jwt.SignedJWT;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * 키 집합 캐시 (CY-980). 요점은 <b>인증 없는 요청이 발급자를 두드리지 못하는가</b>다.
 */
class JwkSetCacheTest {

    private final AtomicReference<Instant> 지금 = new AtomicReference<>(
            Instant.parse("2026-09-22T00:00:00Z"));

    private final Clock 시계 = new Clock() {
        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return 지금.get();
        }
    };

    private final AtomicInteger 받은_횟수 = new AtomicInteger();

    private final AtomicReference<Mono<String>> 응답 = new AtomicReference<>();

    private final JwkSetCache 캐시 = new JwkSetCache(
            Mono.defer(() -> {
                받은_횟수.incrementAndGet();
                return 응답.get();
            }), 시계);

    private static String 집합(String... kids) throws Exception {
        List<JWK> keys = new ArrayList<>();
        for (String kid : kids) {
            keys.add(new RSAKeyGenerator(2048).keyID(kid).generate().toPublicJWK());
        }
        return new JWKSet(keys).toString();
    }

    private static SignedJWT 토큰(String kid) throws Exception {
        Base64URL 빈 = Base64URL.encode("x");
        return new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(kid).build()
                .toBase64URL(), new Payload("{}").toBase64URL(), 빈);
    }

    private List<String> 키(String kid) throws Exception {
        return 캐시.apply(토큰(kid)).map(JWK::getKeyID).collectList().block(Duration.ofSeconds(5));
    }

    private void 흐른다(Duration d) {
        지금.set(지금.get().plus(d));
    }

    @Test
    @DisplayName("아는 kid 는 받아 둔 것으로 답한다")
    void 받아_둔다() throws Exception {
        응답.set(Mono.just(집합("k1")));

        assertThat(키("k1")).containsExactly("k1");
        assertThat(키("k1")).containsExactly("k1");
        assertThat(받은_횟수).hasValue(1);
    }

    @Test
    @DisplayName("모르는 kid 를 아무리 보내도 간격 안에서는 한 번만 다시 받는다")
    void 모르는_kid_폭주() throws Exception {
        응답.set(Mono.just(집합("k1")));
        키("k1");
        흐른다(JwkSetCache.MIN_INTERVAL);

        for (int i = 0; i < 100; i++) {
            키("위조-" + i);
        }
        assertThat(받은_횟수).as("처음 한 번 + 간격이 지나 한 번").hasValue(2);

        흐른다(JwkSetCache.MIN_INTERVAL);
        키("위조-다음");
        assertThat(받은_횟수).hasValue(3);
    }

    @Test
    @DisplayName("발급자가 키를 돌리면 새 kid 가 간격 뒤에 잡힌다")
    void 키_교체() throws Exception {
        응답.set(Mono.just(집합("k1")));
        키("k1");
        응답.set(Mono.just(집합("k1", "k2")));
        흐른다(JwkSetCache.MIN_INTERVAL);

        assertThat(키("k2")).contains("k2");
    }

    @Test
    @DisplayName("오래되면 다시 받는다 — 발급자가 뺀 키를 계속 받아 주지 않는다")
    void 수명() throws Exception {
        응답.set(Mono.just(집합("k1")));
        키("k1");
        응답.set(Mono.just(집합("k2")));
        흐른다(JwkSetCache.TTL);

        assertThat(키("k1")).containsExactly("k2");
        assertThat(받은_횟수).hasValue(2);
    }

    @Test
    @DisplayName("다시 받다 실패하면 가진 것으로 버틴다")
    void 실패하면_버틴다() throws Exception {
        응답.set(Mono.just(집합("k1")));
        키("k1");
        응답.set(Mono.error(new IllegalStateException("발급자 장애")));
        흐른다(JwkSetCache.TTL);

        assertThat(키("k1")).containsExactly("k1");
    }

    @Test
    @DisplayName("처음부터 못 받으면 오류이고, 간격 안에서는 다시 안 두드린다")
    void 처음부터_실패() {
        응답.set(Mono.error(new IllegalStateException("발급자 장애")));

        for (int i = 0; i < 10; i++) {
            assertThatThrownBy(() -> 키("k1")).hasMessageContaining("발급자 장애");
        }
        assertThat(받은_횟수).hasValue(1);
    }

    @Test
    @DisplayName("응답이 안 오면 시한에 끊는다")
    void 시한() {
        응답.set(Mono.never());

        assertThatThrownBy(() -> 키("k1")).hasRootCauseInstanceOf(
                TimeoutException.class);
    }
}
