package com.kafkick.waiting.gateway;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 회원을 무엇으로 믿는가 (CY-980). 기본은 지금처럼 헤더를 형식만 본다.
 *
 * <p>틀리게 적으면 기동을 멈춘다 — 안 멈추면 첫 요청부터 전원이 401 이거나, 약한 키로 서명된 토큰이 통과한다.
 */
@ConfigurationProperties(prefix = "waiting.auth")
public record AuthProperties(Mode mode, Jwt jwt) {

    private static final Set<String> IDENTITY_HEADERS =
            Set.of("x-member-id", "x-member-grade", "authorization");

    /** 회원 신원의 출처. */
    public enum Mode {
        /** 인증 없이 회원 헤더를 형식만 본다. 서명이 없어 값 자체는 못 믿는다. */
        NONE,
        /** 서명된 토큰의 클레임을 신원으로 쓴다. 클라이언트가 보낸 회원 헤더는 지운다. */
        JWT
    }

    public AuthProperties {
        mode = mode == null ? Mode.NONE : mode;
        if (mode == Mode.JWT && jwt == null) {
            throw new IllegalArgumentException("waiting.auth.mode=JWT 인데 waiting.auth.jwt 가 없다");
        }
    }

    /**
     * 인증을 켰으면 입장 토큰 헤더가 신원 헤더와 겹치면 안 된다 — 클라이언트 값이 검증된 신원을 덮는다.
     */
    public void checkAgainst(EntryTokenDelivery delivery) {
        if (mode != Mode.JWT) {
            return;
        }
        for (String name : List.of(delivery.header(), delivery.backendHeader())) {
            if (IDENTITY_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException(
                        "waiting.auth.mode=JWT 에서 입장 토큰 헤더로 못 쓴다: " + name);
            }
        }
    }

    /**
     * 검증 방법. 키 공급은 알고리즘에 맞는 것 <b>하나</b>만 받는다 — HMAC 은 비밀, RSA·EC 는 PEM 공개키나
     * JWKS 주소다.
     */
    public record Jwt(String algorithm, String secret, String publicKey, String jwksUri,
            String issuer, String audience, String memberClaim, String gradeClaim,
            Duration clockSkew) {

        private static final Set<String> HMAC = Set.of("HS256", "HS384", "HS512");

        private static final Set<String> ASYMMETRIC = Set.of("RS256", "RS384", "RS512",
                "ES256", "ES384", "ES512");

        /** HS256 의 키 길이. 이보다 짧은 비밀은 무차별 대입에 뚫린다 (RFC 7518 3.2). */
        private static final int MIN_SECRET_BYTES = 32;

        public Jwt {
            algorithm = algorithm == null ? "" : algorithm.trim().toUpperCase(Locale.ROOT);
            // `none` 과 모르는 이름은 막는다. 알고리즘을 토큰 머리에서 믿으면 서명 없는 토큰이 통과한다.
            if (!HMAC.contains(algorithm) && !ASYMMETRIC.contains(algorithm)) {
                throw new IllegalArgumentException("waiting.auth.jwt.algorithm 을 모른다: " + algorithm);
            }
            secret = blankToNull(secret);
            publicKey = blankToNull(publicKey);
            jwksUri = blankToNull(jwksUri);
            if (HMAC.contains(algorithm)) {
                if (secret == null || publicKey != null || jwksUri != null) {
                    throw new IllegalArgumentException(algorithm + " 는 secret 하나로만 검증한다");
                }
                if (secret.getBytes(StandardCharsets.UTF_8).length < MIN_SECRET_BYTES) {
                    throw new IllegalArgumentException(
                            "waiting.auth.jwt.secret 이 " + MIN_SECRET_BYTES + " 바이트보다 짧다");
                }
            } else {
                if (secret != null || (publicKey == null) == (jwksUri == null)) {
                    throw new IllegalArgumentException(
                            algorithm + " 는 public-key 나 jwks-uri 중 하나로 검증한다");
                }
                if (jwksUri != null) {
                    String scheme = URI.create(jwksUri).getScheme();
                    if (!"https".equals(scheme) && !"http".equals(scheme)) {
                        throw new IllegalArgumentException(
                                "waiting.auth.jwt.jwks-uri 는 http 나 https 여야 한다: " + jwksUri);
                    }
                }
            }
            issuer = blankToNull(issuer);
            audience = blankToNull(audience);
            memberClaim = blankToNull(memberClaim) == null ? "sub" : memberClaim.trim();
            gradeClaim = blankToNull(gradeClaim);
            clockSkew = clockSkew == null ? Duration.ofSeconds(30) : clockSkew;
        }

        public boolean hmac() {
            return HMAC.contains(algorithm);
        }

        static String blankToNull(String value) {
            return value == null || value.isBlank() ? null : value;
        }
    }
}
