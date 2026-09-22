package com.kafkick.waiting.gateway;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 회원을 무엇으로 믿는가 (CY-980). 기본은 지금처럼 헤더를 형식만 본다.
 *
 * <p>틀리게 적으면 기동을 멈춘다 — 안 멈추면 첫 요청부터 전원이 401 이거나, 약한 키로 서명된 토큰이 통과한다.
 */
@ConfigurationProperties(prefix = "waiting.auth")
public record AuthProperties(Mode mode, Jwt jwt) {

    /** Authorization 은 입장 토큰 쪽이 이미 예약해 둔다. */
    private static final Set<String> IDENTITY_HEADERS = Set.of("x-member-id", "x-member-grade");

    /** 신원 필터가 보는 범위. 이 밖의 경로는 토큰을 안 본다. */
    static final String API_PREFIX = "/api/";

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

    /** 인증을 켰으면 모든 라우트가 신원 필터 안에 있어야 한다. 밖의 경로는 토큰 없이 남의 이름을 쓴다. */
    public void checkCovers(RouteRules routes) {
        if (mode != Mode.JWT) {
            return;
        }
        for (RouteRules.Rule rule : routes.rules()) {
            for (String path : rule.paths()) {
                if (!path.startsWith(API_PREFIX)) {
                    throw new IllegalArgumentException("waiting.auth.mode=JWT 에서 라우트 '"
                            + rule.id() + "' 의 경로가 " + API_PREFIX + " 밖이다: " + path);
                }
            }
        }
    }

    /**
     * 검증 방법. 키 공급은 알고리즘에 맞는 것 <b>하나</b>만 받는다 — HMAC 은 비밀, RSA·EC 는 PEM 공개키나
     * JWKS 주소다. 대상은 필수다 — 같은 발급자의 다른 서비스용 토큰이 통과하면 안 된다.
     */
    public record Jwt(String algorithm, String secret, String publicKey, String jwksUri,
            String issuer, String audience, String memberClaim, String gradeClaim,
            Duration clockSkew, boolean allowHttpJwks) {

        /** 알고리즘별 비밀 하한. 해시 출력보다 짧은 비밀은 무차별 대입에 뚫린다 (RFC 7518 3.2). */
        private static final Map<String, Integer> HMAC = Map.of("HS256", 32, "HS384", 48, "HS512", 64);

        private static final Set<String> ASYMMETRIC = Set.of("RS256", "RS384", "RS512",
                "ES256", "ES384", "ES512");

        public Jwt {
            algorithm = algorithm == null ? "" : algorithm.trim().toUpperCase(Locale.ROOT);
            // `none` 과 모르는 이름은 막는다. 알고리즘을 토큰 머리에서 믿으면 서명 없는 토큰이 통과한다.
            if (!HMAC.containsKey(algorithm) && !ASYMMETRIC.contains(algorithm)) {
                throw new IllegalArgumentException("waiting.auth.jwt.algorithm 을 모른다: " + algorithm);
            }
            secret = blankToNull(secret);
            publicKey = blankToNull(publicKey);
            jwksUri = blankToNull(jwksUri);
            if (HMAC.containsKey(algorithm)) {
                if (secret == null || publicKey != null || jwksUri != null) {
                    throw new IllegalArgumentException(algorithm + " 는 secret 하나로만 검증한다");
                }
                int min = HMAC.get(algorithm);
                if (secret.getBytes(StandardCharsets.UTF_8).length < min) {
                    throw new IllegalArgumentException(
                            "waiting.auth.jwt.secret 이 " + algorithm + " 에 " + min + " 바이트보다 짧다");
                }
            } else {
                if (secret != null) {
                    throw new IllegalArgumentException(algorithm + " 는 secret 을 안 받는다");
                }
                if ((publicKey == null) == (jwksUri == null)) {
                    throw new IllegalArgumentException(
                            algorithm + " 는 public-key 나 jwks-uri 중 하나로 검증한다");
                }
                URI uri = jwksUri == null ? null : URI.create(jwksUri);
                // http 로 받으면 중간에서 키를 바꿔 끼워 아무 신원이나 서명할 수 있다.
                String scheme = uri == null ? "https"
                        : String.valueOf(uri.getScheme()).toLowerCase(Locale.ROOT);
                if (!"https".equals(scheme) && !("http".equals(scheme) && allowHttpJwks)) {
                    throw new IllegalArgumentException(
                            "waiting.auth.jwt.jwks-uri 는 https 여야 한다 (http 는 allow-http-jwks): "
                                    + jwksUri);
                }
                if (uri != null && (uri.isOpaque() || uri.getHost() == null)) {
                    throw new IllegalArgumentException(
                            "waiting.auth.jwt.jwks-uri 에 호스트가 없다: " + jwksUri);
                }
            }
            issuer = blankToNull(issuer);
            if (jwksUri != null && issuer == null) {
                throw new IllegalArgumentException(
                        "waiting.auth.jwt.jwks-uri 를 쓰면 issuer 를 적는다 — 그 키로 서명한 남의 토큰이 통과한다");
            }
            audience = blankToNull(audience);
            if (audience == null) {
                throw new IllegalArgumentException("waiting.auth.jwt.audience 가 없다");
            }
            memberClaim = blankToNull(memberClaim) == null ? "sub" : memberClaim.trim();
            gradeClaim = blankToNull(gradeClaim) == null ? "grade" : gradeClaim.trim();
            clockSkew = clockSkew == null ? Duration.ofSeconds(30) : clockSkew;
        }

        public boolean hmac() {
            return HMAC.containsKey(algorithm);
        }

        /** 비밀을 로그에 안 흘린다. */
        @Override
        public String toString() {
            return "Jwt[algorithm=" + algorithm + ", secret=" + (secret == null ? null : "****")
                    + ", jwksUri=" + jwksUri + ", issuer=" + issuer + ", audience=" + audience + "]";
        }

        static String blankToNull(String value) {
            return value == null || value.isBlank() ? null : value;
        }
    }
}
