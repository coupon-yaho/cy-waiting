package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.gateway.AuthProperties.Jwt;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * JWT 모드의 신원 필터 (CY-980).
 *
 * <p>요점은 둘이다. 검증된 토큰의 신원만 뒤로 넘기고, 클라이언트가 직접 보낸 신원 헤더는 지운다.
 */
class MemberIdentityJwtTest {

    private static final String 비밀 = "0123456789abcdef0123456789abcdef";

    private static final String 주소 = "/api/v1/coupons/c1/issue";

    private static final Instant 지금 = Instant.parse("2026-09-22T00:00:00Z");

    private static final Clock 시계 = Clock.fixed(지금, ZoneOffset.UTC);

    private final AtomicReference<HttpHeaders> 넘어간_헤더 = new AtomicReference<>();

    private static String hs256(JWTClaimsSet claims) throws Exception {
        return sign(new MACSigner(비밀.getBytes(StandardCharsets.UTF_8)), JWSAlgorithm.HS256, claims);
    }

    private static String sign(JWSSigner signer, JWSAlgorithm alg, JWTClaimsSet claims)
            throws Exception {
        SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(alg).keyID("k1").build(), claims);
        jwt.sign(signer);
        return jwt.serialize();
    }

    private static JWTClaimsSet.Builder 클레임(String sub) {
        return new JWTClaimsSet.Builder().subject(sub)
                .expirationTime(Date.from(지금.plusSeconds(300)));
    }

    private MemberIdentityFilter 필터(Jwt 설정) {
        return MemberIdentityFilter.jwt(시계, 설정, JwtDecoders.of(설정, 시계));
    }

    private static Jwt hs(String issuer, String gradeClaim) {
        return new Jwt("HS256", 비밀, null, null, issuer, null, null, gradeClaim, null);
    }

    private MockServerWebExchange 요청(String token, String memberHeader) {
        MockServerHttpRequest.BaseBuilder<?> b = MockServerHttpRequest.post(주소);
        if (token != null) {
            b.header(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        }
        if (memberHeader != null) {
            b.header("X-Member-Id", memberHeader);
        }
        return MockServerWebExchange.from(b.build());
    }

    private MockServerWebExchange 돌린다(MemberIdentityFilter f, MockServerWebExchange exchange) {
        f.filter(exchange, e -> {
            넘어간_헤더.set(e.getRequest().getHeaders());
            return Mono.empty();
        }).block(Duration.ofSeconds(10));
        return exchange;
    }

    @Test
    @DisplayName("검증된 토큰의 식별자가 신원 헤더로 넘어간다")
    void 검증된_신원() throws Exception {
        돌린다(필터(hs(null, null)), 요청(hs256(클레임("user-42").build()), null));

        assertThat(넘어간_헤더.get().getFirst("X-Member-Id")).isEqualTo("user-42");
    }

    @Test
    @DisplayName("클라이언트가 보낸 신원 헤더는 지운다 — 토큰이 말하는 사람이 정본이다")
    void 위조한_헤더() throws Exception {
        돌린다(필터(hs(null, null)), 요청(hs256(클레임("user-42").build()), "999"));

        assertThat(넘어간_헤더.get().get("X-Member-Id"))
                .as("둘을 같이 두면 뒤의 필터가 어느 것을 읽을지 모른다").containsExactly("user-42");
    }

    @Test
    @DisplayName("토큰이 없으면 401 이고 무엇을 요구하는지 알린다")
    void 토큰_없음() {
        MockServerWebExchange 교환 = 돌린다(필터(hs(null, null)), 요청(null, "42"));

        assertThat(교환.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(교환.getResponse().getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE))
                .startsWith("Bearer");
        assertThat(넘어간_헤더.get()).as("뒤로 안 넘어간다").isNull();
    }

    @Test
    @DisplayName("서명이 틀리면 401 이다")
    void 틀린_서명() throws Exception {
        String 남의_비밀로 = sign(new MACSigner("ffffffffffffffffffffffffffffffff".getBytes(
                StandardCharsets.UTF_8)), JWSAlgorithm.HS256, 클레임("user-42").build());

        assertThat(돌린다(필터(hs(null, null)), 요청(남의_비밀로, null)).getResponse().getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    @DisplayName("만료된 토큰은 401 이다")
    void 만료() throws Exception {
        String 만료된 = hs256(new JWTClaimsSet.Builder().subject("user-42")
                .expirationTime(Date.from(지금.minusSeconds(3_600))).build());

        assertThat(돌린다(필터(hs(null, null)), 요청(만료된, null)).getResponse().getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    @DisplayName("발급자를 적었으면 다른 발급자는 401 이다")
    void 발급자() throws Exception {
        String 남의_발급자 = hs256(클레임("user-42").issuer("https://other").build());

        assertThat(돌린다(필터(hs("https://idp", null)), 요청(남의_발급자, null))
                .getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    @DisplayName("식별자에 헤더를 가르는 문자가 있으면 401 이다 — 그 값이 레디스와 토큰에 들어간다")
    void 나쁜_식별자() throws Exception {
        assertThat(돌린다(필터(hs(null, null)), 요청(hs256(클레임("a\nb").build()), null))
                .getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    @DisplayName("등급 클레임을 적었으면 그 값을 등급 헤더로 넘긴다")
    void 등급() throws Exception {
        돌린다(필터(hs(null, "tier")), 요청(hs256(클레임("user-42").claim("tier", "GOLD").build()),
                null));

        assertThat(넘어간_헤더.get().getFirst("X-Member-Grade")).isEqualTo("GOLD");
    }

    @Test
    @DisplayName("API 밖의 경로는 안 본다 — 헬스 체크가 토큰 없이 돈다")
    void api_밖() {
        MockServerWebExchange 교환 = MockServerWebExchange.from(
                MockServerHttpRequest.get("/actuator/health").build());
        돌린다(필터(hs(null, null)), 교환);

        assertThat(교환.getResponse().getStatusCode()).as("401 이 아니다").isNull();
        assertThat(넘어간_헤더.get()).isEqualTo(교환.getRequest().getHeaders());
    }

    @Test
    @DisplayName("RSA 공개키로 검증한다")
    void rsa_공개키() throws Exception {
        KeyPair 키 = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        String pem = "-----BEGIN PUBLIC KEY-----\n"
                + Base64.getMimeEncoder().encodeToString(키.getPublic().getEncoded())
                + "\n-----END PUBLIC KEY-----";
        Jwt 설정 = new Jwt("RS256", null, pem, null, null, null, null, null, null);
        String 토큰 = sign(new RSASSASigner(키.getPrivate()), JWSAlgorithm.RS256,
                클레임("user-7").build());

        돌린다(필터(설정), 요청(토큰, null));

        assertThat(넘어간_헤더.get().getFirst("X-Member-Id")).isEqualTo("user-7");
    }

    @Test
    @DisplayName("JWKS 주소에서 키를 받아 검증한다")
    void jwks() throws Exception {
        KeyPair 키 = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        RSAKey jwk = new RSAKey.Builder((RSAPublicKey) 키.getPublic()).keyID("k1").build();
        HttpServer 서버 = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        byte[] 본문 = new JWKSet(jwk).toString().getBytes(StandardCharsets.UTF_8);
        서버.createContext("/jwks", ex -> {
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(200, 본문.length);
            ex.getResponseBody().write(본문);
            ex.close();
        });
        서버.start();
        try {
            Jwt 설정 = new Jwt("RS256", null, null,
                    "http://127.0.0.1:" + 서버.getAddress().getPort() + "/jwks",
                    null, null, null, null, null);
            String 토큰 = sign(new RSASSASigner(키.getPrivate()), JWSAlgorithm.RS256,
                    클레임("user-9").build());

            돌린다(필터(설정), 요청(토큰, null));

            assertThat(넘어간_헤더.get().getFirst("X-Member-Id")).isEqualTo("user-9");
        } finally {
            서버.stop(0);
        }
    }

    @Test
    @DisplayName("EC 공개키로 검증한다")
    void ec_공개키() throws Exception {
        KeyPairGenerator 생성 = KeyPairGenerator.getInstance("EC");
        생성.initialize(new ECGenParameterSpec("secp256r1"));
        KeyPair 키 = 생성.generateKeyPair();
        String pem = "-----BEGIN PUBLIC KEY-----\n"
                + Base64.getMimeEncoder().encodeToString(키.getPublic().getEncoded())
                + "\n-----END PUBLIC KEY-----";
        Jwt 설정 = new Jwt("ES256", null, pem, null, null, null, null, null, null);
        String 토큰 = sign(new ECDSASigner((ECPrivateKey) 키.getPrivate()), JWSAlgorithm.ES256,
                클레임("user-3").build());

        돌린다(필터(설정), 요청(토큰, null));

        assertThat(넘어간_헤더.get().getFirst("X-Member-Id")).isEqualTo("user-3");
    }

    @Test
    @DisplayName("서명 없는 토큰은 401 이다")
    void 서명_없음() {
        String 서명없는 = new PlainJWT(클레임("user-42").build()).serialize();

        assertThat(돌린다(필터(hs(null, null)), 요청(서명없는, null)).getResponse().getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    @DisplayName("공개키를 HMAC 비밀로 써서 서명한 토큰은 401 이다 — 알고리즘을 고정한 까닭이다")
    void 알고리즘_바꿔치기() throws Exception {
        KeyPair 키 = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        String pem = "-----BEGIN PUBLIC KEY-----\n"
                + Base64.getMimeEncoder().encodeToString(키.getPublic().getEncoded())
                + "\n-----END PUBLIC KEY-----";
        Jwt 설정 = new Jwt("RS256", null, pem, null, null, null, null, null, null);
        // 공개키는 공개다. 토큰 머리의 알고리즘을 믿으면 누구나 그것을 비밀로 HS256 서명을 만든다.
        String 위조 = sign(new MACSigner(pem.getBytes(StandardCharsets.UTF_8)), JWSAlgorithm.HS256,
                클레임("user-42").build());

        assertThat(돌린다(필터(설정), 요청(위조, null)).getResponse().getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    @DisplayName("대상을 적었으면 다른 대상의 토큰은 401 이다")
    void 대상() throws Exception {
        Jwt 설정 = new Jwt("HS256", 비밀, null, null, null, "waiting", null, null, null);
        String 남의_대상 = hs256(클레임("user-42").audience("other").build());
        String 우리_대상 = hs256(클레임("user-42").audience("waiting").build());

        assertThat(돌린다(필터(설정), 요청(남의_대상, null)).getResponse().getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
        돌린다(필터(설정), 요청(우리_대상, null));
        assertThat(넘어간_헤더.get().getFirst("X-Member-Id")).isEqualTo("user-42");
    }
}
