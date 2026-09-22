package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.JwtException;
import reactor.core.publisher.Mono;

/**
 * JWT 모드의 신원 필터 (CY-980).
 *
 * <p>요점은 둘이다. 검증된 토큰의 신원만 뒤로 넘기고, 클라이언트가 직접 보낸 신원 헤더는 지운다.
 */
class MemberIdentityJwtTest {

    private static final String 비밀 = "0123456789abcdef0123456789abcdef";

    private static final String 주소 = "/api/v1/coupons/c1/issue";

    private static final String 대상 = "waiting";

    private static final String 발급자 = "https://idp";

    private static final Instant 지금 = Instant.parse("2026-09-22T00:00:00Z");

    private static final Clock 시계 = Clock.fixed(지금, ZoneOffset.UTC);

    private static final String 없음 = "Bearer";

    private static final String 틀림 = "Bearer error=\"invalid_token\"";

    private final AtomicReference<HttpHeaders> 넘어간_헤더 = new AtomicReference<>();

    private final SimpleMeterRegistry 계측 = new SimpleMeterRegistry();

    private static String hs256(JWTClaimsSet claims) throws Exception {
        return sign(new MACSigner(비밀.getBytes(StandardCharsets.UTF_8)), JWSAlgorithm.HS256, claims);
    }

    private static String sign(JWSSigner signer, JWSAlgorithm alg, JWTClaimsSet claims)
            throws Exception {
        return sign(signer, new JWSHeader.Builder(alg).keyID("k1").build(), claims);
    }

    private static String sign(JWSSigner signer, JWSHeader header, JWTClaimsSet claims)
            throws Exception {
        SignedJWT jwt = new SignedJWT(header, claims);
        jwt.sign(signer);
        return jwt.serialize();
    }

    /** 뒷단 계약에 맞는 신원이다 — 식별자는 양의 정수, 등급은 명세의 넷 중 하나. */
    private static JWTClaimsSet.Builder 클레임(String sub) {
        return new JWTClaimsSet.Builder().subject(sub).claim("grade", "GOLD").audience(대상)
                .issuer(발급자).expirationTime(Date.from(지금.plusSeconds(300)));
    }

    private static String pem(KeyPair 키) {
        return "-----BEGIN PUBLIC KEY-----\n"
                + Base64.getMimeEncoder().encodeToString(키.getPublic().getEncoded())
                + "\n-----END PUBLIC KEY-----";
    }

    private static KeyPair rsa() throws Exception {
        KeyPairGenerator 생성 = KeyPairGenerator.getInstance("RSA");
        생성.initialize(2048);
        return 생성.generateKeyPair();
    }

    private static KeyPair ec() throws Exception {
        KeyPairGenerator 생성 = KeyPairGenerator.getInstance("EC");
        생성.initialize(new ECGenParameterSpec("secp256r1"));
        return 생성.generateKeyPair();
    }

    private MemberIdentityFilter 필터(Jwt 설정) {
        return MemberIdentityFilter.jwt(시계, 설정, JwtDecoders.of(설정, 시계), 계측);
    }

    private static Jwt hs(String issuer, String gradeClaim) {
        return new Jwt("HS256", 비밀, null, null, issuer, 대상, null, gradeClaim, null, false);
    }

    private static Jwt 공개키(String alg, KeyPair 키) {
        return new Jwt(alg, null, pem(키), null, null, 대상, null, null, null, false);
    }

    private MockServerWebExchange 요청(Consumer<MockServerHttpRequest.BaseBuilder<?>> 헤더) {
        MockServerHttpRequest.BaseBuilder<?> b = MockServerHttpRequest.post(주소);
        헤더.accept(b);
        return MockServerWebExchange.from(b.build());
    }

    private MockServerWebExchange 요청(String token) {
        return 요청(b -> b.header(HttpHeaders.AUTHORIZATION, "Bearer " + token));
    }

    private MockServerWebExchange 돌린다(MemberIdentityFilter f, MockServerWebExchange exchange) {
        넘어간_헤더.set(null);
        f.filter(exchange, e -> {
            넘어간_헤더.set(e.getRequest().getHeaders());
            return Mono.empty();
        }).block(Duration.ofSeconds(10));
        return exchange;
    }

    /** 거절은 셋을 같이 본다. 상태만 보면 다른 까닭의 401 도 통과한다. */
    private void 거절(MockServerWebExchange 교환, String challenge) {
        assertThat(교환.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(교환.getResponse().getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE))
                .isEqualTo(challenge);
        assertThat(넘어간_헤더.get()).as("뒤로 안 넘어간다").isNull();
    }

    private void 통과(String 식별자) {
        assertThat(넘어간_헤더.get().get("X-Member-Id")).containsExactly(식별자);
    }

    @Test
    @DisplayName("검증된 토큰의 식별자와 등급이 신원 헤더로 넘어간다")
    void 검증된_신원() throws Exception {
        MockServerWebExchange 교환 = 돌린다(필터(hs(null, null)), 요청(hs256(클레임("42").build())));

        통과("42");
        assertThat(넘어간_헤더.get().get("X-Member-Grade")).containsExactly("GOLD");
        assertThat(교환.getAttributes().get(MemberIdentityFilter.VERIFIED))
                .as("뒤의 필터가 이 요청이 검증됐는지 안다").isEqualTo(Boolean.TRUE);
    }

    @Test
    @DisplayName("클라이언트가 보낸 신원 헤더는 둘 다 지운다 — 토큰이 말하는 사람이 정본이다")
    void 위조한_헤더() throws Exception {
        String 토큰 = hs256(클레임("42").build());
        돌린다(필터(hs(null, null)), 요청(b -> b
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + 토큰)
                .header("X-Member-Id", "999")
                .header("X-Member-Grade", "VIP")));

        통과("42");
        assertThat(넘어간_헤더.get().get("X-Member-Grade"))
                .as("둘을 같이 두면 뒤의 필터가 어느 것을 읽을지 모른다").containsExactly("GOLD");
    }

    @Test
    @DisplayName("토큰이 없으면 401 이고 무엇을 요구하는지 알린다")
    void 토큰_없음() {
        MockServerWebExchange 교환 = 돌린다(필터(hs(null, null)), 요청(b -> b.header("X-Member-Id", "42")));
        거절(교환, 없음);
        assertThat(교환.getAttributes()).doesNotContainKey(MemberIdentityFilter.VERIFIED);
        assertThat(계측.counter(MemberIdentityFilter.REJECTED_METRIC, "reason", "missing").count())
                .isEqualTo(1.0);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"Basic dXNlcjpwdw==", "Bearer ", "Bearer", "Token abc", ""})
    @DisplayName("Bearer 가 아닌 인증 헤더는 토큰이 없는 것이다")
    void bearer_아님(String 값) {
        거절(돌린다(필터(hs(null, null)), 요청(b -> b.header(HttpHeaders.AUTHORIZATION, 값))), 없음);
    }

    @Test
    @DisplayName("인증 헤더가 두 줄이면 토큰이 없는 것이다 — 어느 것을 믿을지 모른다")
    void 두_줄() throws Exception {
        String 토큰 = hs256(클레임("42").build());

        거절(돌린다(필터(hs(null, null)), 요청(b -> b.header(HttpHeaders.AUTHORIZATION,
                "Bearer " + 토큰, "Bearer " + 토큰))), 없음);
    }

    @Test
    @DisplayName("스킴 이름은 대소문자를 안 가리고 뒤의 공백은 무시한다 (RFC 9110 11.1)")
    void 스킴_대소문자() throws Exception {
        String 토큰 = hs256(클레임("42").build());

        돌린다(필터(hs(null, null)), 요청(b -> b.header(HttpHeaders.AUTHORIZATION,
                "bEaReR   " + 토큰)));

        통과("42");
    }

    @Test
    @DisplayName("서명이 틀리면 401 이다")
    void 틀린_서명() throws Exception {
        String 남의_비밀로 = sign(new MACSigner("ffffffffffffffffffffffffffffffff".getBytes(
                StandardCharsets.UTF_8)), JWSAlgorithm.HS256, 클레임("42").build());

        거절(돌린다(필터(hs(null, null)), 요청(남의_비밀로)), 틀림);
        assertThat(계측.counter(MemberIdentityFilter.REJECTED_METRIC, "reason", "invalid").count())
                .isEqualTo(1.0);
    }

    @Test
    @DisplayName("만료는 허용 오차 30초까지만 봐 준다")
    void 만료() throws Exception {
        String 막_지난 = hs256(클레임("42").expirationTime(Date.from(지금.minusSeconds(29))).build());
        String 넘게_지난 = hs256(클레임("42").expirationTime(Date.from(지금.minusSeconds(31))).build());

        돌린다(필터(hs(null, null)), 요청(막_지난));
        통과("42");
        거절(돌린다(필터(hs(null, null)), 요청(넘게_지난)), 틀림);
    }

    @Test
    @DisplayName("만료가 없는 토큰은 401 이다 — 새면 영원히 유효하다")
    void 만료_없음() throws Exception {
        String 영원한 = hs256(클레임("42").expirationTime(null).build());

        거절(돌린다(필터(hs(null, null)), 요청(영원한)), 틀림);
    }

    @Test
    @DisplayName("아직 유효하지 않은 토큰도 허용 오차 30초까지만 봐 준다")
    void 유효_시작() throws Exception {
        String 곧 = hs256(클레임("42").notBeforeTime(Date.from(지금.plusSeconds(29))).build());
        String 한참_뒤 = hs256(클레임("42").notBeforeTime(Date.from(지금.plusSeconds(31))).build());

        돌린다(필터(hs(null, null)), 요청(곧));
        통과("42");
        거절(돌린다(필터(hs(null, null)), 요청(한참_뒤)), 틀림);
    }

    @Test
    @DisplayName("발급자를 적었으면 다른 발급자는 401 이다")
    void 발급자() throws Exception {
        String 우리_발급자 = hs256(클레임("42").build());
        String 남의_발급자 = hs256(클레임("42").issuer("https://other").build());

        돌린다(필터(hs(발급자, null)), 요청(우리_발급자));
        통과("42");
        거절(돌린다(필터(hs(발급자, null)), 요청(남의_발급자)), 틀림);
    }

    @Test
    @DisplayName("다른 대상의 토큰은 401 이다 — 같은 발급자의 다른 서비스용 토큰이다")
    void 대상() throws Exception {
        String 남의_대상 = hs256(클레임("42").audience("other").build());
        String 대상_없음 = hs256(클레임("42").audience((String) null).build());

        거절(돌린다(필터(hs(null, null)), 요청(남의_대상)), 틀림);
        거절(돌린다(필터(hs(null, null)), 요청(대상_없음)), 틀림);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"user-42", "042", "0", "-1", "9223372036854775808", "a\nb", "４２"})
    @DisplayName("식별자는 헤더 모드와 같은 계약이다 — 양의 정수, 앞의 0 없음, Long 범위")
    void 나쁜_식별자(String 식별자) throws Exception {
        거절(돌린다(필터(hs(null, null)), 요청(hs256(클레임(식별자).build()))), 틀림);
    }

    @Test
    @DisplayName("식별자 클레임이 없으면 401 이다")
    void 식별자_없음() throws Exception {
        거절(돌린다(필터(hs(null, null)), 요청(hs256(클레임(null).build()))), 틀림);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"ADMIN", "gold", ""})
    @DisplayName("등급이 명세의 넷 밖이면 401 이다 — 넓히면 뒷단이 모르는 등급이 흘러간다")
    void 나쁜_등급(String 등급) throws Exception {
        거절(돌린다(필터(hs(null, null)), 요청(hs256(클레임("42").claim("grade", 등급).build()))),
                틀림);
    }

    @Test
    @DisplayName("등급 클레임이 없으면 401 이다 — 뒷단 계약이 등급을 요구한다")
    void 등급_없음() throws Exception {
        거절(돌린다(필터(hs(null, null)), 요청(hs256(클레임("42").claim("grade", null).build()))),
                틀림);
    }

    @Test
    @DisplayName("등급 클레임 이름을 바꿀 수 있다")
    void 등급_클레임_이름() throws Exception {
        String 토큰 = hs256(클레임("42").claim("tier", "VIP").build());

        돌린다(필터(hs(null, "tier")), 요청(토큰));

        assertThat(넘어간_헤더.get().get("X-Member-Grade")).containsExactly("VIP");
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

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"HS384", "HS512"})
    @DisplayName("HMAC 은 적은 알고리즘으로만 검증한다")
    void hmac_알고리즘(String alg) throws Exception {
        byte[] 긴_비밀 = (비밀 + 비밀).getBytes(StandardCharsets.UTF_8);
        Jwt 설정 = new Jwt(alg, 비밀 + 비밀, null, null, null, 대상, null, null, null, false);
        String 토큰 = sign(new MACSigner(긴_비밀), JWSAlgorithm.parse(alg), 클레임("42").build());
        String 다른_알고리즘 = sign(new MACSigner(긴_비밀), JWSAlgorithm.HS256, 클레임("42").build());

        돌린다(필터(설정), 요청(토큰));
        통과("42");
        거절(돌린다(필터(설정), 요청(다른_알고리즘)), 틀림);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"RS256", "RS384", "RS512"})
    @DisplayName("RSA 공개키는 적은 알고리즘으로만 검증한다")
    void rsa_알고리즘(String alg) throws Exception {
        KeyPair 키 = rsa();
        String 토큰 = sign(new RSASSASigner(키.getPrivate()), JWSAlgorithm.parse(alg),
                클레임("7").build());
        JWSAlgorithm 다른 = "RS256".equals(alg) ? JWSAlgorithm.RS512 : JWSAlgorithm.RS256;
        String 다른_알고리즘 = sign(new RSASSASigner(키.getPrivate()), 다른, 클레임("7").build());

        돌린다(필터(공개키(alg, 키)), 요청(토큰));
        통과("7");
        거절(돌린다(필터(공개키(alg, 키)), 요청(다른_알고리즘)), 틀림);
    }

    @Test
    @DisplayName("2048 비트보다 짧은 RSA 공개키는 기동에서 막는다")
    void 짧은_rsa() throws Exception {
        KeyPairGenerator 생성 = KeyPairGenerator.getInstance("RSA");
        생성.initialize(1024);
        Jwt 설정 = 공개키("RS256", 생성.generateKeyPair());

        assertThatThrownBy(() -> JwtDecoders.of(설정, 시계))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("2048");
    }

    @Test
    @DisplayName("EC 공개키로 검증한다 — kid 가 있든 없든")
    void ec_공개키() throws Exception {
        KeyPair 키 = ec();
        ECDSASigner 서명기 = new ECDSASigner((ECPrivateKey) 키.getPrivate());
        String kid_있음 = sign(서명기, JWSAlgorithm.ES256, 클레임("3").build());
        String kid_없음 = sign(서명기, new JWSHeader(JWSAlgorithm.ES256), 클레임("3").build());

        돌린다(필터(공개키("ES256", 키)), 요청(kid_있음));
        통과("3");
        돌린다(필터(공개키("ES256", 키)), 요청(kid_없음));
        통과("3");
    }

    @Test
    @DisplayName("EC 알고리즘과 키의 곡선이 어긋나면 기동에서 막는다 — 안 막으면 전원 401 이다")
    void ec_곡선() throws Exception {
        assertThatThrownBy(() -> JwtDecoders.of(공개키("ES384", ec()), 시계))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("곡선");
    }

    @Test
    @DisplayName("다른 EC 키로 서명한 토큰은 401 이다")
    void ec_다른_키() throws Exception {
        String 남의_키로 = sign(new ECDSASigner((ECPrivateKey) ec().getPrivate()), JWSAlgorithm.ES256,
                클레임("3").build());

        거절(돌린다(필터(공개키("ES256", ec())), 요청(남의_키로)), 틀림);
    }

    @Test
    @DisplayName("서명 없는 토큰은 401 이다")
    void 서명_없음() {
        String 서명없는 = new PlainJWT(클레임("42").build()).serialize();

        거절(돌린다(필터(hs(null, null)), 요청(서명없는)), 틀림);
    }

    @Test
    @DisplayName("공개키를 HMAC 비밀로 써서 서명한 토큰은 401 이다 — 알고리즘을 고정한 까닭이다")
    void 알고리즘_바꿔치기() throws Exception {
        KeyPair 키 = rsa();
        // 공개키는 공개다. 토큰 머리의 알고리즘을 믿으면 누구나 그것을 비밀로 HS256 서명을 만든다.
        String 위조 = sign(new MACSigner(pem(키).getBytes(StandardCharsets.UTF_8)),
                JWSAlgorithm.HS256, 클레임("42").build());

        거절(돌린다(필터(공개키("RS256", 키)), 요청(위조)), 틀림);
    }

    @Test
    @DisplayName("JWKS 주소에서 키를 받아 검증하고, 집합에 없는 키는 401 이다")
    void jwks() throws Exception {
        KeyPair 키 = rsa();
        RSAKey jwk = new RSAKey.Builder((RSAPublicKey) 키.getPublic()).keyID("k1").build();
        HttpServer 서버 = jwks서버(200, new JWKSet(jwk).toString().getBytes(StandardCharsets.UTF_8));
        try {
            MemberIdentityFilter 필터 = 필터(jwks설정(서버));
            String 토큰 = sign(new RSASSASigner(키.getPrivate()), JWSAlgorithm.RS256,
                    클레임("9").build());
            String 남의_키로 = sign(new RSASSASigner(rsa().getPrivate()), JWSAlgorithm.RS256,
                    클레임("9").build());
            String 모르는_kid = sign(new RSASSASigner(키.getPrivate()),
                    new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("k2").build(),
                    클레임("9").build());

            돌린다(필터, 요청(토큰));
            통과("9");
            for (String 나쁜 : List.of(남의_키로, 모르는_kid)) {
                거절(돌린다(필터, 요청(나쁜)), 틀림);
            }
        } finally {
            서버.stop(0);
        }
    }

    @Test
    @DisplayName("키 집합을 못 받으면 401 이 아니라 503 이다 — 토큰 탓이 아니다")
    void jwks_장애() throws Exception {
        HttpServer 서버 = jwks서버(500, new byte[0]);
        try {
            String 토큰 = sign(new RSASSASigner(rsa().getPrivate()), JWSAlgorithm.RS256,
                    클레임("9").build());

            MockServerWebExchange 교환 = 돌린다(필터(jwks설정(서버)), 요청(토큰));

            assertThat(교환.getResponse().getStatusCode())
                    .as("401 이면 클라이언트는 다시 로그인하고, 운영은 장애를 못 본다")
                    .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
            assertThat(넘어간_헤더.get()).isNull();
            assertThat(계측.counter(MemberIdentityFilter.REJECTED_METRIC, "reason", "unavailable")
                    .count()).isEqualTo(1.0);
        } finally {
            서버.stop(0);
        }
    }

    private static HttpServer jwks서버(int 상태, byte[] 본문) throws Exception {
        HttpServer 서버 = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        서버.createContext("/jwks", ex -> {
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(상태, 본문.length == 0 ? -1 : 본문.length);
            ex.getResponseBody().write(본문);
            ex.close();
        });
        서버.start();
        return 서버;
    }

    /** 시험 서버는 http 라 명시적으로 허용한다. */
    private static Jwt jwks설정(HttpServer 서버) {
        return new Jwt("RS256", null, null,
                "http://127.0.0.1:" + 서버.getAddress().getPort() + "/jwks",
                발급자, 대상, null, null, null, true);
    }

    @Test
    @DisplayName("스프링이 쓰는 생성자가 기동 검사를 건다")
    void 기동_검사() throws Exception {
        AuthProperties 켬 = new AuthProperties(AuthProperties.Mode.JWT, hs(null, null));
        EntryTokenDelivery 기본 = new EntryTokenDelivery(null, null, null);
        EntryTokenDelivery 겹침 = new EntryTokenDelivery(null, null, "X-Member-Id");

        assertThatThrownBy(() -> new MemberIdentityFilter(시계, 켬, 겹침, 계측))
                .hasMessageContaining("입장 토큰 헤더로 못 쓴다");

        MemberIdentityFilter 필터 = new MemberIdentityFilter(시계, 켬, 기본, 계측);
        거절(돌린다(필터, 요청(b -> b.header("X-Member-Id", "42"))), 없음);
        돌린다(필터, 요청(hs256(클레임("42").build())));
        통과("42");
    }

    @Test
    @DisplayName("토큰 탓이 아닌 검증기 오류는 401 로 바꾸지 않는다 — 내부 장애가 다시 로그인하라로 보이면 안 된다")
    void 검증기_내부_오류() throws Exception {
        MemberIdentityFilter 내부_오류 = MemberIdentityFilter.jwt(시계, hs(null, null),
                t -> Mono.error(new JwtException("암호 처리 실패")), 계측);
        MockServerWebExchange 교환 = 요청(hs256(클레임("42").build()));

        assertThatThrownBy(() -> 돌린다(내부_오류, 교환)).isInstanceOf(JwtException.class);
        assertThat(교환.getResponse().getStatusCode()).isNull();
        assertThat(교환.getResponse().getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE)).isNull();
        assertThat(계측.counter(MemberIdentityFilter.REJECTED_METRIC, "reason", "invalid").count())
                .isZero();

        MemberIdentityFilter 토큰_탓 = MemberIdentityFilter.jwt(시계, hs(null, null),
                t -> Mono.error(new BadJwtException("서명 틀림")), 계측);
        거절(돌린다(토큰_탓, 요청(hs256(클레임("42").build()))), 틀림);
        assertThat(계측.counter(MemberIdentityFilter.REJECTED_METRIC, "reason", "invalid").count())
                .isEqualTo(1.0);
    }
}
