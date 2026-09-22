package com.kafkick.waiting.gateway;

import java.time.Clock;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;
import reactor.core.publisher.Mono;

/**
 * 회원 헤더의 <b>형식만</b> 본다. 서명이 없어 값 자체는 못 믿고, 막는 것은 깨진
 * 값이 뒷단까지 흘러가는 것뿐이다. 필터 계층에 두는 것은 순번 조회가 라우트를 안
 * 타서, 라우트에만 붙이면 그 경로가 통째로 뚫리기 때문이다.
 */
@Component
@Order(FilterOrder.IDENTITY)
public final class MemberIdentityFilter implements WebFilter {

    private static final String MEMBER_ID = "X-Member-Id";
    private static final String MEMBER_GRADE = "X-Member-Grade";

    /** 발급 계층 API 명세가 정한 값. 넓히면 뒷단이 모르는 등급이 흘러간다. */
    private static final Set<String> GRADES = Set.of("WELCOME", "SILVER", "GOLD", "VIP");

    /**
     * <b>핸들러와 같은 방식으로 맞춘다.</b> 원본 경로를 문자열로 비교하면
     * {@code /%61pi/...} 처럼 인코딩된 요청이 여기서는 회원 API 가 아닌 것으로
     * 보이는데 라우트는 그대로 잡는다 — 검증 없이 지나간다.
     */
    private static final PathPattern API = PathPatternParser.defaultInstance.parse("/api/**");

    /**
     * 토큰에서 꺼낸 식별자가 받을 수 있는 모양. <b>그 값이 레디스 줄과 서명 토큰에 들어간다</b> — 헤더를
     * 가르는 문자나 서명 구분자를 품으면 경계가 옮겨진다.
     */
    private static final Pattern SUBJECT = Pattern.compile("^[A-Za-z0-9_.:@+-]{1,64}$");

    private static final String BEARER = "bearer ";

    private final ApiError error;

    /** 인증을 켰을 때만 있다. 없으면 지금처럼 헤더를 형식만 본다. */
    private final ReactiveJwtDecoder decoder;

    private final AuthProperties.Jwt jwt;

    private MemberIdentityFilter(Clock clock, AuthProperties.Jwt jwt, ReactiveJwtDecoder decoder) {
        this.error = ApiError.of(clock);
        this.jwt = jwt;
        this.decoder = decoder;
    }

    @Autowired
    MemberIdentityFilter(Clock clock, AuthProperties auth, EntryTokenDelivery delivery) {
        this(clock, auth.jwt(),
                auth.mode() == AuthProperties.Mode.JWT ? JwtDecoders.of(auth.jwt(), clock) : null);
        auth.checkAgainst(delivery);
    }

    public static MemberIdentityFilter of(Clock clock) {
        return new MemberIdentityFilter(clock, (AuthProperties.Jwt) null, null);
    }

    /** 인증을 켠 필터. 시험이 검증기를 직접 꽂는다. */
    public static MemberIdentityFilter jwt(Clock clock, AuthProperties.Jwt jwt,
            ReactiveJwtDecoder decoder) {
        return new MemberIdentityFilter(clock, jwt, decoder);
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (!API.matches(exchange.getRequest().getPath().pathWithinApplication())) {
            return chain.filter(exchange);
        }
        if (decoder != null) {
            return authenticated(exchange, chain);
        }
        HttpHeaders headers = exchange.getRequest().getHeaders();
        if (!validId(headers.get(MEMBER_ID)) || !validGrade(headers.get(MEMBER_GRADE))) {
            return error.write(exchange, ApiError.Code.INVALID_REQUEST);
        }
        // 지우지도 넣지도 않는다. 넣을 검증된 신원이 없고, 지우면 뒷단이 누구인지 모른다.
        return chain.filter(exchange);
    }

    /**
     * 검증된 토큰의 신원만 뒤로 넘긴다. <b>클라이언트가 보낸 회원 헤더는 지운다</b> — 둘을 같이 두면 뒤의
     * 필터가 어느 것을 읽을지 모르고, 뒷단은 위조한 값을 믿는다.
     */
    private Mono<Void> authenticated(ServerWebExchange exchange, WebFilterChain chain) {
        String token = bearer(exchange.getRequest().getHeaders().get(HttpHeaders.AUTHORIZATION));
        if (token == null) {
            return unauthorized(exchange, "Bearer");
        }
        // **검증 실패만 401 로 바꾼다.** 뒤의 필터에서 난 오류까지 삼키면 장애가 인증 실패로 보인다.
        return decoder.decode(token)
                .map(this::identity)
                .onErrorResume(JwtException.class, e -> Mono.just(Identity.INVALID))
                .flatMap(id -> id == Identity.INVALID
                        ? unauthorized(exchange, "Bearer error=\"invalid_token\"")
                        : chain.filter(exchange.mutate().request(r -> r.headers(h -> {
                            h.remove(MEMBER_ID);
                            h.remove(MEMBER_GRADE);
                            h.set(MEMBER_ID, id.member());
                            if (id.grade() != null) {
                                h.set(MEMBER_GRADE, id.grade());
                            }
                        })).build()));
    }

    private Identity identity(Jwt token) {
        String member = token.getClaimAsString(jwt.memberClaim());
        if (member == null || !SUBJECT.matcher(member).matches()) {
            return Identity.INVALID;
        }
        if (jwt.gradeClaim() == null) {
            return new Identity(member, null);
        }
        String grade = token.getClaimAsString(jwt.gradeClaim());
        return grade != null && GRADES.contains(grade) ? new Identity(member, grade) : Identity.INVALID;
    }

    private Mono<Void> unauthorized(ServerWebExchange exchange, String challenge) {
        exchange.getResponse().getHeaders().set(HttpHeaders.WWW_AUTHENTICATE, challenge);
        return error.write(exchange, ApiError.Code.UNAUTHORIZED);
    }

    /** 한 줄짜리 {@code Bearer} 만 받는다. 줄이 둘이면 어느 토큰을 믿을지 모른다. */
    private String bearer(List<String> values) {
        String raw = single(values);
        if (raw == null || raw.length() <= BEARER.length()
                || !raw.substring(0, BEARER.length()).toLowerCase(Locale.ROOT).equals(BEARER)) {
            return null;
        }
        String token = raw.substring(BEARER.length()).trim();
        return token.isEmpty() ? null : token;
    }

    /** 검증된 신원. 등급은 등급 클레임을 적었을 때만 있다. */
    private record Identity(String member, String grade) {
        static final Identity INVALID = new Identity("", null);
    }

    /**
     * <b>줄이 둘이면 거절한다.</b> 판정은 첫 줄만 보는데 전달은 전부 그대로 간다.
     * 뒷단이 마지막 값을 쓰면 판정한 값과 실제로 쓰이는 값이 달라진다.
     */
    private String single(List<String> values) {
        return values == null || values.size() != 1 ? null : values.get(0);
    }

    /** 회원 식별자는 양의 정수다. 앞의 0 도 안 받는다 — 같은 사람이 두 값이 된다. */
    private boolean validId(List<String> values) {
        String raw = single(values);
        if (raw == null || raw.isEmpty() || raw.length() > 19 || raw.charAt(0) == '0') {
            return false;
        }
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            // Character.isDigit 은 아라비아 숫자 밖의 자릿수도 참이다.
            if (c < '0' || c > '9') {
                return false;
            }
        }
        // 자릿수만 보면 뒷단 파싱이 넘친다. 헤더 한 줄로 500 을 만들 수 있다.
        try {
            Long.parseLong(raw);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean validGrade(List<String> values) {
        // 불변 집합은 null 조회에 던진다. 그대로 두면 400 자리에 500 이 나간다.
        String raw = single(values);
        return raw != null && GRADES.contains(raw);
    }
}
