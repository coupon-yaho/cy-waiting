package com.kafkick.waiting.gateway;

import java.time.Clock;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Set;
import org.springframework.http.HttpHeaders;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;
import reactor.core.publisher.Mono;

/**
 * 회원 헤더의 <b>형식만</b> 본다. 서명이 없어 값 자체는 못 믿는다 — 여기서 막는
 * 것은 깨진 값이 뒷단까지 흘러가는 것뿐이다.
 *
 * <p><b>필터 계층에 둔다.</b> 순번 조회는 게이트웨이 라우트를 안 타므로,
 * 라우트에만 붙이면 그 경로가 통째로 뚫린다.
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

    private final ApiError error;

    MemberIdentityFilter(Clock clock) {
        this.error = ApiError.of(clock);
    }

    public static MemberIdentityFilter of(Clock clock) {
        return new MemberIdentityFilter(clock);
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (!API.matches(exchange.getRequest().getPath().pathWithinApplication())) {
            return chain.filter(exchange);
        }
        HttpHeaders headers = exchange.getRequest().getHeaders();
        if (!validId(headers.get(MEMBER_ID)) || !validGrade(headers.get(MEMBER_GRADE))) {
            return error.write(exchange, ApiError.Code.INVALID_REQUEST);
        }
        // 지우지도 넣지도 않는다. 넣을 검증된 신원이 없고, 지우면 뒷단이 누구인지 모른다.
        return chain.filter(exchange);
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
