package com.kafkick.waiting.gateway;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * 회원 헤더의 <b>형식만</b> 본다. 서명이 없어 값 자체는 못 믿는다 — 여기서 막는
 * 것은 깨진 값이 뒷단까지 흘러가는 것뿐이다.
 *
 * <p><b>필터 계층에 둔다.</b> 순번 조회는 게이트웨이 라우트를 안 타므로,
 * 라우트에만 붙이면 그 경로가 통째로 뚫린다.
 */
public final class MemberIdentityFilter implements WebFilter {

    private static final String MEMBER_ID = "X-Member-Id";
    private static final String MEMBER_GRADE = "X-Member-Grade";

    /** 발급 계층 API 명세가 정한 값. 넓히면 뒷단이 모르는 등급이 흘러간다. */
    private static final Set<String> GRADES = Set.of("WELCOME", "SILVER", "GOLD", "VIP");

    /** 이 접두사 밖은 회원 API 가 아니다. 프로브에 회원 헤더를 붙일 리 없다. */
    private static final String API = "/api/";

    /**
     * 이유를 안 나눈다. 어느 헤더가 왜 틀렸는지 알려 주면 형식을 맞추는 데 쓰인다.
     * 뒷단과 같은 봉투를 쓴다 — 다르면 클라이언트가 둘을 다르게 다뤄야 한다.
     */
    private static final String BODY = """
            {"success":false,"data":null,"error":{"status":400,\
            "code":"COMMON-001","message":"요청 헤더가 올바르지 않습니다."}}""";

    private MemberIdentityFilter() {
    }

    /** 상태가 없지만 인스턴스다 — 검사가 늘면 여기 필드가 생긴다 (JS-13). */
    public static MemberIdentityFilter create() {
        return new MemberIdentityFilter();
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (!exchange.getRequest().getPath().value().startsWith(API)) {
            return chain.filter(exchange);
        }
        HttpHeaders headers = exchange.getRequest().getHeaders();
        if (!validId(headers.getFirst(MEMBER_ID)) || !validGrade(headers.getFirst(MEMBER_GRADE))) {
            return reject(exchange.getResponse());
        }
        // 지우지도 넣지도 않는다. 넣을 검증된 신원이 없고, 지우면 뒷단이 누구인지 모른다.
        return chain.filter(exchange);
    }

    /** 회원 식별자는 양의 정수다. 앞의 0 도 안 받는다 — 같은 사람이 두 값이 된다. */
    private boolean validId(String raw) {
        if (raw == null || raw.isEmpty() || raw.length() > 19) {
            return false;
        }
        if (raw.charAt(0) == '0') {
            return false;
        }
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            // Character.isDigit 은 아라비아 숫자 밖의 자릿수도 참이다.
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    private boolean validGrade(String raw) {
        return raw != null && GRADES.contains(raw);
    }

    private Mono<Void> reject(ServerHttpResponse response) {
        response.setStatusCode(HttpStatus.BAD_REQUEST);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        DataBuffer body = response.bufferFactory()
                .wrap(BODY.getBytes(StandardCharsets.UTF_8));
        return response.writeWith(Mono.just(body));
    }
}
