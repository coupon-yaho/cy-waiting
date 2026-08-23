package com.kafkick.waiting.gateway;

import java.nio.charset.StandardCharsets;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import reactor.core.publisher.Mono;

/**
 * 게이트웨이가 직접 내는 에러도 <b>뒷단과 같은 봉투</b>를 쓴다. 다르면 클라이언트가
 * 게이트웨이 응답과 뒷단 응답을 다르게 다뤄야 하고, 그 차이로 게이트웨이의 존재와
 * 상태를 알아낼 수 있다.
 */
public final class ApiError {

    /** 검증 실패·필수 헤더 누락. 발급 계층 에러 코드 체계를 따른다. */
    public static final String INVALID_REQUEST = "COMMON-001";

    private ApiError() {
    }

    /** 상태가 없지만 인스턴스다 — 요청 추적 키가 붙으면 여기 필드가 생긴다. */
    public static ApiError create() {
        return new ApiError();
    }

    /**
     * <b>이유를 나누지 않는다.</b> 무엇이 왜 틀렸는지 알려 주면 형식을 맞추는 데
     * 쓰인다. 본문은 미리 만들어 둔다 — 거절마다 인코딩하면 그게 곧 비용이다.
     */
    public Mono<Void> write(ServerHttpResponse response, HttpStatus status, byte[] body) {
        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        return response.writeWith(Mono.just(response.bufferFactory().wrap(body)));
    }

    /** 봉투를 손으로 짜지 않게 한 곳에서 만든다. 사본이 생기면 둘이 갈라진다. */
    public byte[] body(HttpStatus status, String code, String message) {
        return """
                {"success":false,"data":null,"error":{"status":%d,"code":"%s","message":"%s"}}"""
                .formatted(status.value(), code, message).getBytes(StandardCharsets.UTF_8);
    }
}
