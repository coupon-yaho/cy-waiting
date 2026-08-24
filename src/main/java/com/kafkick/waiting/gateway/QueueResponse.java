package com.kafkick.waiting.gateway;

import java.nio.charset.StandardCharsets;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 대기 응답을 쓴다.
 *
 * <p><b>게이트웨이만 내는 응답이다.</b> 사람마다 순번이 다르므로 캐시를 막고,
 * 뒷단이 못 내는 응답이라 그 헤더가 표지가 되지도 않는다.
 *
 * <p>본문의 값은 전부 우리가 만든 것이라 봉투를 깨는 글자가 못 들어온다.
 */
public final class QueueResponse {

    private static final String NO_STORE = "no-store";

    private QueueResponse() {
    }

    public static QueueResponse create() {
        return new QueueResponse();
    }

    /**
     * 줄에 세웠다고 알린다.
     *
     * @param pollAfterSec 다음에 물을 때까지의 초. 흔들어서 파도를 흩는다
     */
    public Mono<Void> waiting(ServerWebExchange exchange, String queueToken, long position,
            long etaSec, String queueMode, long pollAfterSec) {
        return write(exchange, HttpStatus.ACCEPTED, """
                {"success":true,"data":{"admitted":false,"queueToken":"%s",\
                "position":%d,"etaSeconds":%d,"queueMode":"%s"}}"""
                .formatted(queueToken, position, etaSec, queueMode), pollAfterSec);
    }

    /**
     * 폴링에 답한다. <b>차례가 왔는지만 말하고 토큰은 아직 안 준다</b> —
     * 입장 토큰은 다음 티켓이다.
     */
    public Mono<Void> status(ServerWebExchange exchange, String state, long position,
            long etaSec, long pollAfterSec) {
        String data = switch (state) {
            case "WAITING" -> """
                    {"status":"WAITING","position":%d,"etaSeconds":%d}"""
                    .formatted(position, etaSec);
            case "ADMITTED" -> """
                    {"status":"ADMITTED"}""";
            default -> """
                    {"status":"CLOSED","reason":"STOCK_EXHAUSTED"}""";
        };
        return write(exchange, HttpStatus.OK,
                """
                {"success":true,"data":%s}""".formatted(data), pollAfterSec);
    }

    private Mono<Void> write(ServerWebExchange exchange, HttpStatus status, String body,
            long pollAfterSec) {
        ServerHttpResponse response = exchange.getResponse();
        // 헤더가 이미 나갔으면 상태 코드를 못 바꾼다. 되돌리려 들면 그 예외가
        // 원래의 결과를 덮는다.
        if (response.isCommitted()) {
            return Mono.empty();
        }
        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        response.getHeaders().setCacheControl(NO_STORE);
        if (pollAfterSec > 0) {
            response.getHeaders().set(HttpHeaders.RETRY_AFTER, Long.toString(pollAfterSec));
        }
        return response.writeWith(
                Mono.just(response.bufferFactory().wrap(body.getBytes(StandardCharsets.UTF_8))));
    }
}
