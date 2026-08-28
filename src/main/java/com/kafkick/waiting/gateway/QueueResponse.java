package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.queue.QueueState;
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
    public Mono<Void> status(ServerWebExchange exchange, QueueState state, long position,
            long etaSec, long pollAfterSec) {
        // **전수로 적는다.** 빠뜨린 상태가 조용히 매진으로 나가면, 기다리던
        // 사람에게 끝났다고 말하는 셈이다.
        String data = switch (state) {
            case WAITING -> """
                    {"status":"WAITING","position":%d,"etaSeconds":%d}"""
                    .formatted(position, etaSec);
            // 입장은 토큰을 실어야 하므로 여기로 안 온다.
            case ADMITTED -> throw new IllegalArgumentException("입장은 따로 쓴다: " + state);
            // 줄에 없다. 이탈로 걷혔거나 큐가 정리됐다 — 어느 쪽이든 다시 서야 한다.
            //
            // **매진과 사유를 갈라 쓴다.** 매진은 앞에서 `SOLD_OUT` 으로 끝나므로
            // 여기까지 오는 것은 재고와 무관한 이유다. 둘이 같은 사유를 쓰면
            // 이탈로 지워진 사람이 "다 팔렸다" 는 말을 듣고, 다시 설 수 있는데도
            // 안 선다.
            case NOT_QUEUED -> """
                    {"status":"CLOSED","reason":"NOT_IN_QUEUE"}""";
            // 조회로는 안 나온다. 등록 결과에만 있는 상태다.
            case REJECTED -> throw new IllegalArgumentException("조회 결과가 아니다: " + state);
        };
        return write(exchange, HttpStatus.OK,
                """
                {"success":true,"data":%s}""".formatted(data), pollAfterSec);
    }

    /** 끝난 캠페인이 폴링을 계속 만들어 내지 않게, 다시 올 시각을 안 준다. */
    public Mono<Void> soldOut(ServerWebExchange exchange) {
        return write(exchange, HttpStatus.OK,
                """
                {"success":true,"data":{"status":"SOLD_OUT","reason":"STOCK_EXHAUSTED"}}""",
                0);
    }

    /**
     * 차례가 왔다고 알린다. <b>토큰을 여기서 준다</b> — 폴링해 온 사람에게 그
     * 자리에서 주면 안 돌아온 사람 몫이 안 버려진다.
     */
    public Mono<Void> admitted(ServerWebExchange exchange, String entryToken, long expiresIn) {
        return write(exchange, HttpStatus.OK, """
                {"success":true,"data":{"status":"ADMITTED",\
                "entryToken":"%s","expiresIn":%d}}"""
                .formatted(entryToken, expiresIn), 0);
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
