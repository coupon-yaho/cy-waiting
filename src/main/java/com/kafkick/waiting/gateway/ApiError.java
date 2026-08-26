package com.kafkick.waiting.gateway;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 게이트웨이가 직접 내는 에러도 <b>뒷단과 같은 봉투</b>를 쓴다. 다르면 클라이언트가
 * 게이트웨이 응답과 뒷단 응답을 다르게 다뤄야 하고, 그 차이로 게이트웨이의 존재와
 * 상태를 알아낼 수 있다.
 */
public final class ApiError {

    /**
     * 에러 코드는 한 곳에 모은다.
     *
     * <p>뒷단도 내는 코드는 <b>글자 그대로</b> 옮겼다. 코드가 같아도 문구가 다르면
     * 그것으로 갈리고, 없는 필드는 더 확실한 표지다.
     */
    // RULE-EXCEPTION(EX-5): logLevel 을 안 둔다. 거절은 요청마다 로그를 남기지
    // 않고 사유별 계수로만 센다 — 그 결정의 근거는 AIJ-0061 에 있다.
    public enum Code {

        /** 검증 실패·필수 헤더 누락. 뒷단의 {@code CommonErrorCode.INVALID_INPUT}. */
        INVALID_REQUEST(HttpStatus.BAD_REQUEST, "COMMON-001", "잘못된 요청입니다.", true),

        /**
         * 스냅샷에 없는 쿠폰.
         *
         * <p>뒷단은 발급 경로에서 {@code COUPON-301} 을 낸다. 명세서의
         * {@code COMMON-002} 를 쓰면 재료를 못 믿는 구간에 흘려보낸 요청만 뒷단
         * 코드를 받아, 그 차이가 <b>fail-open 이 열린 순간을 알려 주는 신호</b>가 된다.
         */
        UNKNOWN_COUPON(HttpStatus.NOT_FOUND, "COUPON-301", "쿠폰 회차를 찾을 수 없습니다.", true),

        /** 재고 소진. <b>뒷단이 내는 것과 구별되면 안 된다</b> — 그 차이가 신호가 된다. */
        SOLD_OUT(HttpStatus.CONFLICT, "COUPON-306", "쿠폰 재고가 모두 소진되었습니다.", true),

        /** 본문을 못 만들었을 때. 뒷단의 {@code CommonErrorCode.INTERNAL_ERROR}. */
        INTERNAL(HttpStatus.INTERNAL_SERVER_ERROR, "COMMON-004",
                "일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.", true),

        /**
         * 줄 자체가 꽉 찼다. 매진과 다르다 — 잠시 뒤 다시 오면 된다.
         *
         * <p>뒷단이 못 내는 상황이라 카탈로그에 없다. 구별돼도 재고를 안 알려 준다.
         */
        QUEUE_FULL(HttpStatus.TOO_MANY_REQUESTS, "QUEUE_FULL", "대기열이 가득 찼습니다.", false),

        /** 한 사람이 너무 빨리 두드린다. 큐에 안 넣는다 — 넣으면 자리를 차지한다. */
        RATE_LIMITED(HttpStatus.TOO_MANY_REQUESTS, "RATE_LIMITED",
                "요청이 너무 잦습니다.", false),

        /** 차례가 왔는데 상한을 넘었다. 큐 뒤로 안 돌린다. */
        RETRY_TOKEN(HttpStatus.TOO_MANY_REQUESTS, "RETRY_TOKEN",
                "잠시 후 다시 시도해 주세요.", false),

        /** 노드가 감당량을 넘었다. 조일 것은 쿠폰이 아니라 노드 수다. */
        TEMPORARILY_UNAVAILABLE(HttpStatus.SERVICE_UNAVAILABLE, "TEMPORARILY_UNAVAILABLE",
                "지금은 처리할 수 없습니다. 잠시 후 다시 시도해 주세요.", false);

        private final HttpStatus status;
        private final String code;
        private final String message;
        private final boolean mirrorsBackend;

        Code(HttpStatus status, String code, String message, boolean mirrorsBackend) {
            this.status = status;
            this.code = code;
            this.message = message;
            this.mirrorsBackend = mirrorsBackend;
        }

        public HttpStatus status() {
            return status;
        }

        public String code() {
            return code;
        }

        public String message() {
            return message;
        }

        /**
         * 뒷단도 내는 응답인가.
         *
         * <p>그렇다면 <b>헤더 하나도 더 붙이면 안 된다.</b> 뒷단이 안 다는 헤더를
         * 우리만 달면 그 존재만으로 게이트웨이가 끊은 것이 드러난다.
         */
        public boolean mirrorsBackend() {
            return mirrorsBackend;
        }
    }

    /** 다시 오라는 안내를 안 싣는다는 뜻. 매진처럼 다시 와도 소용없는 경우다. */
    public static final int NO_RETRY = -1;

    /**
     * 추적 키 헤더. <b>여기 한 곳에서 정한다</b> — 노출 목록과 갈리면 응답에는
     * 실리는데 브라우저는 못 읽는 상태가 되고, 그래도 기동은 된다.
     */
    public static final String REQUEST_ID = "X-Request-Id";

    private static final Logger log = LoggerFactory.getLogger(ApiError.class);

    /** 뒷단이 거르는 형식 그대로다. 다르게 거르면 그 차이가 신호가 된다. */
    private static final Pattern SAFE_REQUEST_ID =
            Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$");

    /**
     * 이스케이프가 필요한 글자. <b>문구는 우리가 정한 상수다</b> — 여기 걸리면
     * 입력이 아니라 카탈로그가 틀린 것이라 이스케이프가 아니라 실패가 맞다.
     */
    private static final Pattern NEEDS_ESCAPE = Pattern.compile("[\"\\\\\\p{Cntrl}]");

    private final Clock clock;

    /** 카탈로그 오류를 한 번만 알린다. 문구가 틀렸으면 늘 틀리다. */
    private final AtomicBoolean unserializable = new AtomicBoolean();

    private ApiError(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
    }

    public static ApiError of(Clock clock) {
        return new ApiError(clock);
    }

    /** 다시 와도 소용없는 거절. {@code Retry-After} 를 안 싣는다. */
    public Mono<Void> write(ServerWebExchange exchange, Code code) {
        return write(exchange, code, NO_RETRY);
    }

    /**
     * 거절을 응답으로 쓴다.
     *
     * <p><b>이유를 나누지 않는다.</b> 무엇이 왜 틀렸는지 알려 주면 형식을 맞추는
     * 데 쓰인다. 카탈로그의 문구 그대로만 나간다.
     *
     * @param retryAfterSec 다시 와도 되는 때. {@link #NO_RETRY} 면 안 싣는다
     */
    public Mono<Void> write(ServerWebExchange exchange, Code code, int retryAfterSec) {
        return write(exchange, code.status(), code.code(), code.message(),
                retryAfterSec, code.mirrorsBackend());
    }

    /**
     * 카탈로그를 거치지 않는 경로. <b>시험이 깨진 문구를 넣어 볼 자리다</b> —
     * 열거값만 받으면 폴백이 영영 안 도는 코드가 된다.
     */
    Mono<Void> write(ServerWebExchange exchange, HttpStatus status, String code,
            String message, int retryAfterSec, boolean mirrorsBackend) {
        ServerHttpResponse response = exchange.getResponse();
        // 헤더가 이미 나갔으면 상태 코드를 못 바꾼다. 그대로 쓰려 들면 예외가
        // 나고, 그 예외가 원래의 실패를 덮는다.
        if (response.isCommitted()) {
            return Mono.empty();
        }

        String requestId = requestId(exchange);
        byte[] body = body(status.value(), code, message, requestId);
        boolean fellBack = body == null;
        if (fellBack) {
            body = body(Code.INTERNAL.status().value(), Code.INTERNAL.code(),
                    Code.INTERNAL.message(), requestId);
        }

        response.setStatusCode(fellBack ? Code.INTERNAL.status() : status);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        // 뒷단이 안 다는 헤더는 우리도 안 단다. 프록시 캐시가 걱정되는 것은
        // 사람마다 답이 다른 응답인데, 그건 전부 게이트웨이만 내는 것이다.
        if (!mirrorsBackend && !fellBack) {
            response.getHeaders().setCacheControl("no-store");
        }
        // 뒷단도 요청마다 심는다. 안 실으면 그 부재가 곧 표지다.
        response.getHeaders().set(REQUEST_ID, requestId);
        if (retryAfterSec > NO_RETRY && !fellBack) {
            response.getHeaders().set(HttpHeaders.RETRY_AFTER, Integer.toString(retryAfterSec));
        }
        return response.writeWith(Mono.just(response.bufferFactory().wrap(body)));
    }

    /**
     * 뒷단과 같은 규칙으로 정한다. 받은 값이 형식에 맞으면 그대로 쓰고,
     * 아니면 새로 만든다 — 그대로 되쓰면 응답에 남의 문자열을 싣게 된다.
     */
    private String requestId(ServerWebExchange exchange) {
        String received = exchange.getRequest().getHeaders().getFirst(REQUEST_ID);
        if (received != null && SAFE_REQUEST_ID.matcher(received).matches()) {
            return received;
        }
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 봉투를 손으로 짜지 않게 한 곳에서 만든다. 사본이 생기면 둘이 갈라진다.
     *
     * <p><b>{@code null} 인 자리는 아예 안 쓴다.</b> 뒷단이 {@code non_null} 로
     * 직렬화해 {@code data} 키 자체가 없다 — 우리만 쓰면 그 한 글자로 갈린다.
     *
     * @return 못 만들면 {@code null}
     */
    private byte[] body(int status, String code, String message, String requestId) {
        if (unsafe(message) || unsafe(code) || unsafe(requestId)) {
            if (unserializable.compareAndSet(false, true)) {
                log.error("에러 카탈로그의 문구가 JSON 을 깬다: {}", status);
            }
            return null;
        }
        return """
                {"success":false,"error":{"status":%d,"code":"%s",\
                "message":"%s","requestId":"%s","timestamp":"%s"}}"""
                .formatted(status, code, message, requestId,
                        DateTimeFormatter.ISO_INSTANT.format(clock.instant()))
                .getBytes(StandardCharsets.UTF_8);
    }

    private boolean unsafe(String value) {
        return value == null || NEEDS_ESCAPE.matcher(value).find();
    }
}
