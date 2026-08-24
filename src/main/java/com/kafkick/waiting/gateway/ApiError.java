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
     * 상태·코드·문구를 한 자리에 둔다.
     *
     * <p>따로 두면 코드와 상태가 어긋나는 조합이 생기고, 그 조합은 응답이
     * 나가기 전까지 아무도 못 본다.
     */
    public record Code(HttpStatus status, String code, String message) {
        public Code {
            Objects.requireNonNull(status, "status 는 필수다");
            Objects.requireNonNull(code, "code 는 필수다");
            Objects.requireNonNull(message, "message 는 필수다");
        }
    }

    /** 검증 실패·필수 헤더 누락. 뒷단의 {@code CommonErrorCode} 그대로다. */
    public static final Code INVALID_REQUEST =
            new Code(HttpStatus.BAD_REQUEST, "COMMON-001", "잘못된 요청입니다.");

    /** 스냅샷에 없는 쿠폰. 뒷단이 없는 쿠폰에 내는 것과 같아야 한다. */
    public static final Code NOT_FOUND =
            new Code(HttpStatus.NOT_FOUND, "COMMON-002", "요청한 리소스를 찾을 수 없습니다.");

    /**
     * 재고 소진. <b>뒷단이 내는 것과 구별되면 안 된다</b> — 그 차이가 신호가 된다.
     * 뒷단의 {@code CouponIssueErrorCode.SOLD_OUT} 을 글자 그대로 옮겼다.
     */
    public static final Code SOLD_OUT =
            new Code(HttpStatus.CONFLICT, "COUPON-306", "쿠폰 재고가 모두 소진되었습니다.");

    /**
     * 줄 자체가 꽉 찼다. 매진과 다르다 — 잠시 뒤 다시 오면 된다.
     *
     * <p>뒷단이 못 내는 상황이라 카탈로그에 없다. 구별돼도 재고를 알려 주지 않는다.
     */
    public static final Code QUEUE_FULL =
            new Code(HttpStatus.TOO_MANY_REQUESTS, "QUEUE_FULL", "대기열이 가득 찼습니다.");

    /** 차례가 왔는데 상한을 넘었다. 큐 뒤로 안 돌린다. */
    public static final Code RETRY_TOKEN =
            new Code(HttpStatus.TOO_MANY_REQUESTS, "RETRY_TOKEN", "잠시 후 다시 시도해 주세요.");

    /** 노드가 감당량을 넘었다. 조일 것은 쿠폰이 아니라 노드 수다. */
    public static final Code TEMPORARILY_UNAVAILABLE = new Code(
            HttpStatus.SERVICE_UNAVAILABLE, "TEMPORARILY_UNAVAILABLE",
            "일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.");

    /** 다시 오라는 안내를 안 싣는다는 뜻. 매진처럼 다시 와도 소용없는 경우다. */
    public static final int NO_RETRY = -1;

    /**
     * 본문을 못 만들었을 때 내는 것. <b>미리 인코딩해 둔다</b> — 만들다 실패한
     * 자리에서 또 만들면 같은 이유로 또 실패한다.
     */
    public static final byte[] FALLBACK = ("""
            {"success":false,"data":null,"error":{"status":500,"code":"COMMON-004",\
            "message":"일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.",\
            "requestId":null,"timestamp":null}}""").getBytes(StandardCharsets.UTF_8);

    private static final Logger log = LoggerFactory.getLogger(ApiError.class);

    private static final String REQUEST_ID = "X-Request-Id";

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
        ServerHttpResponse response = exchange.getResponse();
        // 헤더가 이미 나갔으면 상태 코드를 못 바꾼다. 그대로 쓰려 들면 예외가
        // 나고, 그 예외가 원래의 실패를 덮는다.
        if (response.isCommitted()) {
            return Mono.empty();
        }

        String requestId = requestId(exchange);
        byte[] body = body(code, requestId);

        response.setStatusCode(body == FALLBACK ? HttpStatus.INTERNAL_SERVER_ERROR : code.status());
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        // 프록시가 캐시하면 뒤에 온 사람이 남의 답을 받는다. 매진은 재입고로
        // 뒤집히고 순번은 사람마다 다르다.
        response.getHeaders().setCacheControl("no-store");
        response.getHeaders().set(REQUEST_ID, requestId);
        if (retryAfterSec > NO_RETRY) {
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

    /** 봉투를 손으로 짜지 않게 한 곳에서 만든다. 사본이 생기면 둘이 갈라진다. */
    private byte[] body(Code code, String requestId) {
        if (NEEDS_ESCAPE.matcher(code.message()).find()
                || NEEDS_ESCAPE.matcher(code.code()).find()) {
            if (unserializable.compareAndSet(false, true)) {
                log.error("에러 카탈로그의 문구가 JSON 을 깬다: {}", code.code());
            }
            return FALLBACK;
        }
        return """
                {"success":false,"data":null,"error":{"status":%d,"code":"%s",\
                "message":"%s","requestId":"%s","timestamp":"%s"}}"""
                .formatted(code.status().value(), code.code(), code.message(), requestId,
                        DateTimeFormatter.ISO_INSTANT.format(clock.instant()))
                .getBytes(StandardCharsets.UTF_8);
    }
}
