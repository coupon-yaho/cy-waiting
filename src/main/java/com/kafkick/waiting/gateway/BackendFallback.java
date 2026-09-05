package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.admission.AdmissionDecision;
import com.kafkick.waiting.domain.queue.EtaPolicy;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

/**
 * 서킷이 열렸을 때 사용자가 받는 것.
 *
 * <p><b>이 자리가 비면 404 가 나간다.</b> 프레임워크는 fallback 주소로 넘길 뿐이고,
 * 그 주소를 아무도 안 받으면 "없는 경로" 가 된다. 사용자에게 404 는 매진으로
 * 읽히므로 다시 오지 않는다 — 뒷단은 잠깐 흔들렸을 뿐인데.
 */
public final class BackendFallback {

    private static final Logger log = LoggerFactory.getLogger(BackendFallback.class);

    private static final String METRIC = "waiting.backend.fallback";

    /** 재시도를 흩는 폭. 판정 경로와 같은 값이라야 두 안내가 안 갈린다. */
    private static final PollIntervalPolicy POLL = PollIntervalPolicy.of(PollIntervalPolicy.NORMAL_JITTER_RATIO);

    /**
     * <b>줄에 선 사람에게 자리가 그대로라고 말한다.</b> 안 그러면 다시 줄을
     * 서려 하고, 그건 자기 자리를 버리는 일이다.
     */
    private static final String QUEUED =
            "지금은 발급을 처리할 수 없습니다. 대기 순번은 그대로 유지되니 "
                    + "잠시 후 다시 시도해 주세요.";

    /**
     * <b>차례가 온 사람에게는 다른 말을 한다.</b> 그는 이미 큐에서 빠졌고 손에
     * 든 것은 수명이 있는 입장 토큰뿐이다. "순번이 유지된다" 는 그에게 거짓이고,
     * 멀리 보내면 그 사이 토큰이 죽어 줄 맨 뒤로 다시 선다.
     */
    private static final String ADMITTED =
            "지금은 발급을 처리할 수 없습니다. 곧 다시 시도해 주세요.";

    /** 뒷단 카탈로그에 없는 상황이라 우리 코드를 쓴다. */
    private static final String CODE = "BACKEND_UNAVAILABLE";

    private final ApiError error;
    private final MeterRegistry meters;
    private final DoubleSupplier random;

    /** 없으면 상태를 모른다고 적는다. 시험이 레지스트리 없이도 돌 수 있어야 한다. */
    private final CircuitBreakerRegistry circuits;

    private final String circuitName;

    private BackendFallback(Clock clock, MeterRegistry meters, DoubleSupplier random,
            CircuitBreakerRegistry circuits, String circuitName) {
        this.error = ApiError.of(Objects.requireNonNull(clock, "clock 은 필수다"));
        this.meters = Objects.requireNonNull(meters, "meters 는 필수다");
        this.random = Objects.requireNonNull(random, "random 은 필수다");
        this.circuits = circuits;
        this.circuitName = circuitName;
    }

    /** 흔들림의 난수원은 스레드마다 따로 둔다 — 공유하면 그 자체가 경합점이다. */
    public static BackendFallback of(Clock clock, MeterRegistry meters) {
        return new BackendFallback(clock, meters,
                () -> ThreadLocalRandom.current().nextDouble(), null, null);
    }

    /** 서킷 상태를 지표에 싣는다. 없으면 "열렸다" 를 단정하게 되어 지표가 거짓말한다. */
    public static BackendFallback of(Clock clock, MeterRegistry meters,
            CircuitBreakerRegistry circuits, String circuitName) {
        return new BackendFallback(clock, meters,
                () -> ThreadLocalRandom.current().nextDouble(),
                Objects.requireNonNull(circuits, "circuits 는 필수다"),
                Objects.requireNonNull(circuitName, "circuitName 은 필수다"));
    }

    /** 난수원을 받는다. 고정하지 못하면 흔들림이 실제로 붙었는지 못 잰다 (TS-4). */
    public static BackendFallback of(Clock clock, MeterRegistry meters, DoubleSupplier random) {
        return new BackendFallback(clock, meters, random, null, null);
    }

    /**
     * 서킷이 넘긴 요청에 답한다.
     *
     * <p><b>같은 값을 주지 않는다.</b> 전원이 같은 순간에 다시 오면 서킷이
     * 닫히자마자 재포화되어 다시 열리고, 그 진동이 회복을 막는다.
     *
     * <p>봉투는 {@link ApiError} 가 만든다 — 여기서 따로 짜면 같은 게이트웨이가
     * 두 가지 오류 형식을 낸다.
     */
    public Mono<ServerResponse> respond(ServerRequest request) {
        // **"열렸다" 라고 단정하지 않는다.** 폴백은 서킷 오픈뿐 아니라 연결 실패나
        // 뒷단 오류로도 온다. 라벨을 오픈으로 고정하면 서킷이 닫힌 채 실패만 나는
        // 구간에서 지표가 거짓말하고, 그 지표로 회복을 판정한다 (8.4.3).
        meters.counter(METRIC, "state", state()).increment();
        // **무엇이 폴백을 불렀는지 남긴다.** 지표는 서킷 상태만 실어, 서킷이
        // 닫힌 채 실패만 나는 구간에서 원인이 뒷단인지 게이트웨이 자신인지를
        // 못 가른다. 예외 이름 하나면 그 둘이 갈린다 — 실측에서 20 건이 뒷단에
        // 가지도 않고 실패했는데 그것을 지표로만 역산하느라 반나절을 썼다.
        if (log.isDebugEnabled()) {
            request.attribute(ServerWebExchangeUtils.CIRCUITBREAKER_EXECUTION_EXCEPTION_ATTR)
                    .ifPresentOrElse(
                            ex -> log.debug("폴백 원인 — {}: {}", ex.getClass().getSimpleName(),
                                    ex instanceof Throwable th ? th.getMessage() : ex),
                            () -> log.debug("폴백 원인 — 예외 속성이 없다 (서킷이 열린 채 거절)"));
        }
        // **차례가 온 사람과 줄에 선 사람에게 같은 답을 하면 안 된다.** 앞은 손에
        // 든 토큰의 수명 안에 돌아와야 하고, 뒤는 그럴 필요가 없다. 판정 경로가
        // 이미 그렇게 가르고 있는데(RETRY_TOKEN 은 가장 가까운 밴드) 여기만 하나로
        // 답하면 같은 상황에 두 정책이 갈린다.
        boolean admitted = admitted(request);
        ApiError.Envelope envelope = error.render(request.exchange(),
                HttpStatus.SERVICE_UNAVAILABLE, CODE,
                admitted ? ADMITTED : QUEUED,
                retryAfterSec(admitted, pollScale(request)), false);
        return ServerResponse.status(envelope.status())
                .headers(headers -> headers.putAll(envelope.headers()))
                .bodyValue(envelope.body());
    }

    /** 서킷의 실제 상태. 레지스트리를 안 받으면 모른다고 적는다. */
    private String state() {
        return circuits == null ? "unknown"
                : circuits.circuitBreaker(circuitName).getState().name();
    }

    /**
     * 차례가 온 사람인가. <b>판정이 남긴 값으로 본다</b> — 서킷의 전달은 우리
     * 속성을 안 지운다.
     */
    private boolean admitted(ServerRequest request) {
        return request.exchange().getAttribute(AdmissionGatewayFilter.DECISION)
                == AdmissionDecision.PASS_TOKEN;
    }

    /**
     * 이 요청을 판정한 회차의 배수. <b>홀더를 다시 안 읽는다</b> — 서킷을 지나
     * 나중에 도는 자리라 그러면 다른 회차의 값이 나간다.
     */
    // 없으면 1.0 이다. 판정을 안 거친 요청이거나 첫 틱 전이고, 둘 다 배수를
    // 모르는 상태다. 모를 때 늘리면 근거 없이 전원을 멀리 보내는 것이다.
    private double pollScale(ServerRequest request) {
        Double scale = request.exchange().getAttribute(AdmissionGatewayFilter.POLL_SCALE);
        return scale == null ? PollIntervalPolicy.NO_SCALE : scale;
    }

    /**
     * 다시 올 시각.
     *
     * <p>차례가 온 사람은 <b>가장 가까운 밴드</b>로 부른다. 멀리 보내면 그 사이
     * 그의 몫이 남에게 가고, 토큰 수명이 다하면 줄 맨 뒤로 다시 선다.
     */
    // **차례가 온 쪽은 배수도 안 받는다.** 판정 경로가 같은 사람에게 그렇게
    // 답한다 — 배수만큼 멀리 보내면 토큰이 죽어 줄 맨 뒤에 다시 선다.
    private int retryAfterSec(boolean admitted, double pollScale) {
        return admitted
                ? (int) POLL.intervalSec(0, random, PollIntervalPolicy.NO_SCALE)
                : (int) POLL.intervalSec(EtaPolicy.UNKNOWN, random, pollScale);
    }
}
