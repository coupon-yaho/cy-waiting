package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.queue.EtaPolicy;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 서킷이 열렸을 때 사용자가 받는 것.
 *
 * <p><b>이 자리가 비면 404 가 나간다.</b> 프레임워크는 fallback 주소로 넘길 뿐이고,
 * 그 주소를 아무도 안 받으면 "없는 경로" 가 된다. 사용자에게 404 는 매진으로
 * 읽히므로 다시 오지 않는다 — 뒷단은 잠깐 흔들렸을 뿐인데.
 */
public final class BackendFallback {

    private static final String METRIC = "waiting.backend.fallback";

    /** 재시도를 흩는 폭. 판정 경로와 같은 값이라야 두 안내가 안 갈린다. */
    private static final PollIntervalPolicy POLL = PollIntervalPolicy.of(0.2);

    /**
     * <b>줄에 선 사람에게 자리가 그대로라고 말한다.</b> 안 그러면 다시 줄을
     * 서려 하고, 그건 자기 자리를 버리는 일이다.
     */
    private static final String MESSAGE =
            "지금은 발급을 처리할 수 없습니다. 대기 순번은 그대로 유지되니 "
                    + "잠시 후 다시 시도해 주세요.";

    /** 뒷단 카탈로그에 없는 상황이라 우리 코드를 쓴다. */
    private static final String CODE = "BACKEND_UNAVAILABLE";

    private final ApiError error;
    private final MeterRegistry meters;
    private final DoubleSupplier random;

    private BackendFallback(Clock clock, MeterRegistry meters, DoubleSupplier random) {
        this.error = ApiError.of(Objects.requireNonNull(clock, "clock 은 필수다"));
        this.meters = Objects.requireNonNull(meters, "meters 는 필수다");
        this.random = Objects.requireNonNull(random, "random 은 필수다");
    }

    /** 흔들림의 난수원은 스레드마다 따로 둔다 — 공유하면 그 자체가 경합점이다. */
    public static BackendFallback of(Clock clock, MeterRegistry meters) {
        return new BackendFallback(clock, meters,
                () -> ThreadLocalRandom.current().nextDouble());
    }

    /** 난수원을 받는다. 고정하지 못하면 흔들림이 실제로 붙었는지 못 잰다 (TS-4). */
    public static BackendFallback of(Clock clock, MeterRegistry meters, DoubleSupplier random) {
        return new BackendFallback(clock, meters, random);
    }

    /**
     * <b>같은 값을 주지 않는다.</b> 전원이 같은 순간에 다시 오면 서킷이 닫히자마자
     * 재포화되어 다시 열리고, 그 진동이 회복을 막는다.
     */
    public Mono<Void> handle(ServerWebExchange exchange) {
        meters.counter(METRIC, "outcome", "open").increment();
        return error.write(exchange, HttpStatus.SERVICE_UNAVAILABLE, CODE, MESSAGE,
                (int) POLL.intervalSec(EtaPolicy.UNKNOWN, random), false);
    }
}
