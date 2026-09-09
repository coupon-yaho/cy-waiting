package com.kafkick.waiting.gateway;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * 회복 판정에 쓸 호출을 <b>발급 줄 밖에서</b> 보낸다.
 *
 * <p>표본이 줄에서만 나오면 폴링 간격 하나가 회복 속도와 회복 봉우리를 반대로
 * 민다 (AIJ-0249). <b>발급이 아니므로 추월이 아니다</b> — 줄 선 사람의 차례를 안
 * 쓰고 헬스 경로를 친다.
 */
public final class BackendProbe {

    public static final String PASSED = "waiting.probe.passed";

    public static final String FAILED = "waiting.probe.failed";

    public static final String SKIPPED = "waiting.probe.skipped";

    public static final String BUSY = "waiting.probe.busy";

    /**
     * 뒷단이 살아 있고 지금 바쁘다. <b>회복의 증거가 아니다</b> — 요청 경로는
     * 같은 답을 서킷에 안 물므로, 프로브만 실패로 세면 정의가 둘이 된다.
     */
    public static final class Busy extends RuntimeException {

        /** 이 패키지 안에서만 만든다 — 밖에서 만들면 뜻이 흐려진다 (JS-12). */
        Busy(String message) {
            super(message);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(BackendProbe.class);

    private final Supplier<Optional<CircuitBreaker>> breaker;

    /** 뒷단 헬스 경로 한 번. 실패는 오류 신호로 온다. */
    private final Supplier<Mono<Void>> call;

    private final AtomicLong passed = new AtomicLong();

    private final AtomicLong failed = new AtomicLong();

    private final AtomicLong busy = new AtomicLong();

    private final AtomicLong skipped = new AtomicLong();

    /** 실패 구간의 첫 건만 남기는 자물쇠. 성공하면 푼다. */
    private final AtomicReference<Boolean> failingSince = new AtomicReference<>();

    private BackendProbe(Supplier<Optional<CircuitBreaker>> breaker, Supplier<Mono<Void>> call) {
        this.breaker = Objects.requireNonNull(breaker, "breaker 는 필수다");
        this.call = Objects.requireNonNull(call, "call 은 필수다");
    }

    public static BackendProbe of(Supplier<Optional<CircuitBreaker>> breaker,
            Supplier<Mono<Void>> call) {
        return new BackendProbe(breaker, call);
    }

    /**
     * 한 회차. <b>반쯤 열린 구간만 친다</b> — 닫혔을 때 치면 회복과 무관한 부하가
     * 상시로 얹히고, 열렸을 때 치면 허가 시도가 "막은 건수" 를 오염시킨다. 열린
     * 상태에서 반쯤 열린 상태로 넘기는 것은 서킷이 제 타이머로 한다.
     */
    public Mono<Void> probe() {
        Optional<CircuitBreaker> found = breaker.get();
        // **운영자가 끈 상태도 안 친다.** DISABLED·METRICS_ONLY 는 허가가 항상 나서
        // 뒷단에 무한정 꽂히고, 그 결과가 서킷에는 안 남는다.
        if (found.isEmpty() || found.get().getState() != CircuitBreaker.State.HALF_OPEN) {
            skipped.incrementAndGet();
            return Mono.empty();
        }
        CircuitBreaker circuit = found.get();
        if (!circuit.tryAcquirePermission()) {
            skipped.incrementAndGet();
            return Mono.empty();
        }
        long startedAt = circuit.getCurrentTimestamp();
        // **defer 로 감싼다.** 여기서 바로 부르면 공급자가 동기로 던진 것이 아래
        // 연산자를 안 지나, 자리는 먹고 표본은 안 남긴 채 회차가 끝난다.
        return Mono.defer(call)
                .doOnSuccess(ignored -> record(circuit, startedAt, null))
                .doOnError(error -> record(circuit, startedAt, error))
                // **취소도 허가를 돌려준다.** 안 돌려주면 반쯤 열린 자리가 하나
                // 줄어든 채로 남고, 그만큼 회복 표본이 영영 안 찬다.
                .doOnCancel(circuit::releasePermission)
                // **여기서 신호를 끊는다.** 루프도 삼키지만 그쪽은 동기로 던진 것을
                // 받는 자리라, 이 오류 신호는 여기까지만 온다.
                .onErrorResume(error -> Mono.empty());
    }

    /**
     * 회차 결과를 <b>카운터로</b> 낸다. 단조 증가라 게이지로 내면 재기동 리셋을
     * 못 다뤄 회복 구간의 증가율이 틀어진다.
     *
     * <p>셋 다 0 이면 루프가 죽은 것이다. 그 구분이 없으면 배선이 빠진 채 조용히 돈다.
     */
    public BackendProbe bind(MeterRegistry meters) {
        counter(meters, PASSED, "합성 프로브가 표본을 채운 회차 수", passed);
        counter(meters, FAILED, "합성 프로브가 실패로 표본을 채운 회차 수", failed);
        counter(meters, SKIPPED, "반쯤 열리지 않아 안 친 회차 수. 루프 생존의 신호다", skipped);
        counter(meters, BUSY, "뒷단이 바빠 표본으로 안 센 회차 수", busy);
        return this;
    }

    private void counter(MeterRegistry meters, String name, String why, AtomicLong value) {
        FunctionCounter.builder(name, value, AtomicLong::doubleValue)
                .description(why)
                .register(meters);
    }

    /** 표본을 채운 회차 수. */
    public long passed() {
        return passed.get();
    }

    /** 표본을 실패로 채운 회차 수. */
    public long failed() {
        return failed.get();
    }

    /** 반쯤 열리지 않아 안 친 회차 수. */
    public long skipped() {
        return skipped.get();
    }

    /** 뒷단이 바빠 표본으로 안 센 회차 수. */
    public long busy() {
        return busy.get();
    }

    private void record(CircuitBreaker circuit, long startedAt, Throwable error) {
        long elapsed = circuit.getCurrentTimestamp() - startedAt;
        TimeUnit unit = circuit.getTimestampUnit();
        if (error == null) {
            passed.incrementAndGet();
            circuit.onSuccess(elapsed, unit);
            // **멎은 것도 남긴다.** 진입만 찍으면 언제 멎었는지가 안 나와,
            // 회복 구간에 그 줄을 본 운영자가 아직도 실패 중인지 못 가른다.
            if (failingSince.compareAndSet(Boolean.TRUE, null)) {
                log.info("합성 프로브가 다시 표본을 채운다");
            }
            return;
        }
        // **바쁘다는 답은 판정을 안 바꾼다.** 자리를 돌려주고 세기만 한다 —
        // 실패로 세면 프로브가 혼자 서킷을 다시 열고, 성공으로 세면 못 받는
        // 뒷단을 향해 서킷이 닫힌다. 어느 쪽도 회복의 증거가 아니다.
        if (error instanceof Busy) {
            busy.incrementAndGet();
            circuit.releasePermission();
            return;
        }
        failed.incrementAndGet();
        circuit.onError(elapsed, unit, error);
        // **원인을 한 번은 남긴다.** 수만 세면 경로 오설정과 뒷단 사망이 밖에서
        // 같은 값이다. 구간의 첫 건만 남겨 회차마다 쌓이는 것을 막는다.
        if (failingSince.compareAndSet(null, Boolean.TRUE)) {
            log.warn("합성 프로브가 실패로 표본을 채운다 — {}. 경로 설정과 뒷단을 "
                    + "함께 본다", error.toString());
        }
    }
}
