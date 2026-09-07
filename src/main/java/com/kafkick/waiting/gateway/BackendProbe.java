package com.kafkick.waiting.gateway;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

/**
 * 회복 판정에 쓸 호출을 <b>발급 줄 밖에서</b> 보낸다.
 *
 * <p>표본이 줄에서만 나오면 폴링 간격 하나가 회복 속도와 회복 봉우리를 반대로
 * 민다 (AIJ-0249). <b>발급이 아니므로 추월이 아니다</b> — 줄 선 사람의 차례를 안
 * 쓰고 헬스 경로를 친다.
 */
public final class BackendProbe {

    private final Supplier<Optional<CircuitBreaker>> breaker;

    /** 뒷단 헬스 경로 한 번. 실패는 오류 신호로 온다. */
    private final Supplier<Mono<Void>> call;

    private final AtomicLong passed = new AtomicLong();

    private final AtomicLong failed = new AtomicLong();

    private final AtomicLong skipped = new AtomicLong();

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
        return call.get()
                .doOnSuccess(ignored -> record(circuit, startedAt, null))
                .doOnError(error -> record(circuit, startedAt, error))
                // **취소도 허가를 돌려준다.** 안 돌려주면 반쯤 열린 자리가 하나
                // 줄어든 채로 남고, 그만큼 회복 표본이 영영 안 찬다.
                .doOnCancel(circuit::releasePermission)
                // **실패를 밖으로 안 흘린다.** 루프를 타고 올라가면 그 구독이 끊기고,
                // 끊기면 서킷이 열린 채로 아무도 다시 안 친다.
                .onErrorResume(error -> Mono.empty());
    }

    /** 표본을 채운 회차 수. */
    public double passed() {
        return passed.get();
    }

    /** 표본을 실패로 채운 회차 수. */
    public double failed() {
        return failed.get();
    }

    /**
     * 안 친 회차 수. <b>루프가 죽은 것과 칠 일이 없던 것을 가른다</b> — 셋 다 0 이면
     * 도는 것이 없다는 뜻이고, 그 구분이 없으면 배선이 빠진 채 조용히 돈다.
     */
    public double skipped() {
        return skipped.get();
    }

    private void record(CircuitBreaker circuit, long startedAt, Throwable error) {
        long elapsed = circuit.getCurrentTimestamp() - startedAt;
        TimeUnit unit = circuit.getTimestampUnit();
        if (error == null) {
            passed.incrementAndGet();
            circuit.onSuccess(elapsed, unit);
        } else {
            failed.incrementAndGet();
            circuit.onError(elapsed, unit, error);
        }
    }
}
