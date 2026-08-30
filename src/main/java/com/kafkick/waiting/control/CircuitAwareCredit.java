package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.admission.CircuitState;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * 서킷이 열린 동안 배분을 멈춘다 (F3 · CY-787).
 *
 * <p>판정만 고치면 절반이다. 가용량에 하한이 있어 뒷단이 죽어도 전역 크레딧이
 * 0 이 안 되고, 그동안 배분은 평소 속도로 입장 임계를 올린다.
 */
// 임계를 넘은 사람은 조회에서 큐를 빠져나와 입장 토큰을 받는다. 그 사람은
// 사다리에서 서킷보다 앞이라 통과하고, 서킷이 가로채 503 을 준다. 자리는
// 이미 없다 — 장애가 토큰 수명을 넘기면 줄 맨 뒤로 간다.
// **반쯤 열린 동안도 멈춘다.** 그때 뒷단이 받는 것은 차례를 받은 사람 몇
// 건뿐인데, 그만큼 임계를 올리면 나온 사람 대부분이 503 을 받고 자리만 잃는다.
public final class CircuitAwareCredit implements LongSupplier {

    private final LongSupplier capacity;
    private final Supplier<CircuitState> circuit;

    private CircuitAwareCredit(LongSupplier capacity, Supplier<CircuitState> circuit) {
        this.capacity = Objects.requireNonNull(capacity, "capacity 는 필수다");
        this.circuit = Objects.requireNonNull(circuit, "circuit 은 필수다");
    }

    public static LongSupplier of(LongSupplier capacity, Supplier<CircuitState> circuit) {
        return new CircuitAwareCredit(capacity, circuit);
    }

    /** <b>판마다 다시 읽는다.</b> 붙잡아 두면 서킷이 닫힌 뒤에도 0 을 계속 쓴다. */
    @Override
    public long getAsLong() {
        return circuit.get() == CircuitState.CLOSED ? capacity.getAsLong() : 0;
    }
}
