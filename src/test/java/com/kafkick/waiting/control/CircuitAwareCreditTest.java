package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.admission.CircuitState;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 서킷이 열리면 <b>배분도 멈춘다</b> (F3 · CY-787).
 *
 * <p>판정만 고치면 절반이다. 가용량에 하한이 있어 임계가 계속 올라가고, 임계를
 * 넘은 사람은 큐에서 빠져 토큰을 받은 뒤 서킷에 막혀 <b>503 을 받는다.</b>
 */
// 그 사람의 자리는 이미 없다. 장애가 토큰 수명을 넘기면 줄 맨 뒤로 간다 —
// RC5 와 불변식 3 이 함께 깨진다.
class CircuitAwareCreditTest {

    private final AtomicReference<CircuitState> 서킷 =
            new AtomicReference<>(CircuitState.CLOSED);

    private long 크레딧(long 가용량) {
        return CircuitAwareCredit.of(() -> 가용량, 서킷::get).getAsLong();
    }

    @Test
    @DisplayName("서킷이_닫혀_있으면_그대로_쓴다")
    void 서킷이_닫혀_있으면_그대로_쓴다() {
        assertThat(크레딧(500)).isEqualTo(500);
    }

    /** 열린 동안 임계를 올리면 줄의 앞이 503 으로 갈려 나간다. */
    @Test
    @DisplayName("서킷이_열리면_배분을_멈춘다")
    void 서킷이_열리면_배분을_멈춘다() {
        서킷.set(CircuitState.OPEN);

        assertThat(크레딧(500)).isZero();
    }

    /**
     * 반쯤 열린 동안도 멈춘다. 뒷단이 받는 것은 시험 요청 몇 건뿐인데 그만큼
     * 임계를 올리면, 나온 사람 대부분이 503 을 받고 자리만 잃는다.
     */
    @Test
    @DisplayName("반쯤_열려도_배분을_멈춘다")
    void 반쯤_열려도_배분을_멈춘다() {
        서킷.set(CircuitState.HALF_OPEN);

        assertThat(크레딧(500)).isZero();
    }

    /** 닫히면 곧바로 되돌아온다. 안 되돌아오면 회복이 영영 안 된다. */
    @Test
    @DisplayName("닫히면_곧바로_되돌아온다")
    void 닫히면_곧바로_되돌아온다() {
        서킷.set(CircuitState.OPEN);
        assertThat(크레딧(500)).isZero();

        서킷.set(CircuitState.CLOSED);

        assertThat(크레딧(500)).isEqualTo(500);
    }

    /** <b>판마다 읽는다.</b> 붙잡아 두면 서킷이 닫힌 뒤에도 0 을 계속 쓴다. */
    @Test
    @DisplayName("판마다_다시_읽는다")
    void 판마다_다시_읽는다() {
        LongSupplier credit =
                CircuitAwareCredit.of(() -> 500, 서킷::get);

        서킷.set(CircuitState.OPEN);
        assertThat(credit.getAsLong()).isZero();
        서킷.set(CircuitState.CLOSED);

        assertThat(credit.getAsLong()).isEqualTo(500);
    }
}
