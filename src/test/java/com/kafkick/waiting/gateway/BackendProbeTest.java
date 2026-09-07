package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * 회복 판정에 쓸 호출을 <b>발급 줄 밖에서</b> 보낸다 (CY-889).
 *
 * <p>지금은 프로브가 줄을 통해서만 나가 폴링 간격 하나가 RC3 와 RC4 를 반대로 민다 —
 * 촘촘히 집으면 회복이 빠르고 봉우리가 크고, 재시도 간격을 지키면 반대다. 줄 밖에서
 * 표본을 채우면 줄은 제 속도로 빠지고 서킷은 제 속도로 닫힌다.
 */
@Tag("unit")
class BackendProbeTest {

    /** 표본 둘이면 닫히는 서킷. 회차를 손으로 돌려 상태 전이를 본다. */
    private CircuitBreaker 서킷() {
        return CircuitBreaker.of("backend", CircuitBreakerConfig.custom()
                .slidingWindowSize(2)
                .minimumNumberOfCalls(2)
                .failureRateThreshold(50)
                .permittedNumberOfCallsInHalfOpenState(2)
                .waitDurationInOpenState(Duration.ofMillis(1))
                .automaticTransitionFromOpenToHalfOpenEnabled(false)
                .build());
    }

    private void 열어_둔다(CircuitBreaker 서킷) {
        서킷.transitionToOpenState();
    }

    /**
     * <b>평시에는 안 친다.</b> 닫힌 서킷에 프로브를 계속 보내면 회복과 무관한 부하가
     * 뒷단에 상시로 얹힌다.
     */
    @Test
    @DisplayName("닫혀_있으면_안_친다")
    void 닫혀_있으면_안_친다() {
        CircuitBreaker 서킷 = 서킷();
        AtomicInteger 호출 = new AtomicInteger();
        BackendProbe 프로브 = BackendProbe.of(() -> 서킷,
                () -> Mono.fromRunnable(호출::incrementAndGet));

        프로브.probe().block();

        assertThat(호출).hasValue(0);
    }

    /**
     * <b>반쯤 열린 구간의 표본을 프로브가 채운다.</b> 이것이 이 장치의 전부다 —
     * 줄에 선 사람이 차례를 받아야 표본이 생기던 것을 끊는다.
     */
    @Test
    @DisplayName("반쯤_열리면_표본을_채운다")
    void 반쯤_열리면_표본을_채운다() {
        CircuitBreaker 서킷 = 서킷();
        열어_둔다(서킷);
        서킷.transitionToHalfOpenState();
        BackendProbe 프로브 = BackendProbe.of(() -> 서킷, Mono::empty);

        프로브.probe().block();
        프로브.probe().block();

        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    /** 프로브가 실패하면 다시 연다. 성공만 세면 뒷단이 멀쩡하다고 거짓 보고를 한다. */
    @Test
    @DisplayName("프로브가_실패하면_다시_연다")
    void 프로브가_실패하면_다시_연다() {
        CircuitBreaker 서킷 = 서킷();
        열어_둔다(서킷);
        서킷.transitionToHalfOpenState();
        BackendProbe 프로브 = BackendProbe.of(() -> 서킷,
                () -> Mono.error(new IllegalStateException("뒷단이 아직 안 산다")));

        프로브.probe().block();
        프로브.probe().block();

        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.OPEN);
    }

    /**
     * <b>프로브 실패가 밖으로 안 샌다.</b> 루프를 타고 올라가면 그 구독이 끊기고,
     * 끊기면 서킷이 열린 채로 아무도 다시 안 친다.
     */
    @Test
    @DisplayName("실패해도_루프가_안_끊긴다")
    void 실패해도_루프가_안_끊긴다() {
        CircuitBreaker 서킷 = 서킷();
        열어_둔다(서킷);
        서킷.transitionToHalfOpenState();
        BackendProbe 프로브 = BackendProbe.of(() -> 서킷,
                () -> Mono.error(new IllegalStateException("뒷단이 아직 안 산다")));

        assertThat(프로브.probe().block()).isNull();
    }

    /**
     * <b>열린 구간에는 허가가 안 난다.</b> 그때도 치면 대기 시간이 무의미해지고,
     * 죽은 뒷단에 회차마다 호출이 쌓인다.
     */
    @Test
    @DisplayName("열려_있으면_허가를_기다린다")
    void 열려_있으면_허가를_기다린다() {
        CircuitBreaker 서킷 = CircuitBreaker.of("backend", CircuitBreakerConfig.custom()
                .waitDurationInOpenState(Duration.ofHours(1))
                .automaticTransitionFromOpenToHalfOpenEnabled(false)
                .build());
        열어_둔다(서킷);
        AtomicInteger 호출 = new AtomicInteger();
        BackendProbe 프로브 = BackendProbe.of(() -> 서킷,
                () -> Mono.fromRunnable(호출::incrementAndGet));

        프로브.probe().block();

        assertThat(호출).hasValue(0);
        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.OPEN);
    }
}
