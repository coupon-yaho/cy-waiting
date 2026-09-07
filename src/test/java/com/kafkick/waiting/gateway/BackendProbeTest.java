package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Optional;
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
        BackendProbe 프로브 = BackendProbe.of(() -> Optional.of(서킷),
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
        BackendProbe 프로브 = BackendProbe.of(() -> Optional.of(서킷), Mono::empty);

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
        BackendProbe 프로브 = BackendProbe.of(() -> Optional.of(서킷),
                () -> Mono.error(new IllegalStateException("뒷단이 아직 안 산다")));

        프로브.probe().block();
        프로브.probe().block();

        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(프로브.failed()).as("실패 수를 안 세면 그 지표가 영영 0 이다").isEqualTo(2);
        assertThat(프로브.passed()).isZero();
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
        BackendProbe 프로브 = BackendProbe.of(() -> Optional.of(서킷),
                () -> Mono.error(new IllegalStateException("뒷단이 아직 안 산다")));

        assertThat(프로브.probe().block()).isNull();
    }

    /**
     * <b>열린 구간에는 안 친다.</b> 허가 시도 자체가 "막은 건수" 에 세어져, 실사용자
     * 거절과 프로브가 안 갈린다. 반쯤 열린 상태로 넘기는 것은 서킷의 제 타이머다.
     */
    @Test
    @DisplayName("열려_있으면_안_친다")
    void 열려_있으면_안_친다() {
        // 대기가 짧으면 첫 허가 시도가 반쯤 열린 상태로 넘겨, 이 시험이 재려는
        // 것과 다른 것을 재게 된다.
        CircuitBreaker 서킷 = CircuitBreaker.of("backend", CircuitBreakerConfig.custom()
                .waitDurationInOpenState(Duration.ofHours(1))
                .automaticTransitionFromOpenToHalfOpenEnabled(false)
                .build());
        열어_둔다(서킷);
        AtomicInteger 호출 = new AtomicInteger();
        BackendProbe 프로브 = BackendProbe.of(() -> Optional.of(서킷),
                () -> Mono.fromRunnable(호출::incrementAndGet));

        프로브.probe().block();

        assertThat(호출).hasValue(0);
        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(서킷.getMetrics().getNumberOfNotPermittedCalls())
                .as("허가 시도조차 안 한다 — 막은 건수가 프로브로 부풀면 안 된다")
                .isZero();
    }

    /**
     * <b>운영자가 끈 것도 안 친다.</b> DISABLED 는 허가가 항상 나서 뒷단에 무한정
     * 꽂히고, 그 결과가 서킷에는 안 남는다 — 끄는 유일한 수단이 재기동이 된다.
     */
    @Test
    @DisplayName("운영자가_끄면_안_친다")
    void 운영자가_끄면_안_친다() {
        CircuitBreaker 서킷 = 서킷();
        서킷.transitionToDisabledState();
        AtomicInteger 호출 = new AtomicInteger();
        BackendProbe 프로브 = BackendProbe.of(() -> Optional.of(서킷),
                () -> Mono.fromRunnable(호출::incrementAndGet));

        프로브.probe().block();

        assertThat(호출).hasValue(0);
    }

    /** 이름이 없으면 만들지 않는다. 새로 만든 유령은 영원히 닫혀 있다. */
    @Test
    @DisplayName("서킷이_없으면_안_친다")
    void 서킷이_없으면_안_친다() {
        AtomicInteger 호출 = new AtomicInteger();
        BackendProbe 프로브 = BackendProbe.of(Optional::empty,
                () -> Mono.fromRunnable(호출::incrementAndGet));

        프로브.probe().block();

        assertThat(호출).hasValue(0);
        assertThat(프로브.skipped()).isEqualTo(1);
    }

    /** 셋 다 0 이면 루프가 죽은 것이다. 그 구분이 없으면 배선이 빠진 채 조용히 돈다. */
    @Test
    @DisplayName("회차_결과를_센다")
    void 회차_결과를_센다() {
        CircuitBreaker 서킷 = 서킷();
        열어_둔다(서킷);
        서킷.transitionToHalfOpenState();
        BackendProbe 프로브 = BackendProbe.of(() -> Optional.of(서킷), Mono::empty);

        프로브.probe().block();

        assertThat(프로브.passed()).isEqualTo(1);
        assertThat(프로브.failed()).isZero();
        assertThat(프로브.skipped()).isZero();
    }

    /**
     * <b>허가 자리를 안 넘겨 쓴다.</b> 이 게이트가 없으면 프로브가 반쯤 열린 자리를
     * 회차마다 무제한으로 먹고, 이미 큐에서 빠져 입장 토큰을 든 사람이 폴백으로
     * 떨어져 그 토큰이 죽는다.
     */
    @Test
    @DisplayName("허가가_없으면_안_친다")
    void 허가가_없으면_안_친다() {
        CircuitBreaker 서킷 = CircuitBreaker.of("backend", CircuitBreakerConfig.custom()
                .permittedNumberOfCallsInHalfOpenState(1)
                .waitDurationInOpenState(Duration.ofHours(1))
                .automaticTransitionFromOpenToHalfOpenEnabled(false)
                .build());
        열어_둔다(서킷);
        서킷.transitionToHalfOpenState();
        AtomicInteger 호출 = new AtomicInteger();
        BackendProbe 프로브 = BackendProbe.of(() -> Optional.of(서킷),
                () -> Mono.<Void>never().doOnSubscribe(s -> 호출.incrementAndGet()));

        // 하나뿐인 자리를 첫 회차가 쥔 채로 안 놓는다.
        프로브.probe().subscribe();
        프로브.probe().block();

        assertThat(호출).as("자리가 없으면 뒷단을 안 친다").hasValue(1);
        assertThat(프로브.skipped()).isEqualTo(1);
    }

    /** 취소도 자리를 돌려준다. 안 돌려주면 반쯤 열린 자리가 하나씩 영구히 준다. */
    @Test
    @DisplayName("취소하면_자리를_돌려준다")
    void 취소하면_자리를_돌려준다() {
        CircuitBreaker 서킷 = CircuitBreaker.of("backend", CircuitBreakerConfig.custom()
                .permittedNumberOfCallsInHalfOpenState(1)
                .waitDurationInOpenState(Duration.ofHours(1))
                .automaticTransitionFromOpenToHalfOpenEnabled(false)
                .build());
        열어_둔다(서킷);
        서킷.transitionToHalfOpenState();
        BackendProbe 프로브 = BackendProbe.of(() -> Optional.of(서킷), Mono::never);

        프로브.probe().subscribe().dispose();

        assertThat(서킷.tryAcquirePermission()).as("자리가 돌아왔다").isTrue();
    }

    /** 이름과 값이 안 걸리면 회복 구간에 볼 것이 없다. */
    @Test
    @DisplayName("회차_지표를_이름으로_낸다")
    void 회차_지표를_이름으로_낸다() {
        CircuitBreaker 서킷 = 서킷();
        MeterRegistry 지표 = new SimpleMeterRegistry();
        BackendProbe 프로브 =
                BackendProbe.of(() -> Optional.of(서킷), Mono::empty).bind(지표);

        프로브.probe().block();

        assertThat(지표.get(BackendProbe.SKIPPED).functionCounter().count()).isEqualTo(1);
        assertThat(지표.get(BackendProbe.PASSED).functionCounter().count()).isZero();
        assertThat(지표.get(BackendProbe.FAILED).functionCounter().count()).isZero();
    }
}
