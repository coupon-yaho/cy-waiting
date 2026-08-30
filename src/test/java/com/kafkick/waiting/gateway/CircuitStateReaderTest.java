package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.admission.CircuitState;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 서킷 상태를 판정으로 옮기는 자리 (F3).
 *
 * <p>라이브러리 상태는 여섯인데 판정이 아는 것은 셋이다. 그 접힘이 여기 있고,
 * 어느 쪽으로 접느냐가 <b>없는 장애를 만드는지</b>를 가른다.
 */
class CircuitStateReaderTest {

    private static final String NAME = "backend";

    private CircuitStateReader reader(CircuitBreakerRegistry registry) {
        return CircuitStateReader.of(registry, NAME);
    }

    private CircuitBreakerRegistry registry() {
        return CircuitBreakerRegistry.ofDefaults();
    }

    @Test
    @DisplayName("닫힌_서킷은_정상이다")
    void 닫힌_서킷은_정상이다() {
        assertThat(reader(registry()).now()).isEqualTo(CircuitState.CLOSED);
    }

    @Test
    @DisplayName("열린_서킷은_열린_것으로_읽는다")
    void 열린_서킷은_열린_것으로_읽는다() {
        CircuitBreakerRegistry registry = registry();
        registry.circuitBreaker(NAME).transitionToOpenState();

        assertThat(reader(registry).now()).isEqualTo(CircuitState.OPEN);
    }

    /** 운영자가 강제로 연 것도 열린 것이다. 뒷단에 보내면 안 되는 것은 같다. */
    @Test
    @DisplayName("강제로_연_것도_열린_것이다")
    void 강제로_연_것도_열린_것이다() {
        CircuitBreakerRegistry registry = registry();
        registry.circuitBreaker(NAME).transitionToForcedOpenState();

        assertThat(reader(registry).now()).isEqualTo(CircuitState.OPEN);
    }

    @Test
    @DisplayName("반쯤_열린_것은_그대로_읽는다")
    void 반쯤_열린_것은_그대로_읽는다() {
        CircuitBreakerRegistry registry = registry();
        CircuitBreaker breaker = registry.circuitBreaker(NAME);
        breaker.transitionToOpenState();
        breaker.transitionToHalfOpenState();

        assertThat(reader(registry).now()).isEqualTo(CircuitState.HALF_OPEN);
    }

    /**
     * <b>운영자가 끈 것은 뒷단이 죽은 것이 아니다.</b> 여기서 조이면 서킷을
     * 끄는 일이 곧 전 요청을 줄로 보내는 일이 된다 — 없는 장애를 만든다.
     */
    @Test
    @DisplayName("꺼_둔_서킷은_정상으로_본다")
    void 꺼_둔_서킷은_정상으로_본다() {
        CircuitBreakerRegistry registry = registry();
        registry.circuitBreaker(NAME).transitionToDisabledState();

        assertThat(reader(registry).now()).isEqualTo(CircuitState.CLOSED);
    }

    /** 레지스트리를 안 붙인 배치도 있다. 모른다고 줄로 보내면 전 요청이 막힌다. */
    @Test
    @DisplayName("서킷이_없으면_정상으로_본다")
    void 서킷이_없으면_정상으로_본다() {
        assertThat(reader(null).now()).isEqualTo(CircuitState.CLOSED);
    }

    /** <b>레디스를 안 친다.</b> 요청 경로라 왕복이 하나라도 생기면 불변식 1 이 깨진다. */
    @Test
    @DisplayName("상태를_읽어도_레디스를_안_친다")
    void 상태를_읽어도_레디스를_안_친다() {
        CircuitBreakerRegistry registry = registry();
        registry.circuitBreaker(NAME).transitionToOpenState();
        CircuitStateReader reader = reader(registry);

        // 레디스 연결이 아예 없는 자리에서 만 번 읽는다. 하나라도 치면 터진다.
        for (int i = 0; i < 10_000; i++) {
            assertThat(reader.now()).isEqualTo(CircuitState.OPEN);
        }
    }
}
