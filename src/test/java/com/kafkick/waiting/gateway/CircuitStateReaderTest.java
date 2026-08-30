package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.admission.CircuitState;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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

    /**
     * <b>강제 개방은 라우팅을 안 바꾼다.</b>
     *
     * <p>그 상태에는 해제 조건이 없다 — 사람이 풀기 전까지 영원하다. 줄로
     * 돌리면 전 쿠폰이 무기한 큐에 갇히고, 한산한 쿠폰에도 없던 줄이 생겨
     * 스스로 유지된다. 킬스위치는 기존 폴백이 받는 것이 맞다.
     */
    @Test
    @DisplayName("강제_개방은_라우팅을_안_바꾼다")
    void 강제_개방은_라우팅을_안_바꾼다() {
        CircuitBreakerRegistry registry = registry();
        registry.circuitBreaker(NAME).transitionToForcedOpenState();

        assertThat(reader(registry).now()).isEqualTo(CircuitState.CLOSED);
    }

    /**
     * <b>없는 이름을 만들지 않는다.</b> 만들면 그 유령은 영원히 닫혀 있어,
     * 이름이 어긋나도 F3 이 켜진 것처럼 보이면서 실제로는 죽는다.
     */
    @Test
    @DisplayName("이름이_없으면_유령을_안_만든다")
    void 이름이_없으면_유령을_안_만든다() {
        CircuitBreakerRegistry registry = registry();

        assertThat(reader(registry).now()).isEqualTo(CircuitState.CLOSED);
        assertThat(registry.getAllCircuitBreakers()).as("읽었다고 생기지 않는다").isEmpty();
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
    /**
     * <b>안 보는 것과 닫혀 있는 것을 가른다</b> (CY-788).
     *
     * <p>둘이 같은 값을 내므로, 배선이 빠지면 판정도 배분도 조용히 평소대로
     * 돈다 — F3 이 통째로 꺼진 것을 다음 장애 때야 안다.
     */
    @Test
    @DisplayName("서킷을_보고_있는지_지표로_낸다")
    void 서킷을_보고_있는지_지표로_낸다() {
        SimpleMeterRegistry 계기 = new SimpleMeterRegistry();
        CircuitBreakerRegistry registry = registry();
        registry.circuitBreaker(NAME);
        reader(registry).bind(계기);

        assertThat(계기.get(CircuitStateReader.WIRED).gauge().value()).isEqualTo(1);
    }

    /** 레지스트리가 없으면 0 이다. 그 상태가 곧 F3 이 꺼진 상태다. */
    @Test
    @DisplayName("배선이_없으면_지표가_0이다")
    void 배선이_없으면_지표가_0이다() {
        SimpleMeterRegistry 계기 = new SimpleMeterRegistry();
        reader(null).bind(계기);

        assertThat(계기.get(CircuitStateReader.WIRED).gauge().value()).isZero();
    }

    /** 이름이 어긋나도 0 이다. 유령을 안 만드니 그 이름은 영영 안 생긴다. */
    @Test
    @DisplayName("이름이_어긋나면_지표가_0이다")
    void 이름이_어긋나면_지표가_0이다() {
        SimpleMeterRegistry 계기 = new SimpleMeterRegistry();
        CircuitBreakerRegistry registry = registry();
        registry.circuitBreaker("딴이름");
        reader(registry).bind(계기);

        assertThat(계기.get(CircuitStateReader.WIRED).gauge().value()).isZero();
    }
}