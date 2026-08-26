package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

/**
 * 제어 평면 배선.
 *
 * <p>조각이 다 있어도 <b>안 엮이면 아무것도 안 돈다.</b> 리더 판정과 배분 루프가
 * 각자 초록인데 사이가 비어 있으면 배분이 영영 안 돈다 — 그 상태로 뜨는 것이
 * 가장 나쁘다.
 */
@Tag("context")
@SpringBootTest(properties = "waiting.scheduler.enabled=true")
class ControlPlaneConfigTest {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ControlPlaneProperties properties;

    @Test
    @DisplayName("제어_평면_빈이_다_뜬다")
    void 제어_평면_빈이_다_뜬다() {
        // **뜬다는 것만으로 부족하다.** 배선이 서로 다른 설정을 보고 있으면
        // 각자 멀쩡한데 함께 안 맞는다.
        assertThat(context.getBean(Leadership.class).ownerId()).isNotBlank();
        assertThat(context.getBeansOfType(AllocationScheduler.class)).hasSize(1);
        assertThat(context.getBeansOfType(AllocationRound.class)).hasSize(1);
        assertThat(context.getBeansOfType(DemandCollector.class)).hasSize(1);
        // 루프를 켜는 것이 없으면 조각이 다 있어도 아무것도 안 돈다.
        assertThat(context.getBeansOfType(ControlPlaneLifecycle.class)).hasSize(1);
        assertThat(context.getBeansOfType(LeadershipLoop.class)).hasSize(1);
    }

    @Test
    @DisplayName("설정값이_계획값이다")
    void 설정값이_계획값이다() {
        assertThat(properties.scheduler().tick()).isEqualTo(Duration.ofSeconds(1));
        assertThat(properties.leader().lease()).isEqualTo(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("소유자_ID_는_기동마다_다르다")
    void 소유자_ID_는_기동마다_다르다() {
        // 고정하면 재기동한 자신을 이전 소유자로 오인해, 죽기 전에 잡아 둔 락을
        // 새 프로세스가 자기 것으로 알고 연장한다.
        //
        // **한 인스턴스만 보면 이걸 못 잰다** — 상수로 바꿔도 비어 있지 않다.
        assertThat(context.getBean(Leadership.class).ownerId())
                .isNotEqualTo(Leadership.newOwnerId());
        assertThat(Leadership.newOwnerId()).isNotEqualTo(Leadership.newOwnerId());
    }
}
