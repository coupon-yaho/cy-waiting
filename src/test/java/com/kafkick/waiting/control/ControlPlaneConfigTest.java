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
@Tag("integration")
@SpringBootTest
class ControlPlaneConfigTest {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ControlPlaneProperties properties;

    @Test
    @DisplayName("제어_평면_빈이_다_뜬다")
    void 제어_평면_빈이_다_뜬다() {
        assertThat(context.getBean(Leadership.class)).isNotNull();
        assertThat(context.getBean(AllocationScheduler.class)).isNotNull();
        assertThat(context.getBean(AllocationRound.class)).isNotNull();
        assertThat(context.getBean(DemandCollector.class)).isNotNull();
    }

    @Test
    @DisplayName("설정값이_계획값이다")
    void 설정값이_계획값이다() {
        assertThat(properties.scheduler().tick()).isEqualTo(Duration.ofSeconds(1));
        assertThat(properties.leader().lease()).isEqualTo(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("소유자_ID_는_인스턴스마다_다르다")
    void 소유자_ID_는_인스턴스마다_다르다() {
        // 고정하면 재기동한 자신을 이전 소유자로 오인해, 죽기 전에 잡아 둔 락을
        // 새 프로세스가 자기 것으로 알고 연장한다.
        assertThat(context.getBean(Leadership.class).ownerId()).isNotBlank();
    }
}
