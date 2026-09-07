package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

/**
 * 합성 프로브가 배포에 실제로 서는가.
 *
 * <p><b>기본이 꺼짐이라는 것을 배선에서 못 박는다.</b> 뒷단이 부하를 받는 헬스
 * 경로를 내기 전에 켜지면 정적 200 을 보고 서킷이 거짓으로 닫힌다 (CY-890).
 */
@Tag("context")
@SpringBootTest(properties = "waiting.scheduler.enabled=false")
class BackendProbeWiringTest {

    @Autowired
    private ApplicationContext context;

    @Test
    @DisplayName("기본은_안_뜬다")
    void 기본은_안_뜬다() {
        assertThat(context.getBeansOfType(BackendProbe.class)).isEmpty();
        assertThat(context.getBeansOfType(BackendProbeLoop.class)).isEmpty();
    }

    /** 켜면 프로브와 루프가 같이 선다. 하나만 서면 아무도 안 친다. */
    @Nested
    @Tag("context")
    @SpringBootTest(properties = {"waiting.scheduler.enabled=false",
            "waiting.backend.probe.enabled=true"})
    class Enabled {

        @Autowired
        private ApplicationContext context;

        @Test
        @DisplayName("켜면_프로브와_루프가_선다")
        void 켜면_프로브와_루프가_선다() {
            assertThat(context.getBeansOfType(BackendProbe.class)).hasSize(1);
            assertThat(context.getBeansOfType(BackendProbeLoop.class)).hasSize(1);
        }

        /** 루프가 스프링 수명에 걸려야 웹 서버보다 먼저 내려간다. */
        @Test
        @DisplayName("루프가_수명에_걸린다")
        void 루프가_수명에_걸린다() {
            assertThat(context.getBean(BackendProbeLoop.class).isRunning()).isTrue();
        }
    }
}
