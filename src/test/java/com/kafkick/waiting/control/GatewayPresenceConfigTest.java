package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

/**
 * 노드가 자기 존재를 알리는 배선.
 *
 * <p><b>배분 토글 밖이다.</b> 요청만 받는 노드도 분모에 들어가야 한다 — 안 그러면
 * 리더가 그 노드를 못 세고, 남은 노드가 각자 큰 몫을 써서 총 통과가 전역 크레딧을
 * 넘는다. 조각이 다 초록인데 사이가 비어 있으면 분모가 설정값에 얼어붙는다.
 */
@Tag("context")
@SpringBootTest(properties = "waiting.scheduler.enabled=false")
class GatewayPresenceConfigTest {

    @Autowired
    private ApplicationContext context;

    @Test
    @DisplayName("배분을_안_도는_노드도_존재를_알린다")
    void 배분을_안_도는_노드도_존재를_알린다() {
        assertThat(context.getBeansOfType(GatewayHeartbeatLoop.class)).hasSize(1);
        assertThat(context.getBeansOfType(GatewayRegistry.class)).hasSize(1);
    }
}
