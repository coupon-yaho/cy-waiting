package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

/**
 * 설정에 적은 이름이 실제 빈과 맞아야 한다.
 *
 * <p><b>빈 그룹은 항상 통과한다.</b> 이름이 어긋나면 확인이 사라진 것을 아무도
 * 모르고, 판정 못 하는 노드가 계속 트래픽을 받는다.
 */
@Tag("context")
@SpringBootTest
class HealthGroupTest {

    @Autowired
    private ApplicationContext context;

    @Test
    @DisplayName("그룹에_적은_이름의_빈이_있다")
    void 그룹에_적은_이름의_빈이_있다() {
        assertThat(context.containsBean("judging")).isTrue();
        assertThat(context.containsBean("loopAlive")).isTrue();
    }

    @Test
    @DisplayName("의존성_헬스가_그룹에_안_섞인다")
    void 의존성_헬스가_그룹에_안_섞인다() {
        // 레디스 헬스가 자동으로 올라와도 그룹에 안 들어가야 한다. 들어가면
        // 레디스가 흔들릴 때 전 노드가 동시에 빠진다.
        //
        // 이름으로 찾은 빈이 실제로 그 타입이어야 그룹이 이걸 잡는다.
        assertThat(context.getBean("judging")).isInstanceOf(JudgingHealth.class);
        assertThat(context.getBean("loopAlive")).isInstanceOf(LoopAliveHealth.class);
        assertThat(context.getBeansOfType(JudgingHealth.class)).hasSize(1);
    }
}
