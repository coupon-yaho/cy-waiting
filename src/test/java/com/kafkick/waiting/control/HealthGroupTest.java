package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.health.actuate.endpoint.HealthEndpointGroups;
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
    @DisplayName("이름으로_찾은_빈이_그_타입이다")
    void 이름으로_찾은_빈이_그_타입이다() {
        assertThat(context.getBean("judging")).isInstanceOf(JudgingHealth.class);
        assertThat(context.getBean("loopAlive")).isInstanceOf(LoopAliveHealth.class);
        assertThat(context.getBeansOfType(JudgingHealth.class)).hasSize(1);
    }

    @Test
    @DisplayName("의존성_헬스가_그룹에_안_섞인다")
    void 의존성_헬스가_그룹에_안_섞인다() {
        // **런타임 그룹을 본다.** 설정 문자열만 재면 프로파일이나 환경변수로
        // 덮었을 때 아무 시험도 안 깨진다. 그러면 레디스가 흔들릴 때 전 노드가
        // 동시에 빠지는 구멍이 조용히 열린다.
        HealthEndpointGroups groups = context.getBean(HealthEndpointGroups.class);

        assertThat(groups.get("readiness")).isNotNull()
                .satisfies(group -> {
                    assertThat(group.isMember("judging")).isTrue();
                    assertThat(group.isMember("redis")).isFalse();
                    assertThat(group.isMember("db")).isFalse();
                });
        assertThat(groups.get("liveness")).isNotNull()
                .satisfies(group -> {
                    assertThat(group.isMember("loopAlive")).isTrue();
                    assertThat(group.isMember("redis")).isFalse();
                });
    }

    @Test
    @DisplayName("레디스_기여자가_아예_안_올라온다")
    void 레디스_기여자가_아예_안_올라온다() {
        // 그룹에서 빼는 것만으로는 루트가 여전히 합산한다. 프로브 경로를
        // 루트로 적은 배포 설정 한 줄이면 그게 전 노드 이탈이 된다.
        assertThat(context.containsBean("redisHealthContributor")).isFalse();
        assertThat(context.containsBean("redisReactiveHealthContributor")).isFalse();
    }
}
