package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 종료 신호를 받은 뒤 <b>LB 가 우리를 뺄 때까지 기다립니다.</b>
 *
 * <p>readiness 를 내려도 앞단 부하 분산기가 그것을 알아채기 전까지는 계속 보냅니다.
 * 기다리지 않고 곧바로 드레인을 시작하면 그 사이 도착한 요청이 커넥션째 끊깁니다.
 * 롤링 배포마다 사용자가 오류를 보게 됩니다.
 */
class DrainWaitTest {

    private final List<String> 순서 = new CopyOnWriteArrayList<>();

    private final ShutdownState shutdown = ShutdownState.create();

    /** 기다린 시간을 기록만 하고 실제로는 안 잔다. 실제로 자면 장비 속도에 걸린다 (TS-4). */
    private DrainWait 대기(Duration 얼마나) {
        return DrainWait.of(shutdown, 얼마나, ms -> 순서.add("잤다:" + ms));
    }

    /**
     * <b>순서가 뒤집히면 안 됩니다.</b> 기다린 뒤에 내리면 그 대기 시간 동안 LB 는
     * 우리가 멀쩡하다고 보고 계속 보냅니다.
     */
    @Test
    @DisplayName("readiness를_먼저_내리고_기다린다")
    void readiness를_먼저_내리고_기다린다() {
        DrainWait wait = 대기(Duration.ofSeconds(6));

        wait.beforeDrain(() -> 순서.add("드레인"));

        assertThat(순서).containsExactly("잤다:6000", "드레인");
        assertThat(shutdown.isDraining()).isTrue();
    }

    /**
     * <b>대기가 0 이면 이 장치가 없는 것과 같습니다.</b> 값을 0 으로 두면 기다림이
     * 사라졌다는 사실이 설정 어디에도 안 드러납니다.
     */
    @Test
    @DisplayName("대기가_0이면_기동을_막는다")
    void 대기가_0이면_기동을_막는다() {
        assertThatThrownBy(() -> 대기(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> 대기(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>드레인이 실패해도 종료는 이어집니다.</b> 여기서 막히면 노드가 안 죽고,
     * 배포가 그 자리에서 멈춥니다.
     */
    @Test
    @DisplayName("드레인이_터져도_종료는_이어진다")
    void 드레인이_터져도_종료는_이어진다() {
        DrainWait wait = 대기(Duration.ofSeconds(6));

        wait.beforeDrain(() -> {
            throw new IllegalStateException("레디스가 죽었다");
        });

        assertThat(shutdown.isDraining()).isTrue();
    }

    /**
     * <b>두 번 불려도 한 번만 기다립니다.</b> 프레임워크가 종료 이벤트를 여러 번
     * 보내면 대기가 곱해져 배포가 그만큼 느려집니다. 드레인 자체는 부르는 쪽의
     * 몫이라 막지 않습니다.
     */
    @Test
    @DisplayName("두_번_불려도_한_번만_기다린다")
    void 두_번_불려도_한_번만_기다린다() {
        DrainWait wait = 대기(Duration.ofSeconds(6));

        wait.beforeDrain(() -> 순서.add("드레인"));
        wait.beforeDrain(() -> 순서.add("드레인"));

        assertThat(순서).filteredOn(s -> s.startsWith("잤다")).hasSize(1);
        assertThat(순서).containsExactly("잤다:6000", "드레인", "드레인");
    }

    /** 기다리는 중에 끊기면 그 사실을 남기고 계속한다. 끊겼다고 안 죽으면 안 된다. */
    @Test
    @DisplayName("기다리다_끊겨도_드레인은_한다")
    void 기다리다_끊겨도_드레인은_한다() {
        DrainWait wait = DrainWait.of(shutdown, Duration.ofSeconds(6), ms -> {
            throw new IllegalStateException("끊겼다");
        });

        wait.beforeDrain(() -> 순서.add("드레인"));

        assertThat(순서).containsExactly("드레인");
    }
}
