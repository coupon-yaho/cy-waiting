package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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

    /**
     * 기다린 시간을 기록만 하고 실제로는 안 잔다 (TS-4).
     *
     * <p><b>잠든 순간의 readiness 를 같이 적는다.</b> 잤다는 사실만 적으면
     * readiness 를 나중에 내려도 시험이 통과한다 — 순서가 이 클래스의 전부다.
     */
    private DrainWait 대기(Duration 얼마나) {
        return DrainWait.of(shutdown, 얼마나,
                ms -> 순서.add("잤다:" + ms + ":드레이닝=" + shutdown.isDraining()));
    }

    /**
     * <b>순서가 뒤집히면 안 됩니다.</b> 기다린 뒤에 내리면 그 대기 시간 동안 LB 는
     * 우리가 멀쩡하다고 보고 계속 보냅니다.
     */
    @Test
    @DisplayName("readiness를_먼저_내리고_기다린다")
    void readiness를_먼저_내리고_기다린다() {
        DrainWait wait = 대기(Duration.ofSeconds(6));

        wait.beforeDrain();

        assertThat(순서).containsExactly("잤다:6000:드레이닝=true");
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
     * <b>상한이 없으면 오타 하나가 노드를 붙든다.</b> 이 대기는 컨테이너의 단계별
     * 상한 밖이라 프레임워크가 못 끊는다 — {@code 6s} 를 {@code 6m} 로 적으면
     * 오케스트레이터가 진행 중인 요청째 강제 종료할 때까지 안 죽는다.
     */
    @Test
    @DisplayName("드레인_상한보다_긴_대기는_기동을_막는다")
    void 드레인_상한보다_긴_대기는_기동을_막는다() {
        assertThatCode(() -> 대기(Duration.ofSeconds(30))).doesNotThrowAnyException();
        assertThatThrownBy(() -> 대기(Duration.ofSeconds(31)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PT30S");
    }

    /**
     * <b>두 번 불려도 한 번만 기다립니다.</b> 프레임워크가 종료 이벤트를 여러 번
     * 보내면 대기가 곱해져 배포가 그만큼 느려집니다.
     */
    @Test
    @DisplayName("두_번_불려도_한_번만_기다린다")
    void 두_번_불려도_한_번만_기다린다() {
        DrainWait wait = 대기(Duration.ofSeconds(6));

        wait.beforeDrain();
        wait.beforeDrain();

        assertThat(순서).containsExactly("잤다:6000:드레이닝=true");
    }

    /** 기다리는 중에 끊기면 그 사실을 남기고 계속한다. 끊겼다고 안 죽으면 안 된다. */
    @Test
    @DisplayName("기다리다_끊겨도_종료는_이어진다")
    void 기다리다_끊겨도_종료는_이어진다() {
        DrainWait wait = DrainWait.of(shutdown, Duration.ofSeconds(6), ms -> {
            throw new IllegalStateException("끊겼다");
        });

        assertThatCode(wait::beforeDrain).doesNotThrowAnyException();

        assertThat(shutdown.isDraining()).isTrue();
    }

    /**
     * <b>끊긴 표시를 다시 세우면 드레인이 통째로 건너뛴다.</b> 이 스레드는 곧바로
     * 컨테이너의 단계별 정지로 들어가 {@code latch.await} 로 드레인을 기다리는데,
     * 표시가 서 있으면 그 기다림이 즉시 깨져 진행 중인 요청이 끊긴다.
     */
    @Test
    @DisplayName("끊겨도_인터럽트_표시를_남기지_않는다")
    void 끊겨도_인터럽트_표시를_남기지_않는다() {
        // 운영 배선이 쓰는 길이다. 미리 표시를 세워 두면 실제로 안 자고 즉시 끊긴다.
        DrainWait wait = DrainWait.of(shutdown, Duration.ofSeconds(6));
        Thread.currentThread().interrupt();

        assertThatCode(wait::beforeDrain).doesNotThrowAnyException();

        assertThat(Thread.currentThread().isInterrupted())
                .describedAs("표시가 남으면 뒤이은 드레인 대기가 즉시 깨진다")
                .isFalse();
        assertThat(shutdown.isDraining()).isTrue();
    }

    /**
     * <b>넘치는 값이 검증을 우회하면 안 됩니다.</b> {@code toMillis()} 는 넘치면
     * 다른 예외를 던지므로, 상한을 뒤에 두면 아주 큰 값이 통째로 빠져나갑니다.
     */
    @Test
    @DisplayName("넘치는_대기도_기동을_막는다")
    void 넘치는_대기도_기동을_막는다() {
        assertThatThrownBy(() -> 대기(Duration.ofSeconds(Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 밀리초 미만은 0 으로 잘려 안 기다린다. 양수 검사만으로는 안 드러난다. */
    @Test
    @DisplayName("밀리초_미만_대기는_기동을_막는다")
    void 밀리초_미만_대기는_기동을_막는다() {
        assertThatThrownBy(() -> 대기(Duration.ofNanos(500)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
