package com.kafkick.waiting.domain.allocation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 대기열 진입과 해제의 임계를 비대칭으로 둔다.
 *
 * <p>같은 임계를 쓰면 유입이 임계선 근처에서 흔들릴 때 사용자에게
 * <b>"대기 없음 → 500명 → 대기 없음"</b> 이 반복해서 보인다.
 */
class QueueingHysteresisTest {

    /** 진입 100% · 해제 70% · 최소 유지 3틱 */
    private QueueingHysteresis policy() {
        return QueueingHysteresis.of(1.0, 0.7, 3);
    }

    @Test
    @DisplayName("진입과_해제_임계가_비대칭이다")
    void 진입과_해제_임계가_비대칭이다() {
        QueueingHysteresis h = policy();

        // 100% 에 닿아야 켠다
        assertThat(h.shouldQueue(99, 100)).isFalse();
        assertThat(h.shouldQueue(100, 100)).isTrue();

        // 켜진 뒤에는 70% 아래로 내려가야 끈다
        assertThat(h.shouldQueue(80, 100)).isTrue();
        assertThat(h.shouldQueue(69, 100)).isTrue();
    }

    @Test
    @DisplayName("해제_후_최소_유지_시간_동안_재진입하지_않는다")
    void 해제_후_최소_유지_시간_동안_재진입하지_않는다() {
        QueueingHysteresis h = policy();
        h.shouldQueue(100, 100);

        // 3틱을 버텨야 실제로 꺼진다
        assertThat(h.shouldQueue(0, 100)).isTrue();
        assertThat(h.shouldQueue(0, 100)).isTrue();
        assertThat(h.shouldQueue(0, 100)).isFalse();
    }

    @Test
    @DisplayName("유지_시간_중_다시_올라가면_카운트가_초기화된다")
    void 유지_시간_중_다시_올라가면_카운트가_초기화된다() {
        QueueingHysteresis h = policy();
        h.shouldQueue(100, 100);
        h.shouldQueue(0, 100);
        h.shouldQueue(90, 100);

        // 다시 내려가도 3틱을 새로 세야 한다
        assertThat(h.shouldQueue(0, 100)).isTrue();
        assertThat(h.shouldQueue(0, 100)).isTrue();
        assertThat(h.shouldQueue(0, 100)).isFalse();
    }

    @Test
    @DisplayName("임계선_근처_유입에서_전이가_두_번_이하다")
    void 임계선_근처_유입에서_전이가_두_번_이하다() {
        // 히스테리시스가 없으면 매 틱 뒤집혀 20회가 된다.
        QueueingHysteresis h = policy();
        boolean previous = false;
        int transitions = 0;

        for (int tick = 0; tick < 20; tick++) {
            boolean queueing = h.shouldQueue(tick % 2 == 0 ? 101 : 95, 100);
            if (queueing != previous) {
                transitions++;
            }
            previous = queueing;
        }

        assertThat(transitions).isLessThanOrEqualTo(2);
    }

    @Test
    @DisplayName("용량이_0이면_수요가_있는_한_줄을_세운다")
    void 용량이_0이면_수요가_있는_한_줄을_세운다() {
        // 0 으로 나누면 무한이 된다. 배수할 수 없으면 줄이 맞다.
        QueueingHysteresis h = policy();

        assertThat(h.shouldQueue(1, 0)).isTrue();
        assertThat(QueueingHysteresis.of(1.0, 0.7, 3).shouldQueue(0, 0)).isFalse();
    }

    @Test
    @DisplayName("해제_임계가_진입_임계보다_크면_거부한다")
    void 해제_임계가_진입_임계보다_크면_거부한다() {
        // 뒤집히면 히스테리시스가 아니라 진동 증폭기가 된다.
        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> QueueingHysteresis.of(0.7, 1.0, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("임계가_비유한값이면_거부한다")
    void 임계가_비유한값이면_거부한다() {
        // NaN 비교는 전부 false 라 대기열이 영영 안 켜진다 — 조용히.
        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> QueueingHysteresis.of(Double.NaN, 0.7, 3))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> QueueingHysteresis.of(1.0, Double.NaN, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("해제_임계와_같으면_아직_켜진_상태다")
    void 해제_임계와_같으면_아직_켜진_상태다() {
        // 경계에서 어느 쪽인지 정해 두지 않으면 부하가 딱 70% 인 동안
        // 노드마다 다른 답을 내고, 사용자는 새로고침마다 다른 화면을 본다.
        QueueingHysteresis h = policy();
        h.shouldQueue(100, 100);

        assertThat(h.shouldQueue(70, 100)).isTrue();
        assertThat(h.shouldQueue(70, 100)).isTrue();
        assertThat(h.shouldQueue(70, 100)).isTrue();
    }

    @Test
    @DisplayName("진입과_해제_임계가_같아도_만들_수_있다")
    void 진입과_해제_임계가_같아도_만들_수_있다() {
        // 히스테리시스를 끄는 설정이다. 부하 시험에서 원본 거동을 볼 때 쓴다.
        QueueingHysteresis h = QueueingHysteresis.of(1.0, 1.0, 1);

        assertThat(h.shouldQueue(100, 100)).isTrue();
        assertThat(h.shouldQueue(99, 100)).isFalse();
    }

    @Test
    @DisplayName("음수_임계는_거부한다")
    void 음수_임계는_거부한다() {
        // 음수를 허용하면 수요가 0 이어도 load(0) >= enterRatio 가 참이라
        // 아무도 안 왔는데 대기열이 켜진다.
        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> QueueingHysteresis.of(-1, -1, 3))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> QueueingHysteresis.of(1.0, -0.1, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("수요가_0이면_대기열을_켜지_않는다")
    void 수요가_0이면_대기열을_켜지_않는다() {
        assertThat(QueueingHysteresis.of(1.0, 0.7, 3).shouldQueue(0, 100)).isFalse();
    }

    @Test
    @DisplayName("임계_0은_무조건_줄을_세우는_유효한_설정이다")
    void 임계_0은_무조건_줄을_세우는_유효한_설정이다() {
        // 운영자가 이 쿠폰만 항상 큐로 돌리는 값이다. 거부하면 그 조작이 막힌다.
        QueueingHysteresis h = QueueingHysteresis.of(0.0, 0.0, 1);

        assertThat(h.shouldQueue(0, 100)).isTrue();
    }

    @Test
    @DisplayName("이월받은_상태로_시작한다")
    void 이월받은_상태로_시작한다() {
        // 리더가 바뀔 때마다 꺼진 채로 시작하면, 붙잡고 있던 대기열이 한 틱
        // 꺼졌다 다시 켜진다 — 막으려던 진동이 교체 때마다 난다.
        QueueingHysteresis 이월받음 = QueueingHysteresis.restore(0.8, 0.5, 3,
                new QueueingHysteresis.Snapshot(true, 1));

        assertThat(이월받음.shouldQueue(40, 100)).isTrue();
    }

    @Test
    @DisplayName("넘길_상태를_그대로_돌려준다")
    void 넘길_상태를_그대로_돌려준다() {
        QueueingHysteresis 앞선_리더 = QueueingHysteresis.of(0.8, 0.5, 3);
        앞선_리더.shouldQueue(90, 100);
        앞선_리더.shouldQueue(40, 100);

        assertThat(앞선_리더.snapshot())
                .isEqualTo(new QueueingHysteresis.Snapshot(true, 1));
    }

    @Test
    @DisplayName("최소_유지가_0_이어도_이월받는다")
    void 최소_유지가_0_이어도_이월받는다() {
        // 자르기에 하한이 없으면 -1 이 되고, 그 값은 스냅샷이 거부한다 —
        // 히스테리시스를 끈 설정에서 리더가 발행을 통째로 못 하게 된다.
        QueueingHysteresis 이월받음 = QueueingHysteresis.restore(0.8, 0.5, 0,
                new QueueingHysteresis.Snapshot(true, 3));

        assertThatCode(이월받음::snapshot).doesNotThrowAnyException();
        assertThat(이월받음.snapshot().belowExitTicks()).isZero();
    }

    @Test
    @DisplayName("설정이_줄어도_이월값이_유지_범위_안에_든다")
    void 설정이_줄어도_이월값이_유지_범위_안에_든다() {
        // 배포 사이에 최소 유지가 줄면 옛 값이 그대로 실려 온다. 그 값은
        // 스냅샷이 거부하는 범위라, 그대로 들고 있으면 다음에 넘길 때 터진다.
        //
        // 유효 최댓값으로 접는다 — 새 설정에서는 이미 놓았어야 할 상태이므로
        // 다음 틱에 놓는 것이 맞다.
        QueueingHysteresis 이월받음 = QueueingHysteresis.restore(0.8, 0.5, 3,
                new QueueingHysteresis.Snapshot(true, 9));

        assertThat(이월받음.snapshot().belowExitTicks()).isEqualTo(2);
        assertThatCode(이월받음::snapshot).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("모순된_상태는_이월받지_않는다")
    void 모순된_상태는_이월받지_않는다() {
        // 안 붙잡는데 유지 틱이 쌓여 있으면 말이 안 된다. 그대로 이월하면
        // 다음 리더가 켜지자마자 곧바로 끄는 판단을 한다.
        assertThatThrownBy(() -> new QueueingHysteresis.Snapshot(false, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("belowExitTicks");
        assertThatThrownBy(() -> new QueueingHysteresis.Snapshot(true, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("belowExitTicks");
    }

    @Test
    @DisplayName("빈_상태는_안_붙잡는_것이다")
    void 빈_상태는_안_붙잡는_것이다() {
        assertThat(QueueingHysteresis.Snapshot.empty())
                .isEqualTo(new QueueingHysteresis.Snapshot(false, 0));
    }
}
