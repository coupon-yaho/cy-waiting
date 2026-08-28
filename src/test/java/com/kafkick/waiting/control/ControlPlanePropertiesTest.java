package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 제어 평면의 시간 예산.
 *
 * <p><b>어긋나면 안 뜨게 한다.</b> 주석으로 적어 두면 값을 바꾸는 사람이 안 읽고,
 * 배분이 멎는 사고로 배운다. 특히 리더 연장은 어긋나도 예외가 안 나고 조용히
 * 리더가 없어진다.
 */
class ControlPlanePropertiesTest {

    /** 생산 코드가 쓰는 역수에서 유도한다. 여기 다시 적으면 사본이 하나 더 는다. */
    private static final double IDLE_CREDIT_RATIO = 1.0 / CapacityCollector.IDLE_DIVISOR;

    private static ControlPlaneProperties.Leader leader(Duration lease, Duration attempt,
            Duration delay) {
        return new ControlPlaneProperties.Leader(lease, attempt, delay);
    }

    @Test
    @DisplayName("계획값은_예산_안에_있다")
    void 계획값은_예산_안에_있다() {
        assertThatCode(() -> leader(Duration.ofSeconds(2), Duration.ofMillis(300),
                Duration.ofMillis(100))).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("회복하는_판까지_리스_안에_들어와야_한다")
    void 회복하는_판까지_리스_안에_들어와야_한다() {
        // 주기가 완료 후 지연이라 실패한 판은 지연뿐 아니라 시도 상한만큼도 먹는다.
        // 마지막 성공부터 회복하는 판이 끝날 때까지가 실제 예산이다.
        //
        //   4 × (시도 + 지연) + 시도 ≤ lease
        Duration lease = Duration.ofSeconds(2);

        // 4 × 400 + 300 = 1,900 — 들어온다
        assertThatCode(() -> leader(lease, Duration.ofMillis(300), Duration.ofMillis(100)))
                .doesNotThrowAnyException();

        // 4 × 500 + 300 = 2,300 — 두 번 놓치면 리스가 끝난다
        assertThatThrownBy(() -> leader(lease, Duration.ofMillis(300), Duration.ofMillis(200)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("연장 예산");
    }

    @Test
    @DisplayName("경계에서_갈린다")
    void 경계에서_갈린다() {
        // 딱 맞는 값은 통과한다. 한 칸만 넘으면 안 뜬다.
        Duration lease = Duration.ofMillis(1_900);
        assertThatCode(() -> leader(lease, Duration.ofMillis(300), Duration.ofMillis(100)))
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> leader(lease.minusMillis(1), Duration.ofMillis(300),
                Duration.ofMillis(100)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("연장_시도가_리스보다_길면_안_뜬다")
    void 연장_시도가_리스보다_길면_안_뜬다() {
        // 성공해도 도착할 때 이미 만료다. 아무도 리더가 못 되는데 예외도 로그도 없다.
        assertThatThrownBy(() -> leader(Duration.ofMillis(400), Duration.ofMillis(500),
                Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("양수가_아닌_값은_안_뜬다")
    void 양수가_아닌_값은_안_뜬다() {
        assertThatThrownBy(() -> leader(Duration.ZERO, Duration.ofMillis(1), Duration.ofMillis(1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("lease");
        assertThatThrownBy(() -> leader(Duration.ofSeconds(2), Duration.ZERO, Duration.ofMillis(1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("attempt");
        assertThatThrownBy(() -> leader(Duration.ofSeconds(2), Duration.ofMillis(1),
                Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("renewDelay");
    }

    @Test
    @DisplayName("스케줄러_값이_잘못되면_안_뜬다")
    void 스케줄러_값이_잘못되면_안_뜬다() {
        assertThatCode(() -> new ControlPlaneProperties.Scheduler(Duration.ofSeconds(1),
                Duration.ofSeconds(3), 1, 10)).doesNotThrowAnyException();
        assertThatThrownBy(() -> new ControlPlaneProperties.Scheduler(Duration.ZERO,
                Duration.ofSeconds(3), 1, 10))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("tick");
        assertThatThrownBy(() -> new ControlPlaneProperties.Scheduler(Duration.ofSeconds(1),
                Duration.ofSeconds(-1), 1, 10))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("firstTickDelay");
        assertThatThrownBy(() -> new ControlPlaneProperties.Scheduler(Duration.ofSeconds(1),
                Duration.ofSeconds(3), 0, 10))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("shards");
        // **유예가 0 이면 유예가 아니다.** 마지막 폴링이 아직 오는 중에 줄이
        // 사라지면, 그 사람은 매진이 아니라 "줄에 없다" 를 받는다 (7.3.2).
        assertThatThrownBy(() -> new ControlPlaneProperties.Scheduler(Duration.ofSeconds(1),
                Duration.ofSeconds(3), 1, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("매진 유예");
    }

    @Test
    @DisplayName("리스는_틱보다_길어야_한다")
    void 리스는_틱보다_길어야_한다() {
        // 리스가 한 틱도 못 버티면 판을 도는 도중에 리더십을 잃는다. 그 판의
        // 배분은 나갔는데 다음 리더도 같은 판을 돌아 두 배가 나간다.
        //
        // **등호가 이 검사의 이유다.** 엄격히 작은 값만 쓰면 경계를 한 칸
        // 옮겨도 안 죽는다.
        ControlPlaneProperties.Scheduler scheduler =
                new ControlPlaneProperties.Scheduler(
                        Duration.ofSeconds(1), Duration.ofSeconds(3), 1, 10);

        assertThatThrownBy(() -> new ControlPlaneProperties(scheduler,
                leader(Duration.ofSeconds(1), Duration.ofMillis(100), Duration.ofMillis(50)),
                ControlPlaneProperties.defaults().capacity()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("리스");
    }

    /**
     * <b>유예가 낡음 한계보다 길어야 합니다</b> (7.3.2).
     *
     * <p>짧으면 마지막 폴링이 아직 오는 중에 줄이 사라져, 매진이 아니라
     * "줄에 없다" 를 받습니다.
     */
    @Test
    @DisplayName("기본_유예가_낡음_한계보다_길다")
    void 기본_유예가_낡음_한계보다_길다() {
        ControlPlaneProperties.Scheduler scheduler =
                ControlPlaneProperties.defaults().scheduler();

        Duration 유예 = scheduler.tick().multipliedBy(scheduler.soldOutGraceTicks());

        assertThat(유예).isGreaterThan(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("신선도는_틱보다_길어야_한다")
    void 신선도는_틱보다_길어야_한다() {
        // 신선도가 틱보다 짧으면 한 틱만 밀려도 전 인스턴스가 낡음이 된다.
        // 그러면 하한으로 떨어져 아무 문제 없는 쿠폰까지 줄을 세운다.
        ControlPlaneProperties.Scheduler scheduler =
                new ControlPlaneProperties.Scheduler(
                        Duration.ofSeconds(1), Duration.ofSeconds(3), 1, 10);

        assertThatThrownBy(() -> new ControlPlaneProperties(scheduler,
                ControlPlaneProperties.defaults().leader(),
                new ControlPlaneProperties.Capacity(Duration.ofSeconds(60), Duration.ofSeconds(1),
                        5, 10_000, 3, 1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("신선도");
    }

    @Test
    @DisplayName("가용량_값이_잘못되면_안_뜬다")
    void 가용량_값이_잘못되면_안_뜬다() {
        assertThatThrownBy(() -> new ControlPlaneProperties.Capacity(Duration.ZERO,
                Duration.ofSeconds(3), 5, 10_000, 3, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("rampUp");
        assertThatThrownBy(() -> new ControlPlaneProperties.Capacity(Duration.ofSeconds(60),
                Duration.ZERO, 5, 10_000, 3, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("freshness");
        assertThatThrownBy(() -> new ControlPlaneProperties.Capacity(Duration.ofSeconds(60),
                Duration.ofSeconds(3), -1, 10_000, 3, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("floor");
        // 상한이 0 이면 어떤 보고도 0 으로 잘려 크레딧이 영영 안 나간다.
        assertThatThrownBy(() -> new ControlPlaneProperties.Capacity(Duration.ofSeconds(60),
                Duration.ofSeconds(3), 5, 0, 3, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("perInstanceCap");
        assertThatThrownBy(() -> new ControlPlaneProperties.Capacity(Duration.ofSeconds(60),
                Duration.ofSeconds(3), 5, 10_000, 0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("rampDownTicks");
        // 하한을 아무도 안 써 보면 경계를 한 칸 옮겨도 안 죽는다.
        assertThatCode(() -> new ControlPlaneProperties.Capacity(Duration.ofSeconds(60),
                Duration.ofSeconds(3), 0, 1, 1, 1)).doesNotThrowAnyException();
        assertThatThrownBy(() -> new ControlPlaneProperties.Capacity(Duration.ofSeconds(60),
                Duration.ofSeconds(3), 5, 10_000, 3, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("expectedNodes");
    }

    @Test
    @DisplayName("기본값이_계획값과_같다")
    void 기본값이_계획값과_같다() {
        ControlPlaneProperties defaults = ControlPlaneProperties.defaults();

        assertThat(defaults.scheduler().tick()).isEqualTo(Duration.ofSeconds(1));
        assertThat(defaults.leader().lease()).isEqualTo(Duration.ofSeconds(2));
        assertThat(defaults.leader().attempt()).isEqualTo(Duration.ofMillis(300));
        assertThat(defaults.leader().renewDelay()).isEqualTo(Duration.ofMillis(100));
    }

    /**
     * <b>하한은 전면 차단과 달라야 한다.</b> 신선한 보고가 없을 때 쓰는 값인데,
     * 그 값에서 한산 통과 상한이 0 이 되면 하한이 있으나 마나다 — 대기열이 통째로
     * 켜지고 R1 이 죽는다 (G4.10).
     */
    @Test
    @DisplayName("하한에서도_한산_통과가_열린다")
    void 하한에서도_한산_통과가_열린다() {
        ControlPlaneProperties.Capacity capacity = ControlPlaneProperties.defaults().capacity();
        SnapshotMeta 하한 = new SnapshotMeta(capacity.floor(), capacity.expectedNodes());

        assertThat(CouponStates.idle(1_000).idleCap(하한, IDLE_CREDIT_RATIO))
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("하한이_노드_수를_못_받치면_안_뜬다")
    void 하한이_노드_수를_못_받치면_안_뜬다() {
        ControlPlaneProperties.Capacity 기본 = ControlPlaneProperties.defaults().capacity();

        // 설정으로 노드를 늘리면서 하한을 그대로 두면 조용히 전면 차단이 된다.
        // **경계를 손으로 안 적는다.** 유휴 비율이 바뀌면 이 값도 같이 움직인다.
        int 못_받치는_노드 = (int) (기본.floor() / CapacityCollector.IDLE_DIVISOR) + 1;

        assertThatThrownBy(() -> new ControlPlaneProperties.Capacity(기본.rampUp(),
                기본.freshness(), 기본.floor(), 기본.perInstanceCap(),
                기본.rampDownTicks(), 못_받치는_노드))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("하한");
    }
}
