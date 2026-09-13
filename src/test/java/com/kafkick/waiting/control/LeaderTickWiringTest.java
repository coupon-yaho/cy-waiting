package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 배분 틱이 리더로 치는가 (CY-860). <b>경계는 리더십을 보고, 문은 그 뒤에 본다.</b>
 *
 * <p>경계가 문을 원천으로 보면 잠그는 동안의 틱을 "잃었다" 로 읽는다. 그러면 잠금이
 * 끝나는 틱에 다시 "얻었다" 가 되어 또 잠그고, 승계마다 상태를 두 번 버린다.
 */
class LeaderTickWiringTest {

    private final ControlPlaneConfig 배선 = new ControlPlaneConfig();

    private final AtomicBoolean 리더 = new AtomicBoolean(true);

    private final AtomicLong 임기 = new AtomicLong(5);

    private final SealGate 문 = SealGate.of(리더::get);

    /** 마지막으로 시작한 잠금의 세대. 몇 번 잠갔는지도 이 값이 센다. */
    private final AtomicLong 세대 = new AtomicLong();

    private final AtomicInteger 잃음 = new AtomicInteger();

    /** 알림의 차례. 잃음이 얻음 뒤에 오면 새 임기에서 막 세운 상태를 곧바로 멈춘다. */
    private final List<String> 차례 = new ArrayList<>();

    private final BooleanSupplier 틱 = 배선.leaderTick(리더::get, 임기::get, 문,
            () -> {
                차례.add("잠금");
                세대.set(문.sealing());
            },
            () -> {
                차례.add("잃음");
                잃음.incrementAndGet();
            });

    /** 잠그기 시작한 틱에 회차가 돌면 새 리더가 안 만진 쿠폰에 유령의 몫이 들어간다. */
    @Test
    @DisplayName("잠그기_시작한_틱은_회차를_안_돈다")
    void 잠그기_시작한_틱은_회차를_안_돈다() {
        assertThat(틱.getAsBoolean()).isFalse();
    }

    @Test
    @DisplayName("잠그는_동안의_틱을_잃은_것으로_안_본다")
    void 잠그는_동안의_틱을_잃은_것으로_안_본다() {
        assertThat(틱.getAsBoolean()).as("잠금을 시작한 틱").isFalse();
        assertThat(틱.getAsBoolean()).as("잠그는 중인 틱").isFalse();
        assertThat(틱.getAsBoolean()).as("잠그는 중인 틱").isFalse();

        assertThat(잃음).as("잠그는 중은 리더십을 잃은 것이 아니다").hasValue(0);
        assertThat(세대).as("한 번만 잠근다").hasValue(1);

        문.sealed(세대.get());
        assertThat(틱.getAsBoolean()).isTrue();
    }

    /** 발행을 본 뷰. 나이는 홀더가 레디스 시계로 잰 값이다. */
    private static SnapshotHolder.View 뷰(Duration 나이, boolean 시계가_갈림) {
        return new SnapshotHolder.View(new GatewaySnapshot(Map.of(), new SnapshotMeta(10, 1),
                Instant.parse("2026-09-14T00:00:10Z")), Duration.ZERO, Duration.ZERO, 나이,
                시계가_갈림);
    }

    private final AtomicLong 나노 = new AtomicLong(1_000_000_000L);

    /**
     * <b>리더로 치기 시작한 틱에 승계 간격을 건다</b> (CY-928). 이 배선이 빠지면 정상 인계에서 두
     * 리더의 몫이 1초 안에 겹치는데, 간격 자체의 시험은 그대로 초록이다.
     */
    @Test
    @DisplayName("리더로_치기_시작한_틱에_승계_간격을_건다")
    void 리더로_치기_시작한_틱에_승계_간격을_건다() {
        AtomicBoolean 이끈다 = new AtomicBoolean(false);
        BooleanSupplier 승계_틱 = 배선.handoverTick(이끈다::get,
                () -> 뷰(Duration.ofMillis(400), false),
                HandoverSpacing.of(나노::get, Duration.ofSeconds(1)));
        assertThat(승계_틱.getAsBoolean()).isFalse();

        이끈다.set(true);
        assertThat(승계_틱.getAsBoolean()).as("갓 난 발행이면 쉰다").isFalse();
        나노.addAndGet(Duration.ofMillis(1_600).toNanos());
        assertThat(승계_틱.getAsBoolean()).isTrue();

        // 한 번 건 뒤로는 리더인 동안 다시 안 건다 — 매 틱 걸면 영영 못 돈다.
        assertThat(승계_틱.getAsBoolean()).isTrue();
    }

    /** 놓았다 다시 잡으면 다시 건다. 시계가 갈린 뷰는 나이를 못 믿어 안 기다린다. */
    @Test
    @DisplayName("다시_잡으면_다시_걸고_시계가_갈렸으면_안_기다린다")
    void 다시_잡으면_다시_걸고_시계가_갈렸으면_안_기다린다() {
        AtomicBoolean 이끈다 = new AtomicBoolean(true);
        AtomicBoolean 갈림 = new AtomicBoolean(true);
        BooleanSupplier 승계_틱 = 배선.handoverTick(이끈다::get,
                () -> 뷰(Duration.ofMillis(400), 갈림.get()),
                HandoverSpacing.of(나노::get, Duration.ofSeconds(1)));
        assertThat(승계_틱.getAsBoolean()).as("시계가 갈렸으면 안 기다린다").isTrue();

        이끈다.set(false);
        assertThat(승계_틱.getAsBoolean()).isFalse();
        갈림.set(false);
        이끈다.set(true);
        assertThat(승계_틱.getAsBoolean()).as("다시 잡으면 다시 건다").isFalse();
    }

    /** 틱 사이에 갈렸다 돌아오면 앞 임기를 닫고 문을 다시 잠근다. 잠금이 끝날 때까지 안 돈다. */
    @Test
    @DisplayName("틱_사이_승계는_문을_다시_잠근다")
    void 틱_사이_승계는_문을_다시_잠근다() {
        틱.getAsBoolean();
        문.sealed(세대.get());
        assertThat(틱.getAsBoolean()).as("전제 — 잠금이 끝나 돈다").isTrue();

        임기.set(7);

        assertThat(틱.getAsBoolean()).as("새 임기의 잠금이 끝날 때까지 안 돈다").isFalse();
        assertThat(잃음).hasValue(1);
        assertThat(세대).hasValue(2);
        assertThat(차례).as("앞 임기를 닫고 새 임기를 잠근다").containsExactly("잠금", "잃음", "잠금");
    }
}
