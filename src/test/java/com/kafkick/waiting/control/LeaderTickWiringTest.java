package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

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

    private final BooleanSupplier 틱 = 배선.leaderTick(리더::get, 임기::get, 문,
            () -> 세대.set(문.sealing()), 잃음::incrementAndGet);

    /** 잠그기 시작한 틱에 회차가 돌면 새 리더가 안 만진 쿠폰에 유령의 몫이 들어간다. */
    @Test
    @DisplayName("잠그기_시작한_틱은_회차를_안_돈다")
    void 잠그기_시작한_틱은_회차를_안_돈다() {
        assertThat(틱.getAsBoolean()).isFalse();
    }

    @Test
    @DisplayName("잠그는_동안의_틱을_잃은_것으로_안_본다")
    void 잠그는_동안의_틱을_잃은_것으로_안_본다() {
        틱.getAsBoolean();
        틱.getAsBoolean();
        틱.getAsBoolean();

        assertThat(잃음).as("잠그는 중은 리더십을 잃은 것이 아니다").hasValue(0);
        assertThat(세대).as("한 번만 잠근다").hasValue(1);

        문.sealed(세대.get());
        assertThat(틱.getAsBoolean()).isTrue();
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
    }
}
