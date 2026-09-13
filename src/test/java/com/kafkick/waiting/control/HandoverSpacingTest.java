package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.MutableClock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 승계 첫 회차를 앞 리더의 마지막 발행에서 떨어뜨린다 (CY-928).
 *
 * <p><b>정상 인계는 틈이 짧다.</b> 옛 리더가 락을 놓으면 새 리더가 수백 ms 안에 잡고 곧 첫
 * 회차를 돈다. 두 회차가 한 틱 몫씩 들이므로 그 1초에 뒷단 유입이 두 배가 된다.
 */
class HandoverSpacingTest {

    private static final Duration 틱 = Duration.ofSeconds(1);

    /** 발행 시각은 초 단위로 실린다. 초 경계에 맞춰 둬야 올림 여유를 가를 수 있다. */
    private static final Instant 발행 = Instant.parse("2026-09-14T00:00:10Z");

    private final MutableClock 시계 = MutableClock.at(발행.plusMillis(400));

    private final HandoverSpacing 간격 = HandoverSpacing.of(시계, 틱);

    @Test
    @DisplayName("리더가_된_적_없으면_막지_않는다")
    void 리더가_된_적_없으면_막지_않는다() {
        assertThat(간격.getAsBoolean()).isTrue();
    }

    /**
     * <b>마지막 발행에서 한 틱이 안 지났으면 미룬다.</b> 발행 시각이 초 단위라 실제 발행은 그
     * 초 안 어디쯤이다 — 한 초를 더해야 실제 발행에서 한 틱이 보장된다.
     */
    @Test
    @DisplayName("마지막_발행에서_한_틱이_안_지났으면_첫_회차를_미룬다")
    void 마지막_발행에서_한_틱이_안_지났으면_첫_회차를_미룬다() {
        간격.armedFrom(발행);

        assertThat(간격.getAsBoolean()).as("인계 직후").isFalse();
        시계.앞으로(Duration.ofMillis(1_500));
        assertThat(간격.getAsBoolean()).as("발행 시각 + 틱은 지났지만 초 올림 여유 안이다").isFalse();
        시계.앞으로(Duration.ofMillis(100));
        assertThat(간격.getAsBoolean()).as("발행 시각 + 한 초 + 틱").isTrue();
    }

    /** 리더가 죽은 승계는 마지막 발행이 이미 리스 넘게 지났다. 기다리면 승계만 늦어진다. */
    @Test
    @DisplayName("오래된_발행이면_기다리지_않는다")
    void 오래된_발행이면_기다리지_않는다() {
        시계.앞으로(Duration.ofSeconds(2));

        간격.armedFrom(발행);

        assertThat(간격.getAsBoolean()).isTrue();
    }

    /** 발행을 본 적이 없으면 견줄 것이 없다. 기동 직후 첫 리더다. */
    @Test
    @DisplayName("본_발행이_없으면_기다리지_않는다")
    void 본_발행이_없으면_기다리지_않는다() {
        간격.armedFrom(null);

        assertThat(간격.getAsBoolean()).isTrue();
    }
}
