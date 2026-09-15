package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.scheduler.VirtualTimeScheduler;

class ApplyPacerTest {

    private static final Duration TICK = Duration.ofSeconds(1);

    private final VirtualTimeScheduler 시계 = VirtualTimeScheduler.create();
    private final List<Long> 차례 = new CopyOnWriteArrayList<>();

    private void 차례를_받는다(ApplyPacer pacer) {
        pacer.turn().doOnSuccess(v -> 차례.add(시계.now(TimeUnit.MILLISECONDS))).subscribe();
    }

    @Test
    @DisplayName("첫_차례는_안_기다린다")
    void 첫_차례는_안_기다린다() {
        차례를_받는다(ApplyPacer.of(TICK, 시계));

        assertThat(차례).containsExactly(0L);
    }

    /**
     * <b>앞 적용에서 한 틱이 지나야 다음 차례다</b> (CY-927). 회차 시작 간격은 최소 틱의 4분의 1이라, 느린 회차가
     * 끝에 적용하고 다음 회차가 빨리 적용하면 두 틱 몫이 1초 안에 들어가 뒷단 유입이 두 배가 된다.
     */
    @Test
    @DisplayName("앞_차례에서_한_틱이_지나야_다음_차례다")
    void 앞_차례에서_한_틱이_지나야_다음_차례다() {
        ApplyPacer pacer = ApplyPacer.of(TICK, 시계);
        차례를_받는다(pacer);

        시계.advanceTimeBy(Duration.ofMillis(300));
        차례를_받는다(pacer);
        시계.advanceTimeBy(Duration.ofMillis(699));
        assertThat(차례).as("한 틱이 안 지났다").containsExactly(0L);

        시계.advanceTimeBy(Duration.ofMillis(1));
        assertThat(차례).containsExactly(0L, 1000L);
    }

    @Test
    @DisplayName("한_틱이_넘게_지났으면_안_기다린다")
    void 한_틱이_넘게_지났으면_안_기다린다() {
        ApplyPacer pacer = ApplyPacer.of(TICK, 시계);
        차례를_받는다(pacer);

        시계.advanceTimeBy(Duration.ofMillis(1500));
        차례를_받는다(pacer);

        assertThat(차례).containsExactly(0L, 1500L);
    }

    /** 기다리다 잘린 차례는 적용이 안 나갔다. 표시하면 다음 회차가 안 나간 적용에 맞춰 또 쉰다. */
    @Test
    @DisplayName("기다리다_취소된_차례는_표시하지_않는다")
    void 기다리다_취소된_차례는_표시하지_않는다() {
        ApplyPacer pacer = ApplyPacer.of(TICK, 시계);
        차례를_받는다(pacer);
        시계.advanceTimeBy(Duration.ofMillis(300));
        pacer.turn().subscribe().dispose();

        시계.advanceTimeBy(Duration.ofMillis(700));
        차례를_받는다(pacer);

        assertThat(차례).containsExactly(0L, 1000L);
    }
}
