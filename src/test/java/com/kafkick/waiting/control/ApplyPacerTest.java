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

    /** 기다린 차례는 기다림이 끝난 시각으로 표시한다. 기다리기 전 시각이면 다음 차례가 그만큼 당겨진다. */
    @Test
    @DisplayName("기다린_차례는_기다림이_끝난_시각으로_표시한다")
    void 기다린_차례는_기다림이_끝난_시각으로_표시한다() {
        ApplyPacer pacer = ApplyPacer.of(TICK, 시계);
        차례를_받는다(pacer);
        시계.advanceTimeBy(Duration.ofMillis(300));
        차례를_받는다(pacer);
        시계.advanceTimeBy(Duration.ofMillis(1_000));

        차례를_받는다(pacer);
        시계.advanceTimeBy(Duration.ofMillis(700));

        assertThat(차례).containsExactly(0L, 1000L, 2000L);
    }

    /**
     * <b>다음 회차는 앞 적용에서 한 틱 뒤, 앞 회차가 읽는 데 쓴 만큼 당겨 시작한다</b> (CY-927). 그러면 회차 안의 대기가
     * 지연의 흔들림만큼으로 줄어 틱 시한을 안 먹는다.
     */
    @Test
    @DisplayName("다음_시작은_앞_회차의_읽기만큼_당긴다")
    void 다음_시작은_앞_회차의_읽기만큼_당긴다() {
        ApplyPacer pacer = ApplyPacer.of(TICK, 시계);
        assertThat(pacer.holdOff()).as("적용한 적이 없다").isZero();

        pacer.roundStarted();
        시계.advanceTimeBy(Duration.ofMillis(200));
        차례를_받는다(pacer);
        시계.advanceTimeBy(Duration.ofMillis(300));

        assertThat(pacer.holdOff()).isEqualTo(Duration.ofMillis(500));
        시계.advanceTimeBy(Duration.ofMillis(600));
        assertThat(pacer.holdOff()).as("이미 지났다").isZero();
    }

    /** 승계한 노드는 앞 리더의 마지막 적용에서 한 틱을 잇는다. 제 적용만 보면 첫 적용이 앞 리더 것과 겹친다. */
    @Test
    @DisplayName("앞_리더의_적용_나이를_이어_받는다")
    void 앞_리더의_적용_나이를_이어_받는다() {
        ApplyPacer pacer = ApplyPacer.of(TICK, 시계);
        pacer.appliedAgo(Duration.ofMillis(300));

        assertThat(pacer.holdOff()).isEqualTo(Duration.ofMillis(700));
        차례를_받는다(pacer);
        시계.advanceTimeBy(Duration.ofMillis(699));
        assertThat(차례).isEmpty();
        시계.advanceTimeBy(Duration.ofMillis(1));
        assertThat(차례).containsExactly(700L);
    }

    /** 제 적용이 더 최근이면 앞 리더의 나이로 덮지 않는다. 덮으면 간격이 그만큼 당겨진다. */
    @Test
    @DisplayName("제_적용이_더_최근이면_앞_리더_나이로_안_덮는다")
    void 제_적용이_더_최근이면_앞_리더_나이로_안_덮는다() {
        ApplyPacer pacer = ApplyPacer.of(TICK, 시계);
        시계.advanceTimeBy(Duration.ofMillis(1_000));
        차례를_받는다(pacer);
        시계.advanceTimeBy(Duration.ofMillis(100));

        pacer.appliedAgo(Duration.ofMillis(500));

        assertThat(pacer.holdOff()).isEqualTo(Duration.ofMillis(900));
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
