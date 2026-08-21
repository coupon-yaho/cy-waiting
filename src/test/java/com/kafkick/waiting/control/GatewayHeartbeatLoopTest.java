package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.springframework.context.SmartLifecycle;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * 하트비트 루프의 수명.
 *
 * <p><b>종료가 레디스에 매달리면 안 된다.</b> 등록 해제를 못 해도 파드는 내려가야
 * 한다 — 못 지운 항목은 임계가 지나면 알아서 빠진다. 반대로 종료를 붙들면
 * 오케스트레이터가 강제 종료하고, 그때는 진행 중인 요청까지 함께 끊긴다.
 */
class GatewayHeartbeatLoopTest {

    private static final Duration INTERVAL = Duration.ofMillis(100);
    private static final Duration LEAVE_TIMEOUT = Duration.ofMillis(200);

    @Test
    @DisplayName("돌_때마다_관측한_수를_레지스트리에_넘긴다")
    void 돌_때마다_관측한_수를_레지스트리에_넘긴다() {
        AtomicReference<Integer> 마지막 = new AtomicReference<>();
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        GatewayHeartbeatLoop loop = GatewayHeartbeatLoop.of(
                () -> Mono.just(3), () -> Mono.empty(), 마지막::set, INTERVAL, LEAVE_TIMEOUT);

        loop.start(가상);
        try {
            가상.advanceTimeBy(INTERVAL.multipliedBy(2));
            assertThat(마지막.get()).isEqualTo(3);
        } finally {
            loop.stop();
        }
    }

    @Test
    @DisplayName("관측이_실패해도_루프가_안_멎는다")
    void 관측이_실패해도_루프가_안_멎는다() {
        // 한 번 터지고 끝나면 그 노드는 영영 분모에서 빠지고, 남은 노드가
        // 큰 몫을 쓴다. 실패는 그 판만 버린다.
        AtomicInteger 호출 = new AtomicInteger();
        AtomicReference<Integer> 마지막 = new AtomicReference<>();
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        GatewayHeartbeatLoop loop = GatewayHeartbeatLoop.of(
                () -> 호출.incrementAndGet() == 1
                        ? Mono.error(new IllegalStateException("레디스가 끊겼다"))
                        : Mono.just(2),
                () -> Mono.empty(), 마지막::set, INTERVAL, LEAVE_TIMEOUT);

        loop.start(가상);
        try {
            가상.advanceTimeBy(INTERVAL.multipliedBy(3));
            assertThat(호출.get()).isGreaterThan(1);
            assertThat(마지막.get()).isEqualTo(2);
        } finally {
            loop.stop();
        }
    }

    @Test
    @DisplayName("종료하면_등록을_해제한다")
    void 종료하면_등록을_해제한다() {
        AtomicInteger 해제 = new AtomicInteger();
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        GatewayHeartbeatLoop loop = GatewayHeartbeatLoop.of(
                () -> Mono.just(1),
                () -> Mono.fromRunnable(해제::incrementAndGet),
                n -> { }, INTERVAL, LEAVE_TIMEOUT);
        loop.start(가상);

        loop.stop();

        assertThat(해제.get()).isEqualTo(1);
        assertThat(loop.isRunning()).isFalse();
    }

    @Test
    @DisplayName("등록_해제가_매달려도_안쪽_타임아웃이_끊는다")
    void 등록_해제가_매달려도_안쪽_타임아웃이_끊는다() throws InterruptedException {
        // 여기서 붙들리면 컨테이너가 다음 단계로 못 넘어가고, 오케스트레이터가
        // 강제 종료하면 진행 중인 요청까지 함께 끊긴다. 못 지운 항목은 임계가
        // 지나면 알아서 빠진다.
        //
        // **벽시계로 재지 않는다.** 구독이 실제로 걸린 것을 래치로 확인한 뒤에야
        // 가상 시간을 당긴다 — 당기는 순간 아직 구독 전이면 타임아웃이 안 걸려
        // 시험이 흔들린다.
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        VirtualTimeScheduler 해제타이머 = VirtualTimeScheduler.create();
        CountDownLatch 구독됨 = new CountDownLatch(1);
        CountDownLatch 끝남 = new CountDownLatch(1);
        AtomicBoolean 취소됨 = new AtomicBoolean();
        GatewayHeartbeatLoop loop = GatewayHeartbeatLoop.of(
                () -> Mono.just(1),
                () -> Mono.<Void>never()
                        .doOnSubscribe(sub -> 구독됨.countDown())
                        .doOnCancel(() -> 취소됨.set(true)),
                n -> { }, INTERVAL, LEAVE_TIMEOUT, 해제타이머);
        loop.start(가상);

        loop.stop(끝남::countDown);
        assertThat(구독됨.await(5, TimeUnit.SECONDS)).isTrue();
        해제타이머.advanceTimeBy(LEAVE_TIMEOUT.plusMillis(1));

        assertThat(끝남.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(취소됨).isTrue();
        assertThat(loop.isRunning()).isFalse();
    }

    @Test
    @DisplayName("이미_멈춘_뒤에도_콜백은_온다")
    void 이미_멈춘_뒤에도_콜백은_온다() {
        // **콜백을 안 부르면 컨테이너가 종료 타임아웃까지 기다린다.** 재진입
        // 가드에 걸려 일찍 반환할 때가 정확히 그 자리다.
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        AtomicInteger 콜백 = new AtomicInteger();
        GatewayHeartbeatLoop loop = GatewayHeartbeatLoop.of(
                () -> Mono.just(1), () -> Mono.empty(), n -> { },
                INTERVAL, LEAVE_TIMEOUT, 해제타이머());
        loop.start(가상);

        loop.stop(콜백::incrementAndGet);
        loop.stop(콜백::incrementAndGet);

        assertThat(콜백.get()).isEqualTo(2);
    }

    private static VirtualTimeScheduler 해제타이머() {
        return VirtualTimeScheduler.create();
    }

    @Test
    @DisplayName("등록_해제가_터져도_종료가_끝난다")
    void 등록_해제가_터져도_종료가_끝난다() {
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        GatewayHeartbeatLoop loop = GatewayHeartbeatLoop.of(
                () -> Mono.just(1),
                () -> Mono.error(new IllegalStateException("레디스가 끊겼다")),
                n -> { }, INTERVAL, LEAVE_TIMEOUT);
        loop.start(가상);

        loop.stop();

        assertThat(loop.isRunning()).isFalse();
    }

    @Test
    @DisplayName("이미_멈춘_뒤_또_멈춰도_해제를_두_번_안_한다")
    void 이미_멈춘_뒤_또_멈춰도_해제를_두_번_안_한다() {
        // 스프링이 stop 을 두 번 부를 수 있다. 두 번 지우면 그 사이 다시 뜬
        // 같은 이름의 노드를 지운다.
        AtomicInteger 해제 = new AtomicInteger();
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        GatewayHeartbeatLoop loop = GatewayHeartbeatLoop.of(
                () -> Mono.just(1),
                () -> Mono.fromRunnable(해제::incrementAndGet),
                n -> { }, INTERVAL, LEAVE_TIMEOUT);
        loop.start(가상);

        loop.stop();
        loop.stop();

        assertThat(해제.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("한_판이_안_끝나도_다음_판이_돈다")
    void 한_판이_안_끝나도_다음_판이_돈다() {
        // **타임아웃이 없으면 루프가 조용히 멎는다.** 한 판이 안 끝나면 다음
        // 지연이 시작되지 않는데, 오류가 아니라 무응답이라 로그도 안 나온다.
        // 그동안 이 노드는 하트비트를 못 쓰면서 요청은 계속 받는다.
        AtomicInteger 호출 = new AtomicInteger();
        AtomicReference<Integer> 마지막 = new AtomicReference<>();
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        GatewayHeartbeatLoop loop = GatewayHeartbeatLoop.of(
                () -> 호출.incrementAndGet() == 1 ? Mono.never() : Mono.just(4),
                () -> Mono.empty(), 마지막::set, INTERVAL, LEAVE_TIMEOUT);

        loop.start(가상);
        try {
            가상.advanceTimeBy(INTERVAL.multipliedBy(4));
            assertThat(마지막.get()).isEqualTo(4);
        } finally {
            loop.stop();
        }
    }

    @Test
    @DisplayName("인자_없는_start_도_루프를_띄운다")
    void 인자_없는_start_도_루프를_띄운다() {
        // **스프링이 부르는 것은 이쪽이다.** 시험이 스케줄러를 주는 쪽만 부르면,
        // 이 메서드를 통째로 비워도 전부 초록이다 — 프로덕션에서 하트비트가
        // 아예 안 도는 회귀를 못 잡는다.
        AtomicInteger 호출 = new AtomicInteger();
        GatewayHeartbeatLoop loop = GatewayHeartbeatLoop.of(
                () -> Mono.fromCallable(() -> { 호출.incrementAndGet(); return 1; }),
                () -> Mono.empty(), n -> { }, INTERVAL, LEAVE_TIMEOUT);

        loop.start();
        try {
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> 호출.get() > 0);
            assertThat(loop.isRunning()).isTrue();
        } finally {
            loop.stop();
        }
    }

    @Test
    @DisplayName("돌고_있으면_running_이_참이다")
    void 돌고_있으면_running_이_참이다() {
        // **거짓만 단언하면 항상 false 를 돌려줘도 통과한다.** 그러면 스프링이
        // stop 을 아예 안 불러 등록 해제가 통째로 사라진다.
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        GatewayHeartbeatLoop loop = GatewayHeartbeatLoop.of(
                () -> Mono.just(1), () -> Mono.empty(), n -> { }, INTERVAL, LEAVE_TIMEOUT);

        loop.start(가상);
        try {
            assertThat(loop.isRunning()).isTrue();
        } finally {
            loop.stop();
        }
    }

    @Test
    @DisplayName("두_번_시작해도_스레드를_새로_안_만든다")
    void 두_번_시작해도_스레드를_새로_안_만든다() {
        // 스케줄러를 먼저 만들면 두 번째 호출이 새 스레드를 만들고 참조를
        // 덮어쓴 뒤 반환해, 원래 스레드가 영영 산다.
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        GatewayHeartbeatLoop loop = GatewayHeartbeatLoop.of(
                () -> Mono.just(1), () -> Mono.empty(), n -> { }, INTERVAL, LEAVE_TIMEOUT);

        loop.start(가상);
        loop.start(가상);

        assertThat(loop.isRunning()).isTrue();
        loop.stop();
        assertThat(loop.isRunning()).isFalse();
    }

    @Test
    @DisplayName("커넥션이_닫히기_전에_멈추는_단계다")
    void 커넥션이_닫히기_전에_멈추는_단계다() {
        // **스프링은 phase 를 내림차순으로 멈춘다.** 레디스 커넥션 팩토리가
        // 0 이므로 그보다 커야 커넥션이 살아 있는 동안 등록을 뺄 수 있다.
        // 0 이하면 해제가 매번 실패해 유령 항목이 분모를 부풀린다.
        GatewayHeartbeatLoop loop = GatewayHeartbeatLoop.of(
                () -> Mono.just(1), () -> Mono.empty(), n -> { }, INTERVAL, LEAVE_TIMEOUT);

        assertThat(loop).isInstanceOf(SmartLifecycle.class);
        assertThat(loop.isAutoStartup()).isTrue();
        assertThat(loop.getPhase()).isGreaterThan(0);
    }
}
