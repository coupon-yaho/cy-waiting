package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * 제어 평면을 켜고 끈다.
 *
 * <p><b>순서가 전부다.</b> 커넥션이 닫힌 뒤에 락을 놓으려 하면 매번 실패하고,
 * 그러면 다음 리더가 리스 만료를 기다린다. 웹 서버가 빠진 뒤·커넥션이 닫히기
 * 전, 그 사이가 유일한 창이다.
 */
class ControlPlaneLifecycleTest {

    private ListAppender<ILoggingEvent> 로그;

    @BeforeEach
    void 로그를_받는다() {
        로그 = new ListAppender<>();
        로그.start();
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ControlPlaneLifecycle.class))
                .addAppender(로그);
    }

    @AfterEach
    void 로그를_뗀다() {
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ControlPlaneLifecycle.class))
                .detachAppender(로그);
    }

    private static final Duration DELAY = Duration.ofMillis(100);
    private static final Duration TICK = Duration.ofSeconds(1);

    private final AtomicInteger 연장 = new AtomicInteger();
    private final AtomicInteger 배분 = new AtomicInteger();
    private final AtomicInteger 해제 = new AtomicInteger();

    private ControlPlaneLifecycle lifecycle(VirtualTimeScheduler timer) {
        LeadershipLoop renew = LeadershipLoop.of(DELAY,
                () -> Mono.fromRunnable(연장::incrementAndGet), timer);
        AllocationScheduler allocation = AllocationScheduler.of(TICK, Duration.ZERO,
                () -> true, () -> Mono.fromRunnable(배분::incrementAndGet), nanos -> { }, timer);
        return ControlPlaneLifecycle.of(renew, allocation,
                () -> Mono.fromRunnable(해제::incrementAndGet), Duration.ofSeconds(1), timer);
    }

    @Test
    @DisplayName("시작하면_둘_다_돈다")
    void 시작하면_둘_다_돈다() {
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        ControlPlaneLifecycle lifecycle = lifecycle(timer);

        lifecycle.start();
        timer.advanceTimeBy(TICK);

        assertThat(연장).hasValue(11);
        assertThat(배분).hasValue(2);
        assertThat(lifecycle.isRunning()).isTrue();
        lifecycle.stop(() -> { });
    }

    @Test
    @DisplayName("끝나면_락을_놓는다")
    void 끝나면_락을_놓는다() {
        // 안 놓으면 다음 리더가 리스 만료를 기다린다. 배포마다 그만큼 배분이 빈다.
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        ControlPlaneLifecycle lifecycle = lifecycle(timer);
        lifecycle.start();

        AtomicInteger 콜백 = new AtomicInteger();
        lifecycle.stop(콜백::incrementAndGet);
        lifecycle.destroy();

        assertThat(해제).hasValue(1);
        assertThat(콜백).hasValue(1);
        assertThat(lifecycle.isRunning()).isFalse();
    }

    @Test
    @DisplayName("락을_놓기_전에_연장을_멈춘다")
    void 락을_놓기_전에_연장을_멈춘다() {
        // 순서가 뒤바뀌면 방금 비운 락을 죽는 노드가 다시 잡는다. 그러면 놓은
        // 이득이 통째로 사라진다.
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        ControlPlaneLifecycle lifecycle = lifecycle(timer);
        lifecycle.start();
        int 멈추기_직전 = 연장.get();

        int 배분_직전 = 배분.get();
        lifecycle.stop(() -> { });
        timer.advanceTimeBy(TICK.multipliedBy(5));

        assertThat(연장).hasValue(멈추기_직전);
        // **배분도 같이 멎어야 한다.** 락은 놓았는데 루프가 계속 돌면 다음
        // 리더와 같은 판을 겹쳐 쓴다.
        assertThat(배분).hasValue(배분_직전);
    }

    @Test
    @DisplayName("두_번_멈춰도_콜백을_부른다")
    void 두_번_멈춰도_콜백을_부른다() {
        // 컨테이너가 이걸 기다린다. 안 부르면 종료가 그 자리에서 멎는다.
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        ControlPlaneLifecycle lifecycle = lifecycle(timer);
        lifecycle.start();
        AtomicInteger 콜백 = new AtomicInteger();

        lifecycle.stop(콜백::incrementAndGet);
        lifecycle.stop(콜백::incrementAndGet);

        assertThat(콜백).hasValue(2);
    }

    @Test
    @DisplayName("시작한_적_없어도_콜백을_부른다")
    void 시작한_적_없어도_콜백을_부른다() {
        AtomicInteger 콜백 = new AtomicInteger();

        lifecycle(VirtualTimeScheduler.create()).stop(콜백::incrementAndGet);

        assertThat(콜백).hasValue(1);
    }

    @Test
    @DisplayName("인자_없는_정지도_같이_멎는다")
    void 인자_없는_정지도_같이_멎는다() {
        // 컨테이너가 이 쪽을 부를 수도 있다. 여기만 안 멈추면 종료 뒤에 계속 돈다.
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        ControlPlaneLifecycle lifecycle = lifecycle(timer);
        lifecycle.start();
        int 배분_직전 = 배분.get();

        lifecycle.stop();
        timer.advanceTimeBy(TICK.multipliedBy(5));

        assertThat(lifecycle.isRunning()).isFalse();
        assertThat(배분).hasValue(배분_직전);
    }

    @Test
    @DisplayName("해제가_실패해도_종료를_안_붙든다")
    void 해제가_실패해도_종료를_안_붙든다() {
        // 못 놓아도 리스 만료로 풀린다. 반대로 붙들면 오케스트레이터가 강제로
        // 끊고, 그때는 진행 중인 요청까지 함께 끊긴다.
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        ControlPlaneLifecycle lifecycle = ControlPlaneLifecycle.of(
                LeadershipLoop.of(DELAY, Mono::empty, timer),
                AllocationScheduler.of(TICK, Duration.ZERO, () -> true, Mono::empty,
                        nanos -> { }, timer),
                () -> Mono.error(new IllegalStateException("끊겼다")), Duration.ofSeconds(1), timer);
        lifecycle.start();

        AtomicInteger 콜백 = new AtomicInteger();
        lifecycle.stop(콜백::incrementAndGet);
        lifecycle.destroy();

        assertThat(콜백).hasValue(1);
        // 왜 못 놓았는지 안 남기면, 다음 리더가 리스를 기다린 이유를 못 찾는다.
        assertThat(로그.list).anyMatch(e -> e.getLevel() == Level.WARN
                && e.getMessage().contains("리더 해제 실패"));
    }

    @Test
    @DisplayName("해제가_멈춰도_종료를_안_붙든다")
    void 해제가_멈춰도_종료를_안_붙든다() {
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        ControlPlaneLifecycle lifecycle = ControlPlaneLifecycle.of(
                LeadershipLoop.of(DELAY, Mono::empty, timer),
                AllocationScheduler.of(TICK, Duration.ZERO, () -> true, Mono::empty,
                        nanos -> { }, timer),
                Mono::never, Duration.ofSeconds(1), timer);
        lifecycle.start();

        AtomicInteger 콜백 = new AtomicInteger();
        lifecycle.stop(콜백::incrementAndGet);
        timer.advanceTimeBy(Duration.ofSeconds(2));

        assertThat(콜백).hasValue(1);
    }

    @Test
    @DisplayName("커넥션이_닫히기_전에_멈춘다")
    void 커넥션이_닫히기_전에_멈춘다() {
        // 컨테이너는 단계가 큰 것부터 멈춘다. 커넥션 팩토리가 0 이므로 그보다
        // 커야 락을 놓을 수 있고, 웹 서버보다는 작아야 요청이 먼저 빠진다.
        // 상수가 아니라 그 관계를 못박는다.
        int 커넥션_팩토리 = 0;
        // **상수를 손으로 적지 않는다.** 여기 Integer.MAX_VALUE 를 적어 뒀는데 실제
        // 값은 그보다 1024 작다 — 그래서 이 단언이 거의 아무것도 안 막았다.
        int 웹_서버 = WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE;

        assertThat(lifecycle(VirtualTimeScheduler.create()).getPhase())
                .isGreaterThan(커넥션_팩토리)
                .isLessThan(웹_서버);
    }

    /**
     * <b>일시정지는 종료가 아니다</b> (CY-794).
     *
     * <p>스프링은 캐시된 컨텍스트를 전환할 때 이전 컨텍스트를 멈춘다 —
     * {@code AbstractApplicationContext.pause()} 가 {@code SmartLifecycle.stop()}
     * 을 부른다. 그 자리에서 락을 놓으면 리더십이 종단 상태가 되어, 다시
     * 시작해도 영영 리더가 못 된다. 시험 스위트에서 무관한 시험이 리더를 못
     * 잡고 빨개지는 형태로 나타났다.
     *
     * <p>루프를 멈추는 것은 맞다. 놓는 것은 <b>정말 끝날 때</b>다.
     */
    @Test
    @DisplayName("멈췄다_다시_시작하면_안_놓는다")
    void 멈췄다_다시_시작하면_안_놓는다() {
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        ControlPlaneLifecycle lifecycle = lifecycle(timer);

        lifecycle.start();
        lifecycle.stop();
        timer.advanceTimeBy(Duration.ofSeconds(1));

        assertThat(해제).as("멈춘 것만으로는 안 놓는다").hasValue(0);

        lifecycle.start();
        timer.advanceTimeBy(TICK);
        assertThat(배분).as("다시 돈다").isNotNull();
    }

    /** 컨테이너가 빈을 버릴 때 놓는다. 그때가 정말 끝나는 자리다. */
    @Test
    @DisplayName("빈이_버려질_때_놓는다")
    void 빈이_버려질_때_놓는다() {
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        ControlPlaneLifecycle lifecycle = lifecycle(timer);

        lifecycle.start();
        lifecycle.stop();
        lifecycle.destroy();
        timer.advanceTimeBy(Duration.ofSeconds(1));

        assertThat(해제).hasValue(1);
    }

    /** 두 번 버려도 한 번만 놓는다. 두 번 놓으면 남의 락을 지울 수 있다. */
    @Test
    @DisplayName("두_번_버려도_한_번만_놓는다")
    void 두_번_버려도_한_번만_놓는다() {
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        ControlPlaneLifecycle lifecycle = lifecycle(timer);

        lifecycle.start();
        lifecycle.destroy();
        lifecycle.destroy();
        timer.advanceTimeBy(Duration.ofSeconds(1));

        assertThat(해제).hasValue(1);
    }
}
