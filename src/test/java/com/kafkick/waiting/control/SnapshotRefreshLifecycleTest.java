package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * 판정 재료 갱신 루프를 켜고 끈다.
 *
 * <p><b>종료와 정지를 가른다.</b> 정지는 잠깐 멈출 때도 불리므로, 거기서 종료를
 * 알리면 그 컨텍스트가 다시 못 살아난다. 반대로 종료에서 안 알리면 살아 있음
 * 판정이 진행 중인 요청을 든 파드를 죽인다.
 */
class SnapshotRefreshLifecycleTest {

    private static final Duration INTERVAL = Duration.ofMillis(50);
    /**
     * <b>가상 시계로 잰다.</b> 실제로 기다리면 이 시험만 장비 속도에 걸리고,
     * 관용치로 흔들림을 덮게 된다 (TS-4). 주기를 늘려도 통과하는 시험이 남는다.
     */
    private VirtualTimeScheduler 시계 = VirtualTimeScheduler.create();

    /** 가상 시계는 스케줄러만 가상화한다. 홀더가 찍는 시각도 같이 못 박는다 (TS-4). */
    private final Clock 시계값 = Clock.fixed(
            Instant.ofEpochSecond(1_800_000_000L), ZoneOffset.UTC);

    private final SnapshotHolder holder = SnapshotHolder.of(
            Duration.ofSeconds(3), Duration.ofSeconds(5), 시계값);
    private final ShutdownState shutdown = ShutdownState.create();
    private final AtomicInteger 받아옴 = new AtomicInteger();

    private SnapshotRefreshLifecycle lifecycle() {
        SnapshotRefresher refresher = SnapshotRefresher.of(holder, () -> {
            받아옴.incrementAndGet();
            return Mono.just(Map.<String, String>of());
        }, 시계값);
        // 멈췄다 다시 켜면 스케줄러를 새로 받는다. 버린 것을 다시 쓰면 루프가 죽는다.
        return SnapshotRefreshLifecycle.of(refresher, shutdown, INTERVAL,
                () -> 시계 = VirtualTimeScheduler.create(), 대기());
    }

    /** 실제로 안 잔다. 자면 이 시험만 장비 속도에 걸린다 (TS-4). */
    private DrainWait 대기() {
        return DrainWait.of(shutdown, Duration.ofSeconds(6), ms -> { });
    }

    /** 가상 시계를 이만큼 민다. 판이 도는 것은 여기서만 일어난다. */
    private void 판을_돌린다(int 판수) {
        시계.advanceTimeBy(INTERVAL.multipliedBy(판수));
    }

    @Test
    @DisplayName("시작하면_재료를_받아_온다")
    void 시작하면_재료를_받아_온다() {
        // 이게 없으면 홀더가 영원히 비고, 받는 판정이 영구히 거절한다.
        SnapshotRefreshLifecycle lifecycle = lifecycle();

        lifecycle.start();

        try {
            판을_돌린다(3);
            assertThat(받아옴).hasValue(4);
            assertThat(lifecycle.isRunning()).isTrue();
        } finally {
            lifecycle.stop();
        }
    }

    @Test
    @DisplayName("두_번_시작해도_한_줄기만_돈다")
    void 두_번_시작해도_한_줄기만_돈다() {
        SnapshotRefreshLifecycle lifecycle = lifecycle();

        lifecycle.start();
        lifecycle.start();

        try {
            판을_돌린다(3);

            // **두 줄기면 정확히 두 배다.** 가상 시계라 "언저리" 가 없다.
            assertThat(받아옴).hasValue(4);
        } finally {
            lifecycle.stop();
        }
    }

    @Test
    @DisplayName("멈추면_더_안_받아_온다")
    void 멈추면_더_안_받아_온다() {
        SnapshotRefreshLifecycle lifecycle = lifecycle();
        lifecycle.start();
        판을_돌린다(2);

        lifecycle.stop();
        int 멈춘_뒤 = 받아옴.get();

        판을_돌린다(6);
        assertThat(받아옴).hasValue(멈춘_뒤);
        assertThat(lifecycle.isRunning()).isFalse();
    }

    @Test
    @DisplayName("자기_컨텍스트가_닫히면_드레이닝을_알린다")
    void 자기_컨텍스트가_닫히면_드레이닝을_알린다() {
        // 안 알리면 부하 분산기가 계속 보내고, 그 사이 도착한 요청이 끊긴다.
        GenericApplicationContext 내_컨텍스트 = new GenericApplicationContext();
        SnapshotRefreshLifecycle lifecycle = lifecycle();
        lifecycle.setApplicationContext(내_컨텍스트);
        lifecycle.start();

        try {
            lifecycle.onApplicationEvent(new ContextClosedEvent(내_컨텍스트));
        } finally {
            lifecycle.stop();
        }

        assertThat(shutdown.isDraining()).isTrue();
    }

    @Test
    @DisplayName("남의_컨텍스트가_닫힌_것으로는_안_알린다")
    void 남의_컨텍스트가_닫힌_것으로는_안_알린다() {
        // **하위 컨텍스트의 닫힘도 위로 전해진다.** 관리 포트를 따로 열면 하위가
        // 실제로 생기는데, 그게 닫혔다고 서비스가 종료하는 것은 아니다.
        SnapshotRefreshLifecycle lifecycle = lifecycle();
        lifecycle.setApplicationContext(new GenericApplicationContext());
        lifecycle.start();

        try {
            lifecycle.onApplicationEvent(new ContextClosedEvent(new GenericApplicationContext()));
        } finally {
            lifecycle.stop();
        }

        assertThat(shutdown.isDraining()).isFalse();
    }

    @Test
    @DisplayName("잠깐_멈춘_것으로는_안_알린다")
    void 잠깐_멈춘_것으로는_안_알린다() {
        // **정지는 종료가 아니다.** 프레임워크가 안 쓰는 컨텍스트를 멈췄다 다시
        // 켜는데, 정지에서 알리면 그 컨텍스트가 영영 못 살아난다.
        SnapshotRefreshLifecycle lifecycle = lifecycle();
        lifecycle.start();

        lifecycle.stop();

        assertThat(shutdown.isDraining()).isFalse();
    }

    @Test
    @DisplayName("멈췄다_다시_켜면_다시_받아_온다")
    void 멈췄다_다시_켜면_다시_받아_온다() {
        // **깃발이 아니라 받아오는 것을 본다.** 시작은 구독보다 먼저 깃발을
        // 세우므로, 깃발만 보면 버린 스케줄러를 다시 써서 루프가 죽어도 초록이다.
        SnapshotRefreshLifecycle lifecycle = lifecycle();
        lifecycle.start();
        판을_돌린다(2);
        lifecycle.stop();
        int 멈춘_뒤 = 받아옴.get();

        lifecycle.start();

        try {
            판을_돌린다(2);
            assertThat(받아옴).hasValue(멈춘_뒤 + 3);
        } finally {
            lifecycle.stop();
        }
    }

    @Test
    @DisplayName("두_번_멈춰도_탈이_없다")
    void 두_번_멈춰도_탈이_없다() {
        SnapshotRefreshLifecycle lifecycle = lifecycle();
        lifecycle.start();

        lifecycle.stop();
        lifecycle.stop();

        assertThat(lifecycle.isRunning()).isFalse();
    }

    @Test
    @DisplayName("시작한_적_없어도_멈출_수_있다")
    void 시작한_적_없어도_멈출_수_있다() {
        SnapshotRefreshLifecycle lifecycle = lifecycle();

        lifecycle.stop();

        assertThat(lifecycle.isRunning()).isFalse();
    }

    /**
     * <b>드레이닝이 끝날 때까지 재료가 신선해야 한다.</b> 먼저 멎으면 진행 중인
     * 요청이 늙은 스냅샷으로 판정된다 — 줄 없는 쿠폰은 fail-open 예산을 태우다
     * 503 이 되고, 미지 쿠폰은 상한 없이 뒷단으로 간다. 배포마다 열린다.
     */
    @Test
    @DisplayName("웹_서버가_드레이닝을_끝낸_뒤에_멎는다")
    void 웹_서버가_드레이닝을_끝낸_뒤에_멎는다() {
        // 컨테이너는 단계가 큰 것부터 멈춘다. 작아야 나중에 멎는다.
        // **상수를 손으로 적지 않는다.** 프레임워크가 값을 바꾸면 시험이 같이
        // 움직여야 하고, 틀린 값을 적으면 다음 사람이 그걸 믿는다.
        int 웹_서버_종료 = WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE;
        int 커넥션_팩토리 = 0;

        assertThat(lifecycle().getPhase())
                .isLessThan(웹_서버_종료)
                .isGreaterThan(커넥션_팩토리);
    }

    @Test
    @DisplayName("주기가_잘못되면_안_뜬다")
    void 주기가_잘못되면_안_뜬다() {
        SnapshotRefresher refresher =
                SnapshotRefresher.of(holder, () -> Mono.just(Map.of()), 시계값);

        assertThatThrownBy(() -> SnapshotRefreshLifecycle.of(
                refresher, shutdown, Duration.ZERO, 대기()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("interval");
    }

    /**
     * <b>못 뜨면 깃발도 되돌려야 한다.</b> 안 되돌리면 다음 {@code start()} 가
     * 즉시 돌아가고, 루프는 안 도는데 도는 것처럼 보이는 상태로 굳는다.
     */
    @Test
    @DisplayName("스케줄러를_못_만들면_다시_시작할_수_있다")
    void 스케줄러를_못_만들면_다시_시작할_수_있다() {
        AtomicBoolean 터뜨린다 = new AtomicBoolean(true);
        SnapshotRefresher refresher = SnapshotRefresher.of(holder, () -> {
            받아옴.incrementAndGet();
            return Mono.just(Map.<String, String>of());
        }, 시계값);
        SnapshotRefreshLifecycle lifecycle = SnapshotRefreshLifecycle.of(
                refresher, shutdown, INTERVAL,
                () -> {
                    if (터뜨린다.get()) {
                        throw new IllegalStateException("스케줄러를 못 만든다");
                    }
                    return 시계 = VirtualTimeScheduler.create();
                }, 대기());

        assertThatThrownBy(lifecycle::start).isInstanceOf(IllegalStateException.class);
        assertThat(lifecycle.isRunning()).isFalse();

        터뜨린다.set(false);
        lifecycle.start();

        try {
            판을_돌린다(2);
            assertThat(받아옴).hasValue(3);
        } finally {
            lifecycle.stop();
        }
    }
}
