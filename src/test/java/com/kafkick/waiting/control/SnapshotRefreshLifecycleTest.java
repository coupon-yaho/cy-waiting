package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * 판정 재료 갱신 루프를 켜고 끈다.
 *
 * <p><b>멈추기 전에 드레이닝을 알린다.</b> 순서가 반대면 부하 분산기가 아직
 * 보내는 동안 재료가 늙기 시작하고, 살아 있음 판정이 그걸 정지로 세어 진행 중인
 * 요청을 든 파드를 죽인다.
 */
class SnapshotRefreshLifecycleTest {

    private static final Duration INTERVAL = Duration.ofMillis(50);
    private static final Duration WAIT = Duration.ofSeconds(5);

    private final SnapshotHolder holder = SnapshotHolder.of(
            Duration.ofSeconds(3), Duration.ofSeconds(5), Clock.systemUTC());
    private final ShutdownState shutdown = ShutdownState.create();
    private final AtomicInteger 받아옴 = new AtomicInteger();

    private SnapshotRefreshLifecycle lifecycle() {
        SnapshotRefresher refresher = SnapshotRefresher.of(holder, () -> {
            받아옴.incrementAndGet();
            return Mono.just(Map.<String, String>of());
        });
        return SnapshotRefreshLifecycle.of(refresher, shutdown, INTERVAL);
    }

    @Test
    @DisplayName("시작하면_재료를_받아_온다")
    void 시작하면_재료를_받아_온다() {
        // 이게 없으면 홀더가 영원히 비고, 받는 판정이 영구히 거절한다.
        SnapshotRefreshLifecycle lifecycle = lifecycle();

        lifecycle.start();

        try {
            await().atMost(WAIT).untilAsserted(() -> assertThat(받아옴).hasValueGreaterThan(1));
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
            await().atMost(WAIT).untilAsserted(() -> assertThat(받아옴).hasValueGreaterThan(2));
            int 잰_값 = 받아옴.get();
            // 두 줄기면 같은 시간에 두 배로 는다. 한 줄기면 그 절반 언저리다.
            await().pollDelay(INTERVAL.multipliedBy(4)).atMost(WAIT)
                    .untilAsserted(() -> assertThat(받아옴).hasValueGreaterThan(잰_값));
        } finally {
            lifecycle.stop();
        }
    }

    @Test
    @DisplayName("멈추면_더_안_받아_온다")
    void 멈추면_더_안_받아_온다() {
        SnapshotRefreshLifecycle lifecycle = lifecycle();
        lifecycle.start();
        await().atMost(WAIT).untilAsserted(() -> assertThat(받아옴).hasValueGreaterThan(1));

        lifecycle.stop();
        int 멈춘_뒤 = 받아옴.get();

        await().pollDelay(INTERVAL.multipliedBy(6)).atMost(WAIT)
                .untilAsserted(() -> assertThat(받아옴).hasValueLessThanOrEqualTo(멈춘_뒤 + 1));
        assertThat(lifecycle.isRunning()).isFalse();
    }

    @Test
    @DisplayName("멈추면_드레이닝을_알린다")
    void 멈추면_드레이닝을_알린다() {
        // 안 알리면 부하 분산기가 계속 보내고, 그 사이 도착한 요청이 끊긴다.
        SnapshotRefreshLifecycle lifecycle = lifecycle();
        lifecycle.start();

        lifecycle.stop();

        assertThat(shutdown.isDraining()).isTrue();
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

    @Test
    @DisplayName("웹_서버보다_먼저_종료_신호를_받는다")
    void 웹_서버보다_먼저_종료_신호를_받는다() {
        // 컨테이너는 단계가 큰 것부터 멈춘다. 웹 서버보다 커야 종료 신호를 먼저
        // 받아 부하 분산기가 뺄 시간을 번다. 상수가 아니라 그 관계를 못박는다.
        // 웹 서버는 가장 큰 값 바로 아래에 선다.
        int 웹_서버 = Integer.MAX_VALUE - 1;
        int 커넥션_팩토리 = 0;

        assertThat(lifecycle().getPhase())
                .isGreaterThan(웹_서버)
                .isGreaterThan(커넥션_팩토리);
    }

    @Test
    @DisplayName("주기가_잘못되면_안_뜬다")
    void 주기가_잘못되면_안_뜬다() {
        SnapshotRefresher refresher = SnapshotRefresher.of(holder, () -> Mono.just(Map.of()));

        assertThatThrownBy(() -> SnapshotRefreshLifecycle.of(refresher, shutdown, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("interval");
    }
}
