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
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import reactor.core.publisher.Mono;

/**
 * 판정 재료 갱신 루프를 켜고 끈다.
 *
 * <p><b>종료와 정지를 가른다.</b> 정지는 잠깐 멈출 때도 불리므로, 거기서 종료를
 * 알리면 그 컨텍스트가 다시 못 살아난다. 반대로 종료에서 안 알리면 살아 있음
 * 판정이 진행 중인 요청을 든 파드를 죽인다.
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
        await().atMost(WAIT).untilAsserted(() -> assertThat(받아옴).hasValueGreaterThan(1));
        lifecycle.stop();
        int 멈춘_뒤 = 받아옴.get();

        lifecycle.start();

        try {
            await().atMost(WAIT)
                    .untilAsserted(() -> assertThat(받아옴).hasValueGreaterThan(멈춘_뒤 + 1));
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
        SnapshotRefresher refresher = SnapshotRefresher.of(holder, () -> Mono.just(Map.of()));

        assertThatThrownBy(() -> SnapshotRefreshLifecycle.of(refresher, shutdown, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("interval");
    }
}
