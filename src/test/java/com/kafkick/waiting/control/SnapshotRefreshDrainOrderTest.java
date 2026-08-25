package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThan;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

/**
 * 종료 <b>순서</b>를 행위로 잰다.
 *
 * <p>단계 비교만으로는 부족하다. 컨테이너는 의존하는 빈을 단계와 무관하게 먼저
 * 멈추므로, 나중에 누가 이 빈을 웹 서버 쪽에 의존시키면 순서가 뒤집히는데 단계
 * 단언은 그대로 초록이다.
 */
@Tag("context")
class SnapshotRefreshDrainOrderTest {

    /** 드레이닝을 흉내내는 창. 이 안에서 루프가 몇 판 도는지를 본다. */
    private static final Duration 드레이닝 = Duration.ofSeconds(2);

    private static final Duration 주기 = Duration.ofMillis(20);

    private static final AtomicInteger 받아옴 = new AtomicInteger();

    private static final AtomicBoolean 드레이닝_시작에_살아있었나 = new AtomicBoolean();

    private static final AtomicInteger 드레이닝_동안_받아옴 = new AtomicInteger();

    /**
     * <b>가짜 웹 서버가 갱신 루프를 주입받으면 안 된다.</b> 의존 간선이 생기면
     * 컨테이너가 단계와 무관하게 그것을 먼저 멈춰 줘서, 단계를 잘못 둬도 통과하는
     * 시험이 된다. 그래서 정적 자리로 건넨다.
     */
    private static volatile SnapshotRefreshLifecycle 관찰_대상;

    @Configuration
    static class Wiring {

        @Bean
        SnapshotHolder holder() {
            return SnapshotHolder.of(Duration.ofSeconds(3), Duration.ofSeconds(10),
                    Clock.systemUTC());
        }

        @Bean
        SnapshotRefreshLifecycle 갱신(SnapshotHolder holder) {
            SnapshotRefresher refresher = SnapshotRefresher.of(holder, () -> {
                받아옴.incrementAndGet();
                return Mono.just(Map.of());
            });
            관찰_대상 = SnapshotRefreshLifecycle.of(refresher, ShutdownState.create(), 주기);
            return 관찰_대상;
        }

        @Bean
        SmartLifecycle 가짜_웹서버() {
            return new FakeWebServer();
        }
    }

    /** 웹 서버 종료 단계에 서서 드레이닝을 흉내낸다. */
    static final class FakeWebServer implements SmartLifecycle {

        /** 실제 웹 서버는 기동 전·정지 후에 안 돈다. 항상 참이면 못 만드는 상태다. */
        private final AtomicBoolean running = new AtomicBoolean();

        @Override
        public void start() {
            running.set(true);
        }

        @Override
        public void stop() {
            드레이닝_시작에_살아있었나.set(관찰_대상.isRunning());
            int 시작 = 받아옴.get();
            // **고정 대기가 아니라 판 수를 기다린다.** 이 시험이 재려는 것은
            // 몇 밀리초가 아니라 그동안 루프가 일했는가다. 루프가 이미 죽었으면
            // 여기서 못 기다리고 나가는데, 판정은 아래 단언이 한다.
            try {
                await().atMost(드레이닝).untilAtomic(받아옴, greaterThan(시작 + 3));
            } catch (ConditionTimeoutException e) {
                // 컨테이너 종료를 여기서 끊지 않는다.
            }
            드레이닝_동안_받아옴.set(받아옴.get() - 시작);
            running.set(false);
        }

        @Override
        public boolean isRunning() {
            return running.get();
        }

        @Override
        public int getPhase() {
            return WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE;
        }
    }

    @Test
    @DisplayName("드레이닝_동안_재료가_계속_갱신된다")
    void 드레이닝_동안_재료가_계속_갱신된다() {
        try (AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext(Wiring.class)) {
            context.getBean(SnapshotRefreshLifecycle.class);
            // 몇 판은 돌게 둔다. 안 그러면 시작 자체를 못 재고 통과한다.
            await().atMost(Duration.ofSeconds(5)).untilAtomic(받아옴, greaterThan(2));

            context.close();
        }

        assertThat(드레이닝_시작에_살아있었나).isTrue();
        // **드레이닝 동안 실제로 일했는가.** 살아 있기만 하고 안 돌면 뜻이 없다.
        assertThat(드레이닝_동안_받아옴).hasValueGreaterThan(3);
    }
}
