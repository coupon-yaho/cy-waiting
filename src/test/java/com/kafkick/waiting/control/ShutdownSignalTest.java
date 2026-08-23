package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import reactor.core.publisher.Mono;

/**
 * 종료 알림이 <b>다른 리스너에 선점되지 않는가.</b>
 *
 * <p>알림이 빠지면 살아 있음 판정이 몇 초 뒤 파드를 정지로 세고, 진행 중인
 * 요청을 든 채로 강제 종료된다 — 드레이닝이 막으려던 바로 그 일이다.
 */
@Tag("context")
class ShutdownSignalTest {

    /** 먼저 서서 터지는 리스너. 컨테이너는 경고만 찍고 다음으로 넘어간다. */
    @Configuration
    static class ThrowingListenerWiring {

        @Bean
        ShutdownState shutdownState() {
            return ShutdownState.create();
        }

        @Bean
        SnapshotRefreshLifecycle snapshotRefreshLifecycle(ShutdownState shutdown) {
            SnapshotHolder holder = SnapshotHolder.of(
                    Duration.ofSeconds(3), Duration.ofSeconds(10), Clock.systemUTC());
            return SnapshotRefreshLifecycle.of(
                    SnapshotRefresher.of(holder, () -> Mono.just(Map.of())),
                    shutdown, Duration.ofMillis(50));
        }

        @Bean
        ThrowsFirst throwsFirst() {
            return new ThrowsFirst();
        }
    }

    /**
     * <b>실제로 앞세운다.</b> 람다로 두면 순서 표시가 안 붙어 등록 순서로
     * 밀리고, 그러면 위험을 안 만든 채로 초록인 시험이 된다.
     */
    @Order(Ordered.HIGHEST_PRECEDENCE)
    static class ThrowsFirst implements ApplicationListener<ContextClosedEvent> {

        @Override
        public void onApplicationEvent(ContextClosedEvent event) {
            throw new IllegalStateException("종료 중에 터진 리스너");
        }
    }

    @Test
    @DisplayName("앞선_리스너가_터져도_드레이닝을_알린다")
    void 앞선_리스너가_터져도_드레이닝을_알린다() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext(ThrowingListenerWiring.class);
        ShutdownState shutdown = context.getBean(ShutdownState.class);

        context.close();

        assertThat(shutdown.isDraining()).isTrue();
    }
}
