package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.MutableClock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * 스냅샷을 주기적으로 받아 온다.
 *
 * <p><b>실패해도 들고 있던 것을 지우지 않는다.</b> 지우면 레디스가 잠깐
 * 끊긴 사이 전 노드가 판정 재료를 잃고, 그때부터 낡은 값으로 버티는 대신
 * 아무 값도 없이 버텨야 한다 — 그건 못 버티는 것이다.
 */
class SnapshotRefresherTest {

    private static final Instant 지금 = Instant.parse("2026-08-20T00:00:00Z");
    private static final Map<String, String> 정상 = Map.of(
            "#credit", "1000", "#nodes", "2", "#published", "1787184000",
            "c1", "ADAPTIVE:QUEUEING:100:500:2000:1.0");

    private static SnapshotHolder 홀더(MutableClock clock) {
        return SnapshotHolder.of(Duration.ofSeconds(2), Duration.ofSeconds(5), clock);
    }

    @Test
    @DisplayName("한_번_돌면_홀더가_갱신된다")
    void 한_번_돌면_홀더가_갱신된다() {
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        SnapshotRefresher refresher = SnapshotRefresher.of(holder, () -> Mono.just(정상));

        StepVerifier.create(refresher.한번()).verifyComplete();

        assertThat(holder.current().coupons()).containsOnlyKeys("c1");
        assertThat(holder.isFetchStale()).isFalse();
    }

    @Test
    @DisplayName("실패해도_들고_있던_것을_지우지_않는다")
    void 실패해도_들고_있던_것을_지우지_않는다() {
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        AtomicInteger 호출 = new AtomicInteger();
        SnapshotRefresher refresher = SnapshotRefresher.of(holder,
                () -> 호출.incrementAndGet() == 1
                        ? Mono.just(정상)
                        : Mono.error(new IllegalStateException("레디스가 끊겼다")));

        StepVerifier.create(refresher.한번()).verifyComplete();
        StepVerifier.create(refresher.한번()).verifyComplete();

        // 판정 재료는 그대로다. 낡았을 뿐이다 — 그건 홀더가 알린다.
        assertThat(holder.current().coupons()).containsOnlyKeys("c1");
    }

    @Test
    @DisplayName("실패는_루프를_멈추지_않는다")
    void 실패는_루프를_멈추지_않는다() {
        // 한 번 멎으면 영영 멎는다. 여기서 예외를 흘리면 그 일이 난다.
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        AtomicInteger 호출 = new AtomicInteger();
        SnapshotRefresher refresher = SnapshotRefresher.of(holder, () -> {
            int n = 호출.incrementAndGet();
            return n == 1 ? Mono.error(new IllegalStateException("첫 판이 실패")) : Mono.just(정상);
        });

        StepVerifier.create(refresher.한번()).verifyComplete();
        StepVerifier.create(refresher.한번()).verifyComplete();

        assertThat(호출).hasValue(2);
        assertThat(holder.current().coupons()).containsOnlyKeys("c1");
    }

    @Test
    @DisplayName("실패해도_수신_시각을_새로_찍지_않는다")
    void 실패해도_수신_시각을_새로_찍지_않는다() {
        // 실패를 갱신으로 세면 fetchStale 이 영영 안 뜬다 — 루프가 멎었는데
        // 이 노드는 멀쩡하다고 답하고, LB 가 안 빼 준다.
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        SnapshotRefresher refresher = SnapshotRefresher.of(holder,
                () -> Mono.error(new IllegalStateException("계속 실패")));

        StepVerifier.create(refresher.한번()).verifyComplete();
        clock.앞으로(Duration.ofSeconds(3));
        StepVerifier.create(refresher.한번()).verifyComplete();

        assertThat(holder.isFetchStale()).isTrue();
    }

    @Test
    @DisplayName("정해진_시간을_넘으면_그_판을_포기한다")
    void 정해진_시간을_넘으면_그_판을_포기한다() {
        // 한 판이 매달리면 다음 판이 못 돈다. 포기가 곧 다음 기회다.
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        SnapshotRefresher refresher = SnapshotRefresher.of(holder, Mono::never,
                Duration.ofMillis(50));

        StepVerifier.create(refresher.한번()).verifyComplete();

        assertThat(holder.current().coupons()).isEmpty();
    }
}
