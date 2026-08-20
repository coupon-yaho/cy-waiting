package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.MutableClock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.awaitility.Awaitility;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.scheduler.VirtualTimeScheduler;
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
    @DisplayName("발행되지_않은_빈_응답은_받아들이지_않는다")
    void 발행되지_않은_빈_응답은_받아들이지_않는다() {
        // 빈 해시는 장애가 아니라 흔한 상태다 — 데이터 없는 복제본 승격,
        // 키 만료, 리더 재선출 중 재작성. 성공 응답이라고 그대로 받으면
        // "빈 값으로 덮지 않는다" 가 실패 경로에서만 참이 된다.
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        AtomicInteger 호출 = new AtomicInteger();
        SnapshotRefresher refresher = SnapshotRefresher.of(holder,
                () -> 호출.incrementAndGet() == 1 ? Mono.just(정상) : Mono.just(Map.of()));

        StepVerifier.create(refresher.한번()).verifyComplete();
        StepVerifier.create(refresher.한번()).verifyComplete();

        assertThat(holder.current().coupons()).containsOnlyKeys("c1");
    }

    @Test
    @DisplayName("루프는_판마다_새로_읽는다")
    void 루프는_판마다_새로_읽는다() {
        // Supplier 를 한 번만 부르면 프로세스 수명 내내 최초 것만 쓴다.
        // 키·커넥션·설정을 매 판 다시 읽는 구현을 끼우면 그때 드러난다.
        //
        // 한번() 은 값을 안 흘리므로 방출 수로는 못 센다 — 호출 수를 본다.
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        AtomicInteger 호출 = new AtomicInteger();
        SnapshotRefresher refresher = SnapshotRefresher.of(holder, () -> {
            호출.incrementAndGet();
            return Mono.just(정상);
        });

        Disposable 구독 = refresher.루프(Duration.ofMillis(10), Schedulers.single()).subscribe();
        try {
            Awaitility.await().atMost(Duration.ofSeconds(5))
                    .until(() -> 호출.get() >= 3);
        } finally {
            구독.dispose();
        }
    }


    @Test
    @DisplayName("타임아웃_타이머도_주어진_스케줄러에서_돈다")
    void 타임아웃_타이머도_주어진_스케줄러에서_돈다() {
        // 공용 풀에 두면 부하로 그 풀이 밀릴 때 포기 자체가 늦어진다 —
        // 부하가 가장 높을 때 노드가 로테이션에서 빠진다 (RX-3).
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        SnapshotRefresher refresher = SnapshotRefresher.of(holder, Mono::never,
                Duration.ofMillis(50));
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();

        StepVerifier.create(refresher.한번(가상))
                .then(() -> 가상.advanceTimeBy(Duration.ofMillis(60)))
                .verifyComplete();

        assertThat(holder.current().coupons()).isEmpty();
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
