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

    private static final Duration FETCH_STALE = Duration.ofSeconds(2);

    private static SnapshotHolder 홀더(MutableClock clock) {
        return SnapshotHolder.of(FETCH_STALE, Duration.ofSeconds(5), clock);
    }

    @Test
    @DisplayName("한_번_돌면_홀더가_갱신된다")
    void 한_번_돌면_홀더가_갱신된다() {
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        SnapshotRefresher refresher = SnapshotRefresher.of(holder, () -> Mono.just(정상));

        StepVerifier.create(refresher.once()).verifyComplete();

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

        StepVerifier.create(refresher.once()).verifyComplete();
        StepVerifier.create(refresher.once()).verifyComplete();

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

        // **루프로 본다.** 한번() 을 시험이 두 번 부르면 "두 번 불렸다" 는
        // 시험 자신이 만든 사실이라, 실패가 루프를 끊는지 아무것도 못 본다.
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        Disposable 구독 = refresher.loop(Duration.ofMillis(10), 가상).subscribe();
        try {
            assertThat(호출).hasValue(1);
            가상.advanceTimeBy(Duration.ofMillis(10));
            assertThat(호출).hasValue(2);
            assertThat(holder.current().coupons()).containsOnlyKeys("c1");
        } finally {
            구독.dispose();
        }
    }

    @Test
    @DisplayName("실패해도_루프가_돌았으면_낡지_않는다")
    void 실패해도_루프가_돌았으면_낡지_않는다() {
        // **받아오기 실패는 이 노드의 결함이 아니다.** 레디스가 느리면 모든
        // 노드가 같이 실패하는데, 그걸 fetchStale 로 흘리면 전 노드가 한꺼번에
        // 로테이션에서 빠져 100% 장애가 된다. 못 받은 사실은 dataStale 이
        // 나이로 이미 드러내고, 그쪽은 fail-open 이라 서비스가 산다.
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        AtomicInteger 호출 = new AtomicInteger();
        SnapshotRefresher refresher = SnapshotRefresher.of(holder,
                () -> 호출.incrementAndGet() == 1
                        ? Mono.just(정상)
                        : Mono.error(new IllegalStateException("계속 실패")));

        StepVerifier.create(refresher.once()).verifyComplete();
        assertThat(holder.isFetchStale()).isFalse();

        clock.앞으로(Duration.ofSeconds(3));
        StepVerifier.create(refresher.once()).verifyComplete();

        assertThat(holder.isFetchStale()).isFalse();
    }

    @Test
    @DisplayName("실패가_이어지면_dataStale_로_드러난다")
    void 실패가_이어지면_dataStale_로_드러난다() {
        // 루프가 도는 것만으로 최신이라고 답하면, 계속 실패하는 노드가
        // 영영 멀쩡해 보인다. **못 받은 사실은 다른 신호로 반드시 드러난다.**
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        AtomicInteger 호출 = new AtomicInteger();
        SnapshotRefresher refresher = SnapshotRefresher.of(holder,
                () -> 호출.incrementAndGet() == 1
                        ? Mono.just(정상)
                        : Mono.error(new IllegalStateException("계속 실패")));

        StepVerifier.create(refresher.once()).verifyComplete();
        assertThat(holder.isDataStale()).isFalse();

        clock.앞으로(Duration.ofSeconds(6));
        StepVerifier.create(refresher.once()).verifyComplete();

        assertThat(holder.isDataStale()).isTrue();
    }

    @Test
    @DisplayName("받아들일_수_없는_스냅샷도_루프는_돈_것이다")
    void 받아들일_수_없는_스냅샷도_루프는_돈_것이다() {
        // 필터로 버리는 경로다. 여기서 하트비트를 안 찍으면 스케줄러 판이
        // 갈린 동안 전 노드가 liveness 실패로 재기동을 반복한다 —
        // 재기동한다고 스케줄러가 고쳐지지 않는다.
        //
        // **빈 해시가 아니라 판 갈림을 쓴다.** 빈 해시는 발행 표시가 없어 첫
        // 조건에서 걸려, 쿠폰을 하나도 못 읽은 두 번째 가지를 안 밟는다.
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        AtomicInteger 호출 = new AtomicInteger();
        Map<String, String> 판이_갈린_스냅샷 = Map.of(
                "#credit", "1000", "#nodes", "2", "#published", "1787184000",
                "c1", "ADAPTIVE:QUEUEING:100:500:2000:1.0:뒷판이_늘린_필드");
        SnapshotRefresher refresher = SnapshotRefresher.of(holder,
                () -> Mono.just(호출.incrementAndGet() == 1 ? 정상 : 판이_갈린_스냅샷));

        StepVerifier.create(refresher.once()).verifyComplete();

        clock.앞으로(Duration.ofSeconds(3));
        StepVerifier.create(refresher.once()).verifyComplete();

        assertThat(holder.isFetchStale()).isFalse();
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

        StepVerifier.create(refresher.once()).verifyComplete();
        StepVerifier.create(refresher.once()).verifyComplete();

        assertThat(holder.current().coupons()).containsOnlyKeys("c1");
    }


    @Test
    @DisplayName("쿠폰을_하나도_못_읽으면_받아들이지_않는다")
    void 쿠폰을_하나도_못_읽으면_받아들이지_않는다() {
        // **발행 표시만 보면 부족하다.** 스케줄러가 값에 필드를 하나 더
        // 실으면 전 쿠폰이 파싱에 실패하는데 #published 는 멀쩡하다 —
        // 그대로 받으면 홀더가 빈 맵으로 덮이고 전 쿠폰이 매진으로 보인다.
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        AtomicInteger 호출 = new AtomicInteger();
        Map<String, String> 판이_다른_스냅샷 = Map.of(
                "#credit", "1000", "#nodes", "2", "#published", "1787184000",
                "c1", "ADAPTIVE:QUEUEING:100:500:2000:1.0:뒷판이_늘린_필드");
        SnapshotRefresher refresher = SnapshotRefresher.of(holder,
                () -> 호출.incrementAndGet() == 1 ? Mono.just(정상)
                        : Mono.just(판이_다른_스냅샷));

        StepVerifier.create(refresher.once()).verifyComplete();
        StepVerifier.create(refresher.once()).verifyComplete();

        assertThat(holder.current().coupons()).containsOnlyKeys("c1");
    }

    @Test
    @DisplayName("루프는_판마다_새로_읽는다")
    void 루프는_판마다_새로_읽는다() {
        // Supplier 를 한 번만 부르면 프로세스 수명 내내 최초 것만 쓴다.
        // 키·커넥션·설정을 매 판 다시 읽는 구현을 끼우면 그때 드러난다.
        //
        // 한번() 은 값을 안 흘리므로 방출 수로는 못 센다 — 호출 수를 본다.
        // **가상 시계로 잰다.** 실제 대기로 "3회 이상" 만 보면 간격 없이
        // 최고 속도로 도는 루프도 통과한다 — 회복 순간 몰아치기를 막겠다는
        // 것이 이 설계의 목적인데 그 실패 모드가 검증되지 않는다.
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        AtomicInteger 호출 = new AtomicInteger();
        SnapshotRefresher refresher = SnapshotRefresher.of(holder, () -> {
            호출.incrementAndGet();
            return Mono.just(정상);
        });
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();

        Disposable 구독 = refresher.loop(Duration.ofMillis(10), 가상).subscribe();
        try {
            // 시간을 안 돌리면 첫 판만 돈다.
            assertThat(호출).hasValue(1);
            가상.advanceTimeBy(Duration.ofMillis(10));
            assertThat(호출).hasValue(2);
            가상.advanceTimeBy(Duration.ofMillis(10));
            assertThat(호출).hasValue(3);
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
        AtomicInteger 호출 = new AtomicInteger();
        SnapshotRefresher refresher = SnapshotRefresher.of(holder,
                () -> 호출.incrementAndGet() == 1 ? Mono.just(정상) : Mono.never(),
                Duration.ofMillis(50));
        // 성공 한 번으로 채워 둔다 — 안 채우면 빈 것을 확인해도 아무 뜻이 없다.
        refresher.once().block(Duration.ofSeconds(5));

        // **먼저 낡혀 둔다.** 안 그러면 타임아웃 뒤의 단언이 첫 성공 덕에
        // 통과해 버려, 하트비트를 지워도 시험이 안 깨진다.
        clock.앞으로(FETCH_STALE.plusSeconds(1));
        assertThat(holder.isFetchStale()).isTrue();

        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        StepVerifier.create(refresher.once(가상))
                .then(() -> 가상.advanceTimeBy(Duration.ofMillis(60)))
                .verifyComplete();

        // 매달린 판을 포기하되 들고 있던 것은 남는다.
        assertThat(holder.current().coupons()).containsOnlyKeys("c1");

        // **그리고 포기한 판도 한 바퀴 돈 것이다.** 이 사고의 발단이 정확히
        // 여기였다 — 지연을 주입하니 모든 판이 타임아웃했고, 그걸 노드별
        // 신호로 세는 바람에 전 노드가 동시에 빠졌다.
        assertThat(holder.isFetchStale()).isFalse();
    }

    @Test
    @DisplayName("루프가_뜯기면_그때는_낡음이_된다")
    void 루프가_뜯기면_그때는_낡음이_된다() {
        // fetchStale 의 존재 이유다. 여기가 비면 "루프가 도는가" 로 옮긴 판정이
        // 한쪽만 검증된 채로 남는다 — 아무도 안 빠지는 것만 확인하고,
        // 정말 멎은 노드가 빠지는지는 확인 안 한 것이다.
        //
        // **취소는 하트비트가 아니라는 계약도 여기서 고정된다.** 그래서 판이
        // 매달린 채로 뜯는다 — 이미 끝난 판을 뜯으면 취소 신호가 한번() 까지
        // 안 내려가고, doFinally 로 옮긴 구현도 그대로 통과한다.
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        AtomicInteger 호출 = new AtomicInteger();
        SnapshotRefresher refresher = SnapshotRefresher.of(holder,
                () -> 호출.incrementAndGet() == 1 ? Mono.just(정상) : Mono.never(),
                Duration.ofSeconds(30));   // 매달린 판이 스스로 포기하지 않게

        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        Disposable 구독 = refresher.loop(Duration.ofMillis(100), 가상).subscribe();
        try {
            가상.advanceTimeBy(Duration.ofMillis(250));   // 1판 성공, 2판은 매달린다
            assertThat(holder.isFetchStale()).isFalse();

            // **가상 시간은 그대로 두고 벽시계만 감는다.** 그래야 루프가 한 판도
            // 더 못 돈 채로 낡는다 — 뜯기 직전 상태를 낡음으로 만들어 둔다.
            clock.앞으로(FETCH_STALE.plusSeconds(1));
            assertThat(holder.isFetchStale()).isTrue();

            구독.dispose();

            assertThat(holder.isFetchStale()).isTrue();
        } finally {
            구독.dispose();
        }
    }

    /**
     * <b>운영 배선이 타는 유일한 경로다.</b> 나이를 레디스 시계 하나로 재는 것이
     * 여기서만 일어나는데, 나머지 시험은 전부 시각 없이 오는 길을 탄다.
     *
     * <p>스크립트가 초 대신 밀리초를 돌려주면 나이가 수천만 초가 되어 전 노드가
     * 영구히 낡음으로 굳고, 0 근처를 돌려주면 갱신이 멎어도 영원히 최신이다.
     * 둘 다 빌드는 초록이고 부하 시험 전까지 안 드러난다.
     */
    @Test
    @DisplayName("레디스가_준_시각으로_나이를_잰다")
    void 레디스가_준_시각으로_나이를_잰다() {
        // 발행은 지금, 레디스는 그로부터 7초 뒤를 가리킨다.
        long 레디스_시각 = 지금.getEpochSecond() + 7;
        // 노드 시계는 일부러 한참 어긋나게 둔다. 여기에 기대면 값이 달라진다.
        MutableClock clock = MutableClock.at(지금.plusSeconds(3_600));
        SnapshotHolder holder = 홀더(clock);
        SnapshotRefresher refresher = SnapshotRefresher.timed(
                holder, () -> Mono.just(new TimedSnapshot(정상, 레디스_시각)), clock);

        StepVerifier.create(refresher.once()).verifyComplete();

        // 노드 시계로 쟀으면 3,600 초다. 임계 5 초를 넘은 것은 레디스 쪽 7 초다.
        assertThat(holder.dataAge()).isEqualTo(Duration.ofSeconds(7));
        assertThat(holder.isDataStale()).isTrue();
    }

    /**
     * <b>받아온 뒤 흐른 시간은 더한다.</b> 재는 순간의 나이만 들고 있으면 갱신이
     * 멎어도 그 값이 안 늙어, 낡은 재료로 계속 판정한다.
     */
    @Test
    @DisplayName("받아온_뒤_흐른_시간이_나이에_더해진다")
    void 받아온_뒤_흐른_시간이_나이에_더해진다() {
        MutableClock clock = MutableClock.at(지금);
        SnapshotHolder holder = 홀더(clock);
        SnapshotRefresher refresher = SnapshotRefresher.timed(holder,
                () -> Mono.just(new TimedSnapshot(정상, 지금.getEpochSecond() + 1)), clock);
        StepVerifier.create(refresher.once()).verifyComplete();

        clock.앞으로(Duration.ofSeconds(4));

        // 잰 나이 1 초 + 그 뒤로 흐른 4 초. dataStaleAfter 는 5 초다.
        assertThat(holder.dataAge()).isEqualTo(Duration.ofSeconds(5));
        assertThat(holder.isDataStale()).isFalse();
    }
}
