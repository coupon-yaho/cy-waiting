package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.kafkick.waiting.adapter.redis.ClockSkewTracker;
import com.kafkick.waiting.domain.admission.CircuitState;
import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.Grant;
import com.kafkick.waiting.domain.allocation.ReleaseRamp;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.coupon.RuntimeState;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.queue.PollBudgetPlanner;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import java.lang.ref.Reference;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * 한 회차. 수요를 모아 크레딧을 나누고 적용한 뒤 발행한다.
 *
 * <p><b>대기 수를 한 번만 읽는다.</b> 크레딧을 산출한 뒤 다시 읽으면 그 사이에
 * 사람이 빠져 도메인이 막는 조합이 발행되고, 코덱이 그 쿠폰만 떨군다. 떨어진
 * 쿠폰은 판정에서 없는 쿠폰 — 매진으로 보인다.
 */
class AllocationRoundTest {

    private final List<String> 적용 = new CopyOnWriteArrayList<>();
    private ListAppender<ILoggingEvent> 로그;
    private Level 원래_수준;

    @BeforeEach
    void 로그를_받는다() {
        로그 = new ListAppender<>();
        로그.start();
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(AllocationRound.class);
        // 회차마다 세는 값은 지표 자리라 낮은 수준으로 찍는다. 시험은 그걸 봐야 한다.
        원래_수준 = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(로그);
    }

    @AfterEach
    void 로그를_뗀다() {
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(AllocationRound.class);
        logger.detachAppender(로그);
        logger.setLevel(원래_수준);
    }

    /** 재료를 읽은 시각. 발행 시각이 여기서 나온다. */
    private static final long 읽은_시각 = 1_700_000_000L;

    private final Map<String, Map<String, String>> 발행 = new LinkedHashMap<>();

    /**
     * <b>발행이 실패하면 지우지 않고 다음 틱에 다시 옵니다.</b>
     *
     * <p>정리 판단을 `.then()` 의 인자로 부르면 자바가 먼저 평가해서, 셈과
     * 삭제 표시와 로그가 <b>발행이 구독되기도 전에</b> 일어납니다. 그러면
     * 발행이 터져도 지운 것으로 기록되고, 죽은 줄이 영구히 남으면서 로그는
     * 지웠다고 말합니다.
     */
    /**
     * <b>이월 실패는 곧바로 포기가 아니다</b> (CY-859).
     *
     * <p>폴백을 그 자리에 설치하면 다음 회차가 아예 안 시도한다. 그 폴백은
     * 미관측이라 첫 관측치를 평활 없이 그대로 발행하는데, 승계 직후에는 그것이
     * 뒷단이 감당 못 할 수다.
     */
    @Test
    @DisplayName("이월이_실패하면_다음_회차에_다시_받는다")
    void 이월이_실패하면_다음_회차에_다시_받는다() {
        AtomicInteger 시도 = new AtomicInteger();
        AtomicBoolean 터진다 = new AtomicBoolean(true);
        List<Long> 발행된_크레딧 = new ArrayList<>();
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(
                        List.of(new CouponDemand("c1", 5, 100, QueueMode.ADAPTIVE)), 읽은_시각)),
                () -> 1_000, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> {
                    발행된_크레딧.add(Long.parseLong(hash.get("#credit")));
                    return Mono.empty();
                },
                () -> Instant.ofEpochSecond(읽은_시각),
                () -> {
                    시도.incrementAndGet();
                    return 터진다.get()
                            ? Mono.error(new IllegalStateException("레디스가 흔들린다"))
                            : Mono.just(CreditSmoother.restore(0.3,
                                    new CreditSmoother.Snapshot(200.0, true)));
                },
                SnapshotCodec.create(), () -> 0L);

        round.run().block();
        assertThat(시도.get()).as("한 회차 실패했다").isEqualTo(1);

        터진다.set(false);
        round.run().block();

        assertThat(시도.get()).as("다음 회차에 다시 받는다 — 폴백을 그 자리에 설치하면 안 받는다")
                .isEqualTo(2);
        // 이월값 200 과 관측 1,000 사이. 알파가 0.3 이라 440 이 나온다 —
        // 관측치를 생으로 내보내면 1,000 이다.
        assertThat(발행된_크레딧).as("이월을 받은 회차는 평활한 값을 낸다")
                .containsExactly(1_000L, 440L);
    }

    @Test
    @DisplayName("발행이_실패하면_지우지_않고_다음_틱에_다시_온다")
    void 발행이_실패하면_지우지_않고_다음_틱에_다시_온다() {
        List<String> 지운_것 = new ArrayList<>();
        SoldOutCleanup cleanup = SoldOutCleanup.of(1, new SimpleMeterRegistry());
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(
                        List.of(new CouponDemand("c1", 0, 0, QueueMode.ADAPTIVE)), 읽은_시각)),
                () -> 1_000, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> Mono.error(new IllegalStateException("레디스가 끊겼다")),
                () -> Instant.ofEpochSecond(읽은_시각),
                () -> Mono.just(CreditSmoother.of(1.0)),
                SnapshotCodec.create(), () -> 0L, Optional::empty,
                cleanup, ids -> {
                    지운_것.addAll(ids);
                    return Mono.just(ids);
                }, ids -> Mono.just(List.of()), 안_걷는_스위퍼(), () -> false, () -> CircuitState.CLOSED);

        for (int i = 0; i < 5; i++) {
            round.run().onErrorResume(e -> Mono.empty()).block();
        }

        assertThat(지운_것).as("발행이 실패했으므로 아무것도 안 지운다").isEmpty();
        // **셈도 안 올라야 한다.** 올라 있으면 발행이 살아난 첫 틱에 유예를
        // 다 안 채우고 지운다.
        assertThat(cleanup.due(Map.of("c1", CouponStates.closed(0))))
                .as("첫 틱은 아직").isEmpty();
    }

    /**
     * <b>리더가 아니면 안 지웁니다.</b>
     *
     * <p>회차 안에서 유일하게 되돌릴 수 없는 쓰기입니다. 회차가 도는 사이에 리스가
     * 만료되고 다른 노드가 리더가 됐다면, 여기서 내는 삭제는 <b>남의 줄</b>을
     * 지우는 것입니다 — 그 사이에 재입고돼 살아난 줄일 수도 있습니다.
     */
    @Test
    @DisplayName("리더가_아니면_지우지_않는다")
    void 리더가_아니면_지우지_않는다() {
        List<String> 지운_것 = new ArrayList<>();
        // 발행까지는 리더고, 정리 직전에 잃는다.
        AtomicInteger 물어본_횟수 = new AtomicInteger();
        SoldOutCleanup cleanup = SoldOutCleanup.of(1, new SimpleMeterRegistry());
        AllocationRound round = AllocationRound.of(
                () -> 물어본_횟수.incrementAndGet() < 3,
                () -> Mono.just(new TimedDemands(
                        List.of(new CouponDemand("c1", 0, 0, QueueMode.ADAPTIVE)), 읽은_시각)),
                () -> 1_000, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> Mono.empty(),
                () -> Instant.ofEpochSecond(읽은_시각),
                () -> Mono.just(CreditSmoother.of(1.0)),
                SnapshotCodec.create(), () -> 0L, Optional::empty,
                cleanup, ids -> {
                    지운_것.addAll(ids);
                    return Mono.just(ids);
                }, ids -> Mono.just(List.of()), 안_걷는_스위퍼(), () -> false, () -> CircuitState.CLOSED);

        for (int i = 0; i < 5; i++) {
            round.run().onErrorResume(e -> Mono.empty()).block();
        }

        assertThat(지운_것).as("리더가 아닌 채로 지우지 않는다").isEmpty();
    }

    /**
     * <b>못 지운 쿠폰은 다음 틱에 다시 옵니다.</b>
     *
     * <p>요청한 것 전부를 지운 것으로 표시하면 실패한 쿠폰이 대상에서 영영
     * 빠지고, 지표는 지웠다고 말합니다 — 죽은 줄이 남는데 로그가 거짓입니다.
     */
    @Test
    @DisplayName("못_지운_쿠폰은_다음_틱에_다시_온다")
    void 못_지운_쿠폰은_다음_틱에_다시_온다() {
        SoldOutCleanup cleanup = SoldOutCleanup.of(1, new SimpleMeterRegistry());
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(
                        List.of(new CouponDemand("c1", 0, 0, QueueMode.ADAPTIVE)), 읽은_시각)),
                () -> 1_000, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> Mono.empty(),
                () -> Instant.ofEpochSecond(읽은_시각),
                () -> Mono.just(CreditSmoother.of(1.0)),
                SnapshotCodec.create(), () -> 0L, Optional::empty,
                // 하나도 못 지웠다고 답한다.
                cleanup, ids -> Mono.just(List.of()), ids -> Mono.just(List.of()),
                안_걷는_스위퍼(), () -> false, () -> CircuitState.CLOSED);

        for (int i = 0; i < 5; i++) {
            round.run().block();
        }

        assertThat(cleanup.due(Map.of("c1", CouponStates.closed(0))))
                .as("여전히 대상이다").containsExactly("c1");
    }

    /**
     * <b>한 회차가 스위퍼를 실제로 부릅니다.</b>
     *
     * <p>이것이 없으면 스위퍼가 통째로 죽은 코드여도 빌드가 초록입니다 —
     * 게이트도 커서도 스크립트도 전부 안 도는데 지표는 조용합니다. 실제로
     * 그 상태로 리뷰까지 갔습니다.
     */
    @Test
    @DisplayName("한_회차가_스위퍼를_부른다")
    void 한_회차가_스위퍼를_부른다() {
        List<String> 쓴_쿠폰 = new ArrayList<>();
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(
                        List.of(new CouponDemand("c1", 100, 1_000, QueueMode.ADAPTIVE)),
                        읽은_시각)),
                () -> 1_000, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> Mono.empty(),
                () -> Instant.ofEpochSecond(읽은_시각),
                () -> Mono.just(CreditSmoother.of(1.0)),
                SnapshotCodec.create(), () -> 0L, Optional::empty,
                SoldOutCleanup.of(1, new SimpleMeterRegistry()),
                ids -> Mono.just(List.of()),
                ids -> Mono.just(List.of()),
                QueueSweeper.of(
                        SweepGates.warmed(Duration.ofSeconds(1), PollIntervalPolicy.aliveTtl()),
                        (ids, limit, removeFront) -> {
                            쓴_쿠폰.addAll(ids);
                            return Mono.just(QueueSweeper.SweepResult.NOTHING);
                        }), () -> false, () -> CircuitState.CLOSED);

        round.run().block();

        assertThat(쓴_쿠폰).as("줄이 선 쿠폰을 쓸러 간다").containsExactly("c1");
    }

    /** 판단은 돌되 아무것도 안 걷는 스위퍼. 이 시험들의 초점이 아니다. */
    private static QueueSweeper 안_걷는_스위퍼() {
        return QueueSweeper.of(
                SweepGates.warmed(Duration.ofSeconds(1), PollIntervalPolicy.aliveTtl()),
                (ids, limit, removeFront) -> Mono.just(QueueSweeper.SweepResult.NOTHING));
    }

    private AllocationRound round(List<CouponDemand> 수요, long 전역_크레딧, int 노드_수) {
        return round(() -> 수요, 전역_크레딧, 노드_수);
    }

    /** 회차마다 수요가 바뀌는 시험용. 복붙하면 헬퍼가 바뀔 때 그 시험만 옛 배선을 잰다. */
    private AllocationRound round(Supplier<List<CouponDemand>> 수요,
            long 전역_크레딧, int 노드_수) {
        return round(수요, 전역_크레딧, 노드_수, grant -> {
            적용.add(grant.couponId() + "=" + grant.credit());
            return Mono.just(grant.credit());
        });
    }

    /**
     * 적용이 돌려주는 수를 갈아 끼운다.
     *
     * <p><b>기본 픽스처는 몫을 그대로 돌려준다.</b> 그러면 들인 수와 나눠 준
     * 몫이 항등식이라, 둘을 가르는 시험이 아무것도 못 잡는다.
     */
    private AllocationRound round(Supplier<List<CouponDemand>> 수요,
            long 전역_크레딧, int 노드_수,
            Function<Grant, Mono<Long>> 적용_결과) {
        return AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(수요.get(), 읽은_시각)),
                () -> 전역_크레딧,
                () -> 노드_수,
                적용_결과,
                hash -> {
                    발행.put("last", hash);
                    return Mono.empty();
                },
                () -> Instant.ofEpochSecond(1_700_000_000L),
                () -> Mono.just(CreditSmoother.of(1.0)),
                SnapshotCodec.create(), () -> 0L);
    }

    private List<String> 로그_메시지() {
        return 로그.list.stream().map(ILoggingEvent::getMessage).toList();
    }

    private Object[] 로그_인자(String 패턴조각) {
        return 로그.list.stream().filter(e -> e.getMessage().contains(패턴조각))
                .map(ILoggingEvent::getArgumentArray).findFirst().orElseThrow();
    }

    private double 발행된_배수() {
        return SnapshotCodec.create().decode(발행.get("last")).meta().pollScale();
    }

    private CouponState 발행된(String couponId) {
        return SnapshotCodec.create().decode(발행.get("last")).coupons().get(couponId);
    }

    @Test
    @DisplayName("예산을_넘는_폴링_수요면_배수가_스냅샷에_실린다")
    void 예산을_넘는_폴링_수요면_배수가_스냅샷에_실린다() {
        // 배수를 안 실으면 게이트웨이가 늘 1.0 으로 답하고, 폴링 예산이라는
        // 계산은 아무 데도 안 닿는 순수 함수로 남는다.
        AllocationRound round = round(List.of(new CouponDemand("c1", 100_000, 1_000_000)), 10, 1);

        round.run().block();

        // **값으로 못 박는다.** 시계도 크레딧도 고정이라 결정적인 값인데
        // 부등호로 두면 예산 상수가 열 배 틀려도 통과한다.
        // 밴드별로 50 + 250/3 + 90 + 98800/30 = 3516.67 rps, 예산 200.
        assertThat(발행된_배수()).as("예산 초과 배수").isCloseTo(17.583, within(0.001));
    }

    @Test
    @DisplayName("매진_큐의_대기자는_배수를_안_올린다")
    void 매진_큐의_대기자는_배수를_안_올린다() {
        // 3.3 절. 죽은 큐 10만 명이 예산을 먹으면 **살아 있는 쿠폰의** 대기자까지
        // 폴링 간격이 늘어난다 — 배분에서 막아 둔 기아가 폴링으로 되살아난다.
        AllocationRound round = round(List.of(
                new CouponDemand("dead", 100_000, 0),
                new CouponDemand("live", 10, 100)), 100, 1);

        round.run().block();
        double 매진일_때 = 발행된_배수();

        // **음성 대조를 붙인다.** 1.0 은 배수 배선을 통째로 지웠을 때도 나오는
        // 값이라, 이것만 보면 걸러 낸 것을 증명하지 못한다. 같은 회차에서 재고만
        // 채우면 배수가 크게 오른다 — 두 값을 가르는 것이 `isActive` 필터뿐이다.
        round(List.of(
                new CouponDemand("dead", 100_000, 1_000_000),
                new CouponDemand("live", 10, 100)), 100, 1).run().block();

        assertThat(매진일_때).as("살아 있는 쿠폰의 배수").isEqualTo(1.0);
        // 형제 시험처럼 값으로 못 박는다. dead 가 90, live 가 10 을 받아
        // 4,983.33 + 10 rps, 예산 200.
        assertThat(발행된_배수()).as("같은 줄이 살아 있으면 예산을 먹는다")
                .isCloseTo(24.967, within(0.001));
    }

    @Test
    @DisplayName("예산을_넘긴_틱을_한_회차에_한_번만_센다")
    void 예산을_넘긴_틱을_한_회차에_한_번만_센다() {
        // 한 회차가 쿠폰 상태를 세 번 만든다 — 발행·정리·청소. 배수 계산이
        // 거기 묻어 있으면 누적 틱이 3배로 오르고, "틱 수" 라고 적힌 지표가
        // 틱이 아닌 것을 센다. 해제 로그의 지속 시간도 같이 부푼다.
        AllocationRound round = round(List.of(new CouponDemand("c1", 100_000, 1_000_000)), 10, 1);

        round.run().block();
        assertThat(round.pollBudgetOvershootTicks()).as("한 회차").isEqualTo(1);

        round.run().block();
        assertThat(round.pollBudgetOvershootTicks()).as("두 회차").isEqualTo(2);
    }

    @Test
    @DisplayName("배수가_풀리면_해제를_남긴다")
    void 배수가_풀리면_해제를_남긴다() {
        // **같은 인스턴스 위에서 오름과 내림을 잰다.** 회차마다 새 인스턴스를
        // 만들면 진입만 돌고 해제 갈래는 한 번도 안 돈다 — 그 자리에 상태가
        // 있는데(초과 창) 회복 전이를 아무도 안 밟는 것이다.
        AtomicReference<List<CouponDemand>> 수요 = new AtomicReference<>(
                List.of(new CouponDemand("c1", 100_000, 1_000_000)));
        AllocationRound round = round(수요::get, 10, 1);

        round.run().block();
        double 배수 = 발행된_배수();
        assertThat(배수).as("진입").isGreaterThan(1.0);
        assertThat(로그_메시지()).anyMatch(m -> m.contains("폴링 예산 초과 —"));
        // **인자를 값으로 못 박는다.** 문구만 보면 예상과 예산이 뒤바뀌어
        // 찍혀도 초록이다. 쿠폰 ID 를 라벨로 못 쓰므로(LG-4) 어느 규모에서
        // 배수가 걸렸는지는 이 로그만이 답한다.
        assertThat(로그_인자("폴링 예산 초과 —"))
                .as("예상 rps · 노드 수 · 예산 rps · 소수 한 자리로 자른 배수")
                .containsExactly(Math.round(PollBudgetPlanner.expectedPollRps(100_000, 10)),
                        1, Math.round(PollBudgetPlanner.budgetRps(1)),
                        Math.round(배수 * 10) / 10.0);

        // 재고가 마르면 그 줄은 예산에서 빠진다. 배수가 풀리는 전이다.
        수요.set(List.of(new CouponDemand("c1", 100_000, 0)));
        round.run().block();

        assertThat(발행된_배수()).as("해제").isEqualTo(1.0);
        assertThat(로그_메시지()).anyMatch(m -> m.contains("폴링 예산 초과 해제 —"));
    }

    @Test
    @DisplayName("예산_안이면_틱을_안_센다")
    void 예산_안이면_틱을_안_센다() {
        AllocationRound round = round(List.of(new CouponDemand("c1", 10, 100)), 100, 1);
        round.run().block();

        assertThat(round.pollBudgetOvershootTicks()).as("초과 없음").isZero();
    }

    /**
     * <b>지표가 어느 값을 읽는지 못 박는다.</b>
     *
     * <p>이름이 스크레이프에 있는지만 보면 네 지표가 서로의 값을 읽어도 통과한다 —
     * 이름 넷이 다 보이는데 둘이 같은 숫자를 내는, 조사할 때 정확히 헷갈리는 모양이다.
     */
    @Test
    @DisplayName("선행_지표가_각자의_값을_읽는다")
    void 선행_지표가_각자의_값을_읽는다() {
        AllocationRound round = round(
                List.of(new CouponDemand("c1", 100_000, 1_000_000)), 10, 1);
        round.run().block();
        SimpleMeterRegistry meters = new SimpleMeterRegistry();

        // **반환값을 붙잡는다.** FunctionCounter 는 상태 객체를 약한 참조로
        // 잡으므로, 버리면 GC 뒤에 계수가 0 으로 굳는다.
        InvariantMetrics 지표 = InvariantMetrics.bind(round, ClockSkewTracker.create(), meters);
        System.gc();

        assertThat(round.pollBudgetOvershootTicks()).as("전제 — 이 회차는 예산을 넘겼다")
                .isEqualTo(1);
        assertThat(meters.get("waiting.poll.budget.overshoot.ticks")
                .functionCounter().count()).as("폴링 예산 초과 틱").isEqualTo(1);
        // 나머지 셋은 이 회차에서 안 움직인다. 값이 갈려야 서로를 읽는 배선이 잡힌다.
        assertThat(meters.get("waiting.allocation.budget.overshoot")
                .functionCounter().count()).as("배분 초과량").isZero();
        assertThat(meters.get("waiting.allocation.entered.overshoot")
                .functionCounter().count()).as("초과 입장 인원").isZero();
        assertThat(meters.get("waiting.snapshot.clock.floor.applied")
                .functionCounter().count()).as("시계 바닥값").isZero();
        // 이 회차는 줄이 10만인데 크레딧이 10 이라 열 명의 차례가 왔다.
        assertThat(meters.get("waiting.allocation.admitted")
                .functionCounter().count()).as("차례를 준 인원").isEqualTo(10);
        // **마지막 단언 뒤에 둔다.** 앞에 두면 그 뒤 계수들이 수거된 객체를 읽어
        // 0 이 나올 수 있다 — 붙잡은 뜻이 절반만 산다.
        Reference.reachabilityFence(지표);
    }

    /**
     * <b>하트비트가 다 만료돼 0 으로 보이는 순간 배수가 꺼지면 안 된다.</b>
     *
     * <p>클러스터가 흔들리는 바로 그 순간이라, 예산이 0 이 되면 배수가 통째로
     * 꺼지고 전원의 폴링이 한꺼번에 짧아진다. 방어를 한 곳에 모은 근거가 이것인데
     * 부르는 쪽이 그 메서드를 쓰는지는 아무도 안 재고 있었다.
     */
    /**
     * <b>들인 인원을 누적으로 남긴다</b> (G7.5).
     *
     * <p>크레딧 낭비는 <b>차례를 준 인원</b>에서 <b>실제로 받아 간 인원</b>을 뺀
     * 값이다. 뒤엣것은 판정 지표가 세지만 앞엣것을 아무도 안 세고 있어서, 낭비율을
     * 잴 방법이 없었다 — 이탈 30%에서 낭비 5% 미만이라는 기준이 측정 수단 없이
     * 계획서에만 있었다.
     */
    @Test
    @DisplayName("들인_인원을_누적으로_센다")
    void 들인_인원을_누적으로_센다() {
        // **몫과 다른 수를 돌려준다.** 같으면 나눠 준 몫을 세는 구현도 통과한다 —
        // 그 구현은 낭비율의 분모를 부풀려 실제보다 좋아 보이게 만든다.
        AllocationRound round = round(
                () -> List.of(new CouponDemand("c1", 100, 1_000)), 8, 1,
                grant -> Mono.just(3L));

        round.run().block();
        assertThat(round.admitted()).as("몫 8 인데 셋만 들어왔다").isEqualTo(3);

        round.run().block();
        assertThat(round.admitted()).as("회차를 넘어 누적된다").isEqualTo(6);
    }

    /**
     * <b>몫보다 많이 들어온 회차도 그대로 센다.</b>
     *
     * <p>동점 score 로 임계 하나에 여럿이 걸리면 준 몫보다 많이 들어간다. 그것을
     * 몫으로 깎아 세면 초과 발급의 직접 증거가 지표에서 사라진다.
     */
    @Test
    @DisplayName("몫보다_많이_들어와도_그대로_센다")
    void 몫보다_많이_들어와도_그대로_센다() {
        AllocationRound round = round(
                () -> List.of(new CouponDemand("c1", 100, 1_000)), 8, 1,
                grant -> Mono.just(grant.credit() + 2));

        round.run().block();

        assertThat(round.admitted()).as("실제로 들어온 수").isEqualTo(10);
        assertThat(round.enteredOvershoot()).as("몫을 넘은 몫").isEqualTo(2);
    }

    /**
     * <b>발행이 실패하면 배수도 안 센다.</b>
     *
     * <p>스냅샷 샤드만 죽은 구간이 여기다. 리더는 배수 17 을 계산하는데 전 노드는
     * 옛 재료로 1.0 을 쓰고 있으므로, 그 틱을 세면 없었던 부하를 보고하는 셈이다.
     * 운영자는 그 지표를 보고 노드를 늘린다.
     */
    @Test
    @DisplayName("발행이_실패하면_배수를_안_센다")
    void 발행이_실패하면_배수를_안_센다() {
        AllocationRound round = AllocationRound.of(
                () -> true,
                // **미상을 같이 넣는다.** 아는 쿠폰만 두면 "발행한 쿠폰·틱" 을
                // 발행 앞에서 세는 구현이 그대로 통과한다.
                () -> Mono.just(new TimedDemands(
                        List.of(new CouponDemand("c1", 100_000, 1_000_000),
                                CouponDemand.stockUnknown("c2", 30, QueueMode.ADAPTIVE)),
                        읽은_시각)),
                () -> 10L, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> Mono.error(new IllegalStateException("스냅샷 샤드가 끊겼다")),
                () -> Instant.ofEpochSecond(읽은_시각),
                () -> Mono.just(CreditSmoother.of(1.0)),
                SnapshotCodec.create(), () -> 0L);

        round.run().onErrorResume(e -> Mono.empty()).block();

        assertThat(round.pollBudgetOvershootTicks()).as("안 닿은 배수는 안 센다").isZero();
        assertThat(round.stockUnknownTicks()).as("안 나간 회차를 발행한 것으로 안 센다").isZero();
        assertThat(로그_메시지()).as("없었던 초과를 보고하지 않는다")
                .noneMatch(m -> m.contains("폴링 예산 초과 —"));
    }

    @Test
    @DisplayName("노드가_0으로_보여도_한_대로_친다")
    void 노드가_0으로_보여도_한_대로_친다() {
        List<CouponDemand> 수요 = List.of(new CouponDemand("c1", 100_000, 1_000_000));

        round(수요, 10, 1).run().block();
        double 한_대 = 발행된_배수();
        round(수요, 10, 0).run().block();
        double 영_대 = 발행된_배수();
        round(수요, 10, -3).run().block();
        double 음수 = 발행된_배수();

        assertThat(한_대).as("하한에 붙지 않았다 — 붙으면 셋이 다 1.0 이라 못 가린다")
                .isGreaterThan(1.0);
        assertThat(영_대).as("0 은 1 대로 친다").isEqualTo(한_대);
        assertThat(음수).as("음수도 1 대로 친다").isEqualTo(한_대);
    }

    @Test
    @DisplayName("노드가_두_배면_배수가_절반이다")
    void 노드가_두_배면_배수가_절반이다() {
        // 폴링은 게이트웨이가 메모리에서 종결한다. 그래서 예산의 출처는 노드 수다.
        // 상수 하나로 고정하면 증설이 폴링 간격을 못 줄인다.
        //
        // **하한에 붙지 않는 구간에서 잰다.** 배수는 1.0 아래로 안 내려가므로,
        // 클램프 구간에서 비교하면 예산이 어떻게 바뀌든 통과한다.
        List<CouponDemand> 수요 = List.of(new CouponDemand("c1", 100_000, 1_000_000));

        round(수요, 10, 1).run().block();
        double 한_대 = 발행된_배수();
        round(수요, 10, 2).run().block();
        double 두_대 = 발행된_배수();

        assertThat(두_대).as("증설한 만큼 정확히 내려간다").isCloseTo(한_대 / 2, within(0.001));
        assertThat(두_대).as("하한에 붙지 않았다").isGreaterThan(1.0);
    }

    @Test
    @DisplayName("요구가_있는_쿠폰에_크레딧이_간다")
    void 요구가_있는_쿠폰에_크레딧이_간다() {
        AllocationRound round = round(
                List.of(new CouponDemand("c1", 10, 100), new CouponDemand("c2", 4, 100)), 8, 1);

        round.run().block();

        assertThat(적용).containsExactlyInAnyOrder("c1=4", "c2=4");
    }

    @Test
    @DisplayName("발행한_쌍에서_런타임을_유도한다")
    void 발행한_쌍에서_런타임을_유도한다() {
        // 크레딧을 산출한 뒤 대기 수를 다시 읽으면 그 사이에 사람이 빠져,
        // 도메인이 막는 조합이 나간다. 그 쿠폰은 판정에서 매진으로 보인다.
        AllocationRound round = round(List.of(new CouponDemand("c1", 3, 100)), 10, 1);

        round.run().block();

        // 이번 틱에 다 뺄 수 있으면 배수 중이다.
        assertThat(발행된("c1").runtime()).isEqualTo(RuntimeState.DRAINING);
        assertThat(발행된("c1").credit()).isGreaterThanOrEqualTo(발행된("c1").waiting());
    }

    @Test
    @DisplayName("다_못_빼면_줄_서는_중으로_발행한다")
    void 다_못_빼면_줄_서는_중으로_발행한다() {
        AllocationRound round = round(List.of(new CouponDemand("c1", 100, 1_000)), 10, 1);

        round.run().block();

        assertThat(발행된("c1").runtime()).isEqualTo(RuntimeState.QUEUEING);
    }

    @Test
    @DisplayName("줄이_없으면_한산으로_발행한다")
    void 줄이_없으면_한산으로_발행한다() {
        AllocationRound round = round(List.of(new CouponDemand("c1", 0, 100)), 10, 1);

        round.run().block();

        assertThat(발행된("c1").runtime()).isEqualTo(RuntimeState.IDLE);
        assertThat(발행된("c1").credit()).isZero();
    }

    @Test
    @DisplayName("적용이_실패한_쿠폰도_발행에서_안_빠진다")
    void 적용이_실패한_쿠폰도_발행에서_안_빠진다() {
        // 빠지면 그 쿠폰은 판정에서 없는 쿠폰이 되어 매진으로 보인다. 적용이
        // 안 됐다는 것과 매진은 전혀 다른 상태다.
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(List.of(new CouponDemand("c1", 5, 100),
                        new CouponDemand("c2", 5, 100)), 읽은_시각)),
                () -> 10L, () -> 1,
                grant -> "c1".equals(grant.couponId())
                        ? Mono.error(new IllegalStateException("끊겼다"))
                        : Mono.just(grant.credit()),
                hash -> {
                    발행.put("last", hash);
                    return Mono.empty();
                },
                () -> Instant.ofEpochSecond(1_700_000_000L),
                () -> Mono.just(CreditSmoother.of(1.0)),
                SnapshotCodec.create(), () -> 0L);

        round.run().block();

        assertThat(발행.get("last")).containsKeys("c1", "c2");
        // 어느 쿠폰이 실패했는지 안 남기면, 임계가 안 움직인 이유를 사후에 못 찾는다.
        assertThat(로그_인자("배분 적용 실패")[0]).isEqualTo("c1");
        // **한 쿠폰이 실패하고 다른 쿠폰이 성공한 회차는 걷힌 것이 아니다.**
        // 여기서 복귀를 찍으면 실패도 복귀도 아닌 두 줄이 매 회차 반복된다.
        assertThat(로그_메시지()).noneMatch(m -> m.contains("배분 적용 복귀"));
        // 임계가 안 올라간 쿠폰에 몫을 실으면 노드들이 일어나지 않은 배수율로
        // 대기 시간을 계산한다.
        assertThat(발행된("c1").credit()).isZero();
        assertThat(발행된("c2").credit()).isEqualTo(5);
    }

    /**
     * <b>운영자가 정한 모드가 한 회차를 넘겨야 한다.</b> 발행이 모드를 못 실으면
     * 항상 대기로 둔 쿠폰이 다음 틱에 적응형으로 재발행되고, 줄이 빠지면 그냥
     * 통과가 된다. 꺼 둔 쿠폰의 우회도 같은 이유로 조용히 멈춘다.
     */
    @Test
    @DisplayName("운영자가_정한_모드가_한_회차를_넘긴다")
    void 운영자가_정한_모드가_한_회차를_넘긴다() {
        AllocationRound round = round(List.of(
                new CouponDemand("always", 0, 10_000, QueueMode.ALWAYS),
                new CouponDemand("off", 0, 10_000, QueueMode.OFF),
                new CouponDemand("adaptive", 0, 10_000, QueueMode.ADAPTIVE)), 10_000, 1);

        round.run().block();

        assertThat(발행된("always").mode()).isEqualTo(QueueMode.ALWAYS);
        assertThat(발행된("off").mode()).isEqualTo(QueueMode.OFF);
        assertThat(발행된("adaptive").mode()).isEqualTo(QueueMode.ADAPTIVE);
    }

    /**
     * 줄이 남아 있어도 모드는 모드다. 여기가 특히 틀리기 쉽다 — 줄이 있는 쿠폰을
     * 전부 {@code OFF} 로 실으면 대기 응답의 모드가 사실이 아니게 된다.
     */
    @Test
    @DisplayName("줄이_남아도_모드를_그대로_싣는다")
    void 줄이_남아도_모드를_그대로_싣는다() {
        AllocationRound round = round(List.of(
                new CouponDemand("always", 500, 10_000, QueueMode.ALWAYS),
                new CouponDemand("off", 500, 10_000, QueueMode.OFF),
                new CouponDemand("adaptive", 500, 10_000, QueueMode.ADAPTIVE)), 10_000, 1);

        round.run().block();

        assertThat(발행된("always").mode()).isEqualTo(QueueMode.ALWAYS);
        assertThat(발행된("off").mode()).isEqualTo(QueueMode.OFF);
        assertThat(발행된("adaptive").mode()).isEqualTo(QueueMode.ADAPTIVE);
    }

    /** 모드를 안 적은 수요는 적응형으로 발행된다 — 정책이 없다는 것이 곧 기본값이다. */
    @Test
    @DisplayName("모드를_안_적은_수요는_적응형으로_실린다")
    void 모드를_안_적은_수요는_적응형으로_실린다() {
        AllocationRound round = round(List.of(new CouponDemand("c1", 0, 10_000)), 10_000, 1);

        round.run().block();

        assertThat(발행된("c1").mode()).isEqualTo(QueueMode.ADAPTIVE);
    }

    /** 몫이 대기자보다 적은 쪽도 지난다. 넉넉한 회차만 재면 QUEUEING 이 안 걸린다. */
    @Test
    @DisplayName("몫이_모자라도_모드를_그대로_싣는다")
    void 몫이_모자라도_모드를_그대로_싣는다() {
        AllocationRound round = round(
                List.of(new CouponDemand("always", 500, 10_000, QueueMode.ALWAYS)), 10, 1);

        round.run().block();

        assertThat(발행된("always").mode()).isEqualTo(QueueMode.ALWAYS);
        assertThat(발행된("always").runtime()).isEqualTo(RuntimeState.QUEUEING);
    }

    /** 매진돼도 모드는 남는다. 대기 응답이 이미 모드를 싣는다. */
    @Test
    @DisplayName("매진된_쿠폰도_모드를_싣는다")
    void 매진된_쿠폰도_모드를_싣는다() {
        AllocationRound round = round(
                List.of(new CouponDemand("off", 500, 0, QueueMode.OFF)), 10_000, 1);

        round.run().block();

        assertThat(발행된("off").mode()).isEqualTo(QueueMode.OFF);
        assertThat(발행된("off").runtime()).isEqualTo(RuntimeState.CLOSED);
    }

    /**
     * <b>발행 시각은 재료를 읽은 시각이다.</b> 회차가 끝난 시각으로 찍으면 스냅샷
     * 나이가 회차 지속 시간만큼 어리게 나온다. 그만큼 낡음 판정이 늦어지고, 이미
     * 늙은 대기 인원을 믿는 구간이 길어진다 — 그 구간이 추월 창이다 (불변식 4).
     */
    /** 리더 시계가 아니라 재료와 같이 온 시각이다. 리더가 옮겨 다녀도 안 흔들린다. */
    @Test
    @DisplayName("발행_시각은_재료를_읽은_시각이다")
    void 발행_시각은_재료를_읽은_시각이다() {
        // 회차가 도는 동안 시계가 흐른다. 적용 왕복이 쿠폰마다 순차로 일어난다.
        Iterator<Instant> 시계 = List.of(
                Instant.ofEpochSecond(읽은_시각 + 2), Instant.ofEpochSecond(읽은_시각 + 3),
                Instant.ofEpochSecond(읽은_시각 + 4)).iterator();
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(List.of(new CouponDemand("c1", 1_000, 10_000)), 읽은_시각)),
                () -> 100L, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> {
                    발행.put("last", hash);
                    return Mono.empty();
                },
                () -> 시계.hasNext() ? 시계.next() : Instant.ofEpochSecond(읽은_시각 + 9),
                () -> Mono.just(CreditSmoother.of(0.3)),
                SnapshotCodec.create(), () -> 0L);

        round.run().block();

        assertThat(SnapshotCodec.create().decode(발행.get("last")).publishedAt())
                .isEqualTo(Instant.ofEpochSecond(읽은_시각));
    }

    /**
     * <b>하한은 평활을 기다리지 않는다.</b> 하한은 관측이 아니라 정책이다 —
     * 평활을 거치면 앞선 낮은 값에서 올라오는 데 열 틱이 넘게 걸리고, 그동안
     * 노드당 몫이 유휴 비율 아래에 머물러 한산 통과 상한이 0 이다. 하한을 둔
     * 이유가 그 구간에서 사라진다 (R1).
     */
    @Test
    @DisplayName("하한은_평활에_묻히지_않는다")
    void 하한은_평활에_묻히지_않는다() {
        CreditSmoother smoother = CreditSmoother.of(0.3);
        // 앞선 회차가 뒷단의 정직한 0 으로 굳어 있다.
        smoother.observe(0);
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(List.of(new CouponDemand("c1", 1_000, 10_000)), 읽은_시각)),
                () -> 40L, () -> 8,
                grant -> Mono.just(grant.credit()),
                hash -> {
                    발행.put("last", hash);
                    return Mono.empty();
                },
                () -> Instant.ofEpochSecond(1_700_000_000L),
                () -> Mono.just(smoother),
                SnapshotCodec.create(),
                () -> 40L);

        round.run().block();

        // 평활만 거치면 12 다. 노드 여덟에 나누면 노드당 1, 유휴 상한은 0 이다.
        assertThat(SnapshotCodec.create().decode(발행.get("last")).meta().globalCredit())
                .isEqualTo(40);
    }

    @Test
    @DisplayName("평활화한_전역_크레딧을_싣는다")
    void 평활화한_전역_크레딧을_싣는다() {
        CreditSmoother smoother = CreditSmoother.of(0.5);
        smoother.observe(100);
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(List.of(new CouponDemand("c1", 1_000, 10_000)), 읽은_시각)),
                () -> 20L, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> {
                    발행.put("last", hash);
                    return Mono.empty();
                },
                () -> Instant.ofEpochSecond(1_700_000_000L),
                () -> Mono.just(smoother),
                SnapshotCodec.create(), () -> 0L);

        round.run().block();

        // 100 과 20 의 중간이다. 순간값을 그대로 쓰면 스파이크 한 번이 표시
        // 대기 시간을 몇 배로 만든다.
        assertThat(SnapshotCodec.create().decode(발행.get("last")).meta().globalCredit())
                .isEqualTo(60);
    }

    @Test
    @DisplayName("매진이면_종결로_발행한다")
    void 매진이면_종결로_발행한다() {
        // 이 전이를 안 만들면 매진 쿠폰이 줄 서는 중으로 남는다. 크레딧은 0 이라
        // 줄이 영영 안 빠지고, 신규는 큐가 찼다는 이유로 거절당한다.
        AllocationRound round = round(List.of(new CouponDemand("c1", 30, 0)), 10, 1);

        round.run().block();

        assertThat(발행된("c1").runtime()).isEqualTo(RuntimeState.CLOSED);
    }

    /**
     * <b>재고를 못 읽은 쿠폰을 종결로 발행하지 않는다.</b> 종결로 실으면 그
     * 쿠폰이 매진으로 읽히고, 정리가 유예 틱을 채운 뒤 큐를 지운다 — 자동으로
     * 안 낫는 오판이 되돌릴 수 없는 삭제가 된다 (3.1).
     */
    @Test
    @DisplayName("재고를_모르면_종결로_발행하지_않는다")
    void 재고를_모르면_종결로_발행하지_않는다() {
        // **운영자가 끈 쿠폰으로 잰다.** 적응형으로 재면 미상 갈래가 모드를
        // 버려도 안 드러난다 — 끈 쿠폰의 재고 키가 사라지면 그 쿠폰이 되살아나
        // 줄을 세운다.
        AllocationRound round = round(
                List.of(CouponDemand.stockUnknown("c1", 30, QueueMode.OFF)), 10, 1);

        round.run().block();

        CouponState 발행 = 발행된("c1");
        // **값으로 못 박는다.** 크레딧 10 에 대기 30 이라 답이 하나다.
        assertThat(발행.runtime()).isEqualTo(RuntimeState.QUEUEING);
        assertThat(발행.credit()).as("미상이라고 몫을 깎지 않는다").isEqualTo(10);
        assertThat(발행.mode()).as("운영자가 정한 모드가 한 틱을 넘는다")
                .isEqualTo(QueueMode.OFF);
        assertThat(발행.stockKnown()).as("모른다는 것이 선을 건넌다").isFalse();
        assertThat(발행.soldOut()).as("매진이 아니라야 종결도 삭제도 안 온다").isFalse();
    }

    /**
     * <b>서킷이 열리면 임계가 안 올라간다</b> (CY-787).
     *
     * <p>래퍼가 0 을 내는 것만 봐서는 못 잡는다. 그 값이 평활을 거치고 하한과
     * max 를 취하므로, 감싼 자리 뒤에서 두 번 샌다.
     */
    @Test
    @DisplayName("서킷이_열리면_임계가_안_올라간다")
    void 서킷이_열리면_임계가_안_올라간다() {
        AtomicReference<CircuitState> 서킷 = new AtomicReference<>(CircuitState.CLOSED);
        AllocationRound round = 서킷_있는_회차(서킷, 7_300, 40);

        // 정상 회차를 몇 번 돌려 평활을 채운다 — 그래야 0 이 들어와도 천천히
        // 내려오는 누수가 드러난다.
        for (int i = 0; i < 5; i++) {
            round.run().block();
        }
        적용.clear();
        서킷.set(CircuitState.OPEN);

        round.run().block();

        assertThat(적용).as("첫 회차부터 아무 몫도 안 나간다").isEmpty();
    }

    /** 하한이 걸려 있어도 안 나간다. 하한은 평활 뒤라 감싼 자리를 비켜 간다. */
    @Test
    @DisplayName("하한이_있어도_서킷이_열리면_안_나간다")
    void 하한이_있어도_서킷이_열리면_안_나간다() {
        AtomicReference<CircuitState> 서킷 = new AtomicReference<>(CircuitState.OPEN);
        AllocationRound round = 서킷_있는_회차(서킷, 0, 40);

        round.run().block();

        assertThat(적용).isEmpty();
    }

    /**
     * <b>반쯤 열렸으면 소량은 나간다.</b> 0 으로 막으면 뒷단에 닿는 호출이 없어
     * 서킷이 표본을 못 채우고, 반쯤 열림과 열림을 무한히 오간다 — 회복이 영영
     * 안 된다.
     */
    @Test
    @DisplayName("반쯤_열리면_소량은_나간다")
    void 반쯤_열리면_소량은_나간다() {
        AtomicReference<CircuitState> 서킷 = new AtomicReference<>(CircuitState.HALF_OPEN);
        AllocationRound round = 서킷_있는_회차(서킷, 7_300, 40);

        round.run().block();

        // 노드가 하나이므로 노드당 한 건 = 전역 한 건이다. 값으로 못 박아야
        // 소량이 조용히 커지는 것을 잡는다.
        assertThat(적용).as("표본이 나올 만큼은 나간다").containsExactly("c1=1");
        assertThat(발행된("c1").credit()).isEqualTo(1);
    }

    /**
     * <b>배분을 조인 사실이 로그로 남는다</b> (LG-2).
     *
     * <p>안 남기면 배분이 왜 멎었는지 알 방법이 서킷 로그뿐인데, 그건 리더가
     * 아닌 노드에서 날 수도 있다. 두 로그를 시각으로 맞춰 붙여야 한다.
     */
    @Test
    @DisplayName("배분을_조인_것이_쌍으로_남는다")
    void 배분을_조인_것이_쌍으로_남는다() {
        AtomicReference<CircuitState> 서킷 = new AtomicReference<>(CircuitState.OPEN);
        AllocationRound round = 서킷_있는_회차(서킷, 7_300, 40);

        round.run().block();
        round.run().block();
        assertThat(로그_메시지()).as("진입은 한 번만").filteredOn(m -> m.contains("배분을 조인다"))
                .hasSize(1);

        서킷.set(CircuitState.CLOSED);
        round.run().block();

        assertThat(로그_메시지()).as("해제도 남는다")
                .anyMatch(m -> m.contains("서킷 회복"));
    }

    /**
     * <b>조임이 풀리는 순간이 계단이다</b> (RC4).
     *
     * <p>평활은 조여진 값을 한 번도 안 본다 — 관측치는 서킷과 무관하게 계속
     * 원래 몫이다. 그래서 서킷이 닫히는 그 한 틱에 배분이 1 에서 원래 몫으로
     * 그대로 돌아간다. 방금 실패를 끝낸 뒷단이 그것을 받는다.
     */
    @Test
    @DisplayName("서킷이_닫혀도_한_번에_안_열린다")
    void 서킷이_닫혀도_한_번에_안_열린다() {
        AtomicReference<CircuitState> 서킷 = new AtomicReference<>(CircuitState.HALF_OPEN);
        AllocationRound round = 서킷_있는_회차(서킷, 7_300, 40);

        round.run().block();
        assertThat(발행된("c1").credit()).as("조이는 동안은 하나다").isEqualTo(1);
        적용.clear();

        서킷.set(CircuitState.CLOSED);
        round.run().block();

        // 하한 40 에서 다시 출발한다. 램프가 그 아래로 누르면 한산 통과 상한이
        // 0 이 되어, 줄 설 이유가 없는 쿠폰이 전 노드에서 줄을 선다 (R1).
        assertThat(발행된("c1").credit()).as("1 에서 7,300 으로 뛰지 않는다").isEqualTo(40);
    }

    /** 램프는 늦추는 것이지 막는 것이 아니다. 안 그러면 회복이 영영 안 끝난다. */
    @Test
    @DisplayName("램프는_원래_몫까지_올라간다")
    void 램프는_원래_몫까지_올라간다() {
        AtomicReference<CircuitState> 서킷 = new AtomicReference<>(CircuitState.HALF_OPEN);
        AllocationRound round = 서킷_있는_회차(서킷, 7_300, 40);
        round.run().block();
        서킷.set(CircuitState.CLOSED);

        long 앞선 = 40;
        int 틱 = 0;
        while (앞선 < 7_300 && 틱 < 40) {
            round.run().block();
            long 지금 = 발행된("c1").credit();
            assertThat(지금).as("한 회차에 배수를 넘지 않는다")
                    .isLessThanOrEqualTo(Math.max(40, (long) (앞선 * ReleaseRamp.DEFAULT_STEP)));
            앞선 = 지금;
            틱++;
        }

        assertThat(앞선).as("결국 원래 몫에 닿는다").isEqualTo(7_300);
        // 틱이 1초라 틱 수가 곧 초다. 회복은 30초 안에 끝나야 한다 (RC3).
        assertThat(틱).as("회복이 게이트 안에서 끝난다").isLessThanOrEqualTo(20);
    }

    /**
     * <b>리더가 바뀌어도 램프는 안 놓는다.</b> 브레이크라서 그렇다 — 모른다는
     * 것이 놓을 이유가 되면, 회복 도중에 승계가 끼는 순간 계단이 그대로
     * 복원된다. 그 순간은 드물지 않다: 회차 타임아웃과 레디스 압박이 겹치는
     * 구간이 곧 리더가 바뀌기 가장 쉬운 구간이다.
     */
    @Test
    @DisplayName("리더십을_얻어도_램프를_안_놓는다")
    void 리더십을_얻어도_램프를_안_놓는다() {
        AtomicReference<CircuitState> 서킷 = new AtomicReference<>(CircuitState.HALF_OPEN);
        AllocationRound round = 서킷_있는_회차(서킷, 7_300, 40);
        round.run().block();

        round.leadershipAcquired();
        서킷.set(CircuitState.CLOSED);
        round.run().block();

        assertThat(발행된("c1").credit()).as("승계가 계단을 되살리지 않는다").isEqualTo(40);
    }

    /**
     * <b>조임 창이 리더 승계에서 안 닫히고 있었다.</b> 노드 A 가 조임에 진입해
     * 경고를 찍고 리더십을 잃으면, 되찾은 뒤의 회복 로그가 비리더 구간까지
     * 포함한 지속 시간을 찍는다.
     */
    @Test
    @DisplayName("리더십을_얻으면_조임_창을_닫는다")
    void 리더십을_얻으면_조임_창을_닫는다() {
        AtomicReference<CircuitState> 서킷 = new AtomicReference<>(CircuitState.OPEN);
        AllocationRound round = 서킷_있는_회차(서킷, 7_300, 40);
        round.run().block();

        round.leadershipAcquired();
        서킷.set(CircuitState.CLOSED);
        round.run().block();

        assertThat(로그_메시지()).as("안 연 창을 닫았다고 적지 않는다")
                .noneMatch(m -> m.contains("서킷 회복"));
    }

    /**
     * <b>램프 구간에도 몫이 실제로 나가야 한다.</b> 공정 배분은 쿠폰 수보다
     * 크레딧이 적으면 전 쿠폰에 0 을 준다 — 램프가 그 구간을 한 틱에서 여러
     * 틱으로 늘리므로, 하한이 그 아래를 받쳐야 회복 구간이 안 멎는다.
     */
    @Test
    @DisplayName("램프_구간에도_쿠폰_여럿에_몫이_간다")
    void 램프_구간에도_쿠폰_여럿에_몫이_간다() {
        AtomicReference<CircuitState> 서킷 = new AtomicReference<>(CircuitState.HALF_OPEN);
        AllocationRound round = 서킷_있는_회차(서킷, 7_300, 40,
                List.of(new CouponDemand("c1", 20_000, 1_000_000),
                        new CouponDemand("c2", 20_000, 1_000_000),
                        new CouponDemand("c3", 20_000, 1_000_000)));
        round.run().block();
        적용.clear();

        서킷.set(CircuitState.CLOSED);
        round.run().block();

        assertThat(적용).as("셋 다 몫을 받는다").containsExactly("c1=13", "c2=13", "c3=13");
    }

    /**
     * <b>램프도 진입과 해제를 쌍으로 남긴다</b> (LG-2).
     *
     * <p>창을 램프가 걸린 시점에 열면 진입이 조임 시작에 찍히고, 해제의 지속
     * 시간에 장애 구간이 통째로 섞인다 — 서킷이 5분 열려 있었으면 여덟 틱짜리
     * 회복이 300틱으로 찍힌다. 정작 재려던 수가 그 수에 안 남는다.
     */
    @Test
    @DisplayName("램프_로그가_실제_회복_구간만_센다")
    void 램프_로그가_실제_회복_구간만_센다() {
        AtomicReference<CircuitState> 서킷 = new AtomicReference<>(CircuitState.OPEN);
        AllocationRound round = 서킷_있는_회차(서킷, 7_300, 40);
        for (int i = 0; i < 5; i++) {
            round.run().block();
        }
        assertThat(로그_메시지()).as("조인 동안에는 해제 램프가 안 뜬다")
                .noneMatch(m -> m.contains("서킷 해제 램프"));

        서킷.set(CircuitState.CLOSED);
        long 앞선 = 0;
        int 틱 = 0;
        while (앞선 < 7_300 && 틱 < 40) {
            round.run().block();
            앞선 = 발행된("c1").credit();
            틱++;
        }

        assertThat(로그_메시지()).as("진입은 실제로 푸는 회차에 한 번")
                .filteredOn(m -> m.startsWith("서킷 해제 램프 —")).hasSize(1);
        // 조인 다섯 회차가 이 수에 섞이면 여덟 틱짜리 회복이 열세 틱으로 찍힌다.
        assertThat(로그_인자("서킷 해제 램프 끝")[0])
                .as("해제가 센 틱은 조인 구간을 안 담는다").isEqualTo((long) (틱 - 1));
    }

    /** 초과 배분 지표는 게이트 전 값으로 잰다. 아니면 서킷이 열린 시간에 비례해 오른다. */
    @Test
    @DisplayName("배분_정지가_초과_지표를_안_올린다")
    void 배분_정지가_초과_지표를_안_올린다() {
        AtomicReference<CircuitState> 서킷 = new AtomicReference<>(CircuitState.OPEN);
        AllocationRound round = 서킷_있는_회차(서킷, 7_300, 40);

        round.run().block();

        assertThat(round.budgetOvershoot()).isZero();
    }

    /** 서킷을 보는 회차. 하한을 0 이 아니게 둬야 누수가 드러난다. */
    private AllocationRound 서킷_있는_회차(AtomicReference<CircuitState> 서킷,
            long 가용량, long 하한) {
        return 서킷_있는_회차(서킷, 가용량, 하한,
                List.of(new CouponDemand("c1", 20_000, 1_000_000)));
    }

    private AllocationRound 서킷_있는_회차(AtomicReference<CircuitState> 서킷,
            long 가용량, long 하한, List<CouponDemand> 수요) {
        return AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(수요, 읽은_시각)),
                () -> 가용량, () -> 1,
                grant -> {
                    적용.add(grant.couponId() + "=" + grant.credit());
                    return Mono.just(grant.credit());
                },
                hash -> {
                    발행.put("last", hash);
                    return Mono.empty();
                },
                () -> Instant.ofEpochSecond(읽은_시각),
                () -> Mono.just(CreditSmoother.of(1.0)),
                SnapshotCodec.create(), () -> 하한, Optional::empty,
                SoldOutCleanup.of(Integer.MAX_VALUE, new SimpleMeterRegistry()),
                ids -> Mono.just(List.of()), ids -> Mono.just(List.of()),
                안_걷는_스위퍼(), () -> false, 서킷::get);
    }

    /**
     * <b>미상은 아는 쿠폰과 같은 자리에서 나눈다.</b> 재고를 못 읽는 것이
     * 우대도 홀대도 아니다 — 미상은 "재고가 넉넉하다" 와 같은 요구를 낸다.
     *
     * <p>대가가 있다. 재고 키를 잃은 큰 줄은 실제로 매진이어도 살아 있는 것처럼
     * 몫을 가져가므로, 옆 쿠폰의 몫이 그만큼 준다. 값으로 못 박아 두지 않으면
     * 이것이 결정인지 사고인지 아무도 모른다.
     */
    @Test
    @DisplayName("미상은_아는_쿠폰과_몫을_나눠_가진다")
    void 미상은_아는_쿠폰과_몫을_나눠_가진다() {
        // **음성 대조가 대가를 보인다.** 균등 배분이라 미상의 줄 길이는 결과를
        // 안 바꾼다 — 접었을 때와 비교해야 얼마를 뺏겼는지가 드러난다.
        round(List.of(new CouponDemand("lost", 100_000, 0, QueueMode.ADAPTIVE),
                new CouponDemand("live", 100, 100, QueueMode.ADAPTIVE)), 100, 1)
                .run().block();
        assertThat(적용).as("접으면 산 쿠폰이 다 가져간다").containsExactly("live=100");
        적용.clear();

        AllocationRound round = round(List.of(
                CouponDemand.stockUnknown("lost", 100_000, QueueMode.ADAPTIVE),
                new CouponDemand("live", 100, 100, QueueMode.ADAPTIVE)), 100, 1);

        round.run().block();

        assertThat(적용).as("안 접으면 절반으로 준다").containsExactlyInAnyOrder("lost=50", "live=50");
    }

    /**
     * <b>미상인 줄의 폴링은 예산에 든다.</b> 접혀 있을 때 그 사람들은 종료를
     * 받고 폴링을 멈췄다. 안 접으면 계속 폴링하므로 예산이 그만큼 커지고,
     * 배수가 올라 전원의 간격이 늘어난다.
     *
     * <p>이것은 고칠 회귀가 아니라 <b>10만 명을 안 끊은 값</b>이다. 예산에서
     * 빼면 실제로 폴링하는 사람을 안 세는 것이라 레디스가 그만큼 더 맞는다.
     */
    @Test
    @DisplayName("미상인_줄의_폴링도_예산에_든다")
    void 미상인_줄의_폴링도_예산에_든다() {
        AllocationRound 접힌_회차 = round(
                List.of(new CouponDemand("lost", 100_000, 0, QueueMode.ADAPTIVE)), 100, 1);
        접힌_회차.run().block();
        double 접었을_때 = 발행된_배수();

        AllocationRound 미상_회차 = round(
                List.of(CouponDemand.stockUnknown("lost", 100_000, QueueMode.ADAPTIVE)), 100, 1);
        미상_회차.run().block();

        assertThat(접었을_때).as("종료를 받은 줄은 안 센다").isEqualTo(1.0);
        // 밴드별로 500 + 2500/3 + 900 + 88000/30 = 5166.67 rps, 예산 200.
        // **값으로 못 박는다.** 부등호로 두면 미상의 배수율을 0 으로 넣는
        // 구현도 통과하는데, 이 수가 전원의 폴링 간격에 그대로 곱해진다.
        assertThat(발행된_배수()).as("안 끊었으니 그 폴링이 예산에 든다")
                .isCloseTo(25.833, within(0.001));
    }

    /**
     * <b>미상은 정리 대상이 아니다.</b> 발행 값만 보면 정리가 실제로 무엇을
     * 지우는지 못 잰다 — 정리는 코덱을 안 거친 리더 메모리의 상태를 받는다.
     */
    @Test
    @DisplayName("미상인_쿠폰의_줄은_안_지운다")
    void 미상인_쿠폰의_줄은_안_지운다() {
        List<String> 지운_쿠폰 = new ArrayList<>();
        AllocationRound round = 정리하는_회차(
                List.of(CouponDemand.stockUnknown("lost", 30, QueueMode.ADAPTIVE),
                        new CouponDemand("gone", 30, 0, QueueMode.ADAPTIVE)),
                지운_쿠폰);

        // 유예 틱이 1 이라 한 회차로는 아무것도 안 지운다. 두 회차를 돌려야
        // "미상이라서 안 지웠다" 와 "아직 유예 중이라 안 지웠다" 가 갈린다.
        round.run().block();
        round.run().block();

        assertThat(지운_쿠폰).as("매진만 지우고 미상은 안 지운다").containsExactly("gone");
    }

    /** 유예 틱 1 짜리 정리를 붙인 회차. 지운 쿠폰을 받아 적는다. */
    private AllocationRound 정리하는_회차(List<CouponDemand> 수요, List<String> 지운_쿠폰) {
        return AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(수요, 읽은_시각)),
                () -> 100, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> {
                    발행.put("last", hash);
                    return Mono.empty();
                },
                () -> Instant.ofEpochSecond(읽은_시각),
                () -> Mono.just(CreditSmoother.of(1.0)),
                SnapshotCodec.create(), () -> 0L, Optional::empty,
                SoldOutCleanup.of(1, new SimpleMeterRegistry()),
                ids -> {
                    지운_쿠폰.addAll(ids);
                    return Mono.just(List.copyOf(ids));
                },
                ids -> Mono.just(List.of()),
                안_걷는_스위퍼(), () -> false, () -> CircuitState.CLOSED);
    }

    /**
     * 미상은 계측된다. 안 세면 재고 키를 잃은 것을 아무도 모른다 — 판정이
     * 조용히 비켜 가는 종류라 사건 자체가 안 드러난다.
     */
    @Test
    @DisplayName("재고를_모르면_계측한다")
    void 재고를_모르면_계측한다() {
        AllocationRound round = round(List.of(
                CouponDemand.stockUnknown("c1", 30, QueueMode.ADAPTIVE),
                CouponDemand.stockUnknown("c2", 30, QueueMode.ADAPTIVE),
                new CouponDemand("c3", 30, 100)), 10, 1);
        SimpleMeterRegistry 계기 = new SimpleMeterRegistry();
        // **반환값을 붙잡는다.** 버리면 GC 뒤에 계수가 0 으로 굳는다 — 로컬에서는
        // 안 나고 세 계층을 한 JVM 에 돌리는 CI 에서만 났다.
        InvariantMetrics 지표 = InvariantMetrics.bind(round, ClockSkewTracker.create(), 계기);

        // **두 회차를 돌린다.** 한 회차만 보면 누적과 대입이 구분이 안 되고, 미상이
        // 둘인데 하나만 세는 구현도 통과한다. 아는 쿠폰을 같이 둬야 회차마다
        // 한 번이라는 것까지 잡힌다 — 상태를 만드는 자리에서 세면 정리·청소·
        // 발행이 같은 회차를 세 번 훑어 셋이 된다.
        round.run().block();
        round.run().block();
        // **GC 를 강제한다.** 이것이 없으면 이 시험은 장비와 부하에 따라 갈린다.
        // 실제로 로컬은 초록이고 CI 만 빨갰다.
        System.gc();

        assertThat(계기.get("waiting.allocation.stock.unknown.ticks").functionCounter().count())
                .isEqualTo(4);
        // **여기까지 살아 있어야 한다.** 단언 뒤에 두는 것이 핵심이다 — 변수만
        // 두면 JIT 이 마지막 사용 뒤로 수거를 앞당길 수 있다.
        Reference.reachabilityFence(지표);
    }

    @Test
    @DisplayName("매진에_줄도_없으면_한산이다")
    void 매진에_줄도_없으면_한산이다() {
        AllocationRound round = round(List.of(new CouponDemand("c1", 0, 0)), 10, 1);

        round.run().block();

        assertThat(발행된("c1").runtime()).isEqualTo(RuntimeState.IDLE);
    }

    @Test
    @DisplayName("적용_뒤에_리더십을_잃으면_발행을_안_한다")
    void 적용_뒤에_리더십을_잃으면_발행을_안_한다() {
        // 적용 루프가 한 틱을 꽉 채우면 그 사이 다음 리더가 자기 회차를 돈다.
        // 회차 시작에서만 보면 둘이 같은 키에 쓴다.
        AtomicBoolean 리더 = new AtomicBoolean(true);
        AllocationRound round = AllocationRound.of(
                리더::get,
                () -> Mono.just(new TimedDemands(List.of(new CouponDemand("c1", 10, 100)), 읽은_시각)),
                () -> 8L, () -> 1,
                grant -> {
                    적용.add(grant.couponId());
                    리더.set(false);
                    return Mono.just(grant.credit());
                },
                hash -> {
                    발행.put("last", hash);
                    return Mono.empty();
                },
                () -> Instant.ofEpochSecond(1_700_000_000L),
                () -> Mono.just(CreditSmoother.of(1.0)),
                SnapshotCodec.create(), () -> 0L);

        round.run().block();

        assertThat(적용).containsExactly("c1");
        assertThat(발행).isEmpty();
        // **셈도 같이 멎어야 한다.** 발행을 안 하는 회차에서 배수의 계산이 돌면
        // 이 노드는 걸지도 않은 배수를 걸었다고 기록한다. 지금은 삼항의
        // 짧은-회로가 그것을 막지만, 셈이 재료를 만드는 자리로 다시 들어가면
        // 그 보호가 사라진다 — 그때 여기가 붉어진다.
        assertThat(round.pollBudgetOvershootTicks()).as("안 발행한 회차는 안 센다")
                .isZero();
    }

    @Test
    @DisplayName("평활화를_한_번만_이월받는다")
    void 평활화를_한_번만_이월받는다() {
        // 매 회차 받으면 이 리더가 다듬어 온 값을 자기가 방금 쓴 값으로 덮어,
        // 평활화가 아무 일도 안 하게 된다.
        AtomicInteger 이월 = new AtomicInteger();
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(List.of(new CouponDemand("c1", 10, 100)), 읽은_시각)),
                () -> 8L, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> {
                    발행.put("last", hash);
                    return Mono.empty();
                },
                () -> Instant.ofEpochSecond(1_700_000_000L),
                () -> Mono.fromSupplier(() -> {
                    이월.incrementAndGet();
                    return CreditSmoother.of(1.0);
                }),
                SnapshotCodec.create(), () -> 0L);

        round.run().block();
        round.run().block();
        round.run().block();

        assertThat(이월).hasValue(1);
    }

    @Test
    @DisplayName("이월을_못_받아도_회차는_돈다")
    void 이월을_못_받아도_회차는_돈다() {
        // 이월은 있으면 좋은 것이지 배분의 전제가 아니다. 여기서 멈추면
        // 레디스가 흔들릴 때 배분이 통째로 안 시작한다.
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(List.of(new CouponDemand("c1", 10, 100)), 읽은_시각)),
                () -> 8L, () -> 1,
                grant -> {
                    적용.add(grant.couponId());
                    return Mono.just(grant.credit());
                },
                hash -> {
                    발행.put("last", hash);
                    return Mono.empty();
                },
                () -> Instant.ofEpochSecond(1_700_000_000L),
                () -> Mono.error(new IllegalStateException("끊겼다")),
                SnapshotCodec.create(), () -> 0L);

        round.run().block();

        assertThat(적용).containsExactly("c1");
    }

    @Test
    @DisplayName("노드_수를_같이_싣는다")
    void 노드_수를_같이_싣는다() {
        AllocationRound round = round(List.of(new CouponDemand("c1", 10, 100)), 8, 7);

        round.run().block();

        assertThat(SnapshotCodec.create().decode(발행.get("last")).meta().gatewayCount())
                .isEqualTo(7);
    }

    @Test
    @DisplayName("몫이_없는_쿠폰은_안_건드린다")
    void 몫이_없는_쿠폰은_안_건드린다() {
        // 한산한 쿠폰이 수백 개면 그만큼 왕복이 늘어 틱이 밀린다. 임계는 어차피
        // 안 움직이므로 물어볼 이유가 없다.
        AllocationRound round = round(
                List.of(new CouponDemand("c1", 10, 100), new CouponDemand("c2", 0, 100)), 4, 1);

        round.run().block();

        assertThat(적용).containsExactly("c1=4");
    }

    @Test
    @DisplayName("들인_인원을_남긴다")
    void 들인_인원을_남긴다() {
        // 나눠 준 수와 실제로 들어온 수는 다르다. 큐가 몫보다 짧으면 남고,
        // 적용이 실패하면 0 이다. 안 남기면 크레딧이 어디서 새는지 못 가린다.
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(List.of(new CouponDemand("c1", 10, 100)), 읽은_시각)),
                () -> 8L, () -> 1,
                grant -> Mono.just(3L),
                hash -> {
                    발행.put("last", hash);
                    return Mono.empty();
                },
                () -> Instant.ofEpochSecond(1_700_000_000L),
                () -> Mono.just(CreditSmoother.of(1.0)),
                SnapshotCodec.create(), () -> 0L);

        round.run().block();

        assertThat(로그_메시지()).anyMatch(m -> m.contains("들인 인원"));
        assertThat(로그_인자("배분 한 회차")).satisfies(인자 -> assertThat(인자[1]).isEqualTo(3L));
    }

    @Test
    @DisplayName("회차_도중에_리더십을_잃으면_안_쓴다")
    void 회차_도중에_리더십을_잃으면_안_쓴다() {
        // 리스가 10ms 남은 상태로 시작한 회차는 한 틱을 꽉 채워 돌고, 그 사이
        // 다음 리더가 자기 회차를 돈다. 회차 시작에서만 보면 둘이 같은 키에 쓴다.
        AtomicBoolean 리더 = new AtomicBoolean(true);
        AllocationRound round = AllocationRound.of(
                리더::get,
                () -> Mono.fromSupplier(() -> {
                    리더.set(false);
                    return new TimedDemands(List.of(new CouponDemand("c1", 10, 100)), 읽은_시각);
                }),
                () -> 8L, () -> 1,
                grant -> {
                    적용.add(grant.couponId());
                    return Mono.just(grant.credit());
                },
                hash -> {
                    발행.put("last", hash);
                    return Mono.empty();
                },
                () -> Instant.ofEpochSecond(1_700_000_000L),
                () -> Mono.just(CreditSmoother.of(1.0)),
                SnapshotCodec.create(), () -> 0L);

        round.run().block();

        assertThat(적용).isEmpty();
        assertThat(발행).isEmpty();
    }

    /**
     * <b>쿠폰 사이에서 잃는 것이 실제 모습이다.</b> 회차 진입에서만 보면, 첫 쿠폰을
     * 쓰는 동안 리스가 끝난 회차가 남은 쿠폰에 계속 임계를 쓴다.
     *
     * <p>둘째 쿠폰에 임계를 쓰면 새 리더가 쓴 값을 덮고, 발행까지 나가면 새 리더가
     * 이미 나눠 준 크레딧을 스냅샷이 한 번 더 광고한다 — 불변식 2 다.
     */
    @Test
    @DisplayName("쿠폰_사이에서_잃으면_남은_몫이_0이_된다")
    void 쿠폰_사이에서_잃으면_남은_몫이_0이_된다() {
        AtomicBoolean 리더 = new AtomicBoolean(true);
        AllocationRound round = AllocationRound.of(
                리더::get,
                () -> Mono.just(new TimedDemands(
                        List.of(new CouponDemand("c1", 10, 100),
                                new CouponDemand("c2", 10, 100)),
                        읽은_시각)),
                () -> 8L, () -> 1,
                grant -> {
                    // 첫 쿠폰을 쓰는 순간 리스가 끝난다.
                    리더.set(false);
                    적용.add(grant.couponId());
                    return Mono.just(grant.credit());
                },
                hash -> {
                    발행.put("last", hash);
                    return Mono.empty();
                },
                () -> Instant.ofEpochSecond(1_700_000_000L),
                () -> Mono.just(CreditSmoother.of(1.0)),
                SnapshotCodec.create(), () -> 0L);

        round.run().block();

        // 옛 리더가 둘째 쿠폰의 임계를 쓰면 새 리더가 쓴 값을 덮는다.
        assertThat(적용).containsExactly("c1");
        // 발행도 안 나간다. 나갔으면 새 리더가 나눠 준 몫을 한 번 더 광고한다.
        assertThat(발행).isEmpty();
    }

    @Test
    @DisplayName("대상이_없어도_발행은_한다")
    void 대상이_없어도_발행은_한다() {
        // 발행을 건너뛰면 낡음 판정이 스케줄러가 멎은 것으로 본다. 대상이 없는
        // 것과 스케줄러가 죽은 것은 다르다.
        AllocationRound round = round(List.of(), 10, 1);

        round.run().block();

        assertThat(발행).containsKey("last");
    }
}
