package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.RuntimeState;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * 한 판. 수요를 모아 크레딧을 나누고 적용한 뒤 발행한다.
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
        // 판마다 세는 값은 지표 자리라 낮은 수준으로 찍는다. 시험은 그걸 봐야 한다.
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

    private final Map<String, Map<String, String>> 발행 = new LinkedHashMap<>();

    private AllocationRound round(List<CouponDemand> 수요, long 전역_크레딧, int 노드_수) {
        return AllocationRound.of(
                () -> true,
                () -> Mono.just(수요),
                () -> 전역_크레딧,
                () -> 노드_수,
                grant -> {
                    적용.add(grant.couponId() + "=" + grant.credit());
                    return Mono.just(grant.credit());
                },
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

    private CouponState 발행된(String couponId) {
        return SnapshotCodec.create().decode(발행.get("last")).coupons().get(couponId);
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
                () -> Mono.just(List.of(new CouponDemand("c1", 5, 100),
                        new CouponDemand("c2", 5, 100))),
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
        // **한 쿠폰이 실패하고 다른 쿠폰이 성공한 판은 걷힌 것이 아니다.**
        // 여기서 복귀를 찍으면 실패도 복귀도 아닌 두 줄이 매 판 반복된다.
        assertThat(로그_메시지()).noneMatch(m -> m.contains("배분 적용 복귀"));
        // 임계가 안 올라간 쿠폰에 몫을 실으면 노드들이 일어나지 않은 배수율로
        // 대기 시간을 계산한다.
        assertThat(발행된("c1").credit()).isZero();
        assertThat(발행된("c2").credit()).isEqualTo(5);
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
        // 앞선 판이 뒷단의 정직한 0 으로 굳어 있다.
        smoother.observe(0);
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(List.of(new CouponDemand("c1", 1_000, 10_000))),
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
                () -> Mono.just(List.of(new CouponDemand("c1", 1_000, 10_000))),
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
        // 적용 루프가 한 틱을 꽉 채우면 그 사이 다음 리더가 자기 판을 돈다.
        // 판 시작에서만 보면 둘이 같은 키에 쓴다.
        AtomicBoolean 리더 = new AtomicBoolean(true);
        AllocationRound round = AllocationRound.of(
                리더::get,
                () -> Mono.just(List.of(new CouponDemand("c1", 10, 100))),
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
    }

    @Test
    @DisplayName("평활화를_한_번만_이월받는다")
    void 평활화를_한_번만_이월받는다() {
        // 매 판 받으면 이 리더가 다듬어 온 값을 자기가 방금 쓴 값으로 덮어,
        // 평활화가 아무 일도 안 하게 된다.
        AtomicInteger 이월 = new AtomicInteger();
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(List.of(new CouponDemand("c1", 10, 100))),
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
    @DisplayName("이월을_못_받아도_판은_돈다")
    void 이월을_못_받아도_판은_돈다() {
        // 이월은 있으면 좋은 것이지 배분의 전제가 아니다. 여기서 멈추면
        // 레디스가 흔들릴 때 배분이 통째로 안 시작한다.
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(List.of(new CouponDemand("c1", 10, 100))),
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
                () -> Mono.just(List.of(new CouponDemand("c1", 10, 100))),
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
        assertThat(로그_인자("배분 한 판")).satisfies(인자 -> assertThat(인자[1]).isEqualTo(3L));
    }

    @Test
    @DisplayName("판_도중에_리더십을_잃으면_안_쓴다")
    void 판_도중에_리더십을_잃으면_안_쓴다() {
        // 리스가 10ms 남은 상태로 시작한 판은 한 틱을 꽉 채워 돌고, 그 사이
        // 다음 리더가 자기 판을 돈다. 판 시작에서만 보면 둘이 같은 키에 쓴다.
        AtomicBoolean 리더 = new AtomicBoolean(true);
        AllocationRound round = AllocationRound.of(
                리더::get,
                () -> Mono.fromSupplier(() -> {
                    리더.set(false);
                    return List.of(new CouponDemand("c1", 10, 100));
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
