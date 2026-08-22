package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

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

    @BeforeEach
    void 로그를_받는다() {
        로그 = new ListAppender<>();
        로그.start();
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(AllocationRound.class))
                .addAppender(로그);
    }

    @AfterEach
    void 로그를_뗀다() {
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(AllocationRound.class))
                .detachAppender(로그);
    }

    private final Map<String, Map<String, String>> 발행 = new LinkedHashMap<>();

    private AllocationRound round(List<CouponDemand> 수요, long 전역_크레딧, int 노드_수) {
        return AllocationRound.of(
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
                CreditSmoother.of(1.0),
                SnapshotCodec.create());
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
                CreditSmoother.of(1.0),
                SnapshotCodec.create());

        round.run().block();

        assertThat(발행.get("last")).containsKeys("c1", "c2");
        // 어느 쿠폰이 실패했는지 안 남기면, 임계가 안 움직인 이유를 사후에 못 찾는다.
        assertThat(로그_인자("배분 적용 실패")[0]).isEqualTo("c1");
    }

    @Test
    @DisplayName("평활화한_전역_크레딧을_싣는다")
    void 평활화한_전역_크레딧을_싣는다() {
        CreditSmoother smoother = CreditSmoother.of(0.5);
        smoother.observe(100);
        AllocationRound round = AllocationRound.of(
                () -> Mono.just(List.of(new CouponDemand("c1", 1_000, 10_000))),
                () -> 20L, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> {
                    발행.put("last", hash);
                    return Mono.empty();
                },
                () -> Instant.ofEpochSecond(1_700_000_000L),
                smoother,
                SnapshotCodec.create());

        round.run().block();

        // 100 과 20 의 중간이다. 순간값을 그대로 쓰면 스파이크 한 번이 표시
        // 대기 시간을 몇 배로 만든다.
        assertThat(SnapshotCodec.create().decode(발행.get("last")).meta().globalCredit())
                .isEqualTo(60);
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
                () -> Mono.just(List.of(new CouponDemand("c1", 10, 100))),
                () -> 8L, () -> 1,
                grant -> Mono.just(3L),
                hash -> {
                    발행.put("last", hash);
                    return Mono.empty();
                },
                () -> Instant.ofEpochSecond(1_700_000_000L),
                CreditSmoother.of(1.0),
                SnapshotCodec.create());

        round.run().block();

        assertThat(로그_메시지()).anyMatch(m -> m.contains("들인 인원"));
        assertThat(로그_인자("배분 한 판")).satisfies(인자 -> assertThat(인자[1]).isEqualTo(3L));
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
