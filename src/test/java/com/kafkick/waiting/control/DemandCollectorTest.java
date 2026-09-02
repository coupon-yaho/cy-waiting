package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.coupon.QueueMode;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * 이번 틱의 수요를 모은다.
 *
 * <p><b>{@code waiting} 을 한 번만 읽는다.</b> 크레딧을 산출한 뒤 다시 읽으면 그
 * 사이에 사람이 빠져, 도메인이 막는 조합이 발행된다. 코덱이 그 쿠폰만 떨구고,
 * 떨어진 쿠폰은 판정에서 없는 쿠폰 — 즉 매진으로 보인다. 정상 동작이 전면
 * 차단으로 번지는 경로다.
 */
class DemandCollectorTest {

    /** 재료를 읽은 시각. 발행 시각이 여기서 나온다. */
    private static final long 읽은_시각 = 1_700_000_000L;

    private DemandCollector collector(List<String> 쿠폰, Map<String, List<Long>> 큐,
            Map<String, Long> 재고) {
        return collector(쿠폰, 큐, 재고, Map.of());
    }

    private DemandCollector collector(List<String> 쿠폰, Map<String, List<Long>> 큐,
            Map<String, Long> 재고, Map<String, QueueMode> 정책) {
        return DemandCollector.of(
                () -> Mono.just(new TimedCoupons(쿠폰, 읽은_시각)),
                ids -> Mono.just(ids.stream().collect(Collectors.toMap(
                        id -> id, id -> 큐.get(id).stream().mapToLong(Long::longValue).sum()))),
                ids -> Mono.just(재고),
                ids -> Mono.just(정책));
    }

    /**
     * <b>운영자 정책이 여기서 안 실리면 판정에 닿을 길이 없다.</b> 사다리에 분기가
     * 있어도 유일한 발행자가 그 입력을 못 만들면 없는 것과 같다.
     */
    @Test
    @DisplayName("운영자_정책을_수요에_싣는다")
    void 운영자_정책을_수요에_싣는다() {
        DemandCollector collector = collector(List.of("c1", "c2"),
                Map.of("c1", List.of(10L), "c2", List.of(0L)),
                Map.of("c1", 100L, "c2", 100L),
                Map.of("c1", QueueMode.ALWAYS));

        List<CouponDemand> 수요 = collector.collect().block(Duration.ofSeconds(5)).demands();

        assertThat(수요).extracting(CouponDemand::couponId, CouponDemand::mode)
                .containsExactly(
                        tuple("c1", QueueMode.ALWAYS),
                        // 정책을 안 건 쿠폰은 적응형이다.
                        tuple("c2", QueueMode.ADAPTIVE));
    }

    @Test
    @DisplayName("대기와_재고를_한_쿠폰으로_묶는다")
    void 대기와_재고를_한_쿠폰으로_묶는다() {
        DemandCollector collector = collector(List.of("c1"),
                Map.of("c1", List.of(3L, 5L, 0L, 2L)), Map.of("c1", 100L));

        List<CouponDemand> 수요 = collector.collect().block().demands();

        assertThat(수요).singleElement()
                .satisfies(d -> {
                    assertThat(d.couponId()).isEqualTo("c1");
                    assertThat(d.waiting()).isEqualTo(10);
                    assertThat(d.stock()).isEqualTo(100);
                });
    }

    @Test
    @DisplayName("쿠폰마다_제_대기와_제_재고를_받는다")
    void 쿠폰마다_제_대기와_제_재고를_받는다() {
        // 짝이 어긋나면 A 의 대기가 B 의 재고와 붙는다. 그 조합은 도메인이
        // 막지 않으므로 조용히 틀린 배분이 나간다.
        DemandCollector collector = collector(List.of("c1", "c2"),
                Map.of("c1", List.of(1L, 1L, 1L, 1L), "c2", List.of(10L, 0L, 0L, 0L)),
                Map.of("c1", 50L, "c2", 7L));

        List<CouponDemand> 수요 = collector.collect().block().demands();

        assertThat(수요).extracting(CouponDemand::couponId, CouponDemand::waiting,
                        CouponDemand::stock)
                .containsExactly(Tuple.tuple("c1", 4L, 50L),
                        Tuple.tuple("c2", 10L, 7L));
    }

    /**
     * <b>재고를 못 읽은 것을 0 으로 접지 않는다.</b>
     *
     * <p>접으면 재고 키를 잃은 쿠폰이 매진으로 보이고, 줄에 사람이 남아 있어도
     * 종결된다. 재고가 실제로 돌아오는 것이 아니라서 다음 스냅샷도 이것을
     * 안 되돌린다 — 자동으로 안 낫는 유일한 오판 방향이다 (3.1 절).
     */
    // **초과 발급으로 가지 않는다.** 게이트웨이는 재고를 안 깎는다. 진짜 상한은
    // 뒷단이 원자적으로 지키고, 넘겨 보낸 몫은 409 로 돌아와 매진 관찰이
    // 받는다 — 뒷단이 판단하게 두는 것이 게이트웨이가 찍는 것보다 낫다.
    @Test
    @DisplayName("재고를_못_읽으면_미상으로_싣는다")
    void 재고를_못_읽으면_미상으로_싣는다() {
        DemandCollector collector = collector(List.of("c1"),
                Map.of("c1", List.of(9L, 0L, 0L, 0L)), Collections.emptyMap());

        List<CouponDemand> 수요 = collector.collect().block().demands();

        assertThat(수요).singleElement().satisfies(d -> {
            assertThat(d.stockKnown()).as("모른다는 것이 값으로 남는다").isFalse();
            assertThat(d.stock()).as("0 이면 매진과 같아진다").isNotEqualTo(0L);
        });
    }

    /**
     * <b>재고 키의 음수는 미상이 아니다.</b> 그 값은 발급 계층이 소유하고,
     * 차감이 0 을 지나치면 실제로 음수가 된다. 게이트웨이의 미상 표시와 값이
     * 겹친다고 그것을 미상으로 읽으면, 이 티켓이 막으려던 종결이 반대 방향에서
     * 그대로 돌아온다 — 다 팔린 줄이 영영 안 닫힌다.
     */
    @Test
    @DisplayName("재고_키의_음수는_매진으로_본다")
    void 재고_키의_음수는_매진으로_본다() {
        DemandCollector collector = collector(List.of("c1"),
                Map.of("c1", List.of(9L, 0L, 0L, 0L)), Map.of("c1", -1L));

        List<CouponDemand> 수요 = collector.collect().block().demands();

        assertThat(수요).singleElement().satisfies(d -> {
            assertThat(d.stockKnown()).as("읽었으면 아는 것이다").isTrue();
            assertThat(d.stock()).as("음수는 다 팔린 것으로 본다").isZero();
        });
    }

    /** 읽은 0 은 그대로 0 이다. 미상을 들이면서 진짜 매진이 흐려지면 안 된다. */
    @Test
    @DisplayName("읽은_재고_0은_그대로_매진이다")
    void 읽은_재고_0은_그대로_매진이다() {
        DemandCollector collector = collector(List.of("c1"),
                Map.of("c1", List.of(9L, 0L, 0L, 0L)), Map.of("c1", 0L));

        List<CouponDemand> 수요 = collector.collect().block().demands();

        assertThat(수요).singleElement().satisfies(d -> {
            assertThat(d.stockKnown()).isTrue();
            assertThat(d.stock()).isZero();
        });
    }

    @Test
    @DisplayName("대기를_모르면_없는_것으로_본다")
    void 대기를_모르면_없는_것으로_본다() {
        // 값이 안 오는 것과 0 인 것은 같다. 모르는 쪽을 크게 잡으면 초과 배분이다.
        DemandCollector collector = DemandCollector.of(
                () -> Mono.just(new TimedCoupons(List.of("c1"), 읽은_시각)),
                ids -> Mono.just(Collections.singletonMap("c1", null)),
                ids -> Mono.just(Map.of("c1", 100L)),
                ids -> Mono.just(Map.of()));

        List<CouponDemand> 수요 = collector.collect().block().demands();

        assertThat(수요).singleElement().satisfies(d -> assertThat(d.waiting()).isZero());
    }

    @Test
    @DisplayName("배분_대상이_없으면_아무것도_안_묻는다")
    void 배분_대상이_없으면_아무것도_안_묻는다() {
        // 대상이 없는데 빈 인자로 명령을 보내면 레디스가 오류를 낸다. 그 오류가
        // 회차를 죽이면 대상이 생겨도 배분이 안 돈다.
        AtomicInteger 조회 = new AtomicInteger();
        DemandCollector collector = DemandCollector.of(
                () -> Mono.just(new TimedCoupons(List.of(), 읽은_시각)),
                ids -> {
                    조회.incrementAndGet();
                    return Mono.just(Map.of());
                },
                ids -> {
                    조회.incrementAndGet();
                    return Mono.just(Map.of());
                },
                ids -> {
                    조회.incrementAndGet();
                    return Mono.just(Map.of());
                });

        List<CouponDemand> 수요 = collector.collect().block().demands();

        assertThat(수요).isEmpty();
        assertThat(조회).hasValue(0);
    }

    @Test
    @DisplayName("길이가_안_맞으면_회차를_버린다")
    void 길이가_안_맞으면_회차를_버린다() {
        // 빠진 자리를 0 으로 채우면 대기가 0 인 쿠폰이 되어 크레딧이 안 나간다.
        // 줄 선 사람이 통째로 멈추는데 아무 신호도 없다.
        //
        // 기대값이 없으면 어긋난 정도를 모른다.
        DemandCollector collector = DemandCollector.of(
                () -> Mono.just(new TimedCoupons(List.of("c1", "c2"), 읽은_시각)),
                ids -> Mono.just(Map.of("c1", 4L)),
                ids -> Mono.just(Map.of("c1", 10L, "c2", 10L)),
                ids -> Mono.just(Map.of()));

        assertThatThrownBy(() -> collector.collect().block())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("대기").hasMessageContaining("기대=2");
    }

}
