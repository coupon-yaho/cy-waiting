package com.kafkick.waiting.domain.allocation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 공정 배분 — 기아 불가와 유휴 낭비 0 을 동시에 만족시켜야 한다.
 *
 * <p>균등하게만 나누면 한산한 쿠폰이 못 쓰고 남긴 몫이 버려지고, 요구량 비례로만
 * 나누면 몰리는 쿠폰 하나가 전부 가져가 나머지가 굶는다.
 */
class FairShareAllocatorTest {

    private Map<String, Long> allocate(long globalCredit, CouponDemand... demands) {
        return FairShareAllocator.allocate(globalCredit, List.of(demands)).stream()
                .collect(Collectors.toMap(Grant::couponId, Grant::credit));
    }

    @Test
    @DisplayName("균등_배분_후_남은_몫을_재배분한다")
    void 균등_배분_후_남은_몫을_재배분한다() {
        // 1패스: 1000/3 = 333 씩. 콜드는 40·3 만 쓰고 624 를 남긴다.
        // 2패스: 남은 624 가 아직 굶주린 핫으로 간다 → 333 + 624 = 957.
        // 낭비 0 이다 — 957 + 40 + 3 = 1000.
        Map<String, Long> granted = allocate(1000,
                new CouponDemand("hot", 200_000, 200_000),
                new CouponDemand("cold1", 40, 1000),
                new CouponDemand("cold2", 3, 1000));

        assertThat(granted).containsExactlyInAnyOrderEntriesOf(
                Map.of("hot", 957L, "cold1", 40L, "cold2", 3L));
    }

    @Test
    @DisplayName("핫에_20만이_밀려도_콜드는_첫_틱에_전부_빠진다")
    void 핫에_20만이_밀려도_콜드는_첫_틱에_전부_빠진다() {
        // 기아 불가. 1패스가 균등이라 콜드는 자기 요구량을 첫 틱에 다 받는다.
        Map<String, Long> granted = allocate(1000,
                new CouponDemand("hot", 200_000, 200_000),
                new CouponDemand("cold1", 40, 1000),
                new CouponDemand("cold2", 3, 1000));

        assertThat(granted.get("cold1")).isEqualTo(40);
        assertThat(granted.get("cold2")).isEqualTo(3);
    }

    @Test
    @DisplayName("정수_나눗셈_나머지는_다음_틱으로_넘긴다")
    void 정수_나눗셈_나머지는_다음_틱으로_넘긴다() {
        // 10 을 셋이 나누면 3 씩이고 1 이 남는다. 남는 1 을 누구에게 주면
        // 그 쿠폰만 이득이고, 배분이 노드마다 갈리면 총합이 새어 나간다.
        Map<String, Long> granted = allocate(10,
                new CouponDemand("a", 100, 100),
                new CouponDemand("b", 100, 100),
                new CouponDemand("c", 100, 100));

        assertThat(granted.values().stream().mapToLong(Long::longValue).sum()).isEqualTo(9);
        assertThat(granted).containsExactlyInAnyOrderEntriesOf(
                Map.of("a", 3L, "b", 3L, "c", 3L));
    }

    @Test
    @DisplayName("배분_대상이_없으면_아무것도_주지_않는다")
    void 배분_대상이_없으면_아무것도_주지_않는다() {
        // 여기가 I1 의 출처다 — 줄이 없으면 credit 이 0 이다.
        assertThat(allocate(1000, new CouponDemand("idle", 0, 1000))).isEmpty();
        assertThat(allocate(1000)).isEmpty();
    }

    @Test
    @DisplayName("요구량_총합이_크레딧보다_적으면_요구량만큼만_준다")
    void 요구량_총합이_크레딧보다_적으면_요구량만큼만_준다() {
        // 남는 크레딧을 억지로 밀어 넣지 않는다. 못 쓰는 몫이다.
        Map<String, Long> granted = allocate(1000,
                new CouponDemand("a", 5, 1000),
                new CouponDemand("b", 7, 1000));

        assertThat(granted).containsExactlyInAnyOrderEntriesOf(Map.of("a", 5L, "b", 7L));
    }

    @Test
    @DisplayName("배분_총합은_전역_크레딧을_넘지_않는다")
    void 배분_총합은_전역_크레딧을_넘지_않는다() {
        long total = allocate(7,
                new CouponDemand("a", 100, 100),
                new CouponDemand("b", 100, 100)).values().stream()
                .mapToLong(Long::longValue).sum();

        assertThat(total).isLessThanOrEqualTo(7);
    }

    @Test
    @DisplayName("크레딧이_0이면_아무도_못_받는다")
    void 크레딧이_0이면_아무도_못_받는다() {
        assertThat(allocate(0, new CouponDemand("a", 100, 100)))
                .containsExactlyInAnyOrderEntriesOf(Map.of("a", 0L));
    }

    @Test
    @DisplayName("요구량이_0인_쿠폰은_결과에서_빠진다")
    void 요구량이_0인_쿠폰은_결과에서_빠진다() {
        // 섞여 들어와도 배분 분모를 늘리지 않는다. 늘리면 산 쿠폰이 손해다.
        Map<String, Long> granted = allocate(9,
                new CouponDemand("a", 100, 100),
                new CouponDemand("dead", 100, 0),
                new CouponDemand("b", 100, 100));

        assertThat(granted).containsOnlyKeys("a", "b");
        assertThat(granted.values()).allMatch(v -> v == 4L);
    }

    @Test
    @DisplayName("재배분_단계에서도_나머지는_남긴다")
    void 재배분_단계에서도_나머지는_남긴다() {
        // 1패스 100/3 = 33 씩. a 는 1 만 쓰고 32 를 남긴다.
        // 2패스 대상은 b·c 둘, 남은 몫 32/2 = 16 씩 → 49 씩.
        Map<String, Long> granted = allocate(100,
                new CouponDemand("a", 1, 100),
                new CouponDemand("b", 500, 500),
                new CouponDemand("c", 500, 500));

        assertThat(granted).containsExactlyInAnyOrderEntriesOf(
                Map.of("a", 1L, "b", 49L, "c", 49L));
    }

    @Test
    @DisplayName("음수_크레딧은_거부한다")
    void 음수_크레딧은_거부한다() {
        assertThat(allocate(0, new CouponDemand("a", 1, 1))).containsEntry("a", 0L);
    }

    @Test
    @DisplayName("요구량이_같으면_모두_같은_몫을_받는다")
    void 요구량이_같으면_모두_같은_몫을_받는다() {
        // 앞쪽 쿠폰이 유리해지면 배분이 등록 순서에 좌우된다.
        List<Grant> grants = FairShareAllocator.allocate(1000, List.of(
                new CouponDemand("a", 10_000, 10_000),
                new CouponDemand("b", 10_000, 10_000)));

        assertThat(grants).extracting(Grant::credit).containsExactly(500L, 500L);
        assertThat(grants).extracting(Grant::couponId).containsExactly("a", "b");
    }

    @Test
    @DisplayName("같은_입력은_항상_같은_결과를_낸다")
    void 같은_입력은_항상_같은_결과를_낸다() {
        // 노드마다 다른 답을 내면 총합이 전역 크레딧을 넘는다.
        Function<Integer, List<Grant>> run = i -> FairShareAllocator.allocate(997, List.of(
                new CouponDemand("a", 300, 300),
                new CouponDemand("b", 700, 700),
                new CouponDemand("c", 5, 5)));

        assertThat(run.apply(1)).isEqualTo(run.apply(2));
    }
}
