package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

    private DemandCollector collector(List<String> 쿠폰, Map<String, List<Long>> 큐,
            Map<String, Long> 재고) {
        return DemandCollector.of(
                () -> Mono.just(쿠폰),
                ids -> Mono.just(ids.stream().collect(Collectors.toMap(
                        id -> id, id -> 큐.get(id).stream().mapToLong(Long::longValue).sum()))),
                ids -> Mono.just(재고));
    }

    @Test
    @DisplayName("대기와_재고를_한_쿠폰으로_묶는다")
    void 대기와_재고를_한_쿠폰으로_묶는다() {
        DemandCollector collector = collector(List.of("c1"),
                Map.of("c1", List.of(3L, 5L, 0L, 2L)), Map.of("c1", 100L));

        List<CouponDemand> 수요 = collector.collect().block();

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

        List<CouponDemand> 수요 = collector.collect().block();

        assertThat(수요).extracting(CouponDemand::couponId, CouponDemand::waiting,
                        CouponDemand::stock)
                .containsExactly(Tuple.tuple("c1", 4L, 50L),
                        Tuple.tuple("c2", 10L, 7L));
    }

    @Test
    @DisplayName("재고를_모르면_없는_것으로_본다")
    void 재고를_모르면_없는_것으로_본다() {
        // 재고 키가 없는 것은 아직 안 실렸거나 지워진 것이다. 모르는 값을 크게
        // 잡으면 재고보다 많이 통과시킨다 — 초과 발급은 되돌릴 수 없다.
        DemandCollector collector = collector(List.of("c1"),
                Map.of("c1", List.of(9L, 0L, 0L, 0L)), Collections.emptyMap());

        List<CouponDemand> 수요 = collector.collect().block();

        assertThat(수요).singleElement().satisfies(d -> assertThat(d.stock()).isZero());
    }

    @Test
    @DisplayName("대기를_모르면_없는_것으로_본다")
    void 대기를_모르면_없는_것으로_본다() {
        // 값이 안 오는 것과 0 인 것은 같다. 모르는 쪽을 크게 잡으면 초과 배분이다.
        DemandCollector collector = DemandCollector.of(
                () -> Mono.just(List.of("c1")),
                ids -> Mono.just(Collections.singletonMap("c1", null)),
                ids -> Mono.just(Map.of("c1", 100L)));

        List<CouponDemand> 수요 = collector.collect().block();

        assertThat(수요).singleElement().satisfies(d -> assertThat(d.waiting()).isZero());
    }

    @Test
    @DisplayName("배분_대상이_없으면_아무것도_안_묻는다")
    void 배분_대상이_없으면_아무것도_안_묻는다() {
        // 대상이 없는데 빈 인자로 명령을 보내면 레디스가 오류를 낸다. 그 오류가
        // 판을 죽이면 대상이 생겨도 배분이 안 돈다.
        AtomicInteger 조회 = new AtomicInteger();
        DemandCollector collector = DemandCollector.of(
                () -> Mono.just(List.of()),
                ids -> {
                    조회.incrementAndGet();
                    return Mono.just(Map.of());
                },
                ids -> {
                    조회.incrementAndGet();
                    return Mono.just(Map.of());
                });

        List<CouponDemand> 수요 = collector.collect().block();

        assertThat(수요).isEmpty();
        assertThat(조회).hasValue(0);
    }

    @Test
    @DisplayName("길이가_안_맞으면_판을_버린다")
    void 길이가_안_맞으면_판을_버린다() {
        // 빠진 자리를 0 으로 채우면 대기가 0 인 쿠폰이 되어 크레딧이 안 나간다.
        // 줄 선 사람이 통째로 멈추는데 아무 신호도 없다.
        //
        // 기대값이 없으면 어긋난 정도를 모른다.
        DemandCollector collector = DemandCollector.of(
                () -> Mono.just(List.of("c1", "c2")),
                ids -> Mono.just(Map.of("c1", 4L)),
                ids -> Mono.just(Map.of("c1", 10L, "c2", 10L)));

        assertThatThrownBy(() -> collector.collect().block())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("대기").hasMessageContaining("기대=2");
    }

}
