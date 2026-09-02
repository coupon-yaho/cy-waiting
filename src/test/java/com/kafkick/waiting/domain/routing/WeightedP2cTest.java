package com.kafkick.waiting.domain.routing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.IntUnaryOperator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 무작위 둘 중 덜 찬 쪽으로 보낸다.
 *
 * <p><b>최소값을 그냥 고르면 게이트웨이 M 대가 같은 인스턴스로 몰린다.</b>
 * 그놈이 순식간에 가장 바쁜 놈이 되고 다 같이 다음으로 옮겨 간다 — 진동한다.
 * 무작위 둘을 뽑는 것만으로 그 쏠림이 깨진다.
 */
@Tag("unit")
class WeightedP2cTest {

    /** 뽑을 자리를 시험이 정한다. 무작위를 그대로 쓰면 무엇을 재는지가 흐려진다. */
    private static IntUnaryOperator 정해진(int... 자리들) {
        Deque<Integer> 남은 = new ArrayDeque<>();
        for (int 자리 : 자리들) {
            남은.add(자리);
        }
        return bound -> 남은.isEmpty() ? 0 : 남은.poll();
    }

    @Nested
    @DisplayName("고르기")
    class Choosing {

        /** 뽑힌 둘 중 부하율이 낮은 쪽이다. */
        @Test
        @DisplayName("둘_중_덜_찬_쪽으로_보낸다")
        void 둘_중_덜_찬_쪽으로_보낸다() {
            List<RoutingCandidate> 후보 = List.of(
                    RoutingCandidate.of("be-1", 100, 50),
                    RoutingCandidate.of("be-2", 100, 5),
                    RoutingCandidate.of("be-3", 100, 90));

            Optional<RoutingCandidate> 고른 =
                    new WeightedP2c(정해진(0, 1)).choose(후보);

            assertThat(고른).map(RoutingCandidate::instanceId).contains("be-2");
        }

        /**
         * <b>여유를 본다.</b> in-flight 만 보면 여유 40 인 대가 200 인 대와 같은
         * 양을 받는다 — R2 가 요구하는 비율과 다르다.
         */
        @Test
        @DisplayName("in_flight_가_같아도_여유가_큰_쪽으로_보낸다")
        void in_flight_가_같아도_여유가_큰_쪽으로_보낸다() {
            List<RoutingCandidate> 후보 = List.of(
                    RoutingCandidate.of("작은", 40, 20),
                    RoutingCandidate.of("큰", 200, 20));

            Optional<RoutingCandidate> 고른 = new WeightedP2c(정해진(0, 1)).choose(후보);

            assertThat(고른).map(RoutingCandidate::instanceId).contains("큰");
        }

        /** 한 대뿐이면 그 대다. 둘을 뽑으려다 예외가 나면 안 된다 (9.3.3). */
        @Test
        @DisplayName("한_대뿐이면_그_대다")
        void 한_대뿐이면_그_대다() {
            Optional<RoutingCandidate> 고른 = new WeightedP2c(정해진(0, 0))
                    .choose(List.of(RoutingCandidate.of("be-1", 100, 0)));

            assertThat(고른).map(RoutingCandidate::instanceId).contains("be-1");
        }

        /** 한 대도 없으면 비어 있다. 부르는 쪽이 명확한 실패를 내야 한다 (9.3.4). */
        @Test
        @DisplayName("한_대도_없으면_비어_있다")
        void 한_대도_없으면_비어_있다() {
            assertThat(new WeightedP2c(정해진(0, 0)).choose(List.of())).isEmpty();
        }

        /** 같은 자리를 두 번 뽑아도 그 대다. 무작위가 겹치는 것은 흔하다. */
        @Test
        @DisplayName("같은_자리를_두_번_뽑아도_그_대다")
        void 같은_자리를_두_번_뽑아도_그_대다() {
            List<RoutingCandidate> 후보 = List.of(
                    RoutingCandidate.of("be-1", 100, 50),
                    RoutingCandidate.of("be-2", 100, 5));

            Optional<RoutingCandidate> 고른 = new WeightedP2c(정해진(0, 0)).choose(후보);

            assertThat(고른).map(RoutingCandidate::instanceId).contains("be-1");
        }
    }

    @Nested
    @DisplayName("후보에서 빼기")
    class Excluding {

        /** <b>여유 0 은 후보가 아니다</b> (9.3.6). 0 으로 나누지 않는다. */
        @Test
        @DisplayName("여유가_0_인_대는_안_뽑는다")
        void 여유가_0_인_대는_안_뽑는다() {
            List<RoutingCandidate> 후보 = List.of(
                    RoutingCandidate.of("막힌", 0, 0),
                    RoutingCandidate.of("산", 100, 90));

            Optional<RoutingCandidate> 고른 = new WeightedP2c(정해진(0, 1)).choose(후보);

            assertThat(고른).map(RoutingCandidate::instanceId).contains("산");
        }

        /** 전부 여유가 0 이면 비어 있다. 아무 대나 고르면 그 대가 무너진다. */
        @Test
        @DisplayName("전부_막혔으면_비어_있다")
        void 전부_막혔으면_비어_있다() {
            List<RoutingCandidate> 후보 = List.of(
                    RoutingCandidate.of("막힌1", 0, 0),
                    RoutingCandidate.of("막힌2", 0, 0));

            assertThat(new WeightedP2c(정해진(0, 1)).choose(후보)).isEmpty();
        }
    }

    @Nested
    @DisplayName("분포")
    class Distribution {

        /**
         * <b>여유 비율에 가깝게 간다</b> (G9.1 · ±15%).
         *
         * <p>부하율이 균등해지면 {@code in-flight ∝ credits} 가 되고, 지연이
         * 비슷하면 리틀의 법칙으로 처리량도 여유 비율을 따른다.
         */
        @Test
        @DisplayName("여유_비율에_가깝게_간다")
        void 여유_비율에_가깝게_간다() {
            Map<String, Integer> 물린 = new HashMap<>(
                    Map.of("a", 0, "b", 0, "c", 0));
            Map<String, Long> 여유 = Map.of("a", 200L, "b", 40L, "c", 120L);
            Random random = new Random(20260902);
            WeightedP2c 고르개 = new WeightedP2c(random::nextInt);
            Map<String, Integer> 받은 = new HashMap<>(Map.of("a", 0, "b", 0, "c", 0));

            int 총_요청 = 3_600;
            for (int i = 0; i < 총_요청; i++) {
                List<RoutingCandidate> 후보 = 여유.entrySet().stream()
                        .map(e -> RoutingCandidate.of(e.getKey(), e.getValue(),
                                물린.get(e.getKey())))
                        .toList();
                RoutingCandidate 고른 = 고르개.choose(후보).orElseThrow();
                받은.merge(고른.instanceId(), 1, Integer::sum);
                물린.merge(고른.instanceId(), 1, Integer::sum);
                // 물린 것이 안 빠지면 한 번 받은 대가 영영 안 뽑힌다.
                // 실제 부하처럼 여유에 비례한 속도로 빠뜨린다.
                if (i % 10 == 9) {
                    물린.replaceAll((id, n) -> Math.max(0, n - (int) (여유.get(id) / 40)));
                }
            }

            // 여유 비율은 200/40/120 → 55.6% / 11.1% / 33.3%
            assertThat(비율(받은, "a", 총_요청)).isBetween(0.556 * 0.85, 0.556 * 1.15);
            assertThat(비율(받은, "b", 총_요청)).isBetween(0.111 * 0.85, 0.111 * 1.15);
            assertThat(비율(받은, "c", 총_요청)).isBetween(0.333 * 0.85, 0.333 * 1.15);
        }

        private double 비율(Map<String, Integer> 받은, String id, int 총) {
            return (double) 받은.get(id) / 총;
        }
    }
}
