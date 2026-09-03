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

    /**
     * 여유 200/40/120. <b>순서를 못 박는다</b> — {@code Map.of} 는 순회 순서가
     * 실행마다 달라, 씨드를 고정해도 후보 자리가 바뀌어 결과가 흔들린다.
     */
    private static final List<Map.Entry<String, Long>> 여유 = List.of(
            Map.entry("a", 200L), Map.entry("b", 40L), Map.entry("c", 120L));

    /** 같은 값을 키로 찾을 때만 쓴다. 순회하지 않으므로 순서가 상관없다. */
    private static final Map<String, Long> 여유_별 = Map.of("a", 200L, "b", 40L, "c", 120L);

    private static List<RoutingCandidate> 후보(Map<String, Integer> 물린) {
        return 여유.stream()
                .map(e -> RoutingCandidate.of(e.getKey(), e.getValue(), 물린.get(e.getKey())))
                .toList();
    }

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
                    WeightedP2c.of(정해진(0, 1)).choose(후보);

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

            Optional<RoutingCandidate> 고른 = WeightedP2c.of(정해진(0, 1)).choose(후보);

            assertThat(고른).map(RoutingCandidate::instanceId).contains("큰");
        }

        /** 한 대뿐이면 그 대다. 둘을 뽑으려다 예외가 나면 안 된다 (9.3.3). */
        @Test
        @DisplayName("한_대뿐이면_그_대다")
        void 한_대뿐이면_그_대다() {
            Optional<RoutingCandidate> 고른 = WeightedP2c.of(정해진(0, 0))
                    .choose(List.of(RoutingCandidate.of("be-1", 100, 0)));

            assertThat(고른).map(RoutingCandidate::instanceId).contains("be-1");
        }

        /** 한 대도 없으면 비어 있다. 부르는 쪽이 명확한 실패를 내야 한다 (9.3.4). */
        @Test
        @DisplayName("한_대도_없으면_비어_있다")
        void 한_대도_없으면_비어_있다() {
            assertThat(WeightedP2c.of(정해진(0, 0)).choose(List.of())).isEmpty();
        }

        /** 같은 자리를 두 번 뽑아도 그 대다. 무작위가 겹치는 것은 흔하다. */
        @Test
        @DisplayName("같은_자리를_두_번_뽑아도_그_대다")
        void 같은_자리를_두_번_뽑아도_그_대다() {
            List<RoutingCandidate> 후보 = List.of(
                    RoutingCandidate.of("be-1", 100, 50),
                    RoutingCandidate.of("be-2", 100, 5));

            Optional<RoutingCandidate> 고른 = WeightedP2c.of(정해진(0, 0)).choose(후보);

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

            Optional<RoutingCandidate> 고른 = WeightedP2c.of(정해진(0, 1)).choose(후보);

            assertThat(고른).map(RoutingCandidate::instanceId).contains("산");
        }

        /** 전부 여유가 0 이면 비어 있다. 아무 대나 고르면 그 대가 무너진다. */
        @Test
        @DisplayName("전부_막혔으면_비어_있다")
        void 전부_막혔으면_비어_있다() {
            List<RoutingCandidate> 후보 = List.of(
                    RoutingCandidate.of("막힌1", 0, 0),
                    RoutingCandidate.of("막힌2", 0, 0));

            assertThat(WeightedP2c.of(정해진(0, 1)).choose(후보)).isEmpty();
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
            Random random = new Random(20260902);
            WeightedP2c 고르개 = WeightedP2c.of(random::nextInt);
            Map<String, Integer> 받은 = new HashMap<>(Map.of("a", 0, "b", 0, "c", 0));

            int 총_요청 = 3_600;
            for (int i = 0; i < 총_요청; i++) {
                RoutingCandidate 고른 = 고르개.choose(후보(물린)).orElseThrow();
                받은.merge(고른.instanceId(), 1, Integer::sum);
                물린.merge(고른.instanceId(), 1, Integer::sum);
                // 물린 것이 안 빠지면 한 번 받은 대가 영영 안 뽑힌다.
                // 실제 부하처럼 여유에 비례한 속도로 빠뜨린다.
                if (i % 10 == 9) {
                    물린.replaceAll((id, n) -> Math.max(0, n - (int) (여유_별.get(id) / 40)));
                }
            }

            // 여유 비율은 200/40/120 → 55.6% / 11.1% / 33.3%
            assertThat(비율(받은, "a", 총_요청)).isBetween(0.556 * 0.85, 0.556 * 1.15);
            assertThat(비율(받은, "b", 총_요청)).isBetween(0.111 * 0.85, 0.111 * 1.15);
            assertThat(비율(받은, "c", 총_요청)).isBetween(0.333 * 0.85, 0.333 * 1.15);
        }

        /**
         * <b>부하가 얕으면 여유 비율이 안 나온다</b> — 그것이 이 기준의 적용
         * 범위다 (G9.1 의 부하 조건).
         *
         * <p>물린 것이 없으면 세 대의 부하율이 전부 0 이라 여유가 비교에서
         * 빠지고 균등해진다. 실제로 그 구간에서 재고 "여유를 안 본다" 고
         * 기록한 적이 있다 — 그 조건을 여기 못 박아 다시 안 겪게 한다.
         */
        @Test
        @DisplayName("물린_것이_없으면_균등해진다")
        void 물린_것이_없으면_균등해진다() {
            Random random = new Random(20260903);
            WeightedP2c 고르개 = WeightedP2c.of(random::nextInt);
            Map<String, Integer> 받은 = new HashMap<>(Map.of("a", 0, "b", 0, "c", 0));
            // **물린 것을 안 쌓는다.** 뒷단이 빨라 바로 끝나는 구간이다.
            Map<String, Integer> 빈_부하 = Map.of("a", 0, "b", 0, "c", 0);

            int 총_요청 = 3_600;
            for (int i = 0; i < 총_요청; i++) {
                받은.merge(고르개.choose(후보(빈_부하)).orElseThrow().instanceId(), 1, Integer::sum);
            }

            // 여유가 5 배 갈리는데도 셋이 3분의 1씩 간다. 여유 비율(55.6/11.1/33.3)
            // 과는 멀고, 균등(33.3)에는 가깝다.
            assertThat(비율(받은, "b", 총_요청)).isBetween(0.30, 0.37);
            assertThat(비율(받은, "a", 총_요청)).isBetween(0.30, 0.37);
        }

        /**
         * <b>부하가 깊어질수록 여유 비율에 가까워진다</b> (G9.1 의 부하 조건).
         *
         * <p>얕은 쪽이 깊은 쪽보다 나으면 부등호가 뒤집힌 것이다 — 그때는
         * 부하를 키울수록 나빠진다는 뜻이라 기준의 조건 자체가 헛말이 된다.
         */
        @Test
        @DisplayName("부하가_깊을수록_여유_비율에_가까워진다")
        void 부하가_깊을수록_여유_비율에_가까워진다() {
            assertThat(작은_대의_편차(200)).isLessThan(작은_대의_편차(60));
            assertThat(작은_대의_편차(60)).isLessThan(작은_대의_편차(20));
            assertThat(작은_대의_편차(20)).isLessThan(작은_대의_편차(5));
        }

        /**
         * <b>경계를 양쪽에서 밟는다.</b> 한쪽만 보면 부등호가 뒤집혀도 안 걸리고,
         * 하한을 안 적으면 다음 사람이 다시 훑어 찾는다.
         *
         * <p>게이트가 요구하는 조건이 400 이다 (G9.1). 경계는 320 언저리지만
         * 씨드에 따라 0.148~0.152 로 흔들려 그 자리를 못 박지 않는다 — 흔들리지
         * 않는 두 점으로 가른다. 실측 하네스는 이 깊이를 못 만든다.
         */
        @Test
        @DisplayName("기준을_가르는_깊이가_200_과_400_사이다")
        void 기준을_가르는_깊이가_200_과_400_사이다() {
            assertThat(작은_대의_편차(400)).isLessThan(0.15);
            assertThat(작은_대의_편차(200)).isGreaterThan(0.15);
        }

        /**
         * <b>작은 대의 봉우리가 라운드로빈보다 크다</b> (2.3).
         *
         * <p><b>두 전략을 견준다.</b> 봉우리는 표본 최댓값이라 반복수를 늘리면
         * 커져, 절대값을 박으면 제품을 안 건드려도 빨개진다. 견주는 쪽은 그
         * 영향을 함께 받아 상쇄된다. 이 부등호가 뒤집히면 라운드로빈을 고른
         * 근거가 사라진 것이므로 그때는 빨개지는 것이 맞다 (AIJ-0217).
         */
        @Test
        @DisplayName("작은_대의_봉우리가_라운드로빈보다_크다")
        void 작은_대의_봉우리가_라운드로빈보다_크다() {
            double p2c = 작은_대의_최대_사용률(WeightedP2c.of(new Random(20260903)::nextInt), 360);
            double 라운드로빈 = 작은_대의_최대_사용률(WeightedRoundRobin.create(), 360);

            assertThat(p2c).isGreaterThan(라운드로빈);
        }

        /** 동시 {@code 동시성} 건이 물려 있을 때 작은 대의 <b>최대</b> 사용률. */
        private double 작은_대의_최대_사용률(InstanceChooser 고르개, int 동시성) {
            Map<String, Integer> 물린 = new HashMap<>(Map.of("a", 0, "b", 0, "c", 0));
            Deque<String> 진행중 = new ArrayDeque<>();
            double 최대 = 0;

            int 총_요청 = 120_000;
            for (int i = 0; i < 총_요청; i++) {
                String 고른 = 고르개.choose(후보(물린)).orElseThrow().instanceId();
                물린.merge(고른, 1, Integer::sum);
                진행중.addLast(고른);
                if (진행중.size() > 동시성) {
                    물린.merge(진행중.removeFirst(), -1, Integer::sum);
                }
                // 자리를 채우는 동안은 안 센다. 그 구간은 정상 상태가 아니다.
                if (i > 동시성) {
                    최대 = Math.max(최대, 물린.get("b") / 40.0);
                }
            }
            return 최대;
        }

        /** 동시 {@code 동시성} 건이 늘 물려 있는 상태에서 작은 대의 상대 편차. */
        private double 작은_대의_편차(int 동시성) {
            Map<String, Integer> 물린 = new HashMap<>(Map.of("a", 0, "b", 0, "c", 0));
            Random random = new Random(20260903);
            WeightedP2c 고르개 = WeightedP2c.of(random::nextInt);
            Map<String, Integer> 받은 = new HashMap<>(Map.of("a", 0, "b", 0, "c", 0));
            Deque<String> 진행중 = new ArrayDeque<>();

            // **표본이 적으면 깊은 쪽이 손해다.** 자리를 채우는 동안의 초기
            // 구간이 통계에 그대로 남아, 깊을수록 나쁘게 나온다.
            int 총_요청 = 200_000;
            for (int i = 0; i < 총_요청; i++) {
                String 고른 = 고르개.choose(후보(물린)).orElseThrow().instanceId();
                받은.merge(고른, 1, Integer::sum);
                물린.merge(고른, 1, Integer::sum);
                진행중.addLast(고른);
                // **자리를 채워 둔다.** 지연이 같으면 먼저 보낸 것이 먼저 끝난다.
                if (진행중.size() > 동시성) {
                    물린.merge(진행중.removeFirst(), -1, Integer::sum);
                }
            }
            double 기대 = 40.0 / 360.0;
            return Math.abs(비율(받은, "b", 총_요청) - 기대) / 기대;
        }

        private double 비율(Map<String, Integer> 받은, String id, int 총) {
            return (double) 받은.get(id) / 총;
        }
    }
}
