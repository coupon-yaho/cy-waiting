package com.kafkick.waiting.domain.routing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 여유 비율대로 결정적으로 돈다.
 *
 * <p>인스턴스가 3~5대로 줄면 무작위 둘을 뽑는 이득이 없다 — 그 규모에서는
 * 이쪽이 더 정확하고 단순하다 (R-9). 어느 쪽이 나은지는 실측으로 정한다.
 */
@Tag("unit")
class WeightedRoundRobinTest {

    private static List<RoutingCandidate> 후보(long a, long b, long c) {
        return List.of(RoutingCandidate.of("a", a, 0),
                RoutingCandidate.of("b", b, 0),
                RoutingCandidate.of("c", c, 0));
    }

    private static Map<String, Integer> 돌린다(InstanceChooser 고르개,
            List<RoutingCandidate> 후보, int 횟수) {
        Map<String, Integer> 받은 = new HashMap<>();
        for (int i = 0; i < 횟수; i++) {
            받은.merge(고르개.choose(후보).orElseThrow().instanceId(), 1, Integer::sum);
        }
        return 받은;
    }

    @Nested
    @DisplayName("비율")
    class Ratio {

        /** <b>한 바퀴에 정확히 여유만큼이다</b> (9.3.7). 근사가 아니다. */
        @Test
        @DisplayName("한_바퀴에_여유만큼_정확히_간다")
        void 한_바퀴에_여유만큼_정확히_간다() {
            Map<String, Integer> 받은 = 돌린다(WeightedRoundRobin.create(), 후보(200, 40, 120), 360);

            assertThat(받은).containsExactlyInAnyOrderEntriesOf(
                    Map.of("a", 200, "b", 40, "c", 120));
        }

        /** 여러 바퀴를 돌아도 어긋나지 않는다. 나머지가 쌓이면 비율이 흔들린다. */
        @Test
        @DisplayName("세_바퀴를_돌아도_안_어긋난다")
        void 세_바퀴를_돌아도_안_어긋난다() {
            Map<String, Integer> 받은 = 돌린다(WeightedRoundRobin.create(), 후보(200, 40, 120), 1_080);

            assertThat(받은).containsExactlyInAnyOrderEntriesOf(
                    Map.of("a", 600, "b", 120, "c", 360));
        }

        /**
         * <b>몰아 주지 않고 고루 편다.</b> 여유대로 세어 놓고 큰 대를 연달아 200 번
         * 보내면, 한 바퀴의 합은 맞아도 그 구간에 그 대가 무너진다.
         */
        @Test
        @DisplayName("같은_대를_연달아_몰지_않는다")
        void 같은_대를_연달아_몰지_않는다() {
            InstanceChooser 고르개 = WeightedRoundRobin.create();
            List<RoutingCandidate> 후보 = 후보(200, 40, 120);

            String 앞 = null;
            int 연속 = 0;
            int 최대_연속 = 0;
            for (int i = 0; i < 360; i++) {
                String 지금 = 고르개.choose(후보).orElseThrow().instanceId();
                연속 = 지금.equals(앞) ? 연속 + 1 : 1;
                최대_연속 = Math.max(최대_연속, 연속);
                앞 = 지금;
            }

            // 여유 비율이 5:1:3 이라 가장 큰 대도 한 번에 몇 개를 넘지 않는다.
            assertThat(최대_연속).isLessThanOrEqualTo(5);
        }
    }

    @Nested
    @DisplayName("후보가 바뀔 때")
    class Changing {

        /**
         * <b>넘쳐도 보낼 곳은 돌려준다.</b>
         *
         * <p>부호가 뒤집히면 가장 여유 있는 대가 가장 안 뽑히는 대가 된다. 그렇다고
         * 터뜨리면 보낼 곳이 멀쩡한데 요청이 죽는다 — 상한에 재운다.
         */
        @Test
        @DisplayName("크레딧_합이_넘쳐도_고른다")
        void 크레딧_합이_넘쳐도_고른다() {
            List<RoutingCandidate> 큰_후보 = List.of(
                    RoutingCandidate.of("a", Long.MAX_VALUE, 0),
                    RoutingCandidate.of("b", Long.MAX_VALUE, 0));

            assertThat(WeightedRoundRobin.create().choose(큰_후보))
                    .as("넘쳐도 후보 중 하나가 나온다")
                    .isPresent();
        }

        /** <b>여유 0 은 후보가 아니다</b> (9.3.6). */
        @Test
        @DisplayName("여유가_0_인_대는_안_뽑는다")
        void 여유가_0_인_대는_안_뽑는다() {
            Map<String, Integer> 받은 = 돌린다(WeightedRoundRobin.create(), 후보(100, 0, 100), 200);

            assertThat(받은).containsExactlyInAnyOrderEntriesOf(Map.of("a", 100, "c", 100));
        }

        @Test
        @DisplayName("한_대도_없으면_비어_있다")
        void 한_대도_없으면_비어_있다() {
            assertThat(WeightedRoundRobin.create().choose(List.of())).isEmpty();
        }

        /**
         * <b>전부 막혔으면 아무 대나 고르지 않는다.</b> 여유 0 인 대로 보내면
         * 그 대가 무너진다 — 보낼 곳이 없다는 것을 부르는 쪽이 알아야 한다 (9.3.4).
         */
        @Test
        @DisplayName("전부_막혔으면_비어_있다")
        void 전부_막혔으면_비어_있다() {
            assertThat(WeightedRoundRobin.create().choose(후보(0, 0, 0))).isEmpty();
        }

        /**
         * 사라진 대의 누적을 안 지우면 배포를 거듭할수록 자란다. 인스턴스
         * 식별자는 재기동마다 새로 온다 (R-3).
         */
        @Test
        @DisplayName("사라진_대의_누적은_지운다")
        void 사라진_대의_누적은_지운다() {
            WeightedRoundRobin 고르개 = WeightedRoundRobin.create();
            돌린다(고르개, 후보(100, 100, 100), 30);

            고르개.choose(List.of(RoutingCandidate.of("a", 100, 0)));

            assertThat(고르개.tracked()).containsExactly("a");
        }

        /** 대가 하나 늘어도 그 대가 곧바로 몰아 받지 않는다. */
        @Test
        @DisplayName("새_대가_몰아_받지_않는다")
        void 새_대가_몰아_받지_않는다() {
            WeightedRoundRobin 고르개 = WeightedRoundRobin.create();
            List<RoutingCandidate> 둘 = List.of(
                    RoutingCandidate.of("a", 100, 0), RoutingCandidate.of("b", 100, 0));
            돌린다(고르개, 둘, 20);

            List<RoutingCandidate> 셋 = 후보(100, 100, 100);
            Map<String, Integer> 받은 = 돌린다(고르개, 셋, 30);

            assertThat(받은.get("c")).isEqualTo(10);
        }
    }

}
