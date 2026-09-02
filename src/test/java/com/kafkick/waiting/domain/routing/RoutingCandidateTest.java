package com.kafkick.waiting.domain.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 어느 인스턴스로 보낼지 견주는 값.
 *
 * <p><b>in-flight 만 보면 여유가 다른 두 대에 같은 양이 간다.</b> 여유 200 인
 * 대와 40 인 대가 같은 수를 받으면 뒤엣것이 먼저 무너진다 (R-4).
 */
@Tag("unit")
class RoutingCandidateTest {

    @Nested
    @DisplayName("부하율")
    class LoadFactor {

        /** 견주는 것은 절대량이 아니라 <b>제 여유 대비</b> 얼마나 찼는가다. */
        @Test
        @DisplayName("여유_대비로_잰다")
        void 여유_대비로_잰다() {
            RoutingCandidate 큰_대 = RoutingCandidate.of("be-1", 200, 20);
            RoutingCandidate 작은_대 = RoutingCandidate.of("be-2", 40, 20);

            assertThat(큰_대.loadFactor()).isCloseTo(0.1, within(1e-9));
            assertThat(작은_대.loadFactor()).isCloseTo(0.5, within(1e-9));
        }

        /** 여유가 같으면 순수 P2C 와 같아진다 — in-flight 가 그대로 순서를 정한다. */
        @Test
        @DisplayName("여유가_같으면_in_flight_가_순서를_정한다")
        void 여유가_같으면_in_flight_가_순서를_정한다() {
            RoutingCandidate 한가한 = RoutingCandidate.of("be-1", 100, 5);
            RoutingCandidate 바쁜 = RoutingCandidate.of("be-2", 100, 50);

            assertThat(한가한.loadFactor()).isLessThan(바쁜.loadFactor());
        }

        /**
         * <b>여유가 0 이면 후보가 아니다.</b> 부하율을 계산하면 0 으로 나눈다 —
         * 무한대가 나와 "가장 바쁜 대" 로 보이지만, 실제로는 보내면 안 되는 대다.
         */
        @Test
        @DisplayName("여유가_0_이면_후보가_아니다")
        void 여유가_0_이면_후보가_아니다() {
            RoutingCandidate 없는 = RoutingCandidate.of("be-1", 0, 0);

            assertThat(없는.eligible()).isFalse();
            assertThatThrownBy(없는::loadFactor).isInstanceOf(IllegalStateException.class);
        }

        @Test
        @DisplayName("여유가_있으면_후보다")
        void 여유가_있으면_후보다() {
            assertThat(RoutingCandidate.of("be-1", 1, 0).eligible()).isTrue();
        }
    }

    @Nested
    @DisplayName("콜드 스타트 씨앗")
    class ColdStart {

        /**
         * <b>막 뜬 게이트웨이는 전 인스턴스가 0 으로 보인다.</b> 그러면 열화된
         * 대를 못 가려 정상 비율만큼 보낸다 — 배포 때마다 그 구간이 온다 (G9.12).
         *
         * <p>보고된 값은 최대 3초 낡았지만 0 보다는 낫다. 유일한 대안이
         * "아무것도 모른다" 이기 때문이다.
         */
        @Test
        @DisplayName("기동_직후에는_보고된_값을_그대로_쓴다")
        void 기동_직후에는_보고된_값을_그대로_쓴다() {
            double 씨앗 = RoutingCandidate.seed(40, Duration.ZERO, Duration.ofSeconds(60));

            assertThat(씨앗).isCloseTo(40, within(1e-9));
        }

        /** 로컬 관측이 쌓이면 씨앗의 무게가 선형으로 준다. */
        @Test
        @DisplayName("램프_중간이면_절반이다")
        void 램프_중간이면_절반이다() {
            double 씨앗 = RoutingCandidate.seed(40, Duration.ofSeconds(30), Duration.ofSeconds(60));

            assertThat(씨앗).isCloseTo(20, within(1e-9));
        }

        /** 램프가 끝나면 식이 원래대로 돌아간다 — 진단용이라는 원칙이 유지된다. */
        @Test
        @DisplayName("램프가_끝나면_0_이다")
        void 램프가_끝나면_0_이다() {
            assertThat(RoutingCandidate.seed(40, Duration.ofSeconds(60), Duration.ofSeconds(60)))
                    .isZero();
            assertThat(RoutingCandidate.seed(40, Duration.ofSeconds(90), Duration.ofSeconds(60)))
                    .isZero();
        }

        /** 씨앗이 실린 뒤에는 그것까지 세어 견준다. */
        @Test
        @DisplayName("씨앗이_부하율에_실린다")
        void 씨앗이_부하율에_실린다() {
            RoutingCandidate 열화된 = RoutingCandidate.of("be-1", 100, 0, 40);

            assertThat(열화된.loadFactor()).isCloseTo(0.4, within(1e-9));
        }

        /** 램프가 0 이면 씨앗을 안 쓴다. 나누지 않고 바로 0 이다. */
        @Test
        @DisplayName("램프가_0_이면_씨앗이_없다")
        void 램프가_0_이면_씨앗이_없다() {
            assertThat(RoutingCandidate.seed(40, Duration.ZERO, Duration.ZERO)).isZero();
        }

        /** 보고가 음수로 오면 0 으로 본다. 음수 씨앗은 그 대를 영원히 뽑히게 한다. */
        @Test
        @DisplayName("음수_보고는_0_으로_본다")
        void 음수_보고는_0_으로_본다() {
            assertThat(RoutingCandidate.seed(-5, Duration.ZERO, Duration.ofSeconds(60))).isZero();
        }
    }
}
