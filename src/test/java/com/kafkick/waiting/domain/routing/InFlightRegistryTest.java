package com.kafkick.waiting.domain.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 인스턴스별로 지금 물려 있는 요청 수를 센다.
 *
 * <p><b>감소를 한 경로라도 놓치면 그 인스턴스가 영영 배제된다.</b> 카운터가 부풀면
 * 부하율이 계속 높게 보여 P2C 가 그 인스턴스를 안 고른다 — 조용히 용량이 준다.
 */
@Tag("unit")
class InFlightRegistryTest {

    private static final String 갑 = "be-1";

    private static final String 을 = "be-2";

    /** 요청 하나가 살아 있을 수 있는 최대 시간. 이보다 오래된 항목은 만료다. */
    private static final Duration 수명 = Duration.ofSeconds(30);

    /** 시험이 앞으로 감는 시각. <b>도메인은 시계를 안 든다</b> (DS-1). */
    private long 지금 = Instant.parse("2026-09-02T00:00:00Z").toEpochMilli();

    private final InFlightRegistry 레지스트리 = InFlightRegistry.of(수명);

    private void 앞으로(Duration 만큼) {
        지금 += 만큼.toMillis();
    }

    @Nested
    @DisplayName("세기")
    class Counting {

        @Test
        @DisplayName("시작하면_늘고_끝나면_준다")
        void 시작하면_늘고_끝나면_준다() {
            InFlightRegistry.Ticket 표 = 레지스트리.started(갑, 지금);
            assertThat(레지스트리.count(갑, 지금)).isEqualTo(1);

            표.finished();
            assertThat(레지스트리.count(갑, 지금)).isZero();
        }

        /** 같은 표를 두 번 놓으면 카운터가 음수가 된다 — 그 인스턴스만 늘 뽑힌다. */
        @Test
        @DisplayName("같은_표를_두_번_놓아도_한_번만_준다")
        void 같은_표를_두_번_놓아도_한_번만_준다() {
            InFlightRegistry.Ticket 표 = 레지스트리.started(갑, 지금);
            표.finished();
            표.finished();

            assertThat(레지스트리.count(갑, 지금)).isZero();
        }

        @Test
        @DisplayName("인스턴스별로_따로_센다")
        void 인스턴스별로_따로_센다() {
            레지스트리.started(갑, 지금);
            레지스트리.started(갑, 지금);
            레지스트리.started(을, 지금);

            assertThat(레지스트리.count(갑, 지금)).isEqualTo(2);
            assertThat(레지스트리.count(을, 지금)).isEqualTo(1);
        }

        /** 한 번도 안 본 인스턴스는 0 이다. 없는 것과 0 을 가르면 부르는 쪽이 는다. */
        @Test
        @DisplayName("모르는_인스턴스는_0_이다")
        void 모르는_인스턴스는_0_이다() {
            assertThat(레지스트리.count("없는-것", 지금)).isZero();
        }
    }

    @Nested
    @DisplayName("만료")
    class Expiry {

        /**
         * <b>감소를 놓쳐도 누수가 유계다</b> (R-8). 수명을 넘긴 항목은 만료된다.
         *
         * <p>이것이 없으면 취소나 예외로 새어 나간 항목 하나가 그 인스턴스를
         * 영영 배제한다.
         */
        @Test
        @DisplayName("수명을_넘긴_항목은_만료된다")
        void 수명을_넘긴_항목은_만료된다() {
            레지스트리.started(갑, 지금);
            앞으로(수명.plusSeconds(1));

            assertThat(레지스트리.count(갑, 지금)).isZero();
        }

        /** <b>살아 있는 요청은 안 버린다.</b> 수명 안이면 그대로 센다. */
        @Test
        @DisplayName("수명_안의_항목은_안_버린다")
        void 수명_안의_항목은_안_버린다() {
            레지스트리.started(갑, 지금);
            앞으로(수명.minusSeconds(1));

            assertThat(레지스트리.count(갑, 지금)).isEqualTo(1);
        }

        /** 경계는 안 만료다. 정확히 수명이면 아직 살아 있는 것으로 본다. */
        @Test
        @DisplayName("정확히_수명이면_아직_산다")
        void 정확히_수명이면_아직_산다() {
            레지스트리.started(갑, 지금);
            앞으로(수명);

            assertThat(레지스트리.count(갑, 지금)).isEqualTo(1);
        }

        /** 만료된 항목의 표를 뒤늦게 놓아도 음수가 안 된다. */
        @Test
        @DisplayName("만료_뒤_늦게_놓아도_음수가_안_된다")
        void 만료_뒤_늦게_놓아도_음수가_안_된다() {
            InFlightRegistry.Ticket 표 = 레지스트리.started(갑, 지금);
            앞으로(수명.plusSeconds(1));
            표.finished();

            레지스트리.started(갑, 지금);
            assertThat(레지스트리.count(갑, 지금)).isEqualTo(1);
        }

        /** 오래된 것만 만료된다. 같은 인스턴스의 새 항목은 남는다. */
        @Test
        @DisplayName("오래된_것만_만료된다")
        void 오래된_것만_만료된다() {
            레지스트리.started(갑, 지금);
            앞으로(수명.minusSeconds(1));
            레지스트리.started(갑, 지금);
            앞으로(Duration.ofSeconds(2));

            assertThat(레지스트리.count(갑, 지금)).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("정리")
    class Pruning {

        /**
         * 인스턴스 목록에서 사라진 것의 카운터를 지운다.
         *
         * <p>안 지우면 배포마다 항목이 쌓인다 — 인스턴스 식별자가 재기동마다
         * 새로 오므로 무제한으로 자란다.
         */
        @Test
        @DisplayName("사라진_인스턴스는_지운다")
        void 사라진_인스턴스는_지운다() {
            레지스트리.started(갑, 지금);
            레지스트리.started(을, 지금);

            레지스트리.retain(Set.of(을));

            assertThat(레지스트리.count(갑, 지금)).isZero();
            assertThat(레지스트리.count(을, 지금)).isEqualTo(1);
            assertThat(레지스트리.instances()).containsExactly(을);
        }

        /** 빈 목록을 주면 전부 지운다 — 목록을 못 읽은 것과 구분은 부르는 쪽 몫이다. */
        @Test
        @DisplayName("빈_목록이면_전부_지운다")
        void 빈_목록이면_전부_지운다() {
            레지스트리.started(갑, 지금);
            레지스트리.retain(Set.of());

            assertThat(레지스트리.instances()).isEmpty();
        }

        /** 지운 뒤 그 인스턴스의 표를 놓아도 되살아나지 않는다. */
        @Test
        @DisplayName("지운_뒤_늦게_놓아도_안_되살아난다")
        void 지운_뒤_늦게_놓아도_안_되살아난다() {
            InFlightRegistry.Ticket 표 = 레지스트리.started(갑, 지금);
            레지스트리.retain(Set.of());
            표.finished();

            assertThat(레지스트리.instances()).isEmpty();
        }
    }

    @Nested
    @DisplayName("동시 상한")
    class Cap {

        /**
         * 느려진 한 대가 게이트웨이 커넥션을 다 붙잡지 못하게 막는다 (G9.13).
         *
         * <p>상한이 없으면 응답이 안 오는 인스턴스로 간 요청이 계속 쌓여, 멀쩡한
         * 인스턴스로 갈 커넥션까지 그 한 대가 가져간다.
         */
        @Test
        @DisplayName("상한에_닿으면_안_준다")
        void 상한에_닿으면_안_준다() {
            assertThat(레지스트리.tryStarted(갑, 2, 지금)).isPresent();
            assertThat(레지스트리.tryStarted(갑, 2, 지금)).isPresent();

            assertThat(레지스트리.tryStarted(갑, 2, 지금)).isEmpty();
            assertThat(레지스트리.count(갑, 지금)).isEqualTo(2);
        }

        /** 상한은 인스턴스마다 따로다. 한 대가 막혀도 다른 대는 받는다. */
        @Test
        @DisplayName("상한은_인스턴스마다_따로다")
        void 상한은_인스턴스마다_따로다() {
            레지스트리.tryStarted(갑, 1, 지금);

            assertThat(레지스트리.tryStarted(갑, 1, 지금)).isEmpty();
            assertThat(레지스트리.tryStarted(을, 1, 지금)).isPresent();
        }

        /** 하나 놓으면 그 자리가 바로 난다. 안 그러면 상한이 영구 차단이 된다. */
        @Test
        @DisplayName("놓으면_자리가_난다")
        void 놓으면_자리가_난다() {
            InFlightRegistry.Ticket 표 = 레지스트리.tryStarted(갑, 1, 지금).orElseThrow();
            표.finished();

            assertThat(레지스트리.tryStarted(갑, 1, 지금)).isPresent();
        }

        /** 만료도 자리를 낸다 — 감소를 놓친 항목이 상한을 영구히 채우면 안 된다. */
        @Test
        @DisplayName("만료도_자리를_낸다")
        void 만료도_자리를_낸다() {
            레지스트리.tryStarted(갑, 1, 지금);
            앞으로(수명.plusSeconds(1));

            assertThat(레지스트리.tryStarted(갑, 1, 지금)).isPresent();
        }

        @Test
        @DisplayName("상한이_양수가_아니면_거절한다")
        void 상한이_양수가_아니면_거절한다() {
            assertThatThrownBy(() -> 레지스트리.tryStarted(갑, 0, 지금))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("만들 때")
    class Construction {

        /** 수명이 0 이면 모든 항목이 즉시 만료돼 레지스트리가 늘 0 을 돌려준다. */
        @Test
        @DisplayName("수명이_양수가_아니면_거절한다")
        void 수명이_양수가_아니면_거절한다() {
            assertThatThrownBy(() -> InFlightRegistry.of(Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> InFlightRegistry.of(Duration.ofSeconds(-1)))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("수명이_없으면_거절한다")
        void 수명이_없으면_거절한다() {
            assertThatThrownBy(() -> InFlightRegistry.of(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("인스턴스가_없으면_거절한다")
        void 인스턴스가_없으면_거절한다() {
            assertThatThrownBy(() -> 레지스트리.started(null, 지금))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> 레지스트리.retain(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    @DisplayName("부하 뒤 수렴")
    class Draining {

        /** <b>G9.3 이 재는 것이다.</b> 부하가 끝나면 전 인스턴스가 0 이어야 한다. */
        @Test
        @DisplayName("전부_끝나면_전_인스턴스가_0_이다")
        void 전부_끝나면_전_인스턴스가_0_이다() {
            List<InFlightRegistry.Ticket> 표들 = List.of(
                    레지스트리.started(갑, 지금), 레지스트리.started(갑, 지금),
                    레지스트리.started(을, 지금), 레지스트리.started(을, 지금));

            표들.forEach(InFlightRegistry.Ticket::finished);

            assertThat(레지스트리.count(갑, 지금)).isZero();
            assertThat(레지스트리.count(을, 지금)).isZero();
        }
    }
}
