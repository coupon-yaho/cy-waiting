package com.kafkick.waiting.domain.queue;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 폴링 간격은 서버가 정한다 (D-2).
 *
 * <p>부하를 정하는 것은 대기 인원이 아니라 <b>큐의 시간 깊이</b>이고, 개인은
 * 자기가 얼마나 기다릴지를 모른다.
 */
class PollIntervalPolicyTest {

    /** 지터를 끈 정책. 밴드 경계만 볼 때 쓴다. */
    private PollIntervalPolicy noJitter() {
        return PollIntervalPolicy.of(0.0);
    }

    @Test
    @DisplayName("ETA_밴드마다_기본_간격이_다르다")
    void ETA_밴드마다_기본_간격이_다르다() {
        PollIntervalPolicy p = noJitter();

        assertThat(p.intervalSec(1, () -> 0.5)).isEqualTo(1);
        assertThat(p.intervalSec(20, () -> 0.5)).isEqualTo(3);
        assertThat(p.intervalSec(100, () -> 0.5)).isEqualTo(10);
        assertThat(p.intervalSec(1000, () -> 0.5)).isEqualTo(30);
    }

    @Test
    @DisplayName("밴드_경계는_아래쪽_밴드에_속한다")
    void 밴드_경계는_아래쪽_밴드에_속한다() {
        // 경계에서 어느 쪽인지 정해 두지 않으면 5초 남은 사람이 노드마다
        // 다른 간격을 받는다.
        PollIntervalPolicy p = noJitter();

        assertThat(p.intervalSec(5, () -> 0.5)).isEqualTo(3);
        assertThat(p.intervalSec(30, () -> 0.5)).isEqualTo(10);
        assertThat(p.intervalSec(120, () -> 0.5)).isEqualTo(30);
    }

    @Test
    @DisplayName("ETA를_모르면_가장_먼_밴드로_본다")
    void ETA를_모르면_가장_먼_밴드로_본다() {
        // 모를 때 자주 물어보게 하면 모르는 상황일수록 부하가 커진다.
        PollIntervalPolicy p = noJitter();

        assertThat(p.intervalSec(Double.POSITIVE_INFINITY, () -> 0.5)).isEqualTo(30);
        assertThat(p.intervalSec(Double.NaN, () -> 0.5)).isEqualTo(30);
    }

    @Test
    @DisplayName("지터가_적용된다")
    void 지터가_적용된다() {
        // 같은 밴드가 동기화되면 30초마다 전원이 동시에 두드린다.
        PollIntervalPolicy p = PollIntervalPolicy.of(0.2);

        // 난수원을 주입해 결정적으로 본다 (DS-1 — 직접 호출 금지)
        assertThat(p.intervalSec(1000, () -> 0.0)).isEqualTo(24);
        assertThat(p.intervalSec(1000, () -> 0.5)).isEqualTo(30);
        assertThat(p.intervalSec(1000, () -> 1.0)).isEqualTo(36);
    }

    @Test
    @DisplayName("min과_max로_클램프된다")
    void min과_max로_클램프된다() {
        // 지터가 커도 1초 아래로 내려가면 부하 계산이 무너지고,
        // 상한이 없으면 이탈 판정 TTL 을 넘겨 멀쩡한 사람이 지워진다.
        PollIntervalPolicy p = PollIntervalPolicy.of(5.0);

        assertThat(p.intervalSec(1, () -> 0.0)).isEqualTo(1);
        assertThat(p.intervalSec(1000, () -> 1.0)).isEqualTo(60);
    }

    /**
     * <b>배수가 걸린 순간에만 지터가 죽으면 안 된다.</b>
     *
     * <p>흔든 뒤에 자르면 상한 위로 흩어진 값이 전부 상한 하나로 모인다. 배수 2
     * 이상에서 가장 먼 밴드는 전원이 정확히 같은 초에 돌아온다는 뜻이고, 그것이
     * 조회 상한을 다시 밀어 올려 같은 파도가 주기마다 재생산된다.
     *
     * <p>지터가 필요한 구간이 정확히 배수가 걸린 구간이라, 여기서 꺼지면 장치가
     * 있으나 마나다. 평균은 정확히 맞으므로 부하 시험으로는 안 잡힌다.
     */
    @Test
    @DisplayName("상한에_닿아도_지터가_살아_있다")
    void 상한에_닿아도_지터가_살아_있다() {
        PollIntervalPolicy p = PollIntervalPolicy.of(0.2);

        // 가장 먼 밴드(30초)에 배수 5 — 150 초라 흔들기 전에 이미 상한 위다
        long 아래 = p.intervalSec(1000, () -> 0.0, 5.0);
        long 가운데 = p.intervalSec(1000, () -> 0.5, 5.0);
        long 위 = p.intervalSec(1000, () -> 1.0, 5.0);

        assertThat(위).as("상한은 지킨다").isEqualTo(60);
        assertThat(아래).as("흔들림의 아래쪽이 상한에 안 먹힌다").isLessThan(위);
        assertThat(가운데).isStrictlyBetween(아래, 위);
    }

    @Test
    @DisplayName("배수가_적용되면_간격이_늘어난다")
    void 배수가_적용되면_간격이_늘어난다() {
        // 전역 폴링 예산을 넘으면 모두의 간격을 함께 늘린다.
        PollIntervalPolicy p = noJitter();

        assertThat(p.intervalSec(1000, () -> 0.5, 2.0)).isEqualTo(60);
    }

    @Test
    @DisplayName("배수는_1_미만으로_내려가지_않는다")
    void 배수는_1_미만으로_내려가지_않는다() {
        // 한산하다고 더 자주 두드리게 만들면 한산할 때 부하를 만든다.
        PollIntervalPolicy p = noJitter();

        assertThat(p.intervalSec(1000, () -> 0.5, 0.1)).isEqualTo(30);
    }

    @Test
    @DisplayName("가장_긴_간격을_지켜도_약속한_횟수만큼_놓칠_수_있다")
    void 가장_긴_간격을_지켜도_약속한_횟수만큼_놓칠_수_있다() {
        // 서버가 시킨 간격 I 를 지키는 사람이 k 번 놓치고 오는 시각은
        // (k+1)·I 다. 그러니 TTL 이 MISSED_POLLS·I 면 놓칠 수 있는 것은
        // MISSED_POLLS-1 번뿐이고, 마지막 한 번은 초 단위로 정확해야 한다.
        //
        // **배수가 붙기 전에는 이 경계가 안 보였다.** 서버가 말할 수 있는
        // 최대 간격이 36초라 180초 TTL 에 여유가 넉넉했다. 배수가 간격을
        // 상한 60초까지 밀어 올리면서 여유가 0 이 됐고, 그 자리에서 걷히면
        // 재입장은 새 순번이라 **순번 역행**이 된다 (불변식 3·4).
        long 최대_간격 = PollIntervalPolicy.maxInterval().toSeconds();
        long ttl = PollIntervalPolicy.aliveTtl().toSeconds();

        // **약속한 횟수 자체도 못 박는다.** 관계만 재면 이 값이 0 이 돼도
        // 부등식이 성립해, 봐주는 횟수가 사라진 판이 초록으로 남는다.
        assertThat(PollIntervalPolicy.missedPolls())
                .as("백그라운드 탭이 스로틀돼도 안 지워질 만큼").isEqualTo(3);

        // **하한만 재면 길어지는 쪽이 열린다.** 수명이 길수록 죽은 줄이 오래
        // 남아 폴링 예산을 먹고, 청소 재개 유예도 같이 늘어난다 — 이 변경이
        // 없애려는 문제와 같은 것이다. 값으로 못 박는다.
        assertThat(ttl)
                .as("놓쳐도 되는 횟수만큼, 그 이상은 아니게")
                .isEqualTo((PollIntervalPolicy.missedPolls() + 1) * 최대_간격);
    }

    @Test
    @DisplayName("지터_비율이_음수면_거부한다")
    void 지터_비율이_음수면_거부한다() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> PollIntervalPolicy.of(-0.1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("지터_비율이_비유한값이면_거부한다")
    void 지터_비율이_비유한값이면_거부한다() {
        // NaN 이 들어오면 간격 계산이 통째로 NaN 이 되고, 클램프도 못 잡는다.
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> PollIntervalPolicy.of(Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> PollIntervalPolicy.of(Double.POSITIVE_INFINITY))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
