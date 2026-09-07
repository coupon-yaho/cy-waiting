package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 통과 계량기. <b>세는 쪽과 읽는 쪽이 겹치는 자리를 본다</b> — 이 값은 회복
 * 봉우리를 정상과 견줄 재료라, 틀려도 지표로 볼 때는 눈에 안 띈다 (RC4).
 */
class PassRateMeterTest {

    private static final long WINDOW_MS = 5_000;

    /** 관측이 없으면 0 이다. 없는 부하를 지어내면 상한이 실제보다 높아진다. */
    @Test
    @DisplayName("아무것도_안_지나가면_0")
    void 아무것도_안_지나가면_0() {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);

        assertThat(meter.perSecond(1_000)).isZero();
    }

    /** 창을 다 채운 뒤라야 초당으로 나눌 수 있다. */
    @Test
    @DisplayName("창을_채우면_초당으로_낸다")
    void 창을_채우면_초당으로_낸다() {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);
        for (int i = 0; i < 300; i++) {
            meter.passed(1_000 + i);
        }

        assertThat(meter.perSecond(6_000)).isEqualTo(60);
    }

    /**
     * <b>덜 찬 창은 실제 구간으로 나눈다.</b> 창 길이로 나누면 회복 첫 초의
     * 봉우리가 5분의 1로 눌려, 재려던 전이가 지표에서 사라진다.
     */
    @Test
    @DisplayName("덜_찬_창은_실제_구간으로_나눈다")
    void 덜_찬_창은_실제_구간으로_나눈다() {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);
        for (int i = 0; i < 100; i++) {
            meter.passed(1_000 + i * 10);
        }

        // 100건이 1초 동안 왔다. 그 1초로 나눈 100 이다.
        assertThat(meter.perSecond(2_000)).isEqualTo(100);
    }

    /**
     * <b>구간에 바닥을 둔다.</b> 실제 구간만 쓰면 5ms 에 지나간 세 건이 600건/s
     * 가 되고, 이 값을 상한으로 쓰는 순간 그 수에 묶인다.
     */
    @Test
    @DisplayName("첫_창도_부풀리지_않는다")
    void 첫_창도_부풀리지_않는다() {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);
        meter.passed(1_000);
        meter.passed(1_002);
        meter.passed(1_005);

        assertThat(meter.perSecond(1_005)).as("5ms 가 아니라 바닥 1초로 나눈다")
                .isEqualTo(3);
    }

    /**
     * <b>읽어도 상태를 안 바꾼다.</b> 읽기가 창을 접으면 묻는 주기에 따라 값이
     * 달라지고, 그만큼 상한이 흔들린다.
     */
    @Test
    @DisplayName("두_번_물어도_같은_값이다")
    void 두_번_물어도_같은_값이다() {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);
        for (int i = 0; i < 300; i++) {
            meter.passed(1_000 + i);
        }

        assertThat(meter.perSecond(6_000)).isEqualTo(60);
        assertThat(meter.perSecond(6_000)).as("두 번째 물음이 값을 바꾸면 안 된다")
                .isEqualTo(60);
    }

    /** 부하가 멎으면 그 값도 따라 내려간다. 지난 봉우리가 지금의 상한을 정하면 안 된다. */
    @Test
    @DisplayName("부하가_멎으면_다음_창에서_내려간다")
    void 부하가_멎으면_다음_창에서_내려간다() {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);
        for (int i = 0; i < 300; i++) {
            meter.passed(1_000 + i);
        }
        // 창 두 배를 넘겨 접는다. 그 사이에 한 건도 안 왔다는 뜻이라 물려줄
        // 값이 없고, 새 창은 한 건뿐이다.
        meter.passed(20_000);

        assertThat(meter.perSecond(24_000)).as("지난 봉우리 60 이 아니다").isOne();
    }

    /** 읽는 시각이 창보다 앞서면 직전 값을 쓴다. 지어낸 값을 내면 안 된다. */
    @Test
    @DisplayName("읽는_시계가_뒤로_가도_안_터진다")
    void 읽는_시계가_뒤로_가도_안_터진다() {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);
        for (int i = 0; i < 300; i++) {
            meter.passed(1_000 + i);
        }
        // 창을 접어 직전 값 60 을 만든 뒤 그 앞 시각으로 묻는다.
        meter.passed(7_000);

        assertThat(meter.perSecond(1_000)).isEqualTo(60);
    }

    /** 전부 같은 밀리초에 와도 바닥이 있어 무한이 안 된다. */
    @Test
    @DisplayName("같은_밀리초에_몰려도_안_부푼다")
    void 같은_밀리초에_몰려도_안_부푼다() {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);
        for (int i = 0; i < 300; i++) {
            meter.passed(1_000);
        }

        assertThat(meter.perSecond(1_000)).isEqualTo(300);
    }

    /**
     * <b>시계가 뒤로 가면 접고 새로 연다.</b> 안 접으면 따라잡을 때까지 얼고,
     * 그동안 쌓인 수가 한 창 분량으로 접혀 실제의 몇 배를 낸다.
     */
    @Test
    @DisplayName("시계가_뒤로_가면_접고_다시_연다")
    void 시계가_뒤로_가면_접고_다시_연다() {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);
        for (int i = 0; i < 60_000; i++) {
            meter.passed(70_000 + i);
        }
        // NTP 가 60초를 뒤로 돌린다. 그 뒤 같은 부하가 이어진다.
        meter.passed(10_000);

        // 얼린 창을 12000/s 로 내면 안 된다. 접힌 창이 4989건을 5초 동안
        // 받았으므로 998 이고, 실제 부하 1000건/s 와 같은 자리다.
        assertThat(meter.perSecond(11_000)).isEqualTo(998);
    }

    /** 창이 반도 안 찼으면 직전 값을 쓴다. 그 값이 없으면 지금 것을 쓸 수밖에 없다. */
    @Test
    @DisplayName("직전_창이_있으면_덜_찬_창_대신_쓴다")
    void 직전_창이_있으면_덜_찬_창_대신_쓴다() {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);
        for (int i = 0; i < 300; i++) {
            meter.passed(1_000 + i);
        }
        // 창을 접고 새 창을 연다.
        meter.passed(7_000);

        // 300건을 창(5초)으로 나눈 60 이다. 값을 못 박아야 0 으로 바꿔도 잡힌다.
        assertThat(meter.perSecond(7_100)).as("새 창은 100ms 뿐이라 직전 값을 쓴다")
                .isEqualTo(60);
    }

    /**
     * <b>유휴 구간을 분모에 넣으면 안 된다.</b> 부하가 멎어 있는 동안은 창이 안
     * 접히므로, 다음 유입의 첫 건이 그 긴 구간으로 나눈 값을 직전 값으로 만든다.
     * 그러면 세일이 열린 직후 몇 초 동안 한 자리 수를 보고한다.
     */
    @Test
    @DisplayName("유휴_뒤_첫_유입에_감쇠값을_안_낸다")
    void 유휴_뒤_첫_유입에_감쇠값을_안_낸다() {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);
        for (int i = 0; i < 300; i++) {
            meter.passed(1_000 + i);
        }
        // 60초를 쉰다. 그동안 아무도 안 지나간다.
        meter.passed(66_000);

        // 직전 창은 아무도 안 지나간 구간이라 물려줄 값이 없다. 새 창이 차는
        // 대로 그 값을 쓴다 — 60초로 나눈 5 도, 지난 봉우리 60 도 아니다.
        assertThat(meter.perSecond(66_100)).isOne();
        for (int i = 0; i < 500; i++) {
            meter.passed(66_100 + i);
        }
        assertThat(meter.perSecond(66_700)).as("유입이 오는 대로 따라 오른다")
                .isEqualTo(501);
    }

    /**
     * <b>부하 중에 0 을 내면 안 된다.</b> 셋을 따로 두면 읽는 쪽이 새 창의 시작과
     * 리셋된 수를 짝지어, 한 건도 안 끊긴 구간에서 0 이 나온다. 1ms 뒤진 도장
     * 하나로 창을 버려도 같은 값이 나온다.
     */
    @Test
    @DisplayName("읽기와_세기가_겹쳐도_0_이_안_나온다")
    void 읽기와_세기가_겹쳐도_0_이_안_나온다() throws Exception {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);
        for (int i = 0; i < 300; i++) {
            meter.passed(1_000 + i);
        }
        // 창 하나를 접어 둔다. 이 뒤로는 어떤 회차에도 0 이 나오면 안 된다.
        meter.passed(7_000);

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
        CountDownLatch start = new CountDownLatch(1);
        // **시계를 나눠 쓴다.** 창을 여러 번 넘겨야 접힘 경로가 돌고, 스레드가
        // 도장을 각자 찍으므로 뒤진 도장도 실제로 난다 — 그것이 결함이 났던 자리다.
        AtomicLong 시계 = new AtomicLong();
        List<Future<?>> running = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            running.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < 20_000; i++) {
                    meter.passed(7_000 + 시계.getAndIncrement() / 20);
                }
                return null;
            }));
        }
        Future<Long> lowest = pool.submit(() -> {
            start.await();
            long low = Long.MAX_VALUE;
            for (int i = 0; i < 200_000; i++) {
                low = Math.min(low, meter.perSecond(7_000 + 시계.get() / 20));
            }
            return low;
        });
        long low;
        try {
            start.countDown();
            for (Future<?> f : running) {
                f.get(30, TimeUnit.SECONDS);
            }
            low = lowest.get(30, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }

        assertThat(low).isPositive();
    }

    /** 무잠금이라 잃기 쉬운 것은 원자성이 아니라 셈이다. 한 건도 안 샌다. */
    @Test
    @DisplayName("동시에_세도_한_건도_안_샌다")
    void 동시에_세도_한_건도_안_샌다() throws Exception {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> running = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            running.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < 5_000; i++) {
                    // 같은 시각이라 접히지 않는다. 남는 것은 셈뿐이다.
                    meter.passed(1_000);
                }
                return null;
            }));
        }
        try {
            start.countDown();
            for (Future<?> f : running) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        // 도장이 전부 같아 접히지 않는다. 바닥 1초로 나눈 값이 곧 센 수다.
        assertThat(meter.perSecond(1_000)).isEqualTo(threads * 5_000);
    }

    /**
     * <b>두 창이 지나도록 아무도 안 오면 0 이다.</b> 통과가 있었으면 그때 접혔을
     * 것이므로, 남은 값은 최소 한 창 전의 부하다 — 지난 봉우리를 지금 것으로
     * 보고하면 상한이 그 값에 묶인다.
     */
    @Test
    @DisplayName("두_창을_쉬면_0_으로_내려간다")
    void 두_창을_쉬면_0_으로_내려간다() {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);
        for (int i = 0; i < 300; i++) {
            meter.passed(1_000 + i);
        }
        // 창을 접어 직전 값 60 을 만든다.
        meter.passed(7_000);

        assertThat(meter.perSecond(7_100)).as("아직 한 창 안이라 직전 값을 쓴다")
                .isEqualTo(60);
        assertThat(meter.perSecond(17_100)).as("두 창을 넘기면 지난 부하다").isZero();
    }

    /** 뒤진 도장 하나가 창을 버리면, 끊긴 적 없는 부하가 한 창 내내 0 이다. */
    @Test
    @DisplayName("뒤진_도장은_창을_안_버린다")
    void 뒤진_도장은_창을_안_버린다() {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);
        for (int i = 0; i < 5_000; i++) {
            meter.passed(1_000 + i);
        }
        // 창을 접는다. 그 직후 1ms 뒤진 도장이 들어온다.
        meter.passed(6_001);
        meter.passed(6_000);

        assertThat(meter.perSecond(6_001)).as("직전 창의 값이 남아야 한다")
                .isEqualTo(1_000);
    }

    /** 창 길이가 크면 두 배가 넘쳐 음수가 되고 값이 영영 0 이다. */
    /** 창이 길면 한두 건이 0 으로 반올림된다. 통과가 있었으면 0 을 내면 안 된다. */
    @Test
    @DisplayName("한_건도_0_으로_안_반올림한다")
    void 한_건도_0_으로_안_반올림한다() {
        PassRateMeter meter = PassRateMeter.of(WINDOW_MS);
        meter.passed(1_000);

        assertThat(meter.perSecond(1_000)).isOne();
    }

    @Test
    @DisplayName("창은_상한을_넘을_수_없다")
    void 창은_상한을_넘을_수_없다() {
        assertThatThrownBy(() -> PassRateMeter.of(Long.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("창은_양수여야_한다")
    void 창은_양수여야_한다() {
        assertThatThrownBy(() -> PassRateMeter.of(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
