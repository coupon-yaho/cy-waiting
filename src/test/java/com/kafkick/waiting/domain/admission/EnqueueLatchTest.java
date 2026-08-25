package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 줄에 세운 직후의 한 구간을 메운다.
 *
 * <p>스냅샷은 한 틱 늦다. 그 사이 다음 초 창이 열리면 <b>방금 줄 선 사람을 신규
 * 유입이 넘어간다</b> — 스냅샷은 아직 한산하다고 말하기 때문이다 (불변식 4).
 */
class EnqueueLatchTest {

    private static final String COUPON = "c1";

    private final EnqueueLatch latch = EnqueueLatch.of(1_000, 3);

    @Test
    @DisplayName("세운_적_없으면_안_걸린다")
    void 세운_적_없으면_안_걸린다() {
        assertThat(latch.latched(COUPON, 100)).isFalse();
    }

    @Test
    @DisplayName("세우면_걸린다")
    void 세우면_걸린다() {
        latch.mark(COUPON, 100);

        assertThat(latch.latched(COUPON, 100)).isTrue();
    }

    /** 스냅샷이 따라잡을 때까지는 남아 있어야 한다. 한 틱만 살면 그 다음 창이 뚫린다. */
    @Test
    @DisplayName("스냅샷이_따라잡을_때까지_산다")
    void 스냅샷이_따라잡을_때까지_산다() {
        latch.mark(COUPON, 100);

        assertThat(latch.latched(COUPON, 102)).isTrue();
        assertThat(latch.latched(COUPON, 103)).isFalse();
    }

    /** 영원히 걸려 있으면 한 번 붐빈 쿠폰이 영영 안 풀린다. */
    @Test
    @DisplayName("지나면_풀린다")
    void 지나면_풀린다() {
        latch.mark(COUPON, 100);

        assertThat(latch.latched(COUPON, 200)).isFalse();
    }

    @Test
    @DisplayName("쿠폰마다_따로_건다")
    void 쿠폰마다_따로_건다() {
        latch.mark(COUPON, 100);

        assertThat(latch.latched("다른쿠폰", 100)).isFalse();
    }

    /** 시계가 뒤로 가도 미래의 표식이 영원히 살면 안 된다. */
    @Test
    @DisplayName("시계가_뒤로_가도_영원히_안_산다")
    void 시계가_뒤로_가도_영원히_안_산다() {
        latch.mark(COUPON, 1_000);

        assertThat(latch.latched(COUPON, 100)).isFalse();
    }

    /**
     * 인증이 없어 쿠폰 식별자로 아무 문자열이나 들어온다. 상한이 없으면 그것으로
     * 메모리를 밀어낼 수 있다.
     */
    @Test
    @DisplayName("키가_무제한으로_안_는다")
    void 키가_무제한으로_안_는다() {
        EnqueueLatch 좁은_것 = EnqueueLatch.of(10, 3);

        IntStream.range(0, 1_000).forEach(i -> 좁은_것.mark("c" + i, 100));

        assertThat(좁은_것.size()).isLessThanOrEqualTo(10);
    }

    /**
     * 이미 걸린 쿠폰을 다시 세우는 것은 붐비는 동안 늘 일어난다. 그때마다
     * 통째로 비우면 남의 래치가 사라져 그 쿠폰들이 추월당한다.
     */
    @Test
    @DisplayName("있던_쿠폰을_다시_세워도_남의_래치가_안_사라진다")
    void 있던_쿠폰을_다시_세워도_남의_래치가_안_사라진다() {
        EnqueueLatch 좁은_것 = EnqueueLatch.of(2, 3);
        좁은_것.mark("a", 100);
        좁은_것.mark("b", 100);

        좁은_것.mark("a", 101);

        assertThat(좁은_것.latched("b", 101)).isTrue();
        assertThat(좁은_것.size()).isEqualTo(2);
    }

    @Test
    @DisplayName("수명이_양수가_아니면_안_만들어진다")
    void 수명이_양수가_아니면_안_만들어진다() {
        // 0 이면 세우자마자 풀려서 래치가 있으나 마나다.
        assertThatThrownBy(() -> EnqueueLatch.of(10, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 보정하면 0 을 넘긴 사람이 상한 0 을 기대하는데 하나가 남는다. */
    @Test
    @DisplayName("키_상한이_양수가_아니면_안_만들어진다")
    void 키_상한이_양수가_아니면_안_만들어진다() {
        assertThatThrownBy(() -> EnqueueLatch.of(0, 3))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EnqueueLatch.of(-1, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * 검사와 삽입을 나누면 여럿이 동시에 "아직 여유 있다" 를 보고 다 같이 넣는다.
     * 그러면 상한을 넘긴 채로 남는다.
     */
    @Test
    @DisplayName("동시에_세워도_상한을_안_넘긴_채로_남는다")
    void 동시에_세워도_상한을_안_넘긴_채로_남는다() throws InterruptedException {
        int 상한 = 50;
        EnqueueLatch 좁은_것 = EnqueueLatch.of(상한, 3);
        int 스레드 = 32;
        CountDownLatch 출발 = new CountDownLatch(1);
        CountDownLatch 도착 = new CountDownLatch(스레드);
        ExecutorService pool = Executors.newFixedThreadPool(스레드);
        try {
            for (int t = 0; t < 스레드; t++) {
                int 나 = t;
                pool.execute(() -> {
                    try {
                        출발.await();
                        for (int i = 0; i < 200; i++) {
                            좁은_것.mark("c" + 나 + "-" + i, 100);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        도착.countDown();
                    }
                });
            }
            출발.countDown();
            assertThat(도착.await(30, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        // 넘긴 뒤에 비우므로 동시에 들어온 수만큼은 잠깐 넘칠 수 있다.
        // 다 끝난 뒤에는 상한 + 스레드 수 안으로 돌아와 있어야 한다.
        assertThat(좁은_것.size()).isLessThanOrEqualTo(상한 + 스레드);
    }

    /**
     * <b>0 이하를 받으면 수명 1 초짜리 래치가 된다.</b> 래치가 없는 것과 같은데
     * 있는 것처럼 보여, 추월 창이 열린 것을 아무도 모른다.
     */
    @Test
    @DisplayName("덮을_기간이_0_이하면_안_만들어진다")
    void 덮을_기간이_0_이하면_안_만들어진다() {
        assertThatThrownBy(() -> EnqueueLatch.covering(1_000, Duration.ofMillis(-500)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EnqueueLatch.covering(1_000, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EnqueueLatch.covering(1_000, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 소수부가 있으면 올린다. 내리면 절삭까지 겹쳐 덮으려던 기간보다 짧아진다. */
    @Test
    @DisplayName("덮을_기간의_소수부는_올린다")
    void 덮을_기간의_소수부는_올린다() {
        EnqueueLatch 소수 = EnqueueLatch.covering(1_000, Duration.ofMillis(5_500));
        소수.mark(COUPON, 100);

        // 올림(6) + 여유(1) = 7. 106 은 살아 있고 107 은 아니다.
        assertThat(소수.latched(COUPON, 106)).isTrue();
        assertThat(소수.latched(COUPON, 107)).isFalse();
    }

    /**
     * <b>이미 걸린 래치는 시각을 안 고친다.</b> 대기 판정이 다시 표식을 찍는
     * 닫힌 고리가 있어, 갱신하면 트래픽이 이어지는 동안 영영 안 풀린다.
     */
    @Test
    @DisplayName("다시_찍어도_수명이_안_늘어난다")
    void 다시_찍어도_수명이_안_늘어난다() {
        latch.mark(COUPON, 100);
        latch.mark(COUPON, 102);

        // 첫 표식(100)부터 3 초다. 갱신되면 105 에도 살아 있다.
        assertThat(latch.latched(COUPON, 102)).isTrue();
        assertThat(latch.latched(COUPON, 103)).isFalse();
    }

    /** 만료된 표식은 그 자리에서 지운다. 안 지우면 맵이 프로세스 수명 동안 자란다. */
    @Test
    @DisplayName("만료를_보면_그_자리에서_지운다")
    void 만료를_보면_그_자리에서_지운다() {
        latch.mark(COUPON, 100);
        assertThat(latch.size()).isEqualTo(1);

        latch.latched(COUPON, 200);

        assertThat(latch.size()).isZero();
    }
}
