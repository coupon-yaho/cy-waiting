package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 시계 역행 관측.
 *
 * <p>세는 값이 틀리면 <b>있었는지 없었는지도 못 믿는다.</b> 그래서 동시 갱신에서
 * 정확한지까지 본다.
 */
class ClockSkewTrackerTest {

    @Test
    @DisplayName("보정이_없으면_아무것도_세지_않는다")
    void 보정이_없으면_아무것도_세지_않는다() {
        ClockSkewTracker tracker = ClockSkewTracker.create();
        tracker.record(false, 0);
        tracker.record(false, 999);

        assertThat(tracker.appliedCount()).isZero();
        assertThat(tracker.maxSkewMicros()).isZero();
    }

    @Test
    @DisplayName("보정하면_횟수와_최대_폭을_남긴다")
    void 보정하면_횟수와_최대_폭을_남긴다() {
        ClockSkewTracker tracker = ClockSkewTracker.create();
        tracker.record(true, 300);
        tracker.record(true, 1200);
        tracker.record(true, 500);

        assertThat(tracker.appliedCount()).isEqualTo(3);
        assertThat(tracker.maxSkewMicros()).isEqualTo(1200);
    }

    @Test
    @DisplayName("음수_폭은_0으로_본다")
    void 음수_폭은_0으로_본다() {
        // 바닥값이 실제 시각보다 뒤면 역행이 아니다. 음수를 그대로 두면
        // 최대값이 음수가 되어 지표가 말이 안 된다.
        ClockSkewTracker tracker = ClockSkewTracker.create();
        tracker.record(true, -500);

        assertThat(tracker.appliedCount()).isOne();
        assertThat(tracker.maxSkewMicros()).isZero();
    }

    @Test
    @DisplayName("동시에_기록해도_횟수가_정확하다")
    void 동시에_기록해도_횟수가_정확하다() throws InterruptedException {
        // 요청 경로에서 여러 스레드가 동시에 부른다. 세는 값이 틀리면
        // 시계가 뒤로 갔는지조차 못 믿는다.
        ClockSkewTracker tracker = ClockSkewTracker.create();
        int threads = 16;
        int perThread = 1000;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            for (int t = 0; t < threads; t++) {
                int base = t;
                pool.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            tracker.record(true, base * 1000L + i);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(tracker.appliedCount()).isEqualTo((long) threads * perThread);
        assertThat(tracker.maxSkewMicros()).isEqualTo(15 * 1000L + (perThread - 1));
    }
}
