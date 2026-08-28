package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 캐시는 <b>요청 경로의 공유 가변 상태</b>다. 세 연산이 섞인다.
 *
 * <p>한 스레드로만 재면 상한을 넘겨 담는 것도, 새 무장을 지우는 것도 안 보입니다.
 */
class SoldOutCacheConcurrencyTest {

    private static final Instant 발행 = Instant.parse("2026-08-28T00:00:00Z");

    private final AtomicLong 나노 = new AtomicLong();

    private void 동시에(int 스레드, Runnable 일) throws InterruptedException {
        CountDownLatch 출발 = new CountDownLatch(1);
        CountDownLatch 도착 = new CountDownLatch(스레드);
        try (ExecutorService pool = Executors.newFixedThreadPool(스레드)) {
            IntStream.range(0, 스레드).forEach(i -> pool.execute(() -> {
                try {
                    출발.await();
                    일.run();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    도착.countDown();
                }
            }));
            출발.countDown();
            assertThat(도착.await(10, TimeUnit.SECONDS)).as("다 끝났다").isTrue();
        }
    }

    /**
     * <b>상한을 동시에 두드려도 안 넘습니다.</b>
     *
     * <p>검사와 넣기 사이가 원자적이지 않으므로 잠깐 넘을 수는 있습니다. 그러나
     * 그 초과가 스레드 수에 비례해 자라면 상한이 상한이 아닙니다.
     */
    @Test
    @DisplayName("동시에_담아도_상한_근처에_머문다")
    void 동시에_담아도_상한_근처에_머문다() throws InterruptedException {
        int 상한 = 50;
        int 스레드 = 16;
        SoldOutCache 캐시 = SoldOutCache.of(Duration.ofSeconds(10), 상한, 나노::get);
        AtomicInteger 다음 = new AtomicInteger();

        동시에(스레드, () -> {
            for (int i = 0; i < 200; i++) {
                캐시.observed("c" + 다음.incrementAndGet(), 발행);
            }
        });

        assertThat(캐시.size()).as("상한을 크게 안 넘는다").isBetween(상한, 상한 + 스레드);
    }

    /**
     * <b>해제가 새 무장을 지우면 안 됩니다.</b>
     *
     * <p>재입고를 본 순간과 다시 매진이 되는 순간이 겹치면, 값을 안 보고 지우는
     * 구현은 <b>방금 들어온 무장</b>을 지웁니다. 그러면 그 창의 요청이 전부
     * 뒷단으로 갑니다.
     */
    @Test
    @DisplayName("해제가_새_무장을_안_지운다")
    void 해제가_새_무장을_안_지운다() throws InterruptedException {
        SoldOutCache 캐시 = SoldOutCache.of(Duration.ofSeconds(10), 100, 나노::get);
        AtomicInteger 잃음 = new AtomicInteger();

        for (int 회 = 0; 회 < 300; 회++) {
            캐시.observed("c1", 발행);
            동시에(2, () -> {
                // 한쪽은 재입고를 보고 풀고, 다른 쪽은 다시 매진을 관찰한다.
                캐시.restocked("c1", 발행.plusSeconds(1));
                캐시.observed("c1", 발행.plusSeconds(2));
            });
            // 나중 발행으로 무장했으므로, 그 앞의 재료로는 못 푼다.
            if (!캐시.soldOut("c1") && 캐시.restocked("c1", 발행.plusSeconds(1)).isEmpty()) {
                잃음.incrementAndGet();
            }
            캐시.restocked("c1", 발행.plusSeconds(99));
        }

        assertThat(잃음).as("무장이 통째로 사라진 회차").hasValue(0);
    }

    /** 끊은 건수가 새지 않습니다. `LongAdder` 를 쓰는 이유가 여기 있습니다. */
    @Test
    @DisplayName("끊은_건수를_잃지_않는다")
    void 끊은_건수를_잃지_않는다() throws InterruptedException {
        SoldOutCache 캐시 = SoldOutCache.of(Duration.ofSeconds(10), 100, 나노::get);
        캐시.observed("c1", 발행);
        int 스레드 = 8;
        int 회 = 500;

        동시에(스레드, () -> {
            for (int i = 0; i < 회; i++) {
                캐시.soldOut("c1");
            }
        });

        assertThat(캐시.restocked("c1", 발행.plusSeconds(1)).orElseThrow().blocked())
                .isEqualTo((long) 스레드 * 회);
    }
}
