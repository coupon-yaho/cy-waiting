package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 동시에 몇 건이 뒷단에 걸려 있는지를 셉니다.
 *
 * <p><b>레이트 리밋으로는 못 막는 것이 있습니다.</b> 초당 100건이어도 각각 10초가
 * 걸리면 동시 1,000건입니다. 느려진 뒷단 한 대가 커넥션을 다 붙잡으면 한산한
 * 쿠폰의 통과 경로까지 같이 죽습니다.
 */
class BulkheadTest {

    /** 전체 상한을 안 재는 시험이다. 쿠폰별 상한만 보게 넉넉히 둔다. */
    private static final long NO_TOTAL_CAP = Long.MAX_VALUE;


    private final Bulkhead bulkhead = Bulkhead.withMaxKeys(10);

    /** 상한 안에서는 그대로 들여보냅니다. */
    @Test
    @DisplayName("상한_안에서는_들여보낸다")
    void 상한_안에서는_들여보낸다() {
        assertThat(bulkhead.tryEnter("c1", 2, NO_TOTAL_CAP)).isTrue();
        assertThat(bulkhead.tryEnter("c1", 2, NO_TOTAL_CAP)).isTrue();
    }

    /** 상한을 넘으면 막습니다. 이 자리가 없으면 커넥션이 무한히 쌓입니다. */
    @Test
    @DisplayName("상한을_넘으면_막는다")
    void 상한을_넘으면_막는다() {
        bulkhead.tryEnter("c1", 2, NO_TOTAL_CAP);
        bulkhead.tryEnter("c1", 2, NO_TOTAL_CAP);

        assertThat(bulkhead.tryEnter("c1", 2, NO_TOTAL_CAP)).isFalse();
    }

    /**
     * <b>나가면 자리가 돌아옵니다.</b> 안 돌려주면 격벽이 한 번 차고 나서 영영
     * 안 열리고, 그 쿠폰은 뒷단이 멀쩡해져도 계속 막힙니다.
     */
    @Test
    @DisplayName("나가면_자리가_돌아온다")
    void 나가면_자리가_돌아온다() {
        bulkhead.tryEnter("c1", 1, NO_TOTAL_CAP);

        bulkhead.exit("c1");

        assertThat(bulkhead.tryEnter("c1", 1, NO_TOTAL_CAP)).isTrue();
    }

    /**
     * <b>핫 쿠폰이 콜드 쿠폰의 통로를 막으면 안 됩니다.</b> 하나로 세면 몰리는
     * 쿠폰이 자리를 다 쓰고, 한산한 쿠폰이 그 뒤에 밀립니다 — R1 이 뒤집힙니다.
     */
    @Test
    @DisplayName("쿠폰마다_따로_센다")
    void 쿠폰마다_따로_센다() {
        // 선행 조건을 잰다. 핫이 안 들어갔으면 콜드가 통과해도 아무것도 안 잰 것이다.
        assertThat(bulkhead.tryEnter("핫", 1, NO_TOTAL_CAP)).isTrue();
        assertThat(bulkhead.tryEnter("핫", 1, NO_TOTAL_CAP)).as("핫은 자기 상한에서 막힌다").isFalse();

        assertThat(bulkhead.tryEnter("콜드", 1, NO_TOTAL_CAP)).isTrue();
    }

    /**
     * <b>안 들어간 것을 내보내면 안 됩니다.</b> 자리가 음수로 내려가면 그만큼
     * 상한이 늘어나고, 격벽이 있으나 마나가 됩니다.
     */
    @Test
    @DisplayName("안_들어간_것을_내보내도_음수가_안_된다")
    void 안_들어간_것을_내보내도_음수가_안_된다() {
        bulkhead.exit("c1");
        bulkhead.exit("c1");

        assertThat(bulkhead.tryEnter("c1", 1, NO_TOTAL_CAP)).isTrue();
        assertThat(bulkhead.tryEnter("c1", 1, NO_TOTAL_CAP)).isFalse();
    }

    /**
     * <b>상한이 0 이면 아무도 못 들어갑니다.</b> 크레딧이 0 인 구간이 실제로
     * 있으므로, 그때 전면 차단이 되지 않게 부르는 쪽이 폴백을 정해야 합니다.
     */
    @Test
    @DisplayName("상한이_0이면_아무도_못_들어간다")
    void 상한이_0이면_아무도_못_들어간다() {
        assertThat(bulkhead.tryEnter("c1", 0, NO_TOTAL_CAP)).isFalse();
    }

    /**
     * <b>맵이 무한히 자라면 안 됩니다.</b> 쿠폰 식별자는 밖에서 오는 값이라
     * 가짓수에 상한이 없습니다. 리미터와 같은 방식으로 막습니다.
     */
    @Test
    @DisplayName("키가_상한을_넘으면_새_쿠폰을_안_받는다")
    void 키가_상한을_넘으면_새_쿠폰을_안_받는다() {
        for (int i = 0; i < 10; i++) {
            assertThat(bulkhead.tryEnter("c" + i, 5, NO_TOTAL_CAP)).isTrue();
        }

        assertThat(bulkhead.tryEnter("넘친다", 5, NO_TOTAL_CAP)).isFalse();
    }

    /**
     * <b>비면 자리를 내줍니다.</b> 안 그러면 끝난 쿠폰이 맵을 차지한 채 남아,
     * 새 캠페인이 열릴 때 그 쿠폰이 못 들어갑니다.
     */
    @Test
    @DisplayName("비면_맵에서_빠진다")
    void 비면_맵에서_빠진다() {
        for (int i = 0; i < 10; i++) {
            assertThat(bulkhead.tryEnter("c" + i, 5, NO_TOTAL_CAP)).isTrue();
        }
        // 맵이 실제로 찼는지부터 잰다. 안 찼으면 exit 이 없어도 새것이 들어가고,
        // 이 시험은 "비면 빠진다" 를 전혀 안 잰 채 초록으로 남는다.
        assertThat(bulkhead.size()).isEqualTo(10);
        assertThat(bulkhead.tryEnter("가득", 5, NO_TOTAL_CAP)).as("찬 뒤에는 새 쿠폰을 안 받는다").isFalse();

        bulkhead.exit("c0");

        assertThat(bulkhead.tryEnter("새것", 5, NO_TOTAL_CAP)).isTrue();
    }

    /** 지금 몇 건이 걸려 있는지. 지표가 이 값을 읽습니다. */
    @Test
    @DisplayName("걸려_있는_건수를_센다")
    void 걸려_있는_건수를_센다() {
        bulkhead.tryEnter("c1", 3, NO_TOTAL_CAP);
        bulkhead.tryEnter("c1", 3, NO_TOTAL_CAP);
        bulkhead.tryEnter("c2", 3, NO_TOTAL_CAP);

        assertThat(bulkhead.inFlight()).isEqualTo(3);
    }

    /** 키 상한이 0 이하면 아무것도 못 담는다. 그건 설정 실수다. */
    @Test
    @DisplayName("키_상한이_0이하면_거부한다")
    void 키_상한이_0이하면_거부한다() {
        assertThatThrownBy(() -> Bulkhead.withMaxKeys(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 다 나가면 0 으로 돌아온다. 안 돌아오면 지표가 영영 안 내려간다. */
    @Test
    @DisplayName("다_나가면_걸린_건수가_0이_된다")
    void 다_나가면_걸린_건수가_0이_된다() {
        bulkhead.tryEnter("c1", 3, NO_TOTAL_CAP);
        bulkhead.tryEnter("c2", 3, NO_TOTAL_CAP);

        bulkhead.exit("c1");
        bulkhead.exit("c2");

        assertThat(bulkhead.inFlight()).isZero();
        assertThat(bulkhead.size()).isZero();
    }

    /**
     * <b>검사와 증가를 나누면 여럿이 동시에 "아직 자리 있다" 를 보고 다 들어간다.</b>
     * 요청 경로에서 공유하는 값이라 그때 상한이 있으나 마나가 된다.
     */
    @Test
    @DisplayName("동시에_들어와도_상한만큼만_들어간다")
    void 동시에_들어와도_상한만큼만_들어간다() throws InterruptedException {
        int 상한 = 50;
        int 스레드 = 32;
        AtomicInteger 들어간_수 = new AtomicInteger();
        CountDownLatch 출발 = new CountDownLatch(1);
        CountDownLatch 도착 = new CountDownLatch(스레드);
        ExecutorService pool = Executors.newFixedThreadPool(스레드);
        try {
            for (int t = 0; t < 스레드; t++) {
                pool.execute(() -> {
                    try {
                        출발.await();
                        for (int i = 0; i < 200; i++) {
                            if (bulkhead.tryEnter("c1", 상한, NO_TOTAL_CAP)) {
                                들어간_수.incrementAndGet();
                            }
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

        // 아무도 안 나갔으니 정확히 상한만큼만 들어가 있어야 한다.
        assertThat(들어간_수).hasValue(상한);
        assertThat(bulkhead.inFlight()).isEqualTo(상한);
    }

    /**
     * <b>들어간 만큼 나가면 정확히 0 이다.</b> 반납이 경합에서 하나라도 새면
     * 격벽이 조금씩 조여져, 뒷단이 멀쩡한데도 서서히 막히기 시작한다.
     */
    @Test
    @DisplayName("동시에_드나들어도_건수가_안_샌다")
    void 동시에_드나들어도_건수가_안_샌다() throws InterruptedException {
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
                            String 쿠폰 = "c" + (나 % 4);
                            if (bulkhead.tryEnter(쿠폰, Long.MAX_VALUE, NO_TOTAL_CAP)) {
                                bulkhead.exit(쿠폰);
                            }
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

        assertThat(bulkhead.inFlight()).isZero();
        assertThat(bulkhead.size()).isZero();
    }

    /**
     * <b>쿠폰별 상한만으로는 합이 안 묶인다.</b> 캠페인이 여럿이면 각자 제
     * 상한까지 쓰고, 그 합이 노드가 감당할 수 있는 양을 넘는다 — 한산한 쿠폰의
     * 통과 경로까지 같이 죽는 자리다.
     */
    @Test
    @DisplayName("전체_상한이_쿠폰들의_합을_묶는다")
    void 전체_상한이_쿠폰들의_합을_묶는다() {
        assertThat(bulkhead.tryEnter("c1", 5, 3)).isTrue();
        assertThat(bulkhead.tryEnter("c1", 5, 3)).isTrue();
        assertThat(bulkhead.tryEnter("c1", 5, 3)).isTrue();

        assertThat(bulkhead.tryEnter("c1", 5, 3))
                .as("쿠폰별로는 남았지만 노드 전체가 찼다")
                .isFalse();
    }

    /** 자리를 놓으면 전체도 같이 풀린다. 안 그러면 한 번 차고 영영 안 열린다. */
    @Test
    @DisplayName("놓으면_전체_상한도_풀린다")
    void 놓으면_전체_상한도_풀린다() {
        bulkhead.tryEnter("c1", 5, 2);
        bulkhead.tryEnter("c1", 5, 2);
        assertThat(bulkhead.tryEnter("c1", 5, 2)).isFalse();

        bulkhead.exit("c1");

        assertThat(bulkhead.tryEnter("c1", 5, 2)).isTrue();
    }

    /**
     * <b>전체가 차도 한산한 쿠폰의 첫 자리는 지난다.</b> 막으면 몰리는 쿠폰이
     * 노드를 채운 동안 한산한 쿠폰이 통째로 밀린다 — 불변식 1(R1)이 뒤집힌다.
     */
    @Test
    @DisplayName("전체가_차도_한산한_쿠폰은_지나간다")
    void 전체가_차도_한산한_쿠폰은_지나간다() {
        bulkhead.tryEnter("핫", 5, 2);
        bulkhead.tryEnter("핫", 5, 2);
        assertThat(bulkhead.tryEnter("핫", 5, 2)).as("핫은 막힌다").isFalse();

        assertThat(bulkhead.tryEnter("콜드", 5, 2))
                .as("한산한 쿠폰의 첫 자리는 지난다")
                .isTrue();
    }

    /**
     * <b>첫 자리 하나로는 R1 이 아니다.</b> 몰리는 쿠폰이 노드를 채운 동안
     * 한산한 쿠폰이 동시 1건에 묶이면, 그 쿠폰의 처리량이 제 몫이 아니라
     * 뒷단 지연에 묶인다 — 응답이 느릴수록 R1 이 더 크게 깨진다.
     *
     * <p>노드가 천장에 닿으면 <b>제 몫보다 많이 쥔 쿠폰이 물러난다.</b>
     */
    @Test
    @DisplayName("전체가_차도_한산한_쿠폰은_제_몫을_쓴다")
    void 전체가_차도_한산한_쿠폰은_제_몫을_쓴다() {
        // 핫이 노드 전체(8)를 혼자 채운다.
        for (int i = 0; i < 8; i++) {
            assertThat(bulkhead.tryEnter("핫", 20, 8)).as("%d 번째", i).isTrue();
        }

        // 한산한 쿠폰이 온다. 둘이 나눠 쓰면 넷씩이므로 넷까지는 지나야 한다.
        for (int i = 0; i < 4; i++) {
            assertThat(bulkhead.tryEnter("콜드", 20, 8))
                    .as("한산한 쿠폰의 %d 번째 자리", i).isTrue();
        }

        // 제 몫을 넘어서면 그때는 물러난다. 천장이 없으면 상한이 아니다.
        assertThat(bulkhead.tryEnter("콜드", 20, 8))
                .as("제 몫을 넘으면 막힌다").isFalse();
        // 이미 제 몫을 넘겨 쥐고 있는 핫도 더는 못 늘린다.
        assertThat(bulkhead.tryEnter("핫", 20, 8))
                .as("많이 쥔 쪽이 먼저 물러난다").isFalse();
    }
}
