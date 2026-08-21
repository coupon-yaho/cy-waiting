package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 배분의 분모.
 *
 * <p><b>증감을 같은 속도로 다루면 스케일아웃마다 초과 배분이 난다.</b> 각 노드는
 * {@code credit / N} 을 자기 몫으로 쓰는데, 새 노드가 늘었는데 옛 {@code N} 을
 * 계속 쓰면 총합이 전역 크레딧을 넘는다. 미달은 지연이고 초과는 장애다.
 */
class GatewayRegistryTest {

    private static final int RAMP_DOWN = 3;
    private static final int INITIAL = 10;

    private GatewayRegistry registry() {
        return GatewayRegistry.of(RAMP_DOWN, INITIAL);
    }

    @Test
    @DisplayName("증가는_한_번_보면_바로_반영한다")
    void 증가는_한_번_보면_바로_반영한다() {
        // 늦으면 기존 노드가 작은 분모로 나눠 총합이 전역 크레딧을 넘는다.
        GatewayRegistry registry = registry();

        registry.observed(INITIAL + 1);

        assertThat(registry.count()).isEqualTo(INITIAL + 1);
    }

    @Test
    @DisplayName("감소는_연속으로_봐야_반영한다")
    void 감소는_연속으로_봐야_반영한다() {
        GatewayRegistry registry = registry();

        for (int i = 0; i < RAMP_DOWN - 1; i++) {
            registry.observed(1);
            assertThat(registry.count()).isEqualTo(INITIAL);
        }
        registry.observed(1);

        assertThat(registry.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("감소가_이어지다_끊기면_다시_센다")
    void 감소가_이어지다_끊기면_다시_센다() {
        // **레디스가 임계보다 오래 끊겼다 돌아오면 이 경로를 탄다.** 노드들이
        // 하나씩 붙는 동안 분모가 1·2·3 으로 올라오는데, 그 사이 한 번이라도
        // 큰 값을 보면 감소 확정을 처음부터 다시 세어야 한다.
        GatewayRegistry registry = registry();
        registry.observed(1);
        registry.observed(1);

        registry.observed(INITIAL);
        registry.observed(1);

        assertThat(registry.count()).isEqualTo(INITIAL);
    }

    @Test
    @DisplayName("관측이_실패하면_직전_값을_지킨다")
    void 관측이_실패하면_직전_값을_지킨다() {
        // 하트비트가 끊기면 모든 노드가 동시에 이 경로를 탄다. 여기서 분모를
        // 낮추면 10대 배포에서 순간 통과량이 전역 크레딧의 10배가 된다.
        GatewayRegistry registry = registry();

        registry.observationFailed();
        registry.observationFailed();
        registry.observationFailed();

        assertThat(registry.count()).isEqualTo(INITIAL);
    }

    @Test
    @DisplayName("실패는_감소_확정을_되돌린다")
    void 실패는_감소_확정을_되돌린다() {
        // 실패는 "더 작게 관측했다" 가 아니다. 섞어 세면 관측 실패만으로
        // 분모가 내려간다.
        GatewayRegistry registry = registry();
        registry.observed(1);
        registry.observed(1);

        registry.observationFailed();
        registry.observed(1);

        assertThat(registry.count()).isEqualTo(INITIAL);
    }

    @Test
    @DisplayName("첫_관측_전에는_예상_레플리카_수를_쓴다")
    void 첫_관측_전에는_예상_레플리카_수를_쓴다() {
        // **1 로 두면 뜨는 노드가 전역 크레딧 전부를 자기 몫으로 쓴다.**
        // 10대 운영 중 11번째가 뜨면 총합이 크레딧의 1.9배가 된다.
        assertThat(GatewayRegistry.of(RAMP_DOWN, INITIAL).count()).isEqualTo(INITIAL);
    }

    @Test
    @DisplayName("스크립트가_0을_주면_무시한다")
    void 스크립트가_0을_주면_무시한다() {
        // 스크립트는 자기 자신을 먼저 쓰므로 정상이면 최소 1 이다. 0 은
        // 판이 갈렸거나 깨진 응답이라 분모로 삼으면 0 으로 나눈다.
        GatewayRegistry registry = registry();

        registry.observed(0);
        registry.observed(-1);

        assertThat(registry.count()).isEqualTo(INITIAL);
    }

    @Test
    @DisplayName("동시에_늘어나면_가장_큰_값이_남는다")
    void 동시에_늘어나면_가장_큰_값이_남는다() throws InterruptedException {
        // **분모와 연속 횟수를 따로 두면 읽고·비교하고·쓰는 사이에 다른 관측이
        // 낀다.** 그러면 큰 값이 작은 값에 덮이고, 분모가 낮아지는 방향은
        // 초과 발급이다.
        //
        // **램프다운이 못 일어나게 임계를 스레드 수보다 크게 둔다.** 안 그러면
        // 큰 값이 먼저 들어온 뒤 작은 값이 연달아 와서 감소가 정당하게 확정되고,
        // 무엇이 원자성 때문이고 무엇이 정책 때문인지 갈리지 않는다.
        // 경합은 한 번으로는 잘 안 나므로 라운드를 반복한다.
        int threads = 16;
        int rounds = 300;
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            for (int round = 0; round < rounds; round++) {
                GatewayRegistry registry = GatewayRegistry.of(threads + 1, 1);
                CountDownLatch 출발 = new CountDownLatch(1);
                CountDownLatch 도착 = new CountDownLatch(threads);
                for (int i = 1; i <= threads; i++) {
                    int observed = i;
                    pool.execute(() -> {
                        try {
                            출발.await();
                            registry.observed(observed);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            도착.countDown();
                        }
                    });
                }
                출발.countDown();
                assertThat(도착.await(10, TimeUnit.SECONDS)).isTrue();

                assertThat(registry.count())
                        .withFailMessage("%d 회차에서 분모가 %d 로 덮였다", round, registry.count())
                        .isEqualTo(threads);
            }
        }
    }

    @Test
    @DisplayName("설정이_0_이하면_기동에_실패한다")
    void 설정이_0_이하면_기동에_실패한다() {
        // 조용히 1 로 올리면 그 값이 어디서 왔는지 아무도 모른다.
        assertThatThrownBy(() -> GatewayRegistry.of(0, INITIAL))
                .hasMessageContaining("rampDownTicks");
        assertThatThrownBy(() -> GatewayRegistry.of(RAMP_DOWN, 0))
                .hasMessageContaining("initial");
    }
}
