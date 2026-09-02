package com.kafkick.waiting.chaos;

import com.kafkick.waiting.control.GatewayRegistry;
import com.kafkick.waiting.control.LeaderLock;
import com.kafkick.waiting.control.Leadership;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * X4 — 리더가 없는 동안 분모가 급변한다.
 *
 * <p>무엇을 왜 재는지는 {@code plan/08-resilience.md} 의 조합 시나리오 절이 든다.
 * 여기는 그것을 어떻게 판정하는가만 든다.
 */
@Tag("chaos")
class NoLeaderDenominatorShiftScenarioTest {

    /** 리스와 시도 상한. 비를 지킨 축소판이다 — C15 와 같은 이유다. */
    private static final Duration 리스 = Duration.ofMillis(250);

    private static final Duration 시도_상한 = Duration.ofMillis(50);

    private static final int 하강_지연_틱 = 3;

    private static final int 처음_노드 = 20;

    private static final int 줄어든_노드 = 10;

    private final AtomicBoolean 느리다 = new AtomicBoolean();

    private final AtomicReference<String> 락주인 = new AtomicReference<>();

    private final AtomicLong 판번호 = new AtomicLong();

    private final GatewayRegistry 분모 = GatewayRegistry.of(하강_지연_틱, 처음_노드);

    private Mono<LeaderLock> 잡는다(String 나) {
        return Mono.defer(() -> {
            if (느리다.get()) {
                return Mono.never();
            }
            String 주인 = 락주인.compareAndSet(null, 나) ? 나 : 락주인.get();
            boolean 내것 = 나.equals(주인);
            return Mono.just(new LeaderLock(내것, 주인, 리스.toMillis(),
                    내것 ? 판번호.incrementAndGet() : 0));
        });
    }

    private Leadership 노드(String 나) {
        return Leadership.of(나, 리스, 시도_상한, () -> 잡는다(나), Mono::empty);
    }

    /**
     * 세 구간을 한 판정으로 잇는다.
     *
     * <p><b>리더가 없는 동안에도 분모는 계속 움직인다.</b> 하트비트는 리더의 일이
     * 아니라서 리더십과 무관하게 돈다 — 그 사이 분모가 절반이 되면, 돌아온 리더가
     * 그 값으로 첫 배분을 낸다.
     */
    @Test
    @DisplayName("X4_리더가_없는_동안_분모가_반이_되어도_돌아온_리더가_그_값을_쓴다")
    void X4_리더가_없는_동안_분모가_반이_되어도_돌아온_리더가_그_값을_쓴다() {
        Leadership 갑 = 노드("node-a");
        Leadership 을 = 노드("node-b");
        int[] 정상_분모 = new int[1];
        long[] 무리더_구간_리더수 = new long[1];
        int[] 무리더_구간_분모 = new int[1];
        long[] 회복_리더수 = new long[1];
        int[] 회복_분모 = new int[1];
        long[] 회복_판번호 = new long[1];

        ChaosScenario.named("X4 리더 없는 동안 분모 급변")
                .baseline(() -> {
                    갑.renew().block(Duration.ofSeconds(5));
                    을.renew().block(Duration.ofSeconds(5));
                    분모.observed(처음_노드);
                    정상_분모[0] = 분모.count();
                })
                .inject(() -> 느리다.set(true))
                .duringFault(() -> {
                    // 리스가 지날 때까지 돈다. 그 사이 하트비트는 계속 센다.
                    long 끝 = System.nanoTime() + 리스.multipliedBy(3).toNanos();
                    while (System.nanoTime() < 끝) {
                        갑.renew().block(Duration.ofSeconds(5));
                        을.renew().block(Duration.ofSeconds(5));
                        분모.observed(줄어든_노드);
                    }
                    무리더_구간_리더수[0] = 리더_수(갑, 을);
                    무리더_구간_분모[0] = 분모.count();
                })
                .recover(() -> 느리다.set(false))
                .afterRecovery(() -> {
                    갑.renew().block(Duration.ofSeconds(5));
                    을.renew().block(Duration.ofSeconds(5));
                    회복_리더수[0] = 리더_수(갑, 을);
                    회복_분모[0] = 분모.count();
                    회복_판번호[0] = Math.max(갑.fence(), 을.fence());
                })
                .assertEntry(() -> RecoveryCriteria.violations(
                        전제_평시_분모가_처음_값이다(정상_분모[0])))
                .assertDuring(() -> RecoveryCriteria.violations(
                        // 리더가 없다 — C15 가 재는 그 구간이다.
                        아무도_리더가_아니다(무리더_구간_리더수[0]),
                        // **분모는 리더십과 무관하게 움직인다.** 하트비트가 리더의
                        // 일이 아니라서다. 리더가 없는 동안에도 절반으로 내려간다.
                        분모가_리더_없이도_내려간다(무리더_구간_분모[0])))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        리더가_하나로_돌아온다(회복_리더수[0]),
                        // **돌아온 리더가 줄어든 분모를 그대로 쓴다.** 옛 값을 들고
                        // 오면 남은 노드가 죽은 노드의 몫까지 쓴다 — 초과 방향이다.
                        돌아온_리더가_지금_분모를_쓴다(회복_분모[0]),
                        // 판 번호를 새로 받는다. 0 이면 울타리가 전부 거절한다.
                        판_번호를_들고_돌아온다(회복_판번호[0])))
                // **RC1~RC6 은 여기서 안 잰다.** 리더십과 분모만 걷는다.
                .run();
    }

    private long 리더_수(Leadership... 노드들) {
        return Stream.of(노드들).filter(Leadership::isLeader).count();
    }

    private Optional<String> 전제_평시_분모가_처음_값이다(int 분모값) {
        return 분모값 == 처음_노드 ? Optional.empty()
                : Optional.of("전제 — 평시 분모가 %d 다. %d 여야 한다"
                        .formatted(분모값, 처음_노드));
    }

    private Optional<String> 아무도_리더가_아니다(long 수) {
        return 수 == 0 ? Optional.empty()
                : Optional.of("리스가 지났는데 리더가 %d 대다 — 확인 없이 안 내려왔다"
                        .formatted(수));
    }

    /** 하트비트는 리더의 일이 아니다. 리더십에 묶으면 그 구간에 분모가 얼어붙는다. */
    private Optional<String> 분모가_리더_없이도_내려간다(int 분모값) {
        return 분모값 == 줄어든_노드 ? Optional.empty()
                : Optional.of("리더 없는 구간에 분모가 %d 다 — %d 로 내려가야 한다"
                        .formatted(분모값, 줄어든_노드));
    }

    private Optional<String> 리더가_하나로_돌아온다(long 수) {
        return 수 == 1 ? Optional.empty()
                : Optional.of("회복 뒤 리더가 %d 대다 — 정확히 하나여야 한다".formatted(수));
    }

    private Optional<String> 돌아온_리더가_지금_분모를_쓴다(int 분모값) {
        return 분모값 == 줄어든_노드 ? Optional.empty()
                : Optional.of("돌아온 리더가 분모 %d 를 쓴다 — 지금 값 %d 여야 한다"
                        .formatted(분모값, 줄어든_노드));
    }

    private Optional<String> 판_번호를_들고_돌아온다(long 판번호) {
        return 판번호 > 0 ? Optional.empty()
                : Optional.of("돌아온 리더의 판 번호가 %d 다 — 0 이면 울타리가 거절한다"
                        .formatted(판번호));
    }
}
