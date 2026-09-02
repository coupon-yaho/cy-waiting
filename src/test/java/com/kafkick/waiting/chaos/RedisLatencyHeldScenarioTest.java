package com.kafkick.waiting.chaos;

import com.kafkick.waiting.control.LeaderLock;
import com.kafkick.waiting.control.Leadership;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * C15 — 레디스 지연이 시도 상한 위로 올라가 유지된다.
 *
 * <p>무엇을 왜 재는지는 {@code plan/08-resilience.md} 의 C15 절이 든다. 여기는
 * 그것을 어떻게 판정하는가만 든다.
 */
@Tag("chaos")
class RedisLatencyHeldScenarioTest {

    /**
     * 리스와 시도 상한. <b>배선값의 축소판이다</b> — 비가 같다(5:1).
     *
     * <p>운영값(2초·300ms)으로 재면 한 판이 수십 초다. 시계를 주입할 수 있으면
     * 그쪽이 낫지만 그 생성자는 제어 평면 패키지 안에서만 열려 있어, 대신 실제
     * 시간을 쓰고 여유를 리스의 세 배로 잡았다.
     */
    private static final Duration 리스 = Duration.ofMillis(250);

    private static final Duration 시도_상한 = Duration.ofMillis(50);

    /** 노드 둘. 하나가 지연에 걸린 동안 다른 하나가 무엇을 하는지가 관심사다. */
    private static final String 갑 = "node-a";

    private static final String 을 = "node-b";

    /** 레디스가 시도 상한 안에 답하는가. 이 스위치로 지연을 넣고 걷는다. */
    private final AtomicBoolean 느리다 = new AtomicBoolean();

    /** 지금 서버 락의 주인. 스크립트가 하는 일을 메모리로 흉내 낸다. */
    private final AtomicReference<String> 락주인 = new AtomicReference<>();

    private final AtomicLong 판번호 = new AtomicLong();

    /**
     * 락을 잡거나 연장한다.
     *
     * <p>지연이 걸리면 <b>답이 안 온다</b> — 오류가 아니다. 시도 상한이 그것을
     * 끊고, 못 끊으면 연장 루프가 조용히 멎는다.
     */
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

    /** 한 판. 두 노드가 같이 연장을 시도한다. */
    private void 한_틱(Leadership 갑노드, Leadership 을노드) {
        갑노드.renew().block(Duration.ofSeconds(5));
        을노드.renew().block(Duration.ofSeconds(5));
    }

    /**
     * 리스가 확실히 지날 때까지 돈다.
     *
     * <p>지연 구간이라 한 판이 시도 상한만큼 걸린다 — 두 노드면 100ms 다.
     * 리스의 세 배를 넘길 때까지 돌아 경계에 안 걸리게 한다.
     */
    private void 리스가_지날_때까지(Leadership 갑노드, Leadership 을노드) {
        long 끝 = System.nanoTime() + 리스.multipliedBy(3).toNanos();
        while (System.nanoTime() < 끝) {
            한_틱(갑노드, 을노드);
        }
    }

    private long 리더_수(Leadership... 노드들) {
        return List.of(노드들).stream().filter(Leadership::isLeader).count();
    }

    /**
     * 세 구간을 한 판정으로 잇는다.
     *
     * <p><b>재는 것은 "둘이 되는가" 가 아니라 "0 이 되는가" 다.</b> 시도 상한이
     * 조여져 있으면 아무도 리더가 아니게 되고, 배분이 멎는데 응답은 정상이다.
     */
    @Test
    @DisplayName("C15_지연이_시도_상한을_넘겨도_리더가_정확히_한_대다")
    void C15_지연이_시도_상한을_넘겨도_리더가_정확히_한_대다() {
        Leadership 갑노드 = 노드(갑);
        Leadership 을노드 = 노드(을);
        long[] 정상_리더 = new long[1];
        long[] 지연중_리더 = new long[1];
        long[] 리스_지난_뒤_리더 = new long[1];
        long[] 회복_리더 = new long[1];
        long[] 회복_판번호 = new long[1];

        ChaosScenario.named("C15 레디스 지연 상승 유지")
                .baseline(() -> {
                    한_틱(갑노드, 을노드);
                    정상_리더[0] = 리더_수(갑노드, 을노드);
                })
                .inject(() -> {
                    느리다.set(true);
                    // 시도 상한이 답 없는 판을 끊는다. 리스 안이라 아직 리더다.
                    한_틱(갑노드, 을노드);
                    지연중_리더[0] = 리더_수(갑노드, 을노드);
                })
                .duringFault(() -> {
                    리스가_지날_때까지(갑노드, 을노드);
                    리스_지난_뒤_리더[0] = 리더_수(갑노드, 을노드);
                })
                .recover(() -> 느리다.set(false))
                .afterRecovery(() -> {
                    한_틱(갑노드, 을노드);
                    회복_리더[0] = 리더_수(갑노드, 을노드);
                    회복_판번호[0] = Math.max(갑노드.fence(), 을노드.fence());
                })
                .assertEntry(() -> RecoveryCriteria.violations(
                        // 전제 — 평시에 정확히 하나여야 아래의 수가 뜻을 갖는다 (G4.1).
                        리더가_정확히_하나다("정상", 정상_리더[0]),
                        // 시도 상한이 답 없는 판을 끊는다. 안 끊으면 연장 루프가
                        // 조용히 멎고 참으로 얼어붙는다.
                        리더가_정확히_하나다("지연 진입", 지연중_리더[0])))
                .assertDuring(() -> RecoveryCriteria.violations(
                        // **여기가 이 시나리오의 전부다.** 확인 없이 리스가 지나면
                        // 내려온다 — 둘이 되는 대신 0 이 된다.
                        리스가_지나면_아무도_리더가_아니다(리스_지난_뒤_리더[0])))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        // 지연이 걷히면 한 판 안에 다시 하나다.
                        리더가_정확히_하나다("회복", 회복_리더[0]),
                        // 판 번호를 들고 돌아온다. 0 이면 되돌릴 수 없는 쓰기가 전부 거절된다.
                        판_번호를_들고_돌아온다(회복_판번호[0])))
                // **RC1~RC6 은 여기서 안 잰다.** 이 판은 리더십만 걷는다 — 줄도
                // 뒷단 유입도 세우지 않는다. 밀린 크레딧의 버스트는 열린 루프가
                // 있어야 재진다 (CY-817).
                .run();
    }

    private Optional<String> 리더가_정확히_하나다(String 구간, long 수) {
        return 수 == 1 ? Optional.empty()
                : Optional.of("%s 구간의 리더가 %d 대다 — 정확히 하나여야 한다 (G4.1)"
                        .formatted(구간, 수));
    }

    /**
     * 리스가 지나면 내려온다. <b>둘이 되는 것보다 0 이 되는 쪽이 이 구간의 위험이다.</b>
     *
     * <p>배분이 멎어 크레딧이 안 갱신되는데 응답은 정상이라 조용히 통과시킨다.
     */
    private Optional<String> 리스가_지나면_아무도_리더가_아니다(long 수) {
        if (수 > 1) {
            return Optional.of("리스가 지났는데 리더가 %d 대다 — 확인 없이 안 내려왔다".formatted(수));
        }
        return 수 == 0 ? Optional.empty()
                : Optional.of("리스가 지났는데 아직 리더다 — 확인 없이 리스가 지나면 내려와야 한다");
    }

    private Optional<String> 판_번호를_들고_돌아온다(long 판번호) {
        return 판번호 > 0 ? Optional.empty()
                : Optional.of("회복한 리더의 판 번호가 %d 다 — 0 이면 울타리가 전부 거절한다"
                        .formatted(판번호));
    }
}
