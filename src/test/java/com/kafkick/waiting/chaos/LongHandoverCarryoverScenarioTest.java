package com.kafkick.waiting.chaos;

import com.kafkick.waiting.WaitingApplication;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.AllocationRound;
import com.kafkick.waiting.control.ControlPlaneLifecycle;
import com.kafkick.waiting.control.Leadership;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.coupon.CouponState;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * C4c — 리더가 <b>오래</b> 갈렸다 돌아온다 (CY-866).
 *
 * <p>C4b 는 공백이 2초라 평활값이 움직일 시간이 없다. 이월(CY-859)이 효과를 내는 조건은 그 반대다 — 남이 리더인 동안
 * 뒷단이 열화해 몫이 내려가고, 되찾은 노드가 제 옛 값으로 시작하면 뒷단이 800 이라고 말한 초에 수천이 들어간다.
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class LongHandoverCarryoverScenarioTest {

    private static final String COUPON = "c4c-queued";

    private static final int 줄_선_사람 = 2_000;

    /** 평시 가용량. 평활이 여기까지 올라간 상태에서 리더가 갈린다. */
    private static final long 평시_가용량 = 7_300;

    /** 갈린 동안의 가용량. 둘째가 이 값으로 열 틱 넘게 돈다. */
    private static final long 열화_가용량 = 800;

    /** 갈린 동안 발행이 내려와야 하는 상한. 평활이 붙은 뒤에도 발행은 한 틱 뒤따라와 여유를 둔다. */
    private static final double 갈린_상한 = 열화_가용량 * 1.5;

    /**
     * 되찾은 평활의 상한. <b>되찾기 직전에 보고를 평시로 되돌린다</b> — 열화 값과 이월 값이 거의 같아, 안 되돌리면
     * 이월을 못 받고 관측에서 시작한 콜드 스무더와 구분이 안 된다. 이월을 받으면 약 2,770, 못 받으면 7,300 이다.
     */
    private static final double 회복_평활_상한 = 평시_가용량 * 0.55;

    private static final Duration 기다림 = Duration.ofSeconds(60);

    /** 둘째가 도는 구간. 평활이 열화 값에 수렴할 만큼은 돌아야 이월이 뜻을 가진다. */
    private static final Duration 갈린_구간 = Duration.ofSeconds(15);

    /** 보고의 신선도 창. 앱이 이 값으로 낡은 보고를 뺀다. */
    private static final Duration 신선도 = Duration.ofSeconds(3);

    private static final BackendStub 뒷단 = BackendStub.항상_받는다();

    private static final ScheduledExecutorService 보고 =
            Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "c4c-reports"));

    private static final AtomicLong 보고가_터진_수 = new AtomicLong();

    /** 보고 태스크. 시나리오가 끝나면 끊는다 — 안 끊으면 다음 시험과 같은 키에 두 값이 교대로 들어간다. */
    private static final ScheduledFuture<?>[] 보고_태스크 = new ScheduledFuture<?>[1];

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        faults = RedisFaults.시작한다();
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("spring.data.redis.url", faults::주소);
    }

    @AfterAll
    static void 내린다() {
        보고.shutdownNow();
        뒷단.close();
        if (faults != null) {
            faults.close();
        }
    }

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @Autowired
    private Leadership 첫_노드;

    @Autowired
    private SnapshotHolder holder;

    @Autowired
    private AllocationRound round;

    @Autowired
    private ControlPlaneLifecycle 수명;

    /** 보고 픽스처. 신선도를 앱과 같은 기준으로 보려고 든다. */
    private BackendReports 보고기;

    /** 보고 사이의 가장 긴 공백(ms). 태스크가 밀린 사실은 구간의 한 점이 아니라 이 값이 든다. */
    private final AtomicLong 보고_최대_공백 = new AtomicLong();

    /** 마지막으로 보고가 닿은 시각(나노). */
    private final AtomicLong 마지막_보고 = new AtomicLong(System.nanoTime());

    @Test
    @DisplayName("C4c_오래_갈렸다_돌아온_리더가_이월받은_값에서_시작한다")
    void C4c_오래_갈렸다_돌아온_리더가_이월받은_값에서_시작한다() {
        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        AtomicLong 보고할_가용량 = new AtomicLong(평시_가용량);
        AtomicLong 보고한_수 = new AtomicLong();
        long[] 회복_전_보고_수 = new long[1];
        double[] 회복_전_이월 = new double[2];
        boolean[] 갈린_동안_신선 = new boolean[1];
        boolean[] 회복_뒤_신선 = new boolean[1];
        long[] 되찾은_몫 = new long[1];
        boolean[] 첫_노드가_내려왔다 = new boolean[1];
        double[] 갈리기_전_평활 = new double[1];
        Instant[] 되찾기_전 = new Instant[1];
        long[] 갈린_동안_몫 = new long[1];
        double[] 갈린_동안_평활 = new double[1];
        double[] 되찾은_평활 = new double[1];
        boolean[] 둘째가_돌았다 = new boolean[1];

        ChaosScenario.named("C4c 오래 갈린 승계")
                .baseline(() -> {
                    재료를_심는다(연결);
                    // **시계는 실시계다.** 신선도는 앱이 레디스 시각과 견줘 판정하므로, 보고 시각을 고정하면 그
                    // 판정이 통째로 무의미해진다(TS-4 의 예외 — 픽스처가 아니라 저장소가 시각의 주인이다).
                    // 태스크가 밀려 보고가 낡는 위험은 아래에서 앱과 같은 기준으로 직접 본다.
                    BackendReports 보고기 = BackendReports.실시계로(연결, 신선도);
                    this.보고기 = 보고기;
                    // **되던 것을 세고 터진 것도 센다.** 반복 태스크는 던지면 영구히 취소되는데 그 사실이 조용하다.
                    보고_태스크[0] = 보고.scheduleAtFixedRate(() -> {
                        try {
                            보고기.보고한다("c4c-be", 보고할_가용량.get());
                            보고한_수.incrementAndGet();
                            long 지금 = System.nanoTime();
                            long 공백 = Duration.ofNanos(지금 - 마지막_보고.getAndSet(지금)).toMillis();
                            보고_최대_공백.accumulateAndGet(공백, Math::max);
                        } catch (Throwable e) {
                            보고가_터진_수.incrementAndGet();
                        }
                    // **보고 간격을 신선도(3초)보다 훨씬 짧게 둔다.** 시계를 고정하면 신선도 판정이 통째로
                    // 무의미해진다 — 앱은 레디스 시각과 견주므로, 막을 것은 보고가 밀려 낡는 것이고 그것은
                    // 위 판정의 평활 하한이 잡는다.
                    }, 0, 250, TimeUnit.MILLISECONDS);
                    Awaitility.await().alias("첫 노드가 리더를 쥔다").atMost(기다림).until(첫_노드::isLeader);
                    // 평활이 평시 값에 붙을 때까지 돈다. 안 붙으면 되찾은 값과 견줄 기준이 없다.
                    Awaitility.await().alias("평활이 평시 가용량에 붙는다").atMost(기다림)
                            .until(() -> round.smoothedCredit() > 평시_가용량 * 0.8);
                    갈리기_전_평활[0] = round.smoothedCredit();
                })
                .inject(() -> {
                    // 제어 평면만 세운다. 락은 리스 만료까지 남아 프로덕션과 같은 승계 지연을 탄다.
                    수명.stop();
                    보고할_가용량.set(열화_가용량);
                })
                .duringFault(() -> {
                    try (SecondNode 둘째 = SecondNode.띄운다(WaitingApplication.class, faults.주소(),
                            "http://localhost:" + 뒷단.port(), true)) {
                        Awaitility.await().alias("둘째가 이어받는다").atMost(기다림).until(둘째::리더인가);
                        둘째가_돌았다[0] = true;
                        // **둘째가 뜨는 동안의 공백은 안 센다.** 컨텍스트 기동이 CPU 를 먹어 보고 태스크가 밀리는데,
                        // 그것은 하네스 사정이고 이 시나리오가 재려는 것이 아니다. 재는 구간은 여기서부터다.
                        공백을_다시_잰다();
                        // **틱 수로 기다린다.** 발행 몫만 보면 승계 직후 램프가 낮게 시작해 곧바로 만족된다 —
                        // 그때는 평활이 아직 평시 값이라 이월이 아무것도 안 잰다.
                        // **둘째의 평활이 열화 값에 수렴할 때까지 기다린다.** 시간으로 기다리면 틱 설정이 바뀌는
                        // 순간 덜 수렴한 채 회복으로 넘어가 이 시나리오가 재려던 조건이 안 만들어진다.
                        AllocationRound 둘째_회차 = 둘째.빈("allocationRound", AllocationRound.class);
                        Awaitility.await().alias("둘째의 평활이 열화 값에 붙는다")
                                // 구간 내내 첫 노드는 리더가 아니어야 한다 — 둘이 다 리더면 승계가 아니라 분단이다.
                                .failFast("첫 노드가 다시 리더가 됐다", 첫_노드::isLeader)
                                .atMost(갈린_구간.plusSeconds(20)).pollInterval(Duration.ofMillis(500))
                                .until(() -> 둘째_회차.smoothedCredit() > 0
                                        && 둘째_회차.smoothedCredit() <= 열화_가용량 * 1.2);
                        첫_노드가_내려왔다[0] = !첫_노드.isLeader();
                        갈린_동안_신선[0] = 보고가_신선한가() && 공백이_신선도_안인가();
                        갈린_동안_평활[0] = 둘째_회차.smoothedCredit();
                        갈린_동안_몫[0] = 발행된_몫();
                    }
                })
                .recover(() -> {
                    // **보고를 평시로 되돌리고 되찾는다.** 이월을 받으면 열화 값에서 올라오고, 못 받으면 관측
                    // 그대로에서 시작한다 — 두 경우가 갈리는 유일한 배치다.
                    보고할_가용량.set(평시_가용량);
                    // **되찾기 전에 기준을 잡는다.** 리더 표시를 기다린 뒤에 잡으면 새 임기의 첫 회차를 이미 놓친다.
                    되찾기_전[0] = holder.view().snapshot().publishedAt();
                    회복_전_보고_수[0] = 보고한_수.get();
                    // 구간마다 공백을 새로 잰다. 앞 구간의 공백이 뒤 구간 판정을 깨면 원인이 흐려진다.
                    공백을_다시_잰다();
                    // **이월 계수는 누적이다.** 앞 임기에 받은 것으로 통과하지 않게 델타로 본다.
                    회복_전_이월[0] = round.carryoverRestored();
                    회복_전_이월[1] = round.carryoverEmpty();
                    수명.start();
                    Awaitility.await().alias("첫 노드가 리더를 되찾는다").atMost(기다림)
                            .until(첫_노드::isLeader);
                })
                .afterRecovery(() -> {
                    // **새 발행이 나온 뒤에 읽는다.** 리더 표시는 회차보다 먼저 서므로, 바로 읽으면 앞 임기의
                    // 굳은 값을 되찾은 값으로 오독한다.
                    Awaitility.await().alias("되찾은 리더가 새로 발행한다").atMost(기다림)
                            .pollInterval(Duration.ofMillis(100))
                            .until(() -> holder.view().snapshot().publishedAt().isAfter(되찾기_전[0])
                                    && !Double.isNaN(round.smoothedCredit()));
                    되찾은_평활[0] = round.smoothedCredit();
                    되찾은_몫[0] = 발행된_몫();
                    회복_뒤_신선[0] = 보고가_신선한가() && 공백이_신선도_안인가();
                })
                .assertEntry(() -> RecoveryCriteria.violations(
                        갈리기_전_평활[0] > 평시_가용량 * 0.8 ? Optional.empty()
                                : Optional.of("전제 — 평활이 평시 값에 안 붙었다: %.0f".formatted(갈리기_전_평활[0]))))
                .assertDuring(() -> RecoveryCriteria.violations(
                        둘째가_돌았다[0] ? Optional.empty() : Optional.of("전제 — 둘째가 안 이어받았다"),
                        첫_노드가_내려왔다[0] ? Optional.empty()
                                : Optional.of("첫 노드가 안 내려왔다 — 승계가 아니라 분단이다"),
                        // 태스크가 밀리면 보고가 낡아 크레딧이 하한으로 내려간다 — 그 사실을 직접 본다.
                        갈린_동안_신선[0] ? Optional.empty()
                                : Optional.of("갈린 동안 보고가 낡았다 — 최대 공백 %dms".formatted(보고_최대_공백.get())),
                        // 하한도 본다. 보고가 낡아 떨어지면 크레딧이 하한으로 내려가 상한만으로는 통과한다.
                        갈린_동안_평활[0] >= 열화_가용량 * 0.5 && 갈린_동안_평활[0] <= 열화_가용량 * 1.2
                                ? Optional.empty()
                                : Optional.of("전제 — 둘째의 평활이 열화 값에 안 붙었다: %.0f"
                                        .formatted(갈린_동안_평활[0])),
                        갈린_동안_몫[0] > 0 && 갈린_동안_몫[0] <= 갈린_상한 ? Optional.empty()
                                : Optional.of("전제 — 갈린 동안 발행이 열화 값까지 안 내려왔다: %d"
                                        .formatted(갈린_동안_몫[0])),
                        보고가_터진_수.get() == 0 ? Optional.empty()
                                : Optional.of("전제 — 가용량 보고가 %d번 터졌다".formatted(보고가_터진_수.get()))))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        // **이월은 평활값에서 산다.** 발행 몫은 램프가 눌러 이월이 없어도 작게 나온다.
                        되찾은_평활[0] <= 회복_평활_상한 ? Optional.empty()
                                : Optional.of("되찾은 평활이 %.0f — 이월을 안 받고 관측에서 시작했다"
                                        .formatted(되찾은_평활[0])),
                        // 하한도 건다. 0 에서 시작하면 되찾은 리더가 아무도 안 들인다.
                        되찾은_평활[0] >= 열화_가용량 * 0.5 ? Optional.empty()
                                : Optional.of("되찾은 평활이 %.0f — 이월이 0 을 물고 왔다"
                                        .formatted(되찾은_평활[0])),
                        // **이월을 실제로 받았는지 계수로 본다.** 못 받아도 관측이 답이 되면 값만으로는 안 갈린다.
                        round.carryoverRestored() > 회복_전_이월[0] ? Optional.empty()
                                : Optional.of("되찾고 이월을 안 받았다 — 받음 %.0f, 없음 %.0f, 실패 %.0f"
                                        .formatted(round.carryoverRestored(), round.carryoverEmpty(),
                                                round.carryoverFailures())),
                        // 빈 이월은 관측에서 시작한 것이다 — 값만 보면 이월받은 것과 안 갈린다.
                        round.carryoverEmpty() == 회복_전_이월[1] ? Optional.empty()
                                : Optional.of("되찾을 때 이월이 비어 있었다 — 없음 %.0f"
                                        .formatted(round.carryoverEmpty())),
                        round.carryoverFailures() == 0 ? Optional.empty()
                                : Optional.of("이월 읽기가 %.0f번 실패했다".formatted(round.carryoverFailures())),
                        보고가_터진_수.get() == 0 ? Optional.empty()
                                : Optional.of("회복 구간에 가용량 보고가 %d번 터졌다".formatted(보고가_터진_수.get())),
                        // 보고가 멎으면 크레딧이 하한으로 떨어져 위 상한들이 공짜로 통과한다.
                        보고한_수.get() > 회복_전_보고_수[0] ? Optional.empty()
                                : Optional.of("회복 구간에 가용량 보고가 한 번도 안 닿았다"),
                        회복_뒤_신선[0] ? Optional.empty()
                                : Optional.of("회복 구간에 보고가 낡았다 — 최대 공백 %dms".formatted(보고_최대_공백.get())),
                        // 쿠폰이 발행에서 빠지면 줄이 영영 안 빠지는데 상한 판정은 그것을 통과시킨다.
                        되찾은_몫[0] > 0 ? Optional.empty()
                                : Optional.of("되찾은 발행에 이 쿠폰의 몫이 없다: %d".formatted(되찾은_몫[0]))))
                .run();
        if (보고_태스크[0] != null) {
            보고_태스크[0].cancel(false);
        }
        연결.close();
    }

    private void 재료를_심는다(StatefulRedisConnection<String, String> 연결) {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON).block(기다림);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "1000000").block(기다림);
        QueueSeed.줄을_세운다(연결, COUPON, 줄_선_사람);
    }

    /** 보고가 앱이 보는 기준으로 신선한가. 태스크가 밀려 낡으면 크레딧이 하한으로 내려간다. */
    private boolean 보고가_신선한가() {
        return 보고기 != null && 보고기.신선한_보고().containsKey("c4c-be");
    }

    /** 이 지점부터 다시 공백을 잰다. */
    private void 공백을_다시_잰다() {
        마지막_보고.set(System.nanoTime());
        보고_최대_공백.set(0);
    }

    /** 구간 내내 공백이 신선도 창 안이었는가. 한 점만 보면 밀렸다 재개된 구간이 초록으로 지나간다. */
    private boolean 공백이_신선도_안인가() {
        return 보고_최대_공백.get() < 신선도.toMillis();
    }

    /** 지금 발행에 실린 이 쿠폰의 몫. 노드들이 실제로 읽는 값이 이것이다. */
    private long 발행된_몫() {
        CouponState 상태 = holder.view().snapshot().coupons().get(COUPON);
        return 상태 == null ? -1 : 상태.credit();
    }
}
