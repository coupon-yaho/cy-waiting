package com.kafkick.waiting.chaos;

import com.kafkick.waiting.WaitingApplication;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.AllocationRound;
import com.kafkick.waiting.control.ControlPlaneLifecycle;
import com.kafkick.waiting.control.Leadership;
import com.kafkick.waiting.control.SnapshotCodec;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.coupon.CouponState;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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

    /** 회복 뒤 수렴을 보는 발행 수. 평활이 이월값에서 평시로 올라오는 구간을 덮어야 한다. */
    private static final int 수렴_표본 = 8;

    /** 내려간 것으로 치는 최소 폭. 잔떨림을 내림으로 세면 이 판정이 하네스 잡음을 잰다. */
    private static final double 내림_사각지대 = 평시_가용량 * 0.01;

    /** 마지막 표본이 닿아야 하는 값. 여기 못 닿으면 평활이 중간에 멎은 것이다. */
    private static final double 수렴_하한 = 평시_가용량 * 0.9;

    /**
     * 평활 계수. <b>제품 상수를 읽지 않고 옮겨 적는다</b> — 읽어 오면 계수를 바꾸는 뮤턴트를 판정이 같이
     * 따라가, 계단을 계단으로 안 본다.
     */
    private static final double 설계_알파 = 0.3;

    /** 한 틱 상승폭에 주는 여유. 보고 지터로 관측이 조금 흔들려도 통과해야 한다. */
    private static final double 상승_여유 = 1.3;

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
    private AllocationRound round;

    @Autowired
    private ControlPlaneLifecycle 수명;

    /** 발행 해시를 읽는 자리. 노드가 실제로 넘겨받는 값을 재려고 든다. */
    private final SnapshotCodec codec = SnapshotCodec.create();

    /** 보고 픽스처. 신선도를 앱과 같은 기준으로 보려고 든다. */
    private BackendReports 보고기;

    /** 표본을 뜨는 내내 첫 노드가 리더였는가. 발행 해시에는 발행자 표시가 없다. */
    private boolean 표본_구간_리더 = true;

    /** 보고 사이의 가장 긴 공백(ms). 태스크가 밀린 사실은 구간의 한 점이 아니라 이 값이 든다. */
    private final AtomicLong 보고_최대_공백 = new AtomicLong();

    /** 마지막으로 보고가 닿은 시각(나노). */
    private final AtomicLong 마지막_보고 = new AtomicLong(System.nanoTime());
    private final AtomicLong 보고한_수 = new AtomicLong();

    @Test
    @DisplayName("C4c_오래_갈렸다_돌아온_리더가_이월받은_값에서_시작한다")
    void C4c_오래_갈렸다_돌아온_리더가_이월받은_값에서_시작한다() {
        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        AtomicLong 보고할_가용량 = new AtomicLong(평시_가용량);
        long[] 회복_전_보고_수 = new long[1];
        double[] 회복_전_이월 = new double[2];
        boolean[] 갈린_동안_신선 = new boolean[1];
        boolean[] 회복_뒤_신선 = new boolean[1];
        long[] 되찾은_몫 = new long[1];
        boolean[] 첫_노드가_내려왔다 = new boolean[1];
        double[] 갈리기_전_평활 = new double[1];
        Publication[] 되찾기_전 = new Publication[1];
        long[] 갈린_동안_몫 = new long[1];
        double[] 갈린_동안_평활 = new double[1];
        double[] 되찾은_평활 = new double[1];
        boolean[] 둘째가_돌았다 = new boolean[1];
        List<Double> 회복_시계열 = new ArrayList<>();

        ChaosScenario.named("C4c 오래 갈린 승계")
                .baseline(() -> {
                    재료를_심는다(연결);
                    // RULE-EXCEPTION(TS-4): 신선도는 앱이 레디스 시각과 견줘 판정한다. 보고 시각을 고정하면
                    // 저장소 시각만 흐르고 보고는 안 흘러, 승계와 무관하게 영영 낡은 것으로 읽힌다 — 이 시나리오가
                    // 재려는 이월이 통째로 안 돈다. 태스크가 밀려 낡는 위험은 앱과 같은 기준으로 아래에서 직접 본다.
                    BackendReports 보고기 = BackendReports.실시계로(연결, 신선도);
                    this.보고기 = 보고기;
                    // **되던 것을 세고 터진 것도 센다.** 반복 태스크는 던지면 영구히 취소되는데 그 사실이 조용하다.
                    보고_태스크[0] = 보고.scheduleAtFixedRate(() -> {
                        try {
                            보고기.보고한다("c4c-be", 보고할_가용량.get());
                            long 지금 = System.nanoTime();
                            long 공백 = Duration.ofNanos(지금 - 마지막_보고.getAndSet(지금)).toMillis();
                            보고_최대_공백.accumulateAndGet(공백, Math::max);
                            // **공백을 적은 뒤에 센다.** 순서가 반대면 구간 경계에서 기다리던 쪽이 아직 안 적힌
                            // 공백을 0 으로 지운다.
                            보고한_수.incrementAndGet();
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
                        // **판정 둘을 같은 발행에서 읽는다** (CY-960). 나눠 읽으면 평활이 붙기를 기다린 뒤에
                        // 그 쿠폰이 아직 안 실린 발행의 몫을 읽어, 다섯 회차에 한 번 빨개진다.
                        AtomicReference<Map<String, String>> 붙은_발행 = new AtomicReference<>();
                        Awaitility.await().alias("둘째의 평활이 열화 값에 붙는다")
                                // 구간 내내 첫 노드는 리더가 아니어야 한다 — 둘이 다 리더면 승계가 아니라 분단이다.
                                .failFast("첫 노드가 다시 리더가 됐다", 첫_노드::isLeader)
                                .atMost(갈린_구간.plusSeconds(20)).pollInterval(Duration.ofMillis(500))
                                .until(() -> {
                                    Map<String, String> 해시 = 발행_해시();
                                    붙은_발행.set(해시);
                                    double 평활 = 평활값(해시);
                                    // **몫은 조건에 안 넣는다.** 넣으면 그 쿠폰이 빠진 발행을 건너뛰고
                                    // 좋은 회차 하나를 골라, 아래 몫 판정이 공짜로 통과한다.
                                    return 평활 > 0 && 평활 <= 열화_가용량 * 1.2;
                                });
                        첫_노드가_내려왔다[0] = !첫_노드.isLeader();
                        갈린_동안_신선[0] = 보고가_신선한가() && 공백이_신선도_안인가();
                        갈린_동안_평활[0] = 평활값(붙은_발행.get());
                        갈린_동안_몫[0] = 몫(붙은_발행.get());
                    }
                })
                .recover(() -> {
                    // **보고를 평시로 되돌리고 되찾는다.** 이월을 받으면 열화 값에서 올라오고, 못 받으면 관측
                    // 그대로에서 시작한다 — 두 경우가 갈리는 유일한 배치다.
                    보고할_가용량.set(평시_가용량);
                    // **되찾기 전에 기준을 잡는다.** 리더 표시를 기다린 뒤에 잡으면 새 임기의 첫 회차를 이미 놓친다.
                    되찾기_전[0] = 표(발행_해시());
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
                    // 굳은 값을 되찾은 값으로 오독한다. 평활과 몫은 그 한 발행에서 같이 꺼낸다.
                    Map<String, String> 되찾은_발행 = 다음_발행을_기다린다(되찾기_전[0]);
                    되찾은_평활[0] = 평활값(되찾은_발행);
                    되찾은_몫[0] = 몫(되찾은_발행);
                    // **수렴은 한 점으로 안 보인다.** 이월받은 값에서 평시까지 올라오는 길이 이 시나리오가
                    // 재려는 것인데, 회복 뒤 한 번만 읽으면 출발점만 보고 도착을 안 본다.
                    회복_시계열.addAll(발행마다_평활을_잰다(수렴_표본));
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
                                : Optional.of("되찾은 발행에 이 쿠폰의 몫이 없다: %d".formatted(되찾은_몫[0])),
                        // **발행자가 해시에 안 적힌다.** 첫 노드가 리더를 잃은 구간의 값을 제 것으로 읽으면
                        // 이 시나리오가 재려는 이월이 통째로 딴 노드 것이 된다.
                        표본_구간_리더 ? Optional.empty()
                                : Optional.of("표본을 뜨는 동안 첫 노드가 리더를 잃었다"),
                        // 표본이 비거나 깨진 값이 섞이면 아래 모양 판정이 뜻을 잃는다.
                        유한한가(회복_시계열) ? Optional.empty()
                                : Optional.of("회복 구간의 평활이 유한값이 아니다: %s"
                                        .formatted(회복_시계열)),
                        내려간_횟수(회복_시계열) == 0 ? Optional.empty()
                                : Optional.of("회복 뒤 평활이 %d번 내려갔다 — %s"
                                        .formatted(내려간_횟수(회복_시계열), 회복_시계열)),
                        // **도착을 본다.** 안 보면 이월값 근처에서 멎은 평활도 내림 0 으로 통과한다.
                        수렴했는가(회복_시계열) ? Optional.empty()
                                : Optional.of("회복 뒤 평활이 %.0f 까지 안 올라왔다 — %s"
                                        .formatted(수렴_하한, 회복_시계열)),
                        // **위로 튀는 것이 이월 결함의 모양이다.** 내림만 보면 이월이 첫 틱 뒤에 풀려
                        // 관측치로 계단처럼 뛰는 것을 통과시킨다.
                        계단(회복_시계열)))
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

    /**
     * 성공한 보고 하나를 본 뒤부터 다시 잰다. <b>시각을 손으로 덮지 않는다</b> — 덮으면 경계를 가로지른
     * 공백이 경계 뒤의 짧은 공백으로 기록돼, 밀렸다 재개된 구간이 초록으로 지나간다.
     */
    private void 공백을_다시_잰다() {
        long 본_수 = 보고한_수.get();
        Awaitility.await().alias("보고 하나가 들어온다").atMost(기다림)
                .pollInterval(Duration.ofMillis(50)).until(() -> 보고한_수.get() > 본_수);
        보고_최대_공백.set(0);
    }

    /** 구간 내내 공백이 신선도 창 안이었는가. 한 점만 보면 밀렸다 재개된 구간이 초록으로 지나간다. */
    private boolean 공백이_신선도_안인가() {
        return 보고_최대_공백.get() < 신선도.toMillis();
    }

    /**
     * 발행 하나를 가리키는 표.
     *
     * <p><b>시각만으로는 못 가른다.</b> 발행 시각은 초 해상도인데, 회차가 틱을 다 쓰면 다음 회차가
     * 틱의 4분의 1 만에 돌아 같은 초에 발행 둘이 든다. 그때 뒤엣것만 보면 앞엣것이 표본에서 빠진다.
     */
    private record Publication(Instant 시각, double 평활) { }

    /**
     * 발행마다 평활을 한 번씩 잰다. 시간으로 나눠 재면 틱 설정이 바뀌는 순간 한 틱을 두 번 센다.
     *
     * <p><b>발행 시각과 평활을 같은 읽기에서 꺼낸다</b> (CY-962). 나눠 읽으면 받아오기가 밀린 회차에
     * 다음 값을 읽어 표본 하나가 조용히 사라진다. 노드가 실제로 넘겨받는 값을 재는 것이기도 하다.
     *
     * @param 표본 몇 번의 발행을 볼 것인가
     */
    private List<Double> 발행마다_평활을_잰다(int 표본) {
        List<Double> 값 = new ArrayList<>();
        Publication 앞선 = 표(발행_해시());
        for (int i = 0; i < 표본; i++) {
            앞선 = 표(다음_발행을_기다린다(앞선));
            값.add(앞선.평활());
            // **누가 발행했는지는 해시에 안 적힌다.** 첫 노드가 중간에 리더를 잃으면 남의 값을 제 것으로
            // 읽으므로, 구간 내내 쥐고 있었는지를 따로 본다.
            표본_구간_리더 &= 첫_노드.isLeader();
        }
        return 값;
    }

    /** 앞선 것과 다른 발행이 실릴 때까지 기다리고 그 해시를 돌려준다. */
    private Map<String, String> 다음_발행을_기다린다(Publication 앞선) {
        AtomicReference<Map<String, String>> 본_것 = new AtomicReference<>();
        // **틱의 4분의 1 까지 좁혀질 수 있다.** 그보다 성기게 물으면 짧은 간격의 발행을 건너뛴다.
        Awaitility.await().alias("다음 발행").atMost(기다림).pollInterval(Duration.ofMillis(100))
                .until(() -> {
                    Map<String, String> 해시 = 발행_해시();
                    본_것.set(해시);
                    return !표(해시).equals(앞선);
                });
        return 본_것.get();
    }

    private Publication 표(Map<String, String> 해시) {
        return new Publication(발행_시각(해시), 평활값(해시));
    }

    private Map<String, String> 발행_해시() {
        return redis.<String, String>opsForHash()
                .entries(RedisKeys.SNAPSHOT).collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .block(기다림);
    }

    private Instant 발행_시각(Map<String, String> 해시) {
        return 해시 == null || 해시.isEmpty() ? Instant.EPOCH : codec.decode(해시).publishedAt();
    }

    /** 발행에 실린 평활값. 안 실렸으면 이월할 것이 없다는 뜻이라 NaN 으로 둔다. */
    private double 평활값(Map<String, String> 해시) {
        CreditSmoother.Snapshot 평활 = codec.smoothing(해시);
        return 평활.seeded() ? 평활.value() : Double.NaN;
    }

    /** 표본이 전부 유한한가. 발행에 평활이 안 실린 회차를 이것부터 가른다. */
    private static boolean 유한한가(List<Double> 값) {
        return !값.isEmpty() && 값.stream().allMatch(Double::isFinite);
    }

    /** 내려간 횟수. 사각지대보다 작은 차이는 안 센다. */
    private static int 내려간_횟수(List<Double> 값) {
        int 내림 = 0;
        for (int i = 1; i < 값.size(); i++) {
            if (값.get(i - 1) - 값.get(i) > 내림_사각지대) {
                내림++;
            }
        }
        return 내림;
    }

    /** 평시 값 가까이 올라왔는가. 안 보면 이월값 근처에서 멎은 평활이 내림 0 으로 통과한다. */
    private static boolean 수렴했는가(List<Double> 값) {
        return !값.isEmpty() && 값.get(값.size() - 1) >= 수렴_하한;
    }

    /**
     * 한 틱에 평활이 설명 못 할 만큼 올랐는가. 합법 상승폭은 {@code 알파 × (관측 − 앞값)} 이다.
     *
     * <p>이월이 풀려 관측치를 다시 초기값으로 먹으면 그 폭을 몇 배로 넘는다.
     */
    private static Optional<String> 계단(List<Double> 값) {
        for (int i = 1; i < 값.size(); i++) {
            double 앞 = 값.get(i - 1);
            double 허용 = 설계_알파 * (평시_가용량 - 앞) * 상승_여유;
            if (값.get(i) - 앞 > 허용) {
                return Optional.of("평활이 한 틱에 %.0f 올랐다 — 평활이 아니라 재시드다 (허용 %.0f): %s"
                        .formatted(값.get(i) - 앞, 허용, 값));
            }
        }
        return Optional.empty();
    }

    /** 그 발행에 실린 이 쿠폰의 몫. 노드들이 실제로 읽는 값이 이것이다. */
    private long 몫(Map<String, String> 해시) {
        if (해시 == null || 해시.isEmpty()) {
            return -1;
        }
        CouponState 상태 = codec.decode(해시).coupons().get(COUPON);
        return 상태 == null ? -1 : 상태.credit();
    }
}
