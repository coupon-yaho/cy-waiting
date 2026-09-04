package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotCodec;
import com.kafkick.waiting.control.SnapshotSource;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.queue.EntryToken;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

/**
 * X3 — 서킷이 열린 채 차례를 받은 사람들이 half-open 자리를 먹는다 (8.3.6 · 5절).
 *
 * <p><b>사다리 2번이 6' 번보다 앞이라 판정은 토큰 보유자를 안 막는다.</b> 그런데
 * 서킷 필터는 판정 <b>뒤</b>에 붙으므로 그 사람도 결국 서킷이 가로챈다 — 실측하면
 * 유지 구간 30 건 중 뒷단에 닿은 것은 두 건이고 나머지는 503·429 다.
 */
// **닿은 그 둘이 half-open 프로브 자리다.** 그리고 폴백이 토큰 보유자에게 가장
// 가까운 밴드(1초)로 다시 오라고 답하므로, 열린 서킷 아래에서 그 사람들이 1 초마다
// 되돌아온다 — half-open 으로 넘어가는 순간 프로브 자리가 그 재시도로 채워지고,
// 아직 찬 뒷단이 그걸 떨어뜨려 곧바로 다시 열린다. F3 이 막으려던 그림이 토큰
// 경로에 그대로 남아 있다 (G8.12).
//
// **C8 과 다른 점은 재료다.** 저쪽은 한산한 쿠폰 하나로 서킷의 열림·닫힘을 잰다.
// 여기는 줄이 선 쿠폰에 토큰을 든 사람을 두어, 그 사람들이 프로브 자리를 어떻게
// 먹는지를 잰다.
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=false")
@Import(CircuitAndSpikeScenarioTest.StallingBackend.class)
class CircuitAndSpikeScenarioTest {

    private static final String COUPON = "x3";

    /**
     * 줄이 없는 쿠폰.
     *
     * <p><b>사다리 6' 번이 여기서만 보인다.</b> 줄이 선 쿠폰은 서킷이 없어도
     * 백로그로 줄에 세우므로, 그 쿠폰으로 재면 이 규칙을 통째로 들어내도 답이
     * 같다 — 실제로 그렇게 재다가 뮤턴트가 살아남는 것을 봤다.
     */
    private static final String 한산한_쿠폰 = "x3-idle";

    /**
     * 반쯤 열린 구간 전용 한산 쿠폰. <b>손이 안 닿은 것이라야 한다.</b>
     *
     * <p>유지 구간에서 이미 줄을 세운 쿠폰으로 재면 그 흔적(붐빔 래치·초당 예산)이
     * 남아, 서킷이 세운 것과 다른 사다리가 세운 것을 구분할 수 없다.
     */
    private static final String 반쯤열림_쿠폰 = "x3-idle-half";

    /** 뒷단이 멎었는가. 이 스위치로 장애를 넣고 걷는다. */
    private static final AtomicBoolean 멎었다 = new AtomicBoolean();

    /** 짧게 잡는다. 운영값으로 재면 시험 하나가 그만큼 걸린다. */
    private static final Duration 응답_상한 = Duration.ofMillis(300);
    // 응답 상한을 줄인 판이라 연결 상한도 그보다 짧아야 한다 — 운영값을 그대로
    // 두면 기동이 막힌다.
    //
    // **응답 상한 바로 밑에 붙인다.** 여기서 연결 상한은 재는 대상이 아니라
    // 통과 조건이라, 재는 것(응답 정체)에 최대한 안 끼어드는 값이어야 한다.
    // 넉넉히 떼어 놓으면 부하 걸린 러너에서 로컬 연결이 그 값을 넘겨 실패하고,
    // 그 실패가 응답 정체와 똑같이 서킷 창에 쌓여 무엇을 쟀는지 알 수 없게 된다.
    private static final Duration 연결_상한 = Duration.ofMillis(250);

    /**
     * 한 번에 몰아치는 수. <b>억눌린 트래픽이다.</b>
     *
     * <p>정상 구간과 회복 구간에 같은 모양으로 쏜다 — 입력이 같아야 나온 봉우리의
     * 차이가 게이트웨이의 것이 된다.
     */
    private static final int 몰아칠_수 = 30;

    /**
     * 줄 선 사람 수.
     *
     * <p><b>용량 안이어야 한다.</b> 용량은 크레딧에 비례하므로 줄을 크게 잡으면
     * 줄이 꽉 찬 것으로 판정돼 토큰 없는 신규가 전부 429 를 받는다 — 서킷이
     * 무엇을 막았는지가 안 보인다. 계획서의 2 만은 규모의 스파이크이고,
     * 그 규모는 k6 부하 게이트(Phase 10)의 몫이다. 여기서 재는 것은 모양이다.
     */
    private static final long 줄_선_사람 = 100;

    /** 재고. RC1 이 이 값을 천장으로 본다. */
    private static final long 재고 = 100_000;

    /** 이 쿠폰이 한 틱에 뺄 수 있는 양. 토큰 통과의 상한이 여기서 나온다. */
    private static final long 크레딧 = 20;

    private static final Duration 기다림 = Duration.ofSeconds(30);

    /** 반쯤 열린 구간에 통과시키는 수. 배선과 같은 값이어야 판정이 뜻을 갖는다. */
    private static final int 프로브_허용 = 2;

    /**
     * 두드릴 때 쓰는 회원 번호. <b>배치와 겹치면 안 된다</b> — 겹치면 스텁이 중복
     * 수신으로 세고, 그건 회복 판정에서 초과 발급으로 보고된다.
     */
    private final AtomicInteger 두드림 = new AtomicInteger(900_000);

    private static final BackendStub 뒷단 = BackendStub.멎을_수_있다(멎었다::get);

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        // 줄 등록이 되어야 토큰 없는 신규가 큐로 간다. 안 띄우면 전량이
        // fail-open 으로 새서 이 시험이 재는 것이 통째로 바뀐다.
        faults = RedisFaults.시작한다();
        registry.add("spring.data.redis.url", faults::주소);
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("waiting.backend.response-timeout", () -> 응답_상한);
        registry.add("waiting.backend.connect-timeout", () -> 연결_상한);
        registry.add("waiting.backend.circuit.minimum-number-of-calls", () -> 3);
        registry.add("waiting.backend.circuit.sliding-window-size", () -> "2s");
        registry.add("waiting.backend.circuit.wait-duration-in-open-state", () -> "1s");
        registry.add("waiting.backend.circuit.permitted-number-of-calls-in-half-open-state",
                () -> 2);
    }

    @AfterAll
    static void 내린다() {
        뒷단.close();
        if (faults != null) {
            faults.close();
        }
    }

    @TestConfiguration
    static class StallingBackend {

        /**
         * 줄이 선 쿠폰. <b>토큰을 든 사람과 안 든 사람이 갈리는 재료다.</b>
         *
         * <p>한산한 쿠폰으로 재면 토큰 없는 사람도 통과해 서킷이 무엇을 막았는지
         * 안 보인다.
         */
        // **부를 때마다 새로 찍는다.** 한 번 만들어 두면 시나리오가 도는 동안
        // 재료가 늙어 낡음이 열리고, 그러면 한산한 쿠폰이 사다리 4번(fail-open)
        // 으로 빠져 서킷 갈래를 한 번도 안 밟는다. 실제로 그렇게 재다가
        // 한산한 쿠폰이 503 을 받는 것을 봤다.
        // RULE-EXCEPTION(TS-4): 나이를 실제로 늙게 두는 것이 이 시나리오의 재료다.
        // 시각을 고정하면 낡음이 영영 안 열리고, 열리는지 안 열리는지가 정확히
        // 이 시나리오가 재는 것이다.
        @Bean
        @Primary
        SnapshotSource 몰리는_재료(Clock clock) {
            return () -> Mono.fromSupplier(() -> SnapshotCodec.create().encode(
                    new GatewaySnapshot(
                            Map.of(COUPON, CouponStates.queueing(크레딧, 재고, 줄_선_사람),
                                    한산한_쿠폰, CouponStates.idle(재고),
                                    반쯤열림_쿠폰, CouponStates.idle(재고)),
                            new SnapshotMeta(크레딧, 1), clock.instant()),
                    CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty()));
        }
    }

    @LocalServerPort
    private int port;

    @Autowired
    private CircuitBreakerRegistry circuits;

    @Autowired
    private EntryToken entryTokens;

    /** 시험이 실시계를 직접 읽으면 앱과 갈릴 수 있다 (TS-4). 같은 시계를 쓴다. */
    @Autowired
    private Clock clock;

    private WebTestClient 클라이언트() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(응답_상한.multipliedBy(30))
                .build();
    }

    /**
     * 차례를 받은 사람. 사다리 2번을 밟는다 — 다만 서킷 필터가 판정 뒤라 결국
     * 그쪽이 가로챈다.
     */
    // RULE-EXCEPTION(TS-4): 토큰의 서명이 실시계로 검증되므로 고정 시각을 실으면
    // 만료로 거절된다. 이 시나리오는 토큰이 통과하는 것을 전제로 한다.
    private int 토큰으로_시도한다(int member) {
        return 클라이언트().post()
                .uri("/api/v1/coupons/" + COUPON + "/issue")
                .header("X-Member-Id", String.valueOf(member))
                .header("X-Member-Grade", "GOLD")
                .header("Entry-Token",
                        entryTokens.issue(COUPON, String.valueOf(member), clock.instant()))
                .exchange()
                .returnResult(Void.class)
                .getStatus()
                .value();
    }

    /** 차례를 못 받은 사람. 서킷이 열려 있으면 한산한 쿠폰이어도 줄로 가야 한다. */
    private int 토큰_없이_시도한다(String couponId, int member) {
        return 클라이언트().post()
                .uri("/api/v1/coupons/" + couponId + "/issue")
                .header("X-Member-Id", String.valueOf(member))
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .returnResult(Void.class)
                .getStatus()
                .value();
    }

    private List<Integer> 여러_번_시도한다(String couponId, int 횟수, int 시작_회원) {
        List<Integer> 상태 = new ArrayList<>();
        for (int i = 0; i < 횟수; i++) {
            상태.add(토큰_없이_시도한다(couponId, 시작_회원 + i));
        }
        return 상태;
    }

    /**
     * 같은 순간에 몰아친다 — <b>열린 루프여야 버스트가 잡힌다.</b>
     *
     * <p>순서대로 보내면 발신 속도가 게이트웨이 지연으로 정해져, 게이트웨이가
     * 몰아쳐도 시험이 같이 빨라질 뿐 봉우리가 안 움직인다.
     */
    private List<Integer> 한꺼번에_토큰으로(int 횟수, int 시작_회원) {
        ExecutorService 일꾼 = Executors.newFixedThreadPool(횟수);
        CountDownLatch 출발 = new CountDownLatch(1);
        try {
            List<Future<Integer>> 결과 = new ArrayList<>();
            for (int i = 0; i < 횟수; i++) {
                int member = 시작_회원 + i;
                결과.add(일꾼.submit(() -> {
                    출발.await();
                    return 토큰으로_시도한다(member);
                }));
            }
            출발.countDown();
            List<Integer> 상태 = new ArrayList<>();
            for (Future<Integer> 하나 : 결과) {
                상태.add(하나.get(기다림.toSeconds(), TimeUnit.SECONDS));
            }
            return 상태;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("몰아치기가 끊겼다", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("몰아치기가 실패했다", e);
        } finally {
            일꾼.shutdownNow();
        }
    }

    private CircuitBreaker 서킷() {
        return circuits.find("backend").orElseThrow(
                () -> new IllegalStateException("서킷이 없다 — 이름이 바뀌었는지 본다"));
    }

    /**
     * X3 — 서킷이 열린 채 억눌린 트래픽이 회복 순간에 몰린다.
     *
     * <p>회복이 곧 2차 장애가 되면 안 된다 (RC4).
     */
    @Test
    @DisplayName("X3_서킷이_열린_뒤_차례를_받은_사람들이_프로브_자리를_먹는다")
    void X3_서킷이_열린_뒤_차례를_받은_사람들이_프로브_자리를_먹는다() {
        AtomicInteger 열린_횟수 = new AtomicInteger();
        // **서킷을 먼저 만들고 그 뒤에 기록기를 세운다.** 기록기는 세우는 순간의
        // 누적값을 기준으로 잡으므로, 예열 요청이 그 앞에 있어야 평시 봉우리에
        // 안 섞인다. 섞이면 RC4 의 분모가 부풀어 진짜 버스트가 한계 아래로 든다.
        토큰으로_시도한다(900);
        BackendRpsRecorder 유입 = new BackendRpsRecorder(뒷단::받은_수);
        List<Integer> 정상_토큰_상태 = new ArrayList<>();
        List<Integer> 장애중_토큰_상태 = new ArrayList<>();
        List<Integer> 장애중_무토큰_상태 = new ArrayList<>();
        List<Integer> 회복_토큰_상태 = new ArrayList<>();
        List<Integer> 반쯤열림_무토큰_상태 = new ArrayList<>();
        long[] 반쯤열림_한산_도착 = new long[1];
        long[] 정상_도착 = new long[1];
        long[] 유지중_도착 = new long[1];
        long[] 회복_도착 = new long[1];
        double[] 정상_봉우리 = new double[1];
        double[] 회복_봉우리 = new double[1];
        long[] 한산한_쿠폰_도착 = new long[1];

        ChaosScenario.named("X3 서킷 + 억눌린 트래픽")
                .baseline(() -> {
                    // 서킷은 첫 요청이 만든다 — 위에서 이미 한 번 쐈다.
                    // **열린 횟수를 센다** (G8.12). 반복 실패는 유입 억제가 안
                    // 걸린다는 뜻이고, 이 시나리오가 그것을 볼 수 있는 유일한 자리다.
                    서킷().getEventPublisher().onStateTransition(e -> {
                        if (e.getStateTransition().getToState() == CircuitBreaker.State.OPEN) {
                            열린_횟수.incrementAndGet();
                        }
                    });
                    다음_초를_기다린다();
                    // RULE-EXCEPTION(TS-4): 봉우리는 벽시계 초 버킷으로만 잰다.
                    // 고정 시각이면 모든 도착이 한 버킷에 들어가 봉우리가 사라진다.
                    유입.sample(clock.instant());
                    Instant 시작 = clock.instant();
                    long 전 = 뒷단.받은_수();
                    정상_토큰_상태.addAll(한꺼번에_토큰으로(몰아칠_수, 1_000));
                    정상_도착[0] = 뒷단.받은_수() - 전;
                    // **봉우리는 초 단위 버킷이라 한 초를 넘겨야 읽힌다.**
                    // 같은 초 안에서 물으면 구간이 비어 늘 0 이 나온다.
                    다음_초를_기다린다();
                    유입.sample(clock.instant());
                    정상_봉우리[0] = 유입.peakRps(시작, clock.instant());
                    assertThat(정상_도착[0]).as("전제 — 평시에 토큰 보유자는 뒷단까지 간다")
                            .isPositive();
                    assertThat(정상_봉우리[0]).as("전제 — 평시 봉우리를 쟀다").isPositive();
                })
                .inject(() -> 멎었다.set(true))
                .duringFault(() -> {
                    // **두드리면서 기다린다.** 창이 시간 기반이라 한 번 쏘고
                    // 기다리면 실패가 창 밖으로 나가 영영 안 열린다. 게다가
                    // 토큰 통과에는 초당 상한이 있어 한꺼번에 쏘면 대부분이
                    // 429 로 끊겨 뒷단까지 안 간다 — 서킷이 표본을 못 얻는다.
                    열릴_때까지_두드린다();
                    // **한산한 쿠폰을 먼저, 새 초에 잰다.** 토큰 배치가 초당
                    // 예산을 먹은 뒤에 물으면 사다리 9번(초당 상한)이 줄에
                    // 세워, 서킷이 세운 것과 구분이 안 된다. 실제로 그렇게
                    // 재다가 서킷 갈래를 들어낸 뮤턴트가 살아남았다.
                    다음_초를_기다린다();
                    long 한산_전 = 뒷단.받은_수(한산한_쿠폰);
                    장애중_무토큰_상태.addAll(여러_번_시도한다(한산한_쿠폰, 3, 2_500));
                    한산한_쿠폰_도착[0] = 뒷단.받은_수(한산한_쿠폰) - 한산_전;

                    다음_초를_기다린다();
                    long 전 = 뒷단.받은_수();
                    장애중_토큰_상태.addAll(한꺼번에_토큰으로(몰아칠_수, 2_100));
                    유지중_도착[0] = 뒷단.받은_수() - 전;
                })
                .recover(() -> 멎었다.set(false))
                .afterRecovery(() -> {
                    // **가장 위험한 구간을 여기서 잰다.** 반쯤 열린 동안은 프로브
                    // 자리가 몇 개뿐인데, 그 자리를 새 트래픽이 먹으면 약한 뒷단이
                    // 다시 무너진다. 판정 규칙은 반쯤 열림을 열림과 똑같이 다루므로
                    // 토큰 없는 신규는 여전히 줄로 가야 한다.
                    //
                    // **손이 안 닿은 쿠폰으로 잰다.** 유지 구간에서 이미 줄을 세운
                    // 쿠폰을 다시 쓰면 그 흔적이 남아 다른 사다리가 먼저 문다 —
                    // 실제로 그렇게 재다가 반쯤 열림만 도려낸 뮤턴트가 살아남았다.
                    반쯤_열릴_때까지_기다린다();
                    다음_초를_기다린다();
                    long 반쯤_한산_전 = 뒷단.받은_수(반쯤열림_쿠폰);
                    반쯤열림_무토큰_상태.addAll(여러_번_시도한다(반쯤열림_쿠폰, 1, 4_500));
                    반쯤열림_한산_도착[0] = 뒷단.받은_수(반쯤열림_쿠폰) - 반쯤_한산_전;

                    닫힐_때까지_기다린다();
                    다음_초를_기다린다();
                    // RULE-EXCEPTION(TS-4): 봉우리는 벽시계 초 버킷으로만 잰다.
                    // 고정 시각이면 모든 도착이 한 버킷에 들어가 봉우리가 사라진다.
                    유입.sample(clock.instant());
                    Instant 시작 = clock.instant();
                    long 전 = 뒷단.받은_수();
                    회복_토큰_상태.addAll(한꺼번에_토큰으로(몰아칠_수, 3_000));
                    회복_도착[0] = 뒷단.받은_수() - 전;
                    다음_초를_기다린다();
                    유입.sample(clock.instant());
                    회복_봉우리[0] = 유입.peakRps(시작, clock.instant());
                })
                .assertEntry(ChaosScenario.Verdict.none())
                .assertDuring(() -> RecoveryCriteria.violations(
                        서킷이_열렸다(),
                        // **열린 동안 뒷단에 닿는 것은 프로브 자리뿐이다.** 판정은
                        // 토큰 보유자를 안 막지만 서킷 필터가 뒤에서 가로챈다 —
                        // 그 둘이 갈리는 자리를 값으로 못 박는다.
                        프로브_자리만_닿았다(유지중_도착[0]),
                        전부_답을_받았다("유지", 장애중_토큰_상태),
                        // 토큰 없는 신규는 줄로 간다 (F3).
                        줄에_세웠다(장애중_무토큰_상태),
                        // 답만 보면 못 가른다. 뒷단까지 갔는지를 따로 본다 —
                        // 202 를 주고 뒤에서 흘려도 답은 같다.
                        한산한_쿠폰이_뒷단에_안_갔다(한산한_쿠폰_도착[0])))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        전부_답을_받았다("회복", 회복_토큰_상태),
                        // RC1 — 뒷단 도착이 재고를 안 넘는다.
                        RecoveryCriteria.overIssued(뒷단.받은_수(), 재고),
                        // **닫힌 루프라 증폭으로도 본다.** 재전송이나 풀 재시도로
                        // 요청이 불어나면 보낸 것보다 많이 도착한다.
                        RecoveryCriteria.amplified(몰아칠_수, 회복_도착[0]),
                        // RC4 — 같은 모양으로 쐈는데 봉우리가 커졌다면 그것은
                        // 게이트웨이가 억눌러 둔 것을 한꺼번에 푼 것이다.
                        RecoveryCriteria.recoveryBurst(정상_봉우리[0], 회복_봉우리[0]),
                        // **반쯤 열린 동안에도 신규는 줄에 남는다.** 이 구간이
                        // 안 재지면, 프로브 자리를 새 트래픽이 먹는 회귀가 통과한다.
                        줄에_세웠다(반쯤열림_무토큰_상태),
                        한산한_쿠폰이_뒷단에_안_갔다(반쯤열림_한산_도착[0]),
                        // G8.12 — 회복까지 몇 번 열렸는가. C8 이 공허하다고 적어
                        // 둔 자리이고, 이 시나리오가 그것을 볼 수 있는 유일한 자리다.
                        반복해서_안_열렸다(열린_횟수.get()),
                        중복_수신이_없다()))
                // **RC2·RC3·RC5·RC6 은 여기서 안 잰다.** 순번을 안 돌려주고,
                // 배분을 안 돌리므로 자리가 안 움직인다.
                .run();
    }

    /** 초 경계를 넘긴다. 토큰 통과의 상한이 초 단위라 앞 배치와 예산을 안 섞는다. */
    private void 다음_초를_기다린다() {
        long 지금 = clock.instant().getEpochSecond();
        Awaitility.await().atMost(기다림)
                .pollInterval(Duration.ofMillis(20))
                .until(() -> clock.instant().getEpochSecond() > 지금);
    }

    /** 서킷이 열릴 때까지 두드린다. 창이 시간 기반이라 계속 실패를 넣어야 한다. */
    private void 열릴_때까지_두드린다() {
        Awaitility.await().atMost(기다림)
                .pollInterval(Duration.ofMillis(100))
                .until(() -> {
                    토큰으로_시도한다(두드림.incrementAndGet());
                    return 서킷().getState() == CircuitBreaker.State.OPEN;
                });
    }

    /** 서킷이 닫힐 때까지 두드린다. 토큰 보유자만이 반쯤 열린 구간을 지난다. */
    /**
     * 반쯤 열릴 때까지 기다린다.
     *
     * <p><b>두드리지 않는다.</b> 열린 상태에서 반쯤 열림으로 가는 것은 시간이 하는
     * 일이고(자동 전환), 여기서 두드리면 그 요청이 곧 프로브가 되어 재려는 구간을
     * 스스로 지나가 버린다.
     */
    private void 반쯤_열릴_때까지_기다린다() {
        Awaitility.await().atMost(기다림)
                .pollInterval(Duration.ofMillis(100))
                .until(() -> 서킷().getState() == CircuitBreaker.State.HALF_OPEN);
    }

    private void 닫힐_때까지_기다린다() {
        Awaitility.await().atMost(기다림)
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    토큰으로_시도한다(두드림.incrementAndGet());
                    return 서킷().getState() == CircuitBreaker.State.CLOSED;
                });
    }

    private Optional<String> 서킷이_열렸다() {
        CircuitBreaker.State 상태 = 서킷().getState();
        return 상태 == CircuitBreaker.State.OPEN || 상태 == CircuitBreaker.State.HALF_OPEN
                ? Optional.empty()
                : Optional.of("서킷이 %s 다 — 뒷단이 멎었는데 안 열렸다".formatted(상태));
    }

    /**
     * <b>답을 받는가.</b> 도착 수만 보면 전원이 끊긴 경우와 구분이 안 된다.
     */
    // **개수만 보면 전원이 500 이어도 초록이다.** 서킷 폴백(503)과 초당 상한(429)은
    // 원인이 다르고, 회복 판정의 뜻이 거기서 갈린다.
    private Optional<String> 전부_답을_받았다(String 구간, List<Integer> 상태) {
        if (상태.size() != 몰아칠_수) {
            return Optional.of("%s — %d 건만 답을 받았다 (보낸 %d)"
                    .formatted(구간, 상태.size(), 몰아칠_수));
        }
        long 엉뚱한_답 = 상태.stream()
                .filter(status -> status != 200 && status != 202
                        && status != 429 && status != 503)
                .count();
        return 엉뚱한_답 == 0 ? Optional.empty()
                : Optional.of("%s — %d 건이 200·202·429·503 이 아니다: %s"
                        .formatted(구간, 엉뚱한_답, 상태));
    }

    /**
     * <b>열린 동안 뒷단에 닿는 것은 half-open 프로브뿐이다.</b>
     *
     * <p>판정은 토큰 보유자를 안 막지만(사다리 2번이 6' 번보다 앞이다) 서킷
     * 필터가 판정 뒤에 붙어 그 사람도 가로챈다. 닿은 것이 프로브 허용 수를
     * 넘으면 서킷이 유입을 못 조인 것이다.
     */
    private Optional<String> 프로브_자리만_닿았다(long 도착) {
        return 도착 <= 프로브_허용 ? Optional.empty()
                : Optional.of("서킷이 열린 동안 %d 건이 뒷단에 닿았다 — 프로브 허용 %d 이내여야 한다"
                        .formatted(도착, 프로브_허용));
    }

    /**
     * <b>열린 횟수가 둘을 넘으면 회복이 반복 실패한 것이다</b> (G8.12).
     *
     * <p>half-open 순간 그때 도착한 트래픽이 약한 뒷단에 꽂혀 다시 열린다.
     * 차례를 받은 사람들이 1 초 밴드로 되돌아오므로 이 시나리오가 그 그림 그대로다.
     */
    private Optional<String> 반복해서_안_열렸다(int 열린_횟수) {
        return 열린_횟수 <= 2 ? Optional.empty()
                : Optional.of("서킷이 %d 번 열렸다 — 두 번 이내여야 한다".formatted(열린_횟수));
    }

    /** 토큰 없는 신규는 줄로 간다 (F3). 뒷단으로 흘리면 약한 뒷단이 다시 무너진다. */
    private Optional<String> 줄에_세웠다(List<Integer> 상태) {
        long 자리를_못_받은_수 = 상태.stream().filter(status -> status != 202).count();
        return 자리를_못_받은_수 == 0 ? Optional.empty()
                : Optional.of("서킷이 열렸는데 %d 건이 줄에 안 섰다: %s"
                        .formatted(자리를_못_받은_수, 상태));
    }

    /**
     * <b>서킷이 열린 동안 한산한 쿠폰도 뒷단에 안 간다</b> (F3).
     *
     * <p>약한 뒷단에 신규를 흘리면 half-open 이 반복 실패하고, 회복이 영영 안 온다.
     */
    private Optional<String> 한산한_쿠폰이_뒷단에_안_갔다(long 도착) {
        return 도착 == 0 ? Optional.empty()
                : Optional.of("서킷이 열렸는데 한산한 쿠폰에서 %d 건이 뒷단까지 갔다"
                        .formatted(도착));
    }

    /** 같은 사람이 두 번 발급되면 그것이 곧 초과 발급이다. */
    private Optional<String> 중복_수신이_없다() {
        return 뒷단.중복_수신이_없다();
    }
}
