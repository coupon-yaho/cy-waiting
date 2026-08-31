package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.gateway.AdmissionGatewayFilter;
import com.kafkick.waiting.gateway.GatewayRoutes;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.lettuce.core.api.StatefulRedisConnection;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * C18 — 뒷단이 붙기는 하는데 아무것도 안 보낸다 (8.3.5 · 5절).
 *
 * <p>C8 과 갈리는 것은 <b>재는 대상</b>이다. 저쪽은 서킷이 열렸다 닫히는 것을
 * 보고, 여기는 <b>멎은 요청이 격벽 자리를 쥐지 않는가</b>와 <b>끝내는 방식</b>을
 * 본다 — 504 가 아니라 503 에 다시 올 시각을 실어 끝낸다 (6.2.4).
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class BackendSilenceScenarioTest {

    /** 줄이 없는 대조 쿠폰. 구간마다 따로 둬야 초당 예산이 구간을 안 넘는다. */
    private static final String[] 쿠폰 = {"c18-normal", "c18-silent", "c18-recovered"};

    private static final int 보낼_수 = 3;

    private static final Duration 기다림 = Duration.ofSeconds(60);

    private static final long 가용량 = 2_000;

    /**
     * 다시 올 시각의 밴드. 차례를 못 받은 사람은 가장 먼 밴드(30초)로 부르고
     * 지터가 위아래로 흔든다 — 상수로 박으면 회복 순간에 전원이 같은 초로 온다.
     */
    private static final long 밴드_하한 = 20;

    private static final long 밴드_상한 = 40;

    /** 뒷단이 입을 닫았는가. 이 스위치로 장애를 넣고 걷는다. */
    private static final AtomicBoolean 멎었다 = new AtomicBoolean();

    private static final BackendStub 뒷단 = BackendStub.멎을_수_있다(멎었다::get);

    private static final ScheduledExecutorService 보고 =
            Executors.newSingleThreadScheduledExecutor();

    private static final AtomicLong 뛰다_터진_수 = new AtomicLong();

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
        멎었다.set(false);
        뒷단.close();
        if (faults != null) {
            faults.close();
        }
    }

    @LocalServerPort
    private int port;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @Autowired
    private SnapshotHolder holder;

    @Autowired
    private CircuitBreakerRegistry circuits;

    @Autowired
    private AdmissionGatewayFilter 판정;

    @Autowired
    private MeterRegistry meters;

    /**
     * 게이트웨이가 끊는 것을 재려면 클라이언트가 먼저 안 끊어야 한다. 먼저
     * 끊으면 게이트웨이 쪽에서는 취소로 보이고, 취소는 서킷 창에 안 쌓인다.
     */
    private WebTestClient 클라이언트() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(60))
                .build();
    }

    private Reply 물어본다(String couponId, int member) {
        EntityExchangeResult<byte[]> 결과 = 클라이언트().post()
                .uri("/api/v1/coupons/" + couponId + "/issue")
                .header("X-Member-Id", String.valueOf(member))
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .expectBody().returnResult();
        String 다시_올_시각 = 결과.getResponseHeaders().getFirst("Retry-After");
        return new Reply(결과.getStatus().value(),
                다시_올_시각 == null ? -1 : Long.parseLong(다시_올_시각),
                결과.getResponseBody() == null ? "" : new String(결과.getResponseBody()));
    }

    /** 상태와 다시 올 시각과 본문. 셋을 같이 봐야 어떻게 끝냈는지가 갈린다. */
    private record Reply(int 상태, long 다시_올_시각, String 본문) {
    }

    private List<Reply> 여러_번_시도한다(String couponId, int 시작_회원) {
        List<Reply> 모은_것 = new ArrayList<>();
        for (int i = 0; i < 보낼_수; i++) {
            모은_것.add(물어본다(couponId, 시작_회원 + i));
        }
        return 모은_것;
    }

    private void 재료를_심는다(StatefulRedisConnection<String, String> 연결) {
        for (String 하나 : 쿠폰) {
            redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, 하나).block(기다림);
            redis.opsForValue().set(RedisKeys.stock(하나), "100000").block(기다림);
        }
        BackendReports 보고서 = BackendReports.실시계로(연결, Duration.ofSeconds(3));
        보고.scheduleAtFixedRate(() -> {
            try {
                보고서.보고한다("c18-be", 가용량);
            } catch (RuntimeException e) {
                뛰다_터진_수.incrementAndGet();
            }
        }, 0, 500, TimeUnit.MILLISECONDS);
    }

    private CircuitBreaker 서킷() {
        return circuits.find(GatewayRoutes.CIRCUIT).orElseThrow(
                () -> new IllegalStateException("서킷이 없다 — 요청이 서킷을 안 지났다"));
    }

    @Test
    @DisplayName("C18_멎은_뒷단이_격벽을_쥐지_않고_다시_올_시각으로_끝난다")
    void C18_멎은_뒷단이_격벽을_쥐지_않고_다시_올_시각으로_끝난다() {
        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        try {
            List<Reply> 정상 = new ArrayList<>();
            List<Reply> 침묵중 = new ArrayList<>();
            List<Reply> 회복 = new ArrayList<>();
            long[] 도착 = new long[3];
            int[] 격벽 = new int[3];
            long[] 폴백_수 = new long[2];
            long[] 표본 = new long[1];

            ChaosScenario.named("C18 뒷단 무응답")
                    .baseline(() -> {
                        재료를_심는다(연결);
                        Awaitility.await().alias("첫 스냅샷이 닿아 재료가 신선해진다")
                                .atMost(기다림).until(() -> !holder.isDataStale());
                        도착[0] = 뒷단까지_센다(쿠폰[0], () -> 정상.addAll(
                                여러_번_시도한다(쿠폰[0], 1_000)));
                        격벽[0] = 판정.inFlight();
                        폴백_수[0] = 폴백_계수();
                    })
                    .inject(() -> 멎었다.set(true))
                    .duringFault(() -> {
                        // **쥐었는지부터 본다.** 끝난 뒤의 0 만 보면 격벽을
                        // 통째로 들어내도 통과한다 — 0 == 0 이다.
                        격벽[1] = 물려_있는_동안_격벽을_본다();
                        도착[1] = 뒷단까지_센다(쿠폰[1], () -> 침묵중.addAll(
                                여러_번_시도한다(쿠폰[1], 2_000)));
                        // 자리를 놓고 나서 다시 본다. 끝난 뒤에도 남아 있으면
                        // 그 자리는 영영 안 돌아온다.
                        격벽[2] = 판정.inFlight();
                        폴백_수[1] = 폴백_계수();
                        // **실패로 좁혀 센다.** 창에 쌓인 수는 성공도 포함해서,
                        // 끊는 자리가 서킷 밖이어도 성공 표본으로 통과한다.
                        표본[0] = 서킷().getMetrics().getNumberOfFailedCalls()
                                + 서킷().getMetrics().getNumberOfSlowCalls();
                    })
                    .recover(() -> 멎었다.set(false))
                    .afterRecovery(() -> {
                        서킷이_받을_때까지_기다린다();
                        도착[2] = 뒷단까지_센다(쿠폰[2], () -> 회복.addAll(
                                여러_번_시도한다(쿠폰[2], 3_000)));
                    })
                    .assertEntry(() -> RecoveryCriteria.violations(
                            평시에_통했다(정상, 도착[0]),
                            격벽이_비었다("정상", 격벽[0])))
                    .assertDuring(() -> RecoveryCriteria.violations(
                            // **504 가 아니라 503 이다** (6.2.4). 대기열 문맥에서
                            // 504 는 "게이트웨이가 고장났다" 로 읽히고, 다시
                            // 오라는 안내가 없다.
                            끝내는_방식이_맞다(침묵중),
                            // **다시 올 시각이 밴드 안인가** (F7). 0 보다 크기만
                            // 보면 상수로 박아도 통과하고, 그러면 회복 순간에
                            // 전원이 같은 초로 돌아온다.
                            밴드_안에서_부른다(침묵중),
                            // **격벽 자리를 놓았는가.** 멎은 요청이 자리를 쥔
                            // 채로 남으면 뒷단이 살아나도 그 자리는 안 돌아온다.
                            격벽을_쥐었다(격벽[1]),
                            격벽이_비었다("유지", 격벽[2]),
                            // **유지 구간에 요청이 정말 뒷단까지 갔는가.**
                            // 안 갔으면 판정 단계에서 다 끊은 것이라, 격벽도
                            // 서킷도 아무것도 안 겪는다.
                            뒷단까지_갔다(도착[1]),
                            // **막았다는 신호가 남았는가.** 이 구간은 로그가
                            // 한 줄도 안 나온다 — 서킷이 안 열려 전이가 없고
                            // 격벽도 안 차기 때문이다. 남는 것은 이 계수뿐이다.
                            폴백이_세어졌다(폴백_수[0], 폴백_수[1]),
                            // **취소가 아니라 실패로 쌓였는가.** 끊는 자리가
                            // 서킷 밖이면 서킷이 받는 것은 취소이고, 취소는
                            // 창에 안 쌓인다 — 표본이 0 이면 영영 안 열린다.
                            서킷에_쌓였다(표본[0])))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            뒷단이_살아났다(도착[2], 회복),
                            // RC4 — 보낸 것보다 많이 도착하지 않았는가.
                            RecoveryCriteria.amplified(보낼_수, 도착[2]),
                            보고가_안_터졌다(),
                            뒷단.중복_수신이_없다()))
                    .run();
        } finally {
            보고.shutdownNow();
            멎었다.set(false);
            연결.close();
        }
    }

    private long 뒷단까지_센다(String couponId, Runnable 배치) {
        long 전 = 뒷단.받은_수(couponId);
        배치.run();
        return 뒷단.받은_수(couponId) - 전;
    }

    /** 서킷이 다시 요청을 받을 때까지. 열린 채면 회복 구간이 아무것도 못 잰다. */
    private void 서킷이_받을_때까지_기다린다() {
        Awaitility.await().alias("서킷이 다시 시도를 받는다").atMost(기다림)
                .until(() -> 서킷().getState() != CircuitBreaker.State.OPEN);
    }

    /** 평시에 길이 열려 있어야 나머지 구간의 관측에 뜻이 생긴다. */
    private Optional<String> 평시에_통했다(List<Reply> 상태, long 도착) {
        if (상태.size() != 보낼_수) {
            return Optional.of("전제 — %d 건을 보냈는데 %d 건만 관측됐다"
                    .formatted(보낼_수, 상태.size()));
        }
        long 못_받은_것 = 상태.stream().filter(하나 -> 하나.상태() != 200).count();
        if (못_받은_것 > 0) {
            return Optional.of("전제 — 평시에 %d 건이 200 이 아니다: %s"
                    .formatted(못_받은_것, 상태));
        }
        return 도착 == 보낼_수 ? Optional.empty()
                : Optional.of("전제 — 평시에 %d 건만 뒷단까지 갔다 (보낸 %d)"
                        .formatted(도착, 보낼_수));
    }

    /**
     * <b>어떻게 끝냈는가.</b> 504 는 게이트웨이가 고장났다는 뜻으로 읽히고 다시
     * 오라는 안내가 없다. 503 에 다시 올 시각과 사유를 실어 끝낸다.
     */
    private Optional<String> 끝내는_방식이_맞다(List<Reply> 상태) {
        if (상태.size() != 보낼_수) {
            return Optional.of("유지 — %d 건을 보냈는데 %d 건만 관측됐다"
                    .formatted(보낼_수, 상태.size()));
        }
        for (Reply 하나 : 상태) {
            if (하나.상태() != 503) {
                return Optional.of("유지 — %d 로 끝냈다 (503 이어야 한다): %s"
                        .formatted(하나.상태(), 상태));
            }
            if (하나.다시_올_시각() <= 0) {
                return Optional.of("유지 — 다시 올 시각이 없다 (%d)".formatted(하나.다시_올_시각()));
            }
            if (!하나.본문().contains("BACKEND_UNAVAILABLE")) {
                return Optional.of("유지 — 사유가 없다: " + 하나.본문());
            }
        }
        return Optional.empty();
    }

    /**
     * 요청 하나를 물려 두고 그동안 격벽을 본다. <b>쥐었는지부터 봐야 한다</b> —
     * 끝난 뒤의 0 만 보면 격벽을 통째로 들어내도 통과한다.
     */
    private int 물려_있는_동안_격벽을_본다() {
        Thread 물리는_사람 = new Thread(() -> 물어본다(쿠폰[1], 9_000));
        물리는_사람.setDaemon(true);
        물리는_사람.start();
        try {
            AtomicLong 최대 = new AtomicLong();
            Awaitility.await().alias("멎은 요청이 격벽 자리를 쥔다").atMost(기다림)
                    .until(() -> {
                        최대.set(Math.max(최대.get(), 판정.inFlight()));
                        return 최대.get() > 0;
                    });
            return (int) 최대.get();
        } finally {
            물리는_사람.interrupt();
        }
    }

    /** 그 계수의 지금 값. 막았다는 신호가 남는 유일한 자리다. */
    private long 폴백_계수() {
        return (long) meters.find("waiting.backend.fallback").counters().stream()
                .mapToDouble(counter -> counter.count()).sum();
    }

    /** 멎은 요청이 격벽 자리를 쥐어야 한다. 안 쥐면 격벽이 아무 일도 안 한 것이다. */
    private Optional<String> 격벽을_쥐었다(int 최대) {
        return 최대 > 0 ? Optional.empty()
                : Optional.of("멎은 요청이 격벽 자리를 한 번도 안 쥐었다 — 격벽을 안 지난다");
    }

    /** 유지 구간에 요청이 뒷단까지 안 갔으면 격벽도 서킷도 아무것도 안 겪는다. */
    private Optional<String> 뒷단까지_갔다(long 도착) {
        return 도착 > 0 ? Optional.empty()
                : Optional.of("유지 구간에 뒷단까지 간 것이 없다 — 판정 단계에서 끊었다");
    }

    /** 막았다는 신호가 계수에 남았는가. 이 구간은 로그가 한 줄도 안 나온다. */
    private Optional<String> 폴백이_세어졌다(long 전, long 후) {
        return 후 - 전 >= 보낼_수 ? Optional.empty()
                : Optional.of("폴백 계수가 %d 만 올랐다 (보낸 %d) — 막았다는 신호가 없다"
                        .formatted(후 - 전, 보낼_수));
    }

    /**
     * <b>다시 올 시각이 밴드 안인가</b> (F7). 0 보다 크기만 보면 상수로 박아도
     * 통과하고, 그러면 회복 순간에 전원이 같은 초로 돌아온다 — 그것이 2차 장애다.
     */
    private Optional<String> 밴드_안에서_부른다(List<Reply> 상태) {
        for (Reply 하나 : 상태) {
            if (하나.다시_올_시각() < 밴드_하한 || 하나.다시_올_시각() > 밴드_상한) {
                return Optional.of("다시 올 시각이 %d 다 — 밴드(%d~%d) 밖이다"
                        .formatted(하나.다시_올_시각(), 밴드_하한, 밴드_상한));
            }
        }
        // 셋이 다 같으면 흔들지 않은 것이다. 표본이 셋이라 값 하나로는 못 가른다.
        return Optional.empty();
    }

    /** 멎은 요청이 자리를 쥔 채로 남으면 뒷단이 살아나도 그 자리는 안 돌아온다. */
    private Optional<String> 격벽이_비었다(String 구간, int 남은) {
        return 남은 == 0 ? Optional.empty()
                : Optional.of("%s — 격벽에 %d 자리가 남아 있다".formatted(구간, 남은));
    }

    /** 취소는 서킷 창에 안 쌓인다. 표본이 0 이면 서킷이 영영 안 열린다. */
    private Optional<String> 서킷에_쌓였다(long 표본) {
        return 표본 > 0 ? Optional.empty()
                : Optional.of("서킷 창에 표본이 없다 — 끊는 자리가 서킷 밖이다");
    }

    /** 뒷단이 정말 살아났는가. 서킷 상태만 보면 복구를 안 해도 회복으로 읽힌다. */
    private Optional<String> 뒷단이_살아났다(long 도착, List<Reply> 상태) {
        if (도착 == 0) {
            return Optional.of("회복 뒤에 뒷단까지 간 것이 없다 — 통과 경로가 안 열렸다");
        }
        long 못_받은_것 = 상태.stream().filter(하나 -> 하나.상태() != 200).count();
        return 못_받은_것 == 0 ? Optional.empty()
                : Optional.of("회복 — %d 건이 200 이 아니다: %s".formatted(못_받은_것, 상태));
    }

    private Optional<String> 보고가_안_터졌다() {
        long 터진 = 뛰다_터진_수.get();
        return 터진 == 0 ? Optional.empty()
                : Optional.of("가용량 보고가 %d 판 터졌다 — 크레딧이 구간마다 다르다"
                        .formatted(터진));
    }
}
