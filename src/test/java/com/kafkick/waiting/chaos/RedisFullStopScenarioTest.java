package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.control.SnapshotCodec;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.domain.queue.QueueToken;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.control.SnapshotSource;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.assertj.core.api.InstanceOfAssertFactories;
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
import org.springframework.http.HttpStatus;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

/**
 * C1 — Redis 완전 정지 → 복구 (8.3.4 · 5절).
 *
 * <p>장애 주입 장치가 도는지가 아니라 <b>시나리오</b>를 잰다.
 */
// 진입·유지·회복을 따로 재고 공통 기준을 건다. 지금까지의 카오스 시험은 장치를
// 쟀지, 그 장치를 겪은 게이트웨이가 무엇을 했는지는 안 쟀다.
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=false")
@Import(RedisFullStopScenarioTest.StubbedBackend.class)
class RedisFullStopScenarioTest {

    /** 회복까지 걸린 시간. 시나리오 안에서 재고 판정에 넘긴다. */
    private Duration 회복까지_걸린_시간;

    /** 창이 열린 시점의 발신 계수. 회복 순간 창의 분모는 이 뒤에 보낸 것이다. */
    private long 창이_열린_수;

    /** 회복 순간에 시험이 보낸 수. 그 구간 도착과의 비가 증폭률이다. */
    private long 회복_순간_보낸_수;

    /**
     * 지금까지 보낸 발급 요청 수. <b>리스트 크기로 세지 않는다</b> — 리스트에
     * 무엇이 들어가느냐가 바뀌면 증폭률의 분모가 조용히 부푼다.
     */
    private long 발급_보낸_수;

    /** 장애 구간에 줄을 쳤을 때의 상태. 레디스가 정말 죽었는지의 증거다. */
    private int 줄을_친_결과;

    /** 장애 전후의 자리. RC5 가 이 둘을 비교한다. */
    private Map<String, Double> 장애_전_자리 = Map.of();

    private Map<String, Double> 회복_뒤_자리 = Map.of();

    /**
     * 회복을 기다리는 동안 볼 시계.
     *
     * <p>판정이 보는 시계는 고정이라 여기 못 쓴다 — 안 흐르면 무한히 기다린다.
     * 이 자리만 실제로 흐르는 시각이 필요하고, 주입해 두면 시험이 그것을 바꿀 수
     * 있다.
     */
    private final Supplier<Instant> 벽시계 = Instant::now;

    private static final Instant 지금 = Instant.parse("2026-08-30T00:00:00Z");
    private static final String COUPON = "c1";

    /** 줄이 서는 쿠폰. RC2·RC5 는 줄에 사람이 있어야 잴 수 있다. */
    private static final String QUEUED = "c2";

    /** 장애 전에 줄을 세울 인원. */
    private static final int 줄_선_사람 = 5;

    /**
     * 전역 크레딧. <b>고정 시계라 초당 예산이 안 채워진다</b> — 시험 전체가 한
     * 초 안에 일어나므로, 보낼 요청 수를 다 덮을 만큼 크게 잡는다.
     */
    // **시험 전체가 보내는 것보다 훨씬 크게 잡는다.** 고정 시계라 초당 예산이
    // 한 번만 채워지므로, 예산이 물리면 그 뒤의 503 이 전부 "레디스를 기다렸다"
    // 로 읽힌다. 여기서 재려는 것은 예산이 아니라 판정이 레디스를 치는가다.
    private static final int 크레딧 = 10_000;

    /** 장애 구간에 보내는 요청 수. 예산 안이라 통과가 곧 정상이다. */
    private static final int 장애중_보낼_수 = 20;

    /**
     * 정상 구간과 버스트 창의 표본 수. <b>해상도가 곧 한계다</b> — 5 건이면
     * 비율이 0.2 단위로 양자화돼 실효 임계가 1.4 배가 된다. 계획서가 요구하는
     * 1.2 배를 재려면 도착 수 기준으로 이만큼이 필요하다.
     */
    private static final int 보낼_수 = 30;

    /** 흉내내기의 정상 구간 도착 수. 확인 뒤 창과 값으로 갈리게 둔다. */
    private static final long 정상_도착 = 20;

    /** 회복을 기다리는 한 판. 이 구간은 표본에서 빠지므로 작게 둔다. */
    private static final int 대기_한_판 = 5;

    /** 재고. 이 시나리오가 보내는 전체보다 커야 미달이 위반으로 안 읽힌다. */
    private static final long 재고 = 1_000;

    /** RC3 의 한계. 이 안에 판정이 정상으로 돌아와야 한다. */
    private static final Duration 회복_한계 = Duration.ofSeconds(30);

    /** 뒷단이 받은 누적 수. 기록기가 1초마다 이걸 읽어 차분을 낸다. */
    private static final AtomicLong 뒷단이_받은_수 = new AtomicLong();

    /**
     * <b>뒷단이 같은 회원을 두 번 받은 횟수.</b>
     *
     * <p>비율로 중복을 재면 늘 여유가 생긴다 — 로컬에서 끝난 요청은 분모에만
     * 들어가고, 그만큼 중복이 숨는다. 요청을 짚어 세면 그 여유가 사라진다.
     */
    private static final AtomicLong 뒷단이_두_번_받은_수 = new AtomicLong();

    private static final Set<String> 뒷단이_본_회원 = ConcurrentHashMap.newKeySet();

    private static final DisposableServer 뒷단 = HttpServer.create()
            .port(0)
            .handle((request, response) -> {
                뒷단이_받은_수.incrementAndGet();
                // 회원 번호는 시험 전체에서 안 겹치게 발급한다. 겹쳐 도착하면
                // 게이트웨이가 한 요청을 두 번 보낸 것이다.
                String member = request.requestHeaders().get("X-Member-Id");
                if (member != null && !뒷단이_본_회원.add(member)) {
                    뒷단이_두_번_받은_수.incrementAndGet();
                }
                return response.status(HttpStatus.OK.value()).send();
            })
            .bindNow();

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        faults = RedisFaults.시작한다();
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("spring.data.redis.url", faults::주소);
    }

    @AfterAll
    static void 내린다() {
        뒷단.disposeNow();
        if (faults != null) {
            faults.close();
        }
    }

    @TestConfiguration
    static class StubbedBackend {

        @Bean
        @Primary
        Clock 고정_시계() {
            return Clock.fixed(지금, ZoneOffset.UTC);
        }

        /**
         * <b>줄이 선 쿠폰에 작은 크레딧을 심는다.</b>
         *
         * <p>한산한 쿠폰으로 재면 fail-open 상한이 안 물린다 — 무제한 통과를
         * 재려는 판정이 아무것도 안 재게 된다.
         */
        @Bean
        @Primary
        SnapshotSource 몰리는_재료() {
            Map<String, String> 재료 = SnapshotCodec.create().encode(
                    new GatewaySnapshot(
                            // **한산한 쿠폰이다.** 줄이 선 쿠폰으로 재면 전원이
                            // 202 로 끝나 뒷단에 아무것도 안 닿고, 그러면 회복
                            // 버스트를 비교할 정상값이 없다. 크레딧을 작게 둬서
                            // 한산해도 상한이 물리게 한다.
                            Map.of(COUPON, CouponStates.idle(1_000),
                                    // **용량이 대기 인원보다 넉넉해야 한다.**
                                    // 용량은 크레딧에 비례하므로, 크레딧이 작으면
                                    // 줄이 꽉 찬 것으로 판정돼 전원이 429 를 받는다.
                                    // 배분 스케줄러는 꺼 뒀으므로 선 줄은 유지된다.
                                    QUEUED, CouponStates.queueing(100, 1_000, 101)),
                            new SnapshotMeta(크레딧, 1), 지금),
                    CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty());
            return () -> Mono.just(재료);
        }
    }

    @LocalServerPort
    private int port;

    /**
     * 순번 조회 토큰. <b>이 경로는 레디스를 친다</b> — 발급 경로가 안 치는 것과
     * 대조가 되어, 레디스가 정말 죽었는지·정말 돌아왔는지를 여기서 본다.
     */
    @Autowired
    private QueueToken tokens;

    /** 자리는 응답에 안 실린다. RC5 를 재려면 줄을 직접 봐야 한다. */
    @Autowired
    private ReactiveStringRedisTemplate redis;

    private WebTestClient 클라이언트() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(10))
                .build();
    }

    /** 한 번 발급을 시도하고 받은 상태를 돌려준다. */
    private int 발급을_시도한다(int member) {
        return 클라이언트().post()
                .uri("/api/v1/coupons/" + COUPON + "/issue")
                .header("X-Member-Id", String.valueOf(member))
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .returnResult(Void.class)
                .getStatus()
                .value();
    }

    /** 줄에 세운다. 장애 전에 불러 자리를 만든다. */
    private void 줄에_세운다() {
        for (int i = 0; i < 줄_선_사람; i++) {
            클라이언트().post()
                    .uri("/api/v1/coupons/" + QUEUED + "/issue")
                    .header("X-Member-Id", String.valueOf(7_000 + i))
                    .header("X-Member-Grade", "GOLD")
                    .exchange()
                    .returnResult(Void.class);
        }
    }

    /** 줄에 선 사람들의 자리. 응답에 안 실리므로 줄을 직접 읽는다. */
    private Map<String, Double> 자리들() {
        String key = RedisKeys.queue(QUEUED, 1, 0);
        Map<String, Double> 본_것 = new LinkedHashMap<>();
        for (int i = 0; i < 줄_선_사람; i++) {
            String member = String.valueOf(7_000 + i);
            Double score = redis.opsForZSet().score(key, member).block(Duration.ofSeconds(5));
            if (score != null) {
                본_것.put(member, score);
            }
        }
        return 본_것;
    }

    /**
     * <b>판정이 살아 있는지를 시험이 스스로 확인한다.</b>
     */
    // RC4 는 창을 어디에 두느냐로 통째로 죽을 수 있고, 죽어도 시나리오는
    // 초록이다. 실제로 한 번 그렇게 죽었다 — 회복 순간을 표본에서 빼자 뒷단
    // 유입이 21 배로 튀는 판이 통과했다. 통과가 근거가 못 되는 자리라서,
    // 몰아침을 직접 흉내 내 판정이 반응하는지 본다.
    //
    // 세 갈래를 다 본다. 빨개지는 것만 보면 항상 빨간 판정도 통과하고, "RC4"
    // 로만 짚으면 두 창 중 어느 것이 반응했는지 못 가린다.
    @Test
    @DisplayName("회복_순간의_몰아침을_판정이_잡는다")
    void 회복_순간의_몰아침을_판정이_잡는다() {
        long 대기_보낸 = 대기_한_판;
        // **순서 방어가 살아 있는지부터 본다.** 아무것도 안 보낸 채로 창을
        // 닫는 것은 대기 루프 앞에서 닫은 것이다.
        assertThatThrownBy(() -> 회복_순간을_닫는다(
                new BackendRpsRecorder(new AtomicLong()::get), 0))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("기다리기 전에");

        // **분모는 창을 닫는 함수가 준 것을 그대로 쓴다.** 시험이 따로 세면
        // 계산식이 바뀌어도 양쪽이 같이 움직여 아무것도 안 잡힌다.
        BackendRpsRecorder 정상 = 유입을_흉내낸다(정상_도착, 대기_보낸, 대기_보낸, 정상_도착);
        assertThat(회복_유입을_판정한다(정상, 마지막_분모, 정상_도착))
                .as("대조군 — 도착이 보낸 수만큼이면 아무 창도 안 울린다")
                .isEmpty();

        // **한 건만 울려야 한다.** 둘 다 울리면 두 창이 겹친 것이고, 그때는
        // 어느 쪽도 자기 구간을 안 보고 있다.
        BackendRpsRecorder 대기_몰아침 = 유입을_흉내낸다(정상_도착, 대기_보낸, 대기_보낸 * 20, 정상_도착);
        assertThat(회복_유입을_판정한다(대기_몰아침, 마지막_분모, 정상_도착))
                .as("회복을 기다리는 동안 20 배가 몰아쳤다")
                .singleElement(InstanceOfAssertFactories.STRING)
                .contains("RC4 회복 증폭");

        // 확인 뒤 창이 정상 창과 구분되는지. 둘 다 정상 구간과 같은 수를
        // 보내므로 값으로는 티가 안 난다 — 정상 구간만 낮춰 잡는다.
        BackendRpsRecorder 재붕괴 = 유입을_흉내낸다(정상_도착 / 2, 대기_보낸, 대기_보낸, 정상_도착);
        assertThat(회복_유입을_판정한다(재붕괴, 마지막_분모, 정상_도착))
                .as("확인 뒤에 봉우리가 섰다")
                .singleElement(InstanceOfAssertFactories.STRING)
                .contains("RC4 회복 버스트");

        // **확인 뒤 창에도 증폭을 건다.** 중복 계수는 같은 요청이 두 번 올 때만
        // 운다. 다른 번호로 여분이 나가면 그건 못 보고, 봉우리 한계 1.2 안에
        // 들어오면 버스트도 안 운다 — 그 사이가 비어 있었다.
        BackendRpsRecorder 확인_뒤_여분 = 유입을_흉내낸다(정상_도착 + 5, 대기_보낸, 대기_보낸,
                정상_도착 + 5);
        assertThat(회복_유입을_판정한다(확인_뒤_여분, 마지막_분모, 정상_도착))
                .as("확인 뒤에 보낸 것보다 많이 갔다")
                .singleElement(InstanceOfAssertFactories.STRING)
                .contains("RC4 회복 증폭");
    }

    /**
     * 시나리오와 같은 순서로 표집한 기록기를 만든다.
     *
     * @param 회복_순간_도착 회복을 기다리는 구간에 뒷단이 받은 수
     * @param 확인_뒤_도착   회복이 확인된 뒤 창에 뒷단이 받은 수
     */
    private static BackendRpsRecorder 유입을_흉내낸다(long 정상_구간_도착, long 보낸_수,
            long 회복_순간_도착, long 확인_뒤_도착) {
        AtomicLong 뒷단 = new AtomicLong();
        BackendRpsRecorder 유입 = new BackendRpsRecorder(뒷단::get);
        유입.sample(지금);
        뒷단.addAndGet(정상_구간_도착);
        유입.sample(지금.plusSeconds(1));
        뒷단.addAndGet(장애중_보낼_수);
        유입.sample(지금.plusSeconds(2));
        뒷단.addAndGet(회복_순간_도착);
        // 시나리오가 여기서 창을 닫는다. 같은 함수를 불러 순서와 분모를 공유한다.
        마지막_분모 = 회복_순간을_닫는다(유입, 보낸_수);
        뒷단.addAndGet(확인_뒤_도착);
        유입.sample(지금.plusSeconds(4));
        return 유입;
    }

    /** 흉내내기가 창을 닫으며 얻은 분모. 시험이 따로 세지 않고 이걸 쓴다. */
    private static long 마지막_분모;

    /**
     * <b>회복 순간 창을 닫고, 그 구간에 보낸 수를 돌려준다.</b>
     *
     * <p>표집 시점과 분모 계산이 갈라지면 분자와 분모가 서로 다른 구간을
     * 가리키게 되고, 그때 판정은 조용히 죽는다. 한 함수로 묶어 순서를 못
     * 바꾸게 한다.
     */
    private static long 회복_순간을_닫는다(BackendRpsRecorder 유입, long 창에_보낸_수) {
        // **기다리기 전에는 못 닫는다.** 창을 대기 루프 앞으로 옮기면 분자는
        // 장애 구간만 담고 대기 루프 도착은 다음 창으로 샌다. 그 판이 한계
        // 안에 우연히 들어오면 판정이 죽은 채로 초록이다 — 숫자가 아니라
        // 구조로 막는다. 장애 구간분에 최소 한 판이 더 얹혀야 정상이다.
        assertThat(창에_보낸_수).as("회복을 기다리기 전에 창을 닫았다").isPositive();
        유입.sample(지금.plusSeconds(3));
        return 창에_보낸_수;
    }

    /**
     * <b>RC4 의 창 배치를 한 자리에 모은다.</b>
     *
     * <p>회복 순간과 확인 뒤는 서로 다른 창이고, 어느 창을 어디에 두는지가 이
     * 판정의 전부다. 시나리오 본문에 흩어 놓으면 창이 조용히 어긋나도 아무도
     * 모른다 — {@link #회복_순간의_몰아침을_판정이_잡는다()} 가 이 함수를 그대로
     * 불러 살아 있는지 확인한다.
     *
     * @param 보낸_수 회복을 기다리는 동안 시험이 보낸 요청 수
     */
    private static List<String> 회복_유입을_판정한다(BackendRpsRecorder 유입, long 보낸_수,
            long 확인_뒤_보낸_수) {
        return RecoveryCriteria.violations(
                // **차분은 표집 시각이 아니라 그것이 온 초에 쌓인다.**
                // 표집과 표집 사이에 온 것이므로 앞 초의 몫이다.
                RecoveryCriteria.recoveryBurst(
                        유입.averageRps(지금, 지금.plusSeconds(1)),
                        유입.peakRps(지금.plusSeconds(3), 지금.plusSeconds(4))),
                // 회복을 기다린 구간. 총량으로 나눈다 — 근거는 amplified 에 있다.
                RecoveryCriteria.amplified(보낸_수,
                        유입.sumIn(지금.plusSeconds(2), 지금.plusSeconds(3))),
                // 확인 뒤 창도 본다. 중복 계수는 같은 요청이 두 번 올 때만 울고,
                // 봉우리 한계 안에 들어오는 여분은 아무도 못 봤다.
                RecoveryCriteria.amplified(확인_뒤_보낸_수,
                        유입.sumIn(지금.plusSeconds(3), 지금.plusSeconds(4))));
    }

    /** 회복 판정 전부. RC4 두 창은 이 안에서 붙으므로 호출부에 안 보인다. */
    @SafeVarargs
    private static List<String> 회복을_판정한다(BackendRpsRecorder 유입, long 보낸_수,
            Optional<String>... 판정) {
        List<String> 깨진_것 = new ArrayList<>(RecoveryCriteria.violations(판정));
        깨진_것.addAll(회복_유입을_판정한다(유입, 보낸_수, 보낼_수));
        return 깨진_것;
    }

    /** 줄을 치는 요청. 레디스가 죽어 있으면 5xx 가 온다. */
    private int 순번을_묻는다(int member) {
        return 클라이언트().get()
                .uri("/api/v1/coupons/" + COUPON + "/queue")
                .header("X-Member-Id", String.valueOf(member))
                .header("X-Member-Grade", "GOLD")
                .header("Queue-Token", tokens.issue(COUPON, String.valueOf(member), 지금))
                .exchange()
                .returnResult(Void.class)
                .getStatus()
                .value();
    }

    private List<Integer> 여러_번_시도한다(int 횟수, int 시작_회원) {
        List<Integer> 상태 = new ArrayList<>();
        for (int i = 0; i < 횟수; i++) {
            상태.add(발급을_시도한다(시작_회원 + i));
            발급_보낸_수++;
        }
        return 상태;
    }

    /**
     * <b>진입에서 전면 차단도 무제한 통과도 없다.</b>
     *
     * <p>Redis 가 죽으면 큐 등록이 불가하므로 fail-open 이 적용되는 유일한
     * 구간이다. 전부 막으면 한산한 쿠폰까지 죽고, 전부 흘리면 상한이 사라진다.
     */
    @Test
    @DisplayName("C1_레디스가_죽었다_살아난다")
    void C1_레디스가_죽었다_살아난다() {
        BackendRpsRecorder 유입 = new BackendRpsRecorder(뒷단이_받은_수::get);
        List<Integer> 정상_상태 = new ArrayList<>();
        List<Integer> 장애중_상태 = new ArrayList<>();
        List<Integer> 회복_상태 = new ArrayList<>();
        List<Integer> 확인_뒤_상태 = new ArrayList<>();

        ChaosScenario.named("C1 Redis 완전 정지")
                .baseline(() -> {
                    줄에_세운다();
                    장애_전_자리 = 자리들();
                    유입.sample(지금);
                    정상_상태.addAll(여러_번_시도한다(보낼_수, 1_000));
                    유입.sample(지금.plusSeconds(1));
                    // **정상 구간이 정상인지부터 본다.** 여기가 이미 장애면
                    // 뒤의 비교가 전부 무의미하다 — 버스트를 잴 기준이 없다.
                    assertThat(정상_상태).as("전제 — 5xx 없이 답한다")
                            .noneMatch(status -> status >= 500);
                    assertThat(정상_상태).as("전제 — 뒷단까지 간 요청이 있다")
                            .anyMatch(status -> status < 300);
                })
                .inject(() -> faults.끊는다())
                .duringFault(() -> {
                    장애중_상태.addAll(여러_번_시도한다(장애중_보낼_수, 2_000));
                    // **레디스가 정말 죽었는지 확인한다.** 안 죽었으면 이 시나리오
                    // 전체가 아무것도 안 잰 것이다.
                    줄을_친_결과 = 순번을_묻는다(8_000);
                })
                .recover(() -> {
                    // **창을 여기서 연다.** 장애 구간을 같이 담으면 그 구간이
                    // 분모를 채워, 회복 순간에 몇 배가 몰아쳐야 한계를 넘는지가
                    // 장애 구간 길이에 따라 달라진다. 재려는 것은 회복이다.
                    유입.sample(지금.plusSeconds(2));
                    창이_열린_수 = 발급_보낸_수;
                    faults.붙인다();
                })
                .afterRecovery(() -> {
                    // **바로 다음 요청이 성공하기를 요구하지 않는다.** 레디스가
                    // 막 돌아온 순간은 연결이 다시 맺히는 중이다. RC3 가 재는
                    // 것은 "즉시" 가 아니라 "한계 안에" 다.
                    회복까지_걸린_시간 = 판정이_돌아올_때까지_기다린다(회복_상태, 벽시계);
                    회복_뒤_자리 = 자리들();

                    회복_순간_보낸_수 = 회복_순간을_닫는다(유입, 발급_보낸_수 - 창이_열린_수);

                    // 확인된 뒤의 정상 창. 회복이 한 번 돌아왔다가 다시 무너지는
                    // 회귀는 여기서만 보인다 — 응답도 판정에 넣는다.
                    확인_뒤_상태.addAll(여러_번_시도한다(보낼_수, 5_000));
                    회복_상태.addAll(확인_뒤_상태);
                    유입.sample(지금.plusSeconds(4));
                })
                // **진입 판정은 주입 직후, 유지 구간이 시작되기 전이다.**
                // 그 시점에는 아직 요청을 안 보냈으므로 여기서 잴 것이 없다.
                .assertEntry(ChaosScenario.Verdict.none())
                // **유지 판정이 장애가 살아 있는 동안 돈다.** 복구 뒤로 미루면
                // 이미 걷힌 상태를 읽어 전면 차단도 무제한 통과도 안 보인다.
                // **fail-open 상한은 여기서 안 잰다.** 한산한 쿠폰은 그 경로를
                // 안 거친다. 상한을 재려면 줄이 선 재료가 필요하고, 그러면
                // 전원이 202 로 끝나 RC4 를 못 잰다 — 재료가 둘 필요하다.
                .assertDuring(() -> RecoveryCriteria.violations(
                        오백이_안_샌다(장애중_상태), 레디스가_정말_죽었다()))
                // RC4 두 창은 회복을_판정한다 가 붙인다. 창 배치가 판정의
                // 전부라 흩어 놓으면 조용히 어긋난다.
                .assertRecovery(() -> 회복을_판정한다(유입, 회복_순간_보낸_수,
                        RecoveryCriteria.slowVerdictReturn(회복까지_걸린_시간, 회복_한계),
                        // RC1 — 뒷단이 받은 수가 재고를 안 넘는다. 이 시나리오는
                        // 발급만 때리므로 수신 수가 곧 발급 시도다.
                        RecoveryCriteria.overIssued(유입.total(), 재고),
                        // RC6 — 회복 뒤 유입이 정상 수준으로 돌아온다.
                        RecoveryCriteria.notConverged("판정 통과 비율",
                                통과_비율(정상_상태), 통과_비율(회복_상태)),
                        // **RC5 는 여기서 못 잰다** (CY-809). 픽스처가
                        // `--appendonly no` 로 띄우므로 컨테이너를 끊었다 붙이면
                        // 줄이 통째로 사라진다. 계획서가 요구하는 "큐 순번 전원
                        // 유지" 를 구조적으로 만족할 수 없다.
                        //
                        // 대신 **줄이 정말 사라졌는지**를 못 박는다. 나중에
                        // 영속성을 켜면 이 단언이 빨개져 그때 RC5 로 바꾼다.
                        //
                        // **RC2 도 같은 이유로 없다.** 순번 역행은 회복 전후의
                        // 순번을 이어 봐야 하는데, 줄이 통째로 사라지므로 비교할
                        // 뒤쪽이 없다. 빠뜨린 것이 아니라 못 재는 것이고, 위
                        // 단언이 빨개지는 날 RC5 와 함께 들어온다.
                        줄이_영속성_없이_사라졌다(),
                        // **중복은 짚어서 센다.** 비율은 로컬에서 끝난 요청만큼
                        // 여유가 생겨 그 안에 중복이 숨는다. 창 밖도 못 본다.
                        중복_수신이_없다(),
                        // **RC4 의 버스트 쪽은 이 하네스로 못 잰다** (CY-817).
                        // 부하 생성기가 닫힌 루프라 발신 속도가 게이트웨이
                        // 지연으로 정해진다 — 게이트웨이가 몰아쳐도 시험이 같이
                        // 빨라질 뿐 비율이 안 움직인다. 위 두 창이 실제로 재는
                        // 것은 중복 발신이고, 그건 발급 경로에서 초과 발급이라
                        // 그 자체로 지킬 값어치가 있다.
                        // **확인 뒤의 배치만 본다.** 대기 루프의 응답에는 5xx 가
                        // 섞여 있는 것이 정상이고, 그 마지막 원소는 루프의 종료
                        // 조건이라 정의상 500 미만이다 — 항진명제다. 회귀는 이
                        // 배치에서만 보인다.
                        오백이_안_샌다(확인_뒤_상태)))
                .run();
    }

    /**
     * <b>503 이 하나도 새면 안 된다.</b>
     *
     * <p>한산한 쿠폰의 판정은 레디스를 안 치므로, 레디스가 죽어도 통과해야 한다.
     * 하나라도 5xx 면 그 요청은 레디스를 기다린 것이다. "전부 5xx 인가" 만 보면
     * 한 건이라도 성공하는 순간 임의의 유출이 통과한다.
     */
    private Optional<String> 오백이_안_샌다(List<Integer> 상태) {
        if (상태.isEmpty()) {
            return Optional.of("장애 구간에서 아무것도 안 봤다 — 판정할 것이 없다");
        }
        long 샌_것 = 상태.stream().filter(s -> s >= 500).count();
        return 샌_것 > 0
                ? Optional.of("장애 구간에서 503 이 %d 건 샜다 — 판정이 레디스를 기다렸다"
                        .formatted(샌_것))
                : Optional.empty();
    }

    /**
     * 장애 구간에 줄을 치면 실패해야 한다.
     *
     * <p>성공하면 레디스가 안 죽은 것이고, 그러면 이 시나리오 전체가 아무것도
     * 안 잰 것이다. 전제를 판정으로 못 박아 둔다.
     */
    private Optional<String> 레디스가_정말_죽었다() {
        return 줄을_친_결과 < 500
                ? Optional.of("장애 구간인데 줄 조회가 %d 로 성공했다 — 레디스가 안 죽었다"
                        .formatted(줄을_친_결과))
                : Optional.empty();
    }

    /**
     * 영속성이 없는 판에서 줄이 사라진 사실을 못 박는다 (CY-809).
     *
     * <p>이 시나리오는 RC5 를 아직 못 잰다. 그 사실을 주석으로만 두면 다음
     * 사람이 "재고 있다" 고 믿는다. 영속성을 켜면 여기가 빨개져 손이 간다.
     */
    private Optional<String> 줄이_영속성_없이_사라졌다() {
        if (장애_전_자리.isEmpty()) {
            return Optional.of("장애 전에 줄이 비어 있었다 — 전제가 안 섰다");
        }
        return 회복_뒤_자리.isEmpty() ? Optional.empty()
                : Optional.of("줄이 살아남았다 — 영속성이 켜졌다면 이제 RC5 로 잰다 (CY-809)");
    }

    /** 뒷단이 같은 요청을 두 번 받았는가. 발급 경로에서 그건 초과 발급이다. */
    private Optional<String> 중복_수신이_없다() {
        long 중복 = 뒷단이_두_번_받은_수.get();
        return 중복 == 0 ? Optional.empty()
                : Optional.of("RC4 뒷단이 같은 요청을 %d 건 두 번 받았다".formatted(중복));
    }

    /** 통과 비율. RC6 이 이것으로 판정 분포의 수렴을 본다. */
    private double 통과_비율(List<Integer> 상태) {
        return 상태.isEmpty() ? 0
                : (double) 상태.stream().filter(s -> s < 300).count() / 상태.size();
    }

    /**
     * 5xx 가 그칠 때까지 눌러 보고 걸린 시간을 돌려준다.
     *
     * <p>끝내 안 그치면 {@code null} 이다 — 판정기가 그것을 위반으로 읽는다.
     *
     * @param 지금 벽시계를 주입받는다. 시험이 실시계를 직접 읽으면 장비에 따라
     *             갈리고, 그때 빨간 것이 결함인지 느린 장비인지 못 가린다
     */
    private Duration 판정이_돌아올_때까지_기다린다(List<Integer> 회복_상태, Supplier<Instant> 지금) {
        Instant 시작 = 지금.get();
        while (Duration.between(시작, 지금.get()).compareTo(회복_한계) < 0) {
            // **발급과 줄 조회를 다른 리스트로 나눈다.** RC6 은 정상 구간
            // (발급만) 과 같은 재료끼리 비교해야 한다. 한 리스트에 담고 순서로
            // 가르면 두 줄을 바꾸는 것만으로 재료가 갈린다.
            List<Integer> 발급_한_판 = 여러_번_시도한다(대기_한_판, 3_000 + 회복_상태.size());
            회복_상태.addAll(발급_한_판);
            // **줄을 치는 경로까지 돌아와야 회복이다.** 발급 경로는 레디스를
            // 안 치므로 죽은 채로도 통과한다 — 그것만 보면 복구를 안 해도
            // 회복으로 읽힌다.
            int 줄_조회 = 순번을_묻는다(9_000 + 회복_상태.size());
            // **202 를 회복으로 안 읽는다.** 정상 구간이 통과였는데 회복 뒤에
            // 줄에 서기 시작했다면 판정 분포가 아직 안 돌아온 것이다.
            if (줄_조회 < 500 && 발급_한_판.stream().noneMatch(s -> s >= 500)) {
                return Duration.between(시작, 지금.get());
            }
        }
        return null;
    }
}
