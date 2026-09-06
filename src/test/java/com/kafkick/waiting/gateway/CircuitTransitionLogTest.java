package com.kafkick.waiting.gateway;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * 서킷의 상태 전이를 <b>진입·해제 쌍으로</b> 남긴다 (LG-2).
 *
 * <p>지표는 초 단위로 뭉개져 남고 보존 기간도 짧다. 장애가 걷힌 뒤
 * "언제 열려 얼마나 오래, 몇 건을 막았는가" 는 전이 로그만 답한다.
 */
class CircuitTransitionLogTest {

    private static final String 이름 = "backend-1";

    /** half-open 이 요구하는 표본 수. 설정과 같은 값을 시험이 직접 든다. */
    private static final int 허용_프로브 = 10;

    /** 느린 호출의 경계. 서킷이 이 값 **초과**만 느림으로 센다. */
    private static final long 느림_임계_ms = 1500;

    private static final BackendCircuitProperties 설정 = new BackendCircuitProperties(
            Duration.ofSeconds(10), 20, 50f, Duration.ofMillis(느림_임계_ms), 50f,
            Duration.ofSeconds(5), Duration.ofSeconds(30), 허용_프로브);

    /**
     * 구간 시계. 고정하지 못하면 지속 시간이 시험에서 늘 0 이라 단위를 틀려도
     * 통과한다 (TS-4).
     *
     * <p>0 에서 시작하면 `지금 - 시작` 과 `지금 + 시작` 이 같다. 그래서 시각을
     * 절대값으로 지정하지 않고 늘 앞 값에서 민다.
     */
    private final AtomicLong 나노 = new AtomicLong(Duration.ofHours(3).toNanos());

    private final CircuitBreakerRegistry registry = BackendCircuit.registry(설정);

    private ListAppender<ILoggingEvent> 로그;
    private Level 원래_수준;

    private static ch.qos.logback.classic.Logger 로거() {
        return ((LoggerContext) LoggerFactory.getILoggerFactory())
                .getLogger(CircuitTransitionLog.class);
    }

    @BeforeEach
    void 로그를_받는다() {
        로그 = new ListAppender<>();
        로그.start();
        원래_수준 = 로거().getLevel();
        로거().setLevel(Level.DEBUG);
        로거().addAppender(로그);
        CircuitTransitionLog.of(나노::get).watch(registry);
    }

    @AfterEach
    void 로그를_뗀다() {
        로거().detachAppender(로그);
        로거().setLevel(원래_수준);
    }

    private List<ILoggingEvent> 남은것(String 조각) {
        return 로그.list.stream().filter(e -> e.getFormattedMessage().contains(조각)).toList();
    }

    private CircuitBreaker 서킷() {
        return registry.circuitBreaker(이름);
    }

    /**
     * <b>열린 것을 모르면 아무도 안 본다.</b> 판정이 유효 credit 을 조이면(F3)
     * 서킷에 닿는 호출이 0 이 되어, 요청 쪽 지표만으로는 열린 사실조차 안 보인다.
     */
    @Test
    @DisplayName("서킷이_열리면_경고를_남긴다")
    void 서킷이_열리면_경고를_남긴다() {
        서킷().transitionToOpenState();

        assertThat(남은것("서킷 열림")).singleElement()
                .satisfies(e -> {
                    // 자동 복구되는 전이다. ERROR 로 올리면 사람을 부르는 알람이 운다 (LG-7).
                    assertThat(e.getLevel()).isEqualTo(Level.WARN);
                    // 어느 인스턴스인지 없으면 인스턴스별로 둔 뜻이 로그에서 사라진다.
                    assertThat(e.getFormattedMessage()).contains(이름);
                });
    }

    /**
     * <b>해제 로그에 지속 시간과 영향을 담는다</b> (LG-2). 없으면 장애가 걷힌 뒤
     * "얼마나 오래, 얼마나 크게" 를 사후에 못 답한다.
     */
    @Test
    @DisplayName("서킷이_닫히면_지속_시간과_막은_건수를_남긴다")
    void 서킷이_닫히면_지속_시간과_막은_건수를_남긴다() {
        서킷().transitionToOpenState();
        서킷().tryAcquirePermission();
        서킷().tryAcquirePermission();
        나노.addAndGet(SECONDS.toNanos(42));

        서킷().transitionToClosedState();

        assertThat(남은것("서킷 닫힘")).singleElement()
                .satisfies(e -> {
                    assertThat(e.getLevel()).isEqualTo(Level.INFO);
                    // 값까지 못 박는다. 담겼는지만 보면 단위를 ms 로 틀려도 통과한다.
                    // 숫자만 담기면 21642초 도 42초 를 담는다.
                    assertThat(e.getFormattedMessage())
                            .contains("가 42초 동안 2건을 막았다");
                });
    }

    /**
     * <b>구간마다 다시 센다.</b> 누적을 그대로 실으면 두 번째 장애의 로그가 첫
     * 번째까지 합쳐 말하고, 그 숫자로 회복 규모를 판정한다.
     */
    @Test
    @DisplayName("다음_구간은_처음부터_다시_센다")
    void 다음_구간은_처음부터_다시_센다() {
        서킷().transitionToOpenState();
        서킷().tryAcquirePermission();
        서킷().transitionToClosedState();

        서킷().transitionToOpenState();
        서킷().tryAcquirePermission();
        서킷().transitionToClosedState();

        assertThat(남은것("서킷 닫힘")).hasSize(2)
                .allSatisfy(e -> assertThat(e.getFormattedMessage()).contains("1건"));
    }

    /** 프로브 구간도 남긴다. 열림과 닫힘만 보면 회복을 몇 번 시도했는지가 빈다. */
    @Test
    @DisplayName("반쯤_열린_구간도_남긴다")
    void 반쯤_열린_구간도_남긴다() {
        서킷().transitionToOpenState();

        서킷().transitionToHalfOpenState();

        assertThat(남은것("서킷 반쯤 열림")).singleElement()
                .satisfies(e -> assertThat(e.getLevel()).isEqualTo(Level.INFO));
    }

    /** 다 채우고 실패한 것과 못 채운 채 만료된 것은 고칠 자리가 다르다. */
    @Test
    @DisplayName("회복_실패에_프로브_수를_남긴다")
    void 회복_실패에_프로브_수를_남긴다() {
        서킷().transitionToOpenState();
        서킷().transitionToHalfOpenState();
        서킷().onError(1, TimeUnit.MILLISECONDS, new RuntimeException("느리다"));
        서킷().onError(1, TimeUnit.MILLISECONDS, new RuntimeException("느리다"));

        서킷().transitionToOpenState();

        assertThat(남은것("회복 시도가 실패했다")).singleElement()
                .satisfies(e -> assertThat(e.getFormattedMessage())
                        .as("몇 건 모았는지가 있어야 다 채우고 실패한 것과 만료가 갈린다")
                        .contains("probes=2"));
    }

    /** 실패만 세면 덜 찬 것처럼 보여 만료와 안 갈린다. */
    @Test
    @DisplayName("성공한_프로브도_센다")
    void 성공한_프로브도_센다() {
        서킷().transitionToOpenState();
        서킷().transitionToHalfOpenState();
        서킷().onSuccess(1, TimeUnit.MILLISECONDS);
        서킷().onError(1, TimeUnit.MILLISECONDS, new RuntimeException("느리다"));

        서킷().transitionToOpenState();

        assertThat(남은것("회복 시도가 실패했다")).singleElement()
                .satisfies(e -> assertThat(e.getFormattedMessage()).contains("probes=2"));
    }

    /** 손으로 전이를 내면 표본이 임계를 채운 경우가 한 번도 안 나온다. */
    @Test
    @DisplayName("표본이_스스로_차서_열려도_다_센다")
    void 표본이_스스로_차서_열려도_다_센다() {
        서킷().transitionToOpenState();
        서킷().transitionToHalfOpenState();
        for (int i = 0; i < 허용_프로브; i++) {
            서킷().onError(1, TimeUnit.MILLISECONDS, new RuntimeException("느리다"));
        }

        assertThat(남은것("회복 시도가 실패했다")).singleElement()
                .satisfies(e -> assertThat(e.getFormattedMessage())
                        .as("마지막 프로브의 사건이 전이보다 먼저 온다는 것에 기댄다")
                        .contains("probes=" + 허용_프로브));
    }

    /** half-open 은 실패율과 느림 비율 두 길로 실패하고, 고칠 자리가 다르다. */
    @Test
    @DisplayName("프로브를_느림과_오류로_나눠_센다")
    void 프로브를_느림과_오류로_나눠_센다() {
        서킷().transitionToOpenState();
        서킷().transitionToHalfOpenState();
        // 임계 1500ms 위아래로 하나씩. 느림은 성공이어도 느림이다.
        서킷().onSuccess(2_000, TimeUnit.MILLISECONDS);
        서킷().onSuccess(10, TimeUnit.MILLISECONDS);
        서킷().onError(10, TimeUnit.MILLISECONDS, new RuntimeException("죽었다"));

        서킷().transitionToOpenState();

        assertThat(남은것("회복 시도가 실패했다")).singleElement()
                .satisfies(e -> assertThat(e.getFormattedMessage())
                        .as("셋을 갈라야 실패한 사유가 갈린다")
                        .contains("probes=3 slow=1 errors=1 notPermitted=0"));
    }

    /** 허가가 소진돼 못 채운 것과 공급이 없어 못 채운 것은 고칠 자리가 다르다. */
    @Test
    @DisplayName("허가가_소진돼_막힌_수를_따로_센다")
    void 허가가_소진돼_막힌_수를_따로_센다() {
        서킷().transitionToOpenState();
        서킷().transitionToHalfOpenState();
        // 허가만 가져가고 완료를 안 낸다 — 프로브가 매달린 모양이다.
        for (int i = 0; i < 허용_프로브; i++) {
            서킷().tryAcquirePermission();
        }

        서킷().tryAcquirePermission();
        서킷().transitionToOpenState();

        assertThat(남은것("회복 시도가 실패했다")).singleElement()
                .satisfies(e -> assertThat(e.getFormattedMessage())
                        .as("완료가 0 이어도 허가는 다 나갔다")
                        .contains("probes=0 slow=0 errors=0 notPermitted=1"));
    }

    /** 빈 문자열로 두면 "안 실렸다" 와 "0 이었다" 가 안 갈린다. */
    @Test
    @DisplayName("구간이_없으면_0_으로_답한다")
    void 구간이_없으면_0_으로_답한다() {
        서킷().transitionToOpenState();

        서킷().transitionToForcedOpenState();

        assertThat(남은것("회복 시도가 실패했다")).singleElement()
                .satisfies(e -> assertThat(e.getFormattedMessage())
                        .contains("probes=0 slow=0 errors=0 notPermitted=0"));
    }

    /** 서킷은 초과만 느림으로 센다. 경계가 갈리면 재구성이 어긋난다. */
    @Test
    @DisplayName("임계에_정확히_걸리면_느림이_아니다")
    void 임계에_정확히_걸리면_느림이_아니다() {
        서킷().transitionToOpenState();
        서킷().transitionToHalfOpenState();
        서킷().onSuccess(느림_임계_ms, TimeUnit.MILLISECONDS);

        서킷().transitionToOpenState();

        assertThat(남은것("회복 시도가 실패했다")).singleElement()
                .satisfies(e -> assertThat(e.getFormattedMessage()).contains("slow=0"));
    }

    /** 상한이 1초 미만일 수 있다. 초로 자르면 그 회차가 0/0 으로 찍힌다. */
    @Test
    @DisplayName("1초_미만_상한도_0_이_아니다")
    void 초_미만_상한도_0_이_아니다() {
        CircuitBreakerRegistry 짧은 = BackendCircuit.registry(new BackendCircuitProperties(
                Duration.ofSeconds(10), 20, 50f, Duration.ofMillis(느림_임계_ms), 50f,
                Duration.ofSeconds(5), Duration.ofMillis(500), 허용_프로브));
        CircuitTransitionLog.of(나노::get).watch(짧은);
        CircuitBreaker 서킷 = 짧은.circuitBreaker(이름);
        서킷.transitionToOpenState();
        서킷.transitionToHalfOpenState();
        나노.addAndGet(Duration.ofMillis(300).toNanos());

        서킷.transitionToOpenState();

        assertThat(남은것("회복 시도가 실패했다")).last()
                .satisfies(e -> assertThat(e.getFormattedMessage())
                        .contains("window=300/500ms"));
    }

    /** 시간은 계수와 달리 사건 순서에 안 눕는다. */
    @Test
    @DisplayName("half_open_이_산_시간을_남긴다")
    void half_open_이_산_시간을_남긴다() {
        서킷().transitionToOpenState();
        // 두 시각이 같으면 구현이 서로 바꿔 넣어도 초록이다.
        나노.addAndGet(Duration.ofSeconds(7).toNanos());
        서킷().transitionToHalfOpenState();
        나노.addAndGet(Duration.ofSeconds(29).toNanos());

        서킷().transitionToOpenState();

        assertThat(남은것("회복 시도가 실패했다")).singleElement()
                .satisfies(e -> {
                    assertThat(e.getFormattedMessage())
                            .as("시한 근방이면 표본을 못 채운 것이다").contains("window=29000/30000ms");
                    assertThat(e.getFormattedMessage())
                            .as("열린 구간은 그보다 길다").contains("가 36초째 열려 있다");
                });
    }

    /** 다음 구간은 처음부터 센다. 안 그러면 앞 구간의 수가 다음 판단에 섞인다. */
    @Test
    @DisplayName("반쯤_열릴_때마다_프로브_수를_다시_센다")
    void 반쯤_열릴_때마다_프로브_수를_다시_센다() {
        서킷().transitionToOpenState();
        서킷().transitionToHalfOpenState();
        서킷().onError(1, TimeUnit.MILLISECONDS, new RuntimeException("느리다"));
        서킷().transitionToOpenState();
        서킷().transitionToHalfOpenState();

        서킷().transitionToOpenState();

        // 뒷 줄만 보면 구간이 없을 때의 문자열과 같아 누락과 안 갈린다.
        assertThat(남은것("회복 시도가 실패했다")).hasSize(2)
                .satisfies(둘 -> {
                    assertThat(둘.get(0).getFormattedMessage()).contains("probes=1");
                    assertThat(둘.get(1).getFormattedMessage())
                            .contains("probes=0 slow=0 errors=0 notPermitted=0");
                });
    }

    /**
     * <b>나중에 생기는 서킷도 받아야 한다.</b> 서킷은 인스턴스별이라 뒷단이 늘면
     * 이름도 는다 — 붙일 때 있던 것만 보면 새 인스턴스의 장애가 통째로 조용하다.
     */
    @Test
    @DisplayName("나중에_생긴_서킷도_따라_붙는다")
    void 나중에_생긴_서킷도_따라_붙는다() {
        registry.circuitBreaker("backend-2").transitionToOpenState();

        assertThat(남은것("backend-2")).singleElement()
                .satisfies(e -> {
                    assertThat(e.getLevel()).isEqualTo(Level.WARN);
                    assertThat(e.getFormattedMessage()).contains("서킷 열림");
                });
    }

    /**
     * <b>다시 열리는 것은 새 구간이 아닙니다.</b> 회복을 시도했다 실패한 것이므로,
     * 덮어쓰면 원래 시작 시각과 그동안 막은 건수가 사라집니다. 그러면 닫힘 로그가
     * 장애를 실제보다 짧고 가볍게 말합니다.
     */
    @Test
    @DisplayName("다시_열려도_구간의_시작과_건수를_지킨다")
    void 다시_열려도_구간의_시작과_건수를_지킨다() {
        서킷().transitionToOpenState();
        서킷().tryAcquirePermission();
        서킷().tryAcquirePermission();

        // 회복을 시도했다 실패한다. 여기서 이력이 사라지면 안 된다.
        나노.addAndGet(SECONDS.toNanos(20));
        서킷().transitionToHalfOpenState();
        서킷().transitionToOpenState();
        서킷().tryAcquirePermission();

        나노.addAndGet(SECONDS.toNanos(10));
        서킷().transitionToClosedState();

        assertThat(남은것("서킷 닫힘")).singleElement()
                .satisfies(e -> assertThat(e.getFormattedMessage())
                        // 두 번째 열림부터가 아니라 처음부터 30초, 막은 것도 셋 다.
                        .contains("가 30초 동안 3건을 막았다"));
    }

    /** 회복 시도가 실패한 사실도 남깁니다. 진동을 사후에 세려면 그 줄이 필요합니다. */
    @Test
    @DisplayName("회복_시도가_실패하면_그_사실을_남긴다")
    void 회복_시도가_실패하면_그_사실을_남긴다() {
        서킷().transitionToOpenState();

        나노.addAndGet(SECONDS.toNanos(20));
        서킷().transitionToHalfOpenState();
        서킷().transitionToOpenState();

        assertThat(남은것("회복 시도가 실패했다")).hasSize(1);
    }

    /**
     * <b>첫 열림에는 회복 실패 줄이 없어야 합니다.</b> 시도한 적이 없는 회복을
     * 실패했다고 적으면, 진동을 세는 사람이 없는 진동을 셉니다.
     */
    @Test
    @DisplayName("첫_열림에는_회복_실패를_안_남긴다")
    void 첫_열림에는_회복_실패를_안_남긴다() {
        서킷().transitionToOpenState();

        assertThat(남은것("회복 시도가 실패했다")).isEmpty();
    }
}
