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

    /** 반쯤 열린 창이 요구하는 표본 수. 설정과 같은 값을 시험이 직접 든다. */
    private static final int 허용_프로브 = 10;

    private static final BackendCircuitProperties 설정 = new BackendCircuitProperties(
            Duration.ofSeconds(10), 20, 50f, Duration.ofMillis(1500), 50f,
            Duration.ofSeconds(5), Duration.ofSeconds(30), 허용_프로브);

    /** 구간 시계. 고정하지 못하면 지속 시간이 시험에서 늘 0 이라 단위를 틀려도 통과한다 (TS-4). */
    private final AtomicLong 나노 = new AtomicLong();

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
        나노.set(SECONDS.toNanos(42));

        서킷().transitionToClosedState();

        assertThat(남은것("서킷 닫힘")).singleElement()
                .satisfies(e -> {
                    assertThat(e.getLevel()).isEqualTo(Level.INFO);
                    // 값까지 못 박는다. 담겼는지만 보면 단위를 ms 로 틀려도 통과한다.
                    assertThat(e.getFormattedMessage()).contains("42초").contains("2건");
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

    /**
     * <b>왜 다시 열렸는지가 로그에 없다.</b> 반쯤 열린 창이 표본을 다 채우고
     * 실패한 것과, 표본을 못 채운 채 시한이 만료된 것은 고칠 값이 다르다 —
     * 앞엣것은 뒷단이고 뒤엣것은 프로브 공급이다. 실측에서 그 둘을 못 갈랐다.
     */
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
                        .as("프로브를 몇 건 모았는지가 있어야 창을 태운 것과 시한 만료가 갈린다")
                        .contains("프로브 2건"));
    }

    /**
     * <b>성공한 프로브도 센다.</b> 창을 채웠는데 실패율이 임계를 넘어 열린
     * 회차에는 성공이 섞여 있다 — 실패만 세면 그 창이 덜 찬 것처럼 보이고,
     * 그러면 시한 만료와 다시 안 갈린다.
     */
    @Test
    @DisplayName("성공한_프로브도_센다")
    void 성공한_프로브도_센다() {
        서킷().transitionToOpenState();
        서킷().transitionToHalfOpenState();
        서킷().onSuccess(1, TimeUnit.MILLISECONDS);
        서킷().onError(1, TimeUnit.MILLISECONDS, new RuntimeException("느리다"));

        서킷().transitionToOpenState();

        assertThat(남은것("회복 시도가 실패했다")).singleElement()
                .satisfies(e -> assertThat(e.getFormattedMessage()).contains("프로브 2건"));
    }

    /**
     * <b>창이 스스로 차서 열리는 길을 밟는다.</b> 손으로 전이를 내면 그 창이
     * 임계를 채운 경우가 한 번도 안 나온다 — 이 로그의 뜻이 전부 "임계면 태운
     * 것, 그 아래면 시한 만료" 에 걸려 있는데 그 임계값이 시험에 없게 된다.
     */
    @Test
    @DisplayName("창이_스스로_차서_열려도_다_센다")
    void 창이_스스로_차서_열려도_다_센다() {
        서킷().transitionToOpenState();
        서킷().transitionToHalfOpenState();
        for (int i = 0; i < 허용_프로브; i++) {
            서킷().onError(1, TimeUnit.MILLISECONDS, new RuntimeException("느리다"));
        }

        assertThat(남은것("회복 시도가 실패했다")).singleElement()
                .satisfies(e -> assertThat(e.getFormattedMessage())
                        .as("마지막 프로브의 사건이 전이보다 먼저 온다는 것에 기댄다")
                        .contains("프로브 " + 허용_프로브 + "건"));
    }

    /**
     * <b>닫힌 뒤에도 창이 살아 있으면 안 된다.</b> 다만 그것은 값이 아니라
     * 비용이다 — 다음 창을 반쯤 열릴 때 새로 만들므로 옛 계수가 로그에 실릴
     * 길은 없다. 로그로는 못 보는 성질이라 여기서는 안 재고, 근거는 저널에 둔다.
     */
    /**
     * <b>창이 산 시간이 계수보다 단단하다.</b> 시한 근방이면 표본을 못 채워
     * 만료된 것이고 훨씬 짧으면 채우고 실패한 것이라, 계수 한 건이 경계에서
     * 어긋나도 이 판단은 안 눕는다.
     */
    @Test
    @DisplayName("창이_산_시간을_남긴다")
    void 창이_산_시간을_남긴다() {
        서킷().transitionToOpenState();
        서킷().transitionToHalfOpenState();
        나노.addAndGet(Duration.ofSeconds(29).toNanos());

        서킷().transitionToOpenState();

        assertThat(남은것("회복 시도가 실패했다")).singleElement()
                .satisfies(e -> assertThat(e.getFormattedMessage())
                        .as("시한 근방이면 표본을 못 채운 것이다").contains("창 29초"));
    }

    /** 다음 창은 처음부터 센다. 안 그러면 앞 창의 수가 다음 판단에 섞인다. */
    @Test
    @DisplayName("반쯤_열릴_때마다_프로브_수를_다시_센다")
    void 반쯤_열릴_때마다_프로브_수를_다시_센다() {
        서킷().transitionToOpenState();
        서킷().transitionToHalfOpenState();
        서킷().onError(1, TimeUnit.MILLISECONDS, new RuntimeException("느리다"));
        서킷().transitionToOpenState();
        서킷().transitionToHalfOpenState();

        서킷().transitionToOpenState();

        assertThat(남은것("회복 시도가 실패했다")).last()
                .satisfies(e -> assertThat(e.getFormattedMessage()).contains("프로브 0건"));
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
        나노.set(SECONDS.toNanos(20));
        서킷().transitionToHalfOpenState();
        서킷().transitionToOpenState();
        서킷().tryAcquirePermission();

        나노.set(SECONDS.toNanos(30));
        서킷().transitionToClosedState();

        assertThat(남은것("서킷 닫힘")).singleElement()
                .satisfies(e -> assertThat(e.getFormattedMessage())
                        // 두 번째 열림부터가 아니라 처음부터 30초, 막은 것도 셋 다.
                        .contains("30초").contains("3건"));
    }

    /** 회복 시도가 실패한 사실도 남깁니다. 진동을 사후에 세려면 그 줄이 필요합니다. */
    @Test
    @DisplayName("회복_시도가_실패하면_그_사실을_남긴다")
    void 회복_시도가_실패하면_그_사실을_남긴다() {
        서킷().transitionToOpenState();

        나노.set(SECONDS.toNanos(20));
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
