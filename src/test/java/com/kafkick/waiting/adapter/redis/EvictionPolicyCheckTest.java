package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * 도는 레디스의 축출 정책. <b>줄과 표가 사라져도 레디스는 오류를 안 낸다</b> — 그래서 정책을
 * 기동 뒤에 따로 봐야 하고, 못 보면 모른다고 내야 한다.
 */
@Tag("unit")
class EvictionPolicyCheckTest {

    private static final Duration 간격 = Duration.ofMinutes(5);

    private static final String 정책_키 = "maxmemory-policy";

    private final SimpleMeterRegistry 계기 = new SimpleMeterRegistry();

    private ListAppender<ILoggingEvent> 로그;

    /** 회차마다 준비한 답을 차례로 낸다. 다 쓰면 마지막 답을 되풀이한다. */
    private static final class ScriptedConfig implements Supplier<Mono<Properties>> {

        private final Deque<Mono<Properties>> 답;
        private Mono<Properties> 마지막 = Mono.empty();
        private int 읽은_수;

        private ScriptedConfig(List<Mono<Properties>> 답) {
            this.답 = new ArrayDeque<>(답);
        }

        @Override
        public Mono<Properties> get() {
            읽은_수++;
            if (!답.isEmpty()) {
                마지막 = 답.poll();
            }
            return 마지막;
        }
    }

    private Logger 로거() {
        return ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(EvictionPolicyCheck.class);
    }

    @BeforeEach
    void 로그를_받는다() {
        로그 = new ListAppender<>();
        로그.start();
        로거().addAppender(로그);
    }

    @AfterEach
    void 로그를_뗀다() {
        로거().detachAppender(로그);
    }

    /** 단독 레디스의 답 모양이다. */
    private Mono<Properties> 단독(String 정책) {
        return 노드들(Map.of(정책_키, 정책));
    }

    /** 클러스터는 노드 주소를 키 앞에 붙여 합쳐 준다. */
    private Mono<Properties> 노드들(Map<String, String> 값) {
        Properties 설정 = new Properties();
        설정.putAll(값);
        return Mono.just(설정);
    }

    private Mono<Properties> 실패() {
        return Mono.error(new IllegalStateException("연결 끊김"));
    }

    @SafeVarargs
    private EvictionPolicyCheck 점검기(Mono<Properties>... 답) {
        return EvictionPolicyCheck.of(new ScriptedConfig(List.of(답)), 간격, 계기);
    }

    private int 판정(EvictionPolicyCheck 점검) {
        return 점검.check().block();
    }

    private double 게이지() {
        return 계기.get(EvictionPolicyCheck.METRIC).gauge().value();
    }

    private long 로그_수(Level 수준) {
        return 로그.list.stream().filter(줄 -> 줄.getLevel() == 수준).count();
    }

    @Test
    @DisplayName("읽기_전에는_모른다")
    void 읽기_전에는_모른다() {
        // 붙잡아 둔다. 게이지는 약참조라 버리면 GC 뒤 NaN 이 된다.
        EvictionPolicyCheck 점검 = 점검기(단독("noeviction"));

        assertThat(게이지()).isEqualTo(EvictionPolicyCheck.UNKNOWN);
        assertThat(점검.isRunning()).isFalse();
    }

    @Test
    @DisplayName("noeviction_이면_안전하다")
    void noeviction_이면_안전하다() {
        EvictionPolicyCheck 점검 = 점검기(단독("noeviction"));

        StepVerifier.create(점검.check()).expectNext(EvictionPolicyCheck.SAFE).verifyComplete();
        assertThat(게이지()).isEqualTo(EvictionPolicyCheck.SAFE);
    }

    @Test
    @DisplayName("앞뒤_공백은_안전을_안_뒤집는다")
    void 앞뒤_공백은_안전을_안_뒤집는다() {
        assertThat(판정(점검기(단독(" noeviction\n")))).isEqualTo(EvictionPolicyCheck.SAFE);
    }

    @Test
    @DisplayName("축출_정책이면_위험하다")
    void 축출_정책이면_위험하다() {
        // volatile-* 도 위험하다. 우리 키 중 수명 있는 것(유예·매진 표)이 먼저 사라진다.
        for (String 정책 : List.of("allkeys-lru", "volatile-ttl", "allkeys-random")) {
            EvictionPolicyCheck 점검 = EvictionPolicyCheck.of(
                    () -> 단독(정책), 간격, new SimpleMeterRegistry());

            assertThat(판정(점검)).as(정책).isEqualTo(EvictionPolicyCheck.UNSAFE);
        }
    }

    @Test
    @DisplayName("클러스터는_노드_하나만_위험해도_위험하다")
    void 클러스터는_노드_하나만_위험해도_위험하다() {
        // 복제본도 센다. 승격되면 그 노드의 정책이 적용된다.
        EvictionPolicyCheck 점검 = 점검기(노드들(Map.of(
                "10.0.0.1:6379." + 정책_키, "noeviction",
                "10.0.0.2:6379." + 정책_키, "allkeys-lru",
                "10.0.0.3:6379." + 정책_키, "noeviction")));

        assertThat(판정(점검)).isEqualTo(EvictionPolicyCheck.UNSAFE);
    }

    @Test
    @DisplayName("클러스터는_전_노드가_noeviction_이면_안전하다")
    void 클러스터는_전_노드가_noeviction_이면_안전하다() {
        EvictionPolicyCheck 점검 = 점검기(노드들(Map.of(
                "10.0.0.1:6379." + 정책_키, "noeviction",
                "10.0.0.2:6379." + 정책_키, "noeviction")));

        assertThat(판정(점검)).isEqualTo(EvictionPolicyCheck.SAFE);
    }

    @Test
    @DisplayName("정책_키가_없으면_모른다")
    void 정책_키가_없으면_모른다() {
        // 비슷한 이름의 키를 정책으로 읽으면 거짓 판정이 난다.
        EvictionPolicyCheck 점검 = 점검기(노드들(Map.of(
                "maxmemory", "0", "maxmemory-policy-extra", "noeviction")));

        assertThat(판정(점검)).isEqualTo(EvictionPolicyCheck.UNKNOWN);
    }

    @Test
    @DisplayName("CONFIG_가_막히면_모른다고_낸다")
    void CONFIG_가_막히면_모른다고_낸다() {
        // 관리형 레디스는 CONFIG 를 이름을 바꾸거나 막는다. 그것이 기동을 막거나 오류로 새면 안 된다.
        EvictionPolicyCheck 점검 = 점검기(
                Mono.error(new IllegalStateException("ERR unknown command 'CONFIG'")));

        StepVerifier.create(점검.check()).expectNext(EvictionPolicyCheck.UNKNOWN).verifyComplete();
        assertThat(게이지()).isEqualTo(EvictionPolicyCheck.UNKNOWN);
    }

    @Test
    @DisplayName("빈_답도_모른다")
    void 빈_답도_모른다() {
        assertThat(판정(점검기(Mono.empty()))).isEqualTo(EvictionPolicyCheck.UNKNOWN);
    }

    @Test
    @DisplayName("한_번_본_답은_순간_실패로_안_지운다")
    void 한_번_본_답은_순간_실패로_안_지운다() {
        // 레디스가 잠깐 끊겨도 정책은 안 바뀐다. 모름으로 덮으면 위험 알람이 그 구간 꺼진다.
        EvictionPolicyCheck 점검 = 점검기(단독("allkeys-lru"), 실패());

        assertThat(판정(점검)).isEqualTo(EvictionPolicyCheck.UNSAFE);
        assertThat(판정(점검)).isEqualTo(EvictionPolicyCheck.UNSAFE);
        assertThat(게이지()).isEqualTo(EvictionPolicyCheck.UNSAFE);
    }

    @Test
    @DisplayName("연달아_못_읽으면_본_답을_버린다")
    void 연달아_못_읽으면_본_답을_버린다() {
        // 읽기가 막힌 뒤 정책이 바뀌어도 게이지가 옛 안전을 영영 들고 있으면 안 된다.
        EvictionPolicyCheck 점검 = 점검기(단독("noeviction"), 실패());
        판정(점검);
        for (int i = 1; i < EvictionPolicyCheck.STALE_AFTER_FAILURES; i++) {
            assertThat(판정(점검)).as("%d 번째 실패까지는 본 답", i).isEqualTo(EvictionPolicyCheck.SAFE);
        }

        assertThat(판정(점검)).isEqualTo(EvictionPolicyCheck.UNKNOWN);
        assertThat(게이지()).isEqualTo(EvictionPolicyCheck.UNKNOWN);
    }

    @Test
    @DisplayName("연달아_못_읽으면_위험도_버린다")
    void 연달아_못_읽으면_위험도_버린다() {
        // 알람이 풀리는 쪽이다. 그래도 옛 위험이 새 안전을 가리는 것보다 낫다.
        EvictionPolicyCheck 점검 = 점검기(단독("allkeys-lru"), 실패());
        판정(점검);
        for (int i = 1; i < EvictionPolicyCheck.STALE_AFTER_FAILURES; i++) {
            assertThat(판정(점검)).isEqualTo(EvictionPolicyCheck.UNSAFE);
        }

        assertThat(판정(점검)).isEqualTo(EvictionPolicyCheck.UNKNOWN);
    }

    @Test
    @DisplayName("띄엄띄엄_난_실패는_쌓이지_않는다")
    void 띄엄띄엄_난_실패는_쌓이지_않는다() {
        // 성공이 실패 수를 안 되돌리면 정상인 레디스에서도 게이지가 모름으로 떨어진다.
        EvictionPolicyCheck 점검 = 점검기(
                단독("noeviction"), 실패(), 실패(), 단독("noeviction"), 실패(), 실패());
        for (int i = 0; i < 6; i++) {
            판정(점검);
        }

        assertThat(게이지()).isEqualTo(EvictionPolicyCheck.SAFE);
    }

    @Test
    @DisplayName("답이_안_오면_시한에서_끊는다")
    void 답이_안_오면_시한에서_끊는다() {
        // 끝나지 않는 회차는 repeatWhen 을 영영 세운다. 전역 명령 시한에 기대지 않는다.
        StepVerifier.withVirtualTime(() -> 점검기(Mono.never()).check())
                .expectSubscription()
                .expectNoEvent(EvictionPolicyCheck.READ_TIMEOUT.minusMillis(1))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(EvictionPolicyCheck.UNKNOWN)
                .expectComplete()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("위험은_진입과_해제를_한_번씩_남긴다")
    void 위험은_진입과_해제를_한_번씩_남긴다() {
        EvictionPolicyCheck 점검 = 점검기(
                단독("noeviction"), 단독("allkeys-lru"), 단독("allkeys-lru"), 단독("noeviction"),
                단독("noeviction"), 단독("allkeys-lru"), 단독("noeviction"));
        for (int i = 0; i < 7; i++) {
            판정(점검);
        }

        // 사람이 고쳐야 풀리는 상태라 ERROR 다. 5분마다 되풀이하면 그 줄이 묻힌다. 구간마다 한 번이다.
        assertThat(로그_수(Level.ERROR)).isEqualTo(2);
        assertThat(로그.list).filteredOn(줄 -> 줄.getLevel() == Level.ERROR)
                .allMatch(줄 -> 줄.getFormattedMessage().contains("policy=allkeys-lru"));
        assertThat(로그.list).filteredOn(줄 -> 줄.getFormattedMessage().contains("돌아왔다"))
                .hasSize(2).allMatch(줄 -> 줄.getLevel() == Level.INFO);
    }

    @Test
    @DisplayName("못_읽은_구간은_구간마다_첫_건과_해제를_남긴다")
    void 못_읽은_구간은_구간마다_첫_건과_해제를_남긴다() {
        EvictionPolicyCheck 점검 = 점검기(실패(), Mono.empty(), 단독("noeviction"), Mono.empty());
        for (int i = 0; i < 4; i++) {
            판정(점검);
        }

        // 둘째 구간도 남아야 한다. 해제가 자물쇠를 안 풀면 그 구간은 조용하다.
        assertThat(로그_수(Level.WARN)).isEqualTo(2);
        assertThat(로그.list).filteredOn(줄 -> 줄.getLevel() == Level.WARN)
                .allMatch(줄 -> 줄.getThrowableProxy() != null);
        assertThat(로그.list).filteredOn(줄 -> 줄.getFormattedMessage().contains("다시 읽었다"))
                .hasSize(1).allMatch(줄 -> 줄.getLevel() == Level.INFO);
    }

    @Test
    @DisplayName("돌면서_바뀐_정책을_잡는다")
    void 돌면서_바뀐_정책을_잡는다() {
        // 기동 뒤 CONFIG SET 으로 바뀌는 것이 이 루프가 있는 이유다. 오류가 끼어도 루프는 산다.
        VirtualTimeScheduler 시계 = VirtualTimeScheduler.create();
        ScriptedConfig 설정 = new ScriptedConfig(List.of(단독("noeviction"), 실패(), 단독("allkeys-lru")));
        EvictionPolicyCheck 점검 = EvictionPolicyCheck.of(설정, 간격, 계기);
        try {
            점검.start(시계);
            assertThat(게이지()).as("기동 즉시 한 번 본다").isEqualTo(EvictionPolicyCheck.SAFE);

            시계.advanceTimeBy(간격);
            assertThat(게이지()).as("실패 회차").isEqualTo(EvictionPolicyCheck.SAFE);

            시계.advanceTimeBy(간격);
            assertThat(게이지()).isEqualTo(EvictionPolicyCheck.UNSAFE);
            assertThat(설정.읽은_수).isEqualTo(3);
        } finally {
            점검.stop();
        }
        assertThat(점검.isRunning()).isFalse();
    }

    @Test
    @DisplayName("간격_전에는_다시_안_읽는다")
    void 간격_전에는_다시_안_읽는다() {
        VirtualTimeScheduler 시계 = VirtualTimeScheduler.create();
        ScriptedConfig 설정 = new ScriptedConfig(List.of(단독("noeviction")));
        EvictionPolicyCheck 점검 = EvictionPolicyCheck.of(설정, 간격, 계기);
        try {
            점검.start(시계);
            시계.advanceTimeBy(간격.minusSeconds(1));

            assertThat(설정.읽은_수).isEqualTo(1);
        } finally {
            점검.stop();
        }
    }

    @Test
    @DisplayName("운영_기동은_두_번_불러도_한_번만_돌고_멈춘_뒤_다시_선다")
    void 운영_기동은_두_번_불러도_한_번만_돌고_멈춘_뒤_다시_선다() {
        EvictionPolicyCheck 점검 = 점검기(단독("noeviction"));
        try {
            점검.start();
            점검.start();
            assertThat(점검.isRunning()).isTrue();

            점검.stop();
            assertThat(점검.isRunning()).isFalse();

            점검.start();
            assertThat(점검.isRunning()).as("멈춘 뒤 다시 켤 수 있다").isTrue();
        } finally {
            점검.stop();
        }
        assertThat(점검.isRunning()).isFalse();
    }
}
