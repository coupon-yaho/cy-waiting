package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
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

    private final SimpleMeterRegistry 계기 = new SimpleMeterRegistry();

    /** 회차마다 준비한 답을 차례로 낸다. 다 쓰면 마지막 답을 되풀이한다. */
    private static final class ScriptedPolicy implements Supplier<Mono<String>> {

        private final Deque<Mono<String>> 답;
        private Mono<String> 마지막 = Mono.empty();
        private int 읽은_수;

        private ScriptedPolicy(List<Mono<String>> 답) {
            this.답 = new ArrayDeque<>(답);
        }

        @Override
        public Mono<String> get() {
            읽은_수++;
            if (!답.isEmpty()) {
                마지막 = 답.poll();
            }
            return 마지막;
        }
    }

    @SafeVarargs
    private EvictionPolicyCheck 점검기(Mono<String>... 답) {
        return EvictionPolicyCheck.of(new ScriptedPolicy(List.of(답)), 간격, 계기);
    }

    private double 게이지() {
        return 계기.get(EvictionPolicyCheck.METRIC).gauge().value();
    }

    @Test
    @DisplayName("읽기_전에는_모른다")
    void 읽기_전에는_모른다() {
        점검기(Mono.just("noeviction"));

        assertThat(게이지()).isEqualTo(EvictionPolicyCheck.UNKNOWN);
    }

    @Test
    @DisplayName("noeviction_이면_안전하다")
    void noeviction_이면_안전하다() {
        EvictionPolicyCheck 점검 = 점검기(Mono.just("noeviction"));

        StepVerifier.create(점검.check()).expectNext(EvictionPolicyCheck.SAFE).verifyComplete();
        assertThat(게이지()).isEqualTo(EvictionPolicyCheck.SAFE);
    }

    @Test
    @DisplayName("축출_정책이면_위험하다")
    void 축출_정책이면_위험하다() {
        // volatile-* 도 위험하다. 우리 키 중 수명 있는 것(유예·매진 표)이 먼저 사라진다.
        for (String 정책 : List.of("allkeys-lru", "volatile-ttl", "allkeys-random")) {
            EvictionPolicyCheck 점검 = EvictionPolicyCheck.of(
                    () -> Mono.just(정책), 간격, new SimpleMeterRegistry());

            StepVerifier.create(점검.check())
                    .as(정책).expectNext(EvictionPolicyCheck.UNSAFE).verifyComplete();
        }
    }

    @Test
    @DisplayName("CONFIG_가_막히면_모른다고_낸다")
    void CONFIG_가_막히면_모른다고_낸다() {
        // 관리형 레디스는 CONFIG 를 이름을 바꾸거나 막는다. 그것이 기동을 막거나 오류로 새면 안 된다.
        EvictionPolicyCheck 점검 = 점검기(Mono.error(new IllegalStateException("ERR unknown command 'CONFIG'")));

        StepVerifier.create(점검.check()).expectNext(EvictionPolicyCheck.UNKNOWN).verifyComplete();
        assertThat(게이지()).isEqualTo(EvictionPolicyCheck.UNKNOWN);
    }

    @Test
    @DisplayName("빈_답도_모른다")
    void 빈_답도_모른다() {
        EvictionPolicyCheck 점검 = 점검기(Mono.empty());

        StepVerifier.create(점검.check()).expectNext(EvictionPolicyCheck.UNKNOWN).verifyComplete();
    }

    @Test
    @DisplayName("한_번_본_답은_읽기_실패로_안_지운다")
    void 한_번_본_답은_읽기_실패로_안_지운다() {
        // 레디스가 잠깐 끊겨도 정책은 안 바뀐다. 모름으로 덮으면 위험 알람이 그 구간 꺼진다.
        EvictionPolicyCheck 점검 = 점검기(
                Mono.just("allkeys-lru"), Mono.error(new IllegalStateException("연결 끊김")));

        StepVerifier.create(점검.check()).expectNext(EvictionPolicyCheck.UNSAFE).verifyComplete();
        StepVerifier.create(점검.check()).expectNext(EvictionPolicyCheck.UNSAFE).verifyComplete();
        assertThat(게이지()).isEqualTo(EvictionPolicyCheck.UNSAFE);
    }

    @Test
    @DisplayName("돌면서_바뀐_정책을_잡는다")
    void 돌면서_바뀐_정책을_잡는다() {
        // 기동 뒤 CONFIG SET 으로 바뀌는 것이 이 루프가 있는 이유다. 오류가 끼어도 루프는 산다.
        VirtualTimeScheduler 시계 = VirtualTimeScheduler.create();
        ScriptedPolicy 정책 = new ScriptedPolicy(List.of(
                Mono.just("noeviction"),
                Mono.error(new IllegalStateException("연결 끊김")),
                Mono.just("allkeys-lru")));
        EvictionPolicyCheck 점검 = EvictionPolicyCheck.of(정책, 간격, 계기);
        try {
            점검.start(시계);
            assertThat(게이지()).as("기동 즉시 한 번 본다").isEqualTo(EvictionPolicyCheck.SAFE);

            시계.advanceTimeBy(간격);
            assertThat(게이지()).as("실패 회차").isEqualTo(EvictionPolicyCheck.SAFE);

            시계.advanceTimeBy(간격);
            assertThat(게이지()).isEqualTo(EvictionPolicyCheck.UNSAFE);
            assertThat(정책.읽은_수).isEqualTo(3);
        } finally {
            점검.stop();
        }
        assertThat(점검.isRunning()).isFalse();
    }

    @Test
    @DisplayName("간격_전에는_다시_안_읽는다")
    void 간격_전에는_다시_안_읽는다() {
        VirtualTimeScheduler 시계 = VirtualTimeScheduler.create();
        ScriptedPolicy 정책 = new ScriptedPolicy(List.of(Mono.just("noeviction")));
        EvictionPolicyCheck 점검 = EvictionPolicyCheck.of(정책, 간격, 계기);
        try {
            점검.start(시계);
            시계.advanceTimeBy(간격.minusSeconds(1));

            assertThat(정책.읽은_수).isEqualTo(1);
        } finally {
            점검.stop();
        }
    }
}
