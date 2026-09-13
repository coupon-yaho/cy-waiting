package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * 승계 첫 회차를 앞 리더의 마지막 발행에서 떨어뜨린다 (CY-928).
 *
 * <p><b>정상 인계는 틈이 짧다.</b> 옛 리더가 락을 놓으면 새 리더가 수백 ms 안에 잡고 곧 첫
 * 회차를 돈다. 두 회차가 한 틱 몫씩 들이므로 그 1초에 뒷단 유입이 두 배가 된다.
 */
class HandoverSpacingTest {

    private static final Duration 틱 = Duration.ofSeconds(1);

    /** 발행 시각이 초 단위로 실려 더하는 여유. 대기의 상한은 이것과 틱의 합이다. */
    private static final Duration 상한 = Duration.ofSeconds(2);

    private final AtomicLong 나노 = new AtomicLong(1_000_000_000L);

    private final HandoverSpacing 간격 = HandoverSpacing.of(나노::get, 틱);

    private ListAppender<ILoggingEvent> 로그;

    private Logger 로거() {
        return (Logger) LoggerFactory.getLogger(HandoverSpacing.class);
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

    private void 흘린다(Duration 만큼) {
        나노.addAndGet(만큼.toNanos());
    }

    private List<String> 정보_로그() {
        return 로그.list.stream().filter(e -> e.getLevel() == Level.INFO)
                .map(ILoggingEvent::getFormattedMessage).toList();
    }

    @Test
    @DisplayName("리더가_된_적_없으면_막지_않는다")
    void 리더가_된_적_없으면_막지_않는다() {
        assertThat(간격.getAsBoolean()).isTrue();
    }

    /**
     * <b>발행의 나이로 남은 대기를 구한다.</b> 벽시계로 빼면 발행 시각(레디스 시계)과 이 노드
     * 시계의 차이만큼 대기가 늘거나 0 이 된다. 나이는 홀더가 레디스 시계로 재 둔 값이다.
     */
    @Test
    @DisplayName("갓_난_발행이면_한_초와_한_틱에서_나이를_뺀_만큼_쉰다")
    void 갓_난_발행이면_한_초와_한_틱에서_나이를_뺀_만큼_쉰다() {
        간격.armedFrom(Duration.ofMillis(400));

        assertThat(간격.getAsBoolean()).as("인계 직후").isFalse();
        흘린다(Duration.ofMillis(1_599));
        assertThat(간격.getAsBoolean()).as("남은 대기 1,600ms 직전").isFalse();
        흘린다(Duration.ofMillis(1));
        assertThat(간격.getAsBoolean()).isTrue();
    }

    /** 리더가 죽은 승계는 마지막 발행이 이미 리스 넘게 지났다. 기다리면 승계만 늦어진다. */
    @Test
    @DisplayName("오래된_발행이면_기다리지_않는다")
    void 오래된_발행이면_기다리지_않는다() {
        간격.armedFrom(상한);

        assertThat(간격.getAsBoolean()).isTrue();
        assertThat(정보_로그()).as("안 쉬면 쉰다고 안 적는다").isEmpty();
    }

    /** 나이를 모르면(발행을 본 적 없거나 시계가 갈림) 견줄 것이 없다. */
    @Test
    @DisplayName("나이를_모르면_기다리지_않는다")
    void 나이를_모르면_기다리지_않는다() {
        간격.armedFrom(null);

        assertThat(간격.getAsBoolean()).isTrue();
    }

    /** <b>상한을 넘겨 쉬지 않는다.</b> 나이가 음수로 틀어져도 대기는 한 초와 한 틱까지다. */
    @Test
    @DisplayName("나이가_틀어져도_상한까지만_쉰다")
    void 나이가_틀어져도_상한까지만_쉰다() {
        간격.armedFrom(Duration.ofSeconds(-5));

        흘린다(상한.minusMillis(1));
        assertThat(간격.getAsBoolean()).isFalse();
        흘린다(Duration.ofMillis(1));
        assertThat(간격.getAsBoolean()).isTrue();
    }

    /** 틱이 1초가 아니어도 한 초와 한 틱이다. 올림과 틱을 서로 바꿔 더하는 구현을 거른다. */
    @Test
    @DisplayName("틱이_짧아도_한_초와_한_틱을_쉰다")
    void 틱이_짧아도_한_초와_한_틱을_쉰다() {
        HandoverSpacing 짧은_틱 = HandoverSpacing.of(나노::get, Duration.ofMillis(250));
        짧은_틱.armedFrom(Duration.ZERO);

        흘린다(Duration.ofMillis(1_249));
        assertThat(짧은_틱.getAsBoolean()).isFalse();
        흘린다(Duration.ofMillis(1));
        assertThat(짧은_틱.getAsBoolean()).isTrue();
    }

    /** 다시 걸면 앞 시한을 버린다. 모르는 나이로 다시 걸었는데 앞 대기가 남으면 승계가 밀린다. */
    @Test
    @DisplayName("다시_걸면_앞_시한을_버린다")
    void 다시_걸면_앞_시한을_버린다() {
        간격.armedFrom(Duration.ZERO);
        간격.armedFrom(null);

        assertThat(간격.getAsBoolean()).isTrue();
    }

    /** 쉬는 구간의 진입과 해제를 쌍으로 남긴다. 안 남기면 리더가 됐는데 발행이 없는 이유를 모른다. */
    @Test
    @DisplayName("쉬는_구간의_진입과_해제를_남긴다")
    void 쉬는_구간의_진입과_해제를_남긴다() {
        간격.armedFrom(Duration.ofMillis(400));
        흘린다(Duration.ofMillis(1_600));
        간격.getAsBoolean();
        간격.getAsBoolean();

        assertThat(정보_로그()).hasSize(2);
        assertThat(정보_로그().get(0)).contains("1600ms");
        assertThat(정보_로그().get(1)).contains("대기 끝");
    }
}
