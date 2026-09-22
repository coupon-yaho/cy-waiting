package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * 청소가 울타리에 막힌 구간의 진입과 해제 (LG-2).
 *
 * <p><b>리더십을 잃으면 청소가 안 돈다.</b> 그 자리에서 구간을 안 닫으면 다음 임기의
 * 막힘이 같은 구간의 연장으로 삼켜져 진입 줄이 안 나간다.
 */
@Tag("unit")
class QueueSweeperFenceLogTest {

    private static final QueueSweeper.SweepResult 막힘 =
            new QueueSweeper.SweepResult(0, 0, 0, 0, 1);

    private ListAppender<ILoggingEvent> 로그;

    private Logger 로거() {
        return ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(QueueSweeper.class);
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

    private QueueSweeper 막히는_청소() {
        return 막히는_청소(System::nanoTime);
    }

    private QueueSweeper 막히는_청소(LongSupplier 시계) {
        return QueueSweeper.of(
                SweepGates.warmed(Duration.ofSeconds(1), PollIntervalPolicy.aliveTtl()),
                (ids, limit, removeFront) -> Mono.just(막힘), new SimpleMeterRegistry(), 시계);
    }

    private Map<String, CouponState> 줄이_있는_쿠폰() {
        return Map.of("c1", CouponStates.queueing(10, 1_000, 100));
    }

    private List<String> 줄들(Level 수준) {
        return 로그.list.stream()
                .filter(e -> e.getLevel() == 수준)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    @Test
    @DisplayName("리더십을_잃으면_막힌_구간을_닫는다")
    void 리더십을_잃으면_막힌_구간을_닫는다() {
        AtomicLong 시각 = new AtomicLong(1_000);
        QueueSweeper 청소 = 막히는_청소(시각::get);
        청소.run(줄이_있는_쿠폰(), false).block();
        시각.addAndGet(Duration.ofSeconds(7).toNanos());

        청소.leadershipLost();

        // 길이와 회차 수를 다른 값으로 둔다. 같으면 둘을 바꿔 넣어도 통과한다.
        assertThat(줄들(Level.INFO)).filteredOn(m -> m.contains("리더십을 잃어"))
                .singleElement().asString().as("그 구간의 길이와 회차 수를 싣는다")
                .contains("7초 동안 1회차");
    }

    /**
     * <b>다음 임기의 막힘은 새 구간이다.</b> 안 닫으면 진입 줄이 안 나가, 운영자는
     * 첫 임기의 경고 한 줄만 보고 그 뒤의 막힘을 모른다.
     */
    @Test
    @DisplayName("다시_쥔_뒤의_막힘은_진입을_다시_남긴다")
    void 다시_쥔_뒤의_막힘은_진입을_다시_남긴다() {
        QueueSweeper 청소 = 막히는_청소();
        청소.run(줄이_있는_쿠폰(), false).block();
        청소.leadershipLost();
        청소.leadershipAcquired();

        청소.run(줄이_있는_쿠폰(), false).block();

        assertThat(줄들(Level.WARN)).filteredOn(m -> m.contains("울타리에 막혔다")).hasSize(2);
    }

    /** 막힌 적이 없으면 잃어도 아무 말도 안 한다. 늘 시끄러우면 사람이 안 본다. */
    @Test
    @DisplayName("막힌_적이_없으면_잃어도_조용하다")
    void 막힌_적이_없으면_잃어도_조용하다() {
        막히는_청소().leadershipLost();

        assertThat(줄들(Level.INFO)).noneMatch(m -> m.contains("리더십을 잃어"));
    }
}
