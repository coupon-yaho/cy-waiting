package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * 상태 전이를 <b>진입·해제 쌍으로</b> 남긴다 (LG-2).
 *
 * <p>안 남기면 크레딧이 하한에 박히거나 감쇠로 깎인 것을 사람이 게이지를 보고
 * 있어야만 안다. 해제 로그에 지속 시간이 없으면 그 구간의 길이도 못 잰다.
 */
class CapacityRefreshLogTest {

    private static final long NOW = 1_800_000_000L;
    private static final long 하한 = 10;

    private ListAppender<ILoggingEvent> 로그;
    private Level 원래_수준;

    @BeforeEach
    void 로그를_받는다() {
        로그 = new ListAppender<>();
        로그.start();
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CapacityRefresh.class);
        원래_수준 = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(로그);
    }

    @AfterEach
    void 로그를_뗀다() {
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CapacityRefresh.class);
        logger.detachAppender(로그);
        logger.setLevel(원래_수준);
    }

    private List<String> 메시지() {
        return 로그.list.stream().map(ILoggingEvent::getMessage).toList();
    }

    /** 그 전이가 <b>몇 번</b> 남았는지. 있기만 하면 통과하는 단언은 반복을 못 본다. */
    private long 남은_횟수(String 조각) {
        return 메시지().stream().filter(m -> m.contains(조각)).count();
    }

    /** 몇 번째 줄에 남았는지. 해제가 진입보다 앞서면 쌍이 아니다. */
    private int 자리(String 조각) {
        List<String> all = 메시지();
        for (int i = 0; i < all.size(); i++) {
            if (all.get(i).contains(조각)) {
                return i;
            }
        }
        return -1;
    }

    private static final Duration 램프 = Duration.ofSeconds(60);

    private CapacityCollector collector() {
        return CapacityCollector.of(램프, Duration.ofSeconds(3), 하한, 100_000);
    }

    private CapacityRefresh refresh(CapacityCollector collector,
            AtomicBoolean 실패, AtomicReference<List<CapacityReport>> 보고) {
        return CapacityRefresh.of(
                () -> 실패.get()
                        ? Mono.error(new IllegalStateException("못 읽는다"))
                        : Mono.just(new CapacitySample(보고.get(), NOW)),
                collector, () -> 1, Duration.ofMillis(50),
                Schedulers.immediate(), new SimpleMeterRegistry());
    }

    @Test
    @DisplayName("하한에_박히면_진입과_해제를_남긴다")
    void 하한에_박히면_진입과_해제를_남긴다() {
        CapacityCollector collector = collector();
        AtomicBoolean 실패 = new AtomicBoolean();
        // **같은 인스턴스로 이어 돈다.** 새로 만들면 창이 달라져 해제가 안 남는다.
        var 시각 = new AtomicLong(NOW);
        var 보고 = new AtomicReference<List<CapacityReport>>(List.of());
        CapacityRefresh refresh = CapacityRefresh.of(
                () -> Mono.just(new CapacitySample(보고.get(), 시각.get())),
                collector, () -> 1, Duration.ofMillis(50),
                Schedulers.immediate(), new SimpleMeterRegistry());
        refresh.refresh().block();

        assertThat(남은_횟수("신선한 가용량 보고가 없다")).isOne();

        // 보고가 돌아오고 램프가 끝나면 해제가 남는다. 진입만 남기면 구간의 끝을 모른다.
        보고.set(List.of(new CapacityReport("i1", 500, NOW)));
        refresh.refresh().block();
        long 램프_뒤 = NOW + 램프.toSeconds();
        시각.set(램프_뒤);
        보고.set(List.of(new CapacityReport("i1", 500, 램프_뒤)));
        refresh.refresh().block();

        assertThat(남은_횟수("가용량 보고가 다시 온다")).isOne();
        // 해제는 진입 뒤다. 순서가 뒤집히면 쌍이 아니라 두 개의 낱말이다.
        assertThat(자리("가용량 보고가 다시 온다"))
                .isGreaterThan(자리("신선한 가용량 보고가 없다"));
    }

    @Test
    @DisplayName("못_읽으면_진입을_한_번만_남긴다")
    void 못_읽으면_진입을_한_번만_남긴다() {
        CapacityCollector collector = collector();
        AtomicBoolean 실패 = new AtomicBoolean(true);
        CapacityRefresh refresh = refresh(collector, 실패,
                new AtomicReference<>(List.of()));

        refresh.refresh().block();
        refresh.refresh().block();
        refresh.refresh().block();

        // 구간으로 오는 장애라 요청마다 찍으면 초당 수천 줄이 쌓인다 (LG-3).
        assertThat(메시지().stream().filter(m -> m.contains("가용량을 못 읽는다")).count())
                .isOne();
    }

    @Test
    @DisplayName("감쇠가_시작되면_얼마나_깎였는지_남긴다")
    void 감쇠가_시작되면_얼마나_깎였는지_남긴다() {
        CapacityCollector collector = collector();
        AtomicBoolean 실패 = new AtomicBoolean();
        CapacityRefresh refresh = refresh(collector, 실패,
                new AtomicReference<>(
                        List.of(new CapacityReport("i1", 10_000, NOW))));
        refresh.refresh().block();

        실패.set(true);
        for (int i = 0; i <= CapacityCollector.HOLD_ROUNDS; i++) {
            refresh.refresh().block();
        }

        assertThat(남은_횟수("크레딧을 깎기 시작한다")).isOne();
    }

    /**
     * <b>지표가 배분값을 따라가야 한다.</b> 성공 판에서만 갱신하면 감쇠가 도는
     * 동안 지표는 장애 직전 값에 얼어 있고, 회복 판정이 "아무 일도 없었다" 로
     * 자동 통과한다 (RC6).
     */
    @Test
    @DisplayName("감쇠한_값이_지표에_실린다")
    void 감쇠한_값이_지표에_실린다() {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        CapacityCollector collector = collector();
        AtomicBoolean 실패 = new AtomicBoolean();
        CapacityRefresh refresh = CapacityRefresh.of(
                () -> 실패.get()
                        ? Mono.error(new IllegalStateException("못 읽는다"))
                        : Mono.just(new CapacitySample(
                                List.of(new CapacityReport("i1", 10_000, NOW)), NOW)),
                collector, () -> 1, Duration.ofMillis(50), Schedulers.immediate(), meters);
        refresh.refresh().block();
        double 정상 = meters.get("waiting.capacity.credit").gauge().value();

        실패.set(true);
        for (int i = 0; i <= CapacityCollector.HOLD_ROUNDS; i++) {
            refresh.refresh().block();
        }

        assertThat(meters.get("waiting.capacity.credit").gauge().value())
                .isEqualTo(collector.lastKnown())
                .isLessThan(정상);
    }
}
