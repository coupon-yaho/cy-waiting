package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.coupon.CouponState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * 이탈자를 걷어 낸다 (7.4).
 *
 * <p><b>멈추는 판단을 필수 인자로 받는다.</b> 계획이 산문으로 적어 둔 것을
 * 기계로 만드는 자리다 — 빠뜨리면 컴파일이 안 된다.
 */
public final class QueueSweeper {

    private static final Logger log = LoggerFactory.getLogger(QueueSweeper.class);

    private final SweepGate gate;
    private final Function<List<String>, Mono<SweepResult>> sweep;
    private final Counter swept;
    private final Counter expiredSignals;
    private final Counter expiredGrace;
    private final Counter failed;

    private QueueSweeper(SweepGate gate, Function<List<String>, Mono<SweepResult>> sweep,
            MeterRegistry meters) {
        this.gate = Objects.requireNonNull(gate, "gate 는 필수다 — 멈추는 판단 없이 쓸면 안 된다");
        this.sweep = Objects.requireNonNull(sweep, "sweep 은 필수다");
        Objects.requireNonNull(meters, "meters 는 필수다");
        // **걷은 수가 곧 우리 오판일 수도 있다.** 그 값이 튈 때 장애인지 버그인지
        // 가르려면 평시 값을 먼저 알아야 하고, 재려면 자리가 있어야 한다 (7.4.6).
        this.swept = meters.counter("waiting.sweep", "kind", "swept");
        this.expiredSignals = meters.counter("waiting.sweep", "kind", "expired-signal");
        this.expiredGrace = meters.counter("waiting.sweep", "kind", "expired-grace");
        // **"걷을 게 없어서 0" 과 "전부 죽어서 0" 을 가른다.** 안 가르면 청소가
        // 멎은 것이 정상으로 보인다.
        this.failed = meters.counter("waiting.sweep", "kind", "failed");
    }

    public static QueueSweeper of(SweepGate gate, Function<List<String>, Mono<SweepResult>> sweep,
            MeterRegistry meters) {
        return new QueueSweeper(gate, sweep, meters);
    }

    /** 계측 없이 만든다. <b>시험 편의다</b> — 운영은 위 팩토리를 쓴다. */
    public static QueueSweeper of(SweepGate gate, Function<List<String>, Mono<SweepResult>> sweep) {
        return new QueueSweeper(gate, sweep, new SimpleMeterRegistry());
    }

    /** 쓸어 낸 결과. 무엇을 몇 개 걷었는지 부르는 쪽이 센다. */
    public record SweepResult(long swept, long expiredSignals, long expiredGrace) {

        public static final SweepResult NOTHING = new SweepResult(0, 0, 0);
    }

    /**
     * 이번 틱의 청소.
     *
     * <p><b>청소 실패가 배분을 막지 않는다.</b> 다음 틱에 다시 온다.
     */
    public Mono<SweepResult> run(Map<String, CouponState> coupons, boolean dataStale) {
        List<String> targets = gate.sweepable(coupons, dataStale);
        if (targets.isEmpty()) {
            return Mono.just(SweepResult.NOTHING);
        }
        return sweep.apply(targets)
                .doOnNext(r -> {
                    swept.increment(r.swept());
                    expiredSignals.increment(r.expiredSignals());
                    expiredGrace.increment(r.expiredGrace());
                    if (r.swept() > 0) {
                        // **걷은 수를 남긴다.** 이탈자와 우리 오판이 같은
                        // 수치로 보이므로, 이 값이 튀는 것이 유일한 신호다.
                        log.info("이탈자 청소 — 쿠폰 {}개에서 {}명을 걷었다",
                                targets.size(), r.swept());
                    }
                })
                .onErrorResume(e -> {
                    failed.increment();
                    log.warn("이탈자 청소 실패 — 다음 틱에 다시 한다: {}", e.toString());
                    return Mono.just(SweepResult.NOTHING);
                });
    }
}
