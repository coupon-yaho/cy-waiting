package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.coupon.CouponState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
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

    /** 배수 인원의 몇 배까지 볼 것인가. 이번 틱에 들일 사람 근처만 정확하면 된다. */
    private static final long SAFETY = 2;

    /** 크레딧이 0 이어도 이만큼은 본다 — 안 그러면 멎은 쿠폰이 영영 안 걷힌다. */
    private static final int MIN_SCAN = 100;

    /** 스크립트의 `unpack` 한계보다 좁게 잡는다. */
    private static final int MAX_SCAN = 3_000;

    private final SweepGate gate;
    private final BiFunction<List<String>, Integer, Mono<SweepResult>> sweep;
    private final Counter swept;
    private final Counter expiredSignals;
    private final Counter expiredGrace;
    private final Counter failed;

    private QueueSweeper(SweepGate gate, BiFunction<List<String>, Integer, Mono<SweepResult>> sweep,
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

    public static QueueSweeper of(SweepGate gate, BiFunction<List<String>, Integer, Mono<SweepResult>> sweep,
            MeterRegistry meters) {
        return new QueueSweeper(gate, sweep, meters);
    }

    /** 계측 없이 만든다. <b>시험 편의다</b> — 운영은 위 팩토리를 쓴다. */
    public static QueueSweeper of(SweepGate gate, BiFunction<List<String>, Integer, Mono<SweepResult>> sweep) {
        return new QueueSweeper(gate, sweep, new SimpleMeterRegistry());
    }

    /**
     * 볼 인원.
     *
     * <p>가장 많이 들이는 쿠폰에 맞춘다 — 한 번에 여럿을 쓸므로 그중 가장 넓은
     * 창이 필요하다. 상수로 두면 뜨거운 쿠폰이 배수 대상 안의 유령을 못 걷는다.
     */
    private int scanLimit(Map<String, CouponState> coupons, List<String> targets) {
        long widest = targets.stream()
                .mapToLong(id -> coupons.get(id).credit())
                .max().orElse(0);
        return (int) Math.clamp(widest * SAFETY, MIN_SCAN, MAX_SCAN);
    }

    /**
     * 쓸어 낸 결과.
     *
     * <p><b>실패를 함께 싣는다.</b> 오류를 성공으로 접으면 "걷을 게 없어서 0"
     * 과 "전부 죽어서 0" 이 같은 값이 되고, 청소가 멎은 것이 정상으로 보인다.
     */
    public record SweepResult(long swept, long expiredSignals, long expiredGrace, long failed) {

        public static final SweepResult NOTHING = new SweepResult(0, 0, 0, 0);

        /** 한 쿠폰이 실패했다. */
        public static final SweepResult FAILED = new SweepResult(0, 0, 0, 1);
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
        // **이번 판에 들일 인원만큼 본다** (7.4.3). 상수로 두면 뜨거운 쿠폰은
        // 배수 대상 안의 유령을 못 걷고, 한산한 쿠폰에는 매 틱 과한 왕복을 낸다.
        return sweep.apply(targets, scanLimit(coupons, targets))
                .doOnNext(r -> {
                    swept.increment(r.swept());
                    expiredSignals.increment(r.expiredSignals());
                    expiredGrace.increment(r.expiredGrace());
                    failed.increment(r.failed());
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
