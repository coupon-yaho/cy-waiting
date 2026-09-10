package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.coupon.CouponState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * 이탈자를 걷어 낸다. <b>멈추는 판단을 필수 인자로 받는다</b> — 계획이 산문으로
 * 적어 둔 것을 기계로 만드는 자리라, 빠뜨리면 컴파일이 안 된다.
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
    private final SweepCall sweep;

    /** 청소 한 회차. 앞줄 제거 여부까지 받아야 유예 구간에도 정리가 돈다. */
    @FunctionalInterface
    public interface SweepCall {
        Mono<SweepResult> apply(List<String> couponIds, int scanLimit, boolean removeFront);
    }
    private final Counter swept;
    private final Counter expiredSignals;
    private final Counter expiredGrace;
    private final Counter failed;

    /** 울타리가 막은 쿠폰 수. 이 값만 오르면 청소가 멎은 것이지 걷을 게 없는 것이 아니다. */
    private final Counter fenced;

    /** 막힌 구간. 진입과 해제를 쌍으로 남겨 얼마나 오래 멎었는지를 사후에 잰다. */
    private final FailureWindow fenceWindow = FailureWindow.create();

    private QueueSweeper(SweepGate gate, SweepCall sweep,
            MeterRegistry meters) {
        this.gate = Objects.requireNonNull(gate, "gate 는 필수다 — 멈추는 판단 없이 쓸면 안 된다");
        this.sweep = Objects.requireNonNull(sweep, "sweep 은 필수다");
        Objects.requireNonNull(meters, "meters 는 필수다");
        // **걷은 수가 곧 우리 오판일 수도 있다.** 그 값이 튈 때 장애인지 버그인지
        // 가르려면 평시 값을 먼저 알아야 하고, 재려면 자리가 있어야 한다.
        this.swept = meters.counter("waiting.sweep", "kind", "swept");
        this.expiredSignals = meters.counter("waiting.sweep", "kind", "expired-signal");
        this.expiredGrace = meters.counter("waiting.sweep", "kind", "expired-grace");
        // **"걷을 게 없어서 0" 과 "전부 죽어서 0" 을 가른다.** 안 가르면 청소가
        // 멎은 것이 정상으로 보인다.
        this.failed = meters.counter("waiting.sweep", "kind", "failed");
        this.fenced = meters.counter("waiting.sweep", "kind", "fenced");
    }

    public static QueueSweeper of(SweepGate gate, SweepCall sweep,
            MeterRegistry meters) {
        return new QueueSweeper(gate, sweep, meters);
    }

    /** 계측 없이 만든다. <b>시험 편의다</b> — 운영은 위 팩토리를 쓴다. */
    public static QueueSweeper of(SweepGate gate, SweepCall sweep) {
        return new QueueSweeper(gate, sweep, new SimpleMeterRegistry());
    }

    /**
     * 볼 인원. 한 번에 여럿을 쓸므로 가장 많이 들이는 쿠폰에 맞춘다 — 상수로 두면 뜨거운
     * 쿠폰이 배수 대상 안의 유령을 못 걷는다.
     */
    private int scanLimit(Map<String, CouponState> coupons, List<String> targets) {
        long widest = targets.stream()
                .mapToLong(id -> coupons.get(id).credit())
                .max().orElse(0);
        return (int) Math.clamp(widest * SAFETY, MIN_SCAN, MAX_SCAN);
    }

    /**
     * 쓸어 낸 결과. <b>0 의 뜻이 셋이다</b> — 걷을 게 없어서, 전부 죽어서, 울타리가
     * 앞줄 제거를 막아서다. 안 가르면 청소가 멎은 것이 정상으로 보인다.
     *
     * @param fenced 울타리가 앞줄 제거를 막은 <b>쿠폰 수</b>. 회차 수가 아니다
     */
    public record SweepResult(long swept, long expiredSignals, long expiredGrace, long failed,
            long fenced) {

        public static final SweepResult NOTHING = new SweepResult(0, 0, 0, 0, 0);

        /** 한 쿠폰이 실패했다. */
        public static final SweepResult FAILED = new SweepResult(0, 0, 0, 1, 0);
    }

    /**
     * 리더가 되면 <b>재개 유예를 처음부터 준다.</b> 재개 표시는 리더 메모리라
     * 승계에서 사라져, 새 리더는 그 쿠폰의 생존 신호가 얼마나 오래 멎었는지 모른다.
     */
    public void leadershipAcquired() {
        gate.leadershipAcquired();
    }

    /**
     * 울타리에 막힌 구간의 진입과 해제를 남긴다. <b>틱마다 찍으면 안 된다</b> —
     * 유령 구간은 임기가 돌아올 때까지 이어져 매 틱 같은 줄이 쌓인다.
     */
    private void watchFence(long blocked) {
        if (blocked > 0) {
            if (fenceWindow.entered()) {
                log.warn("이탈자 청소가 울타리에 막혔다 — 쿠폰 {}개. 앞줄 제거만 멎고 "
                        + "정리는 돈다. 임기와 적용 울타리를 함께 본다", blocked);
            }
            return;
        }
        fenceWindow.exited().ifPresent(r -> log.info(
                "이탈자 청소의 울타리가 풀렸다 — {}초 동안 {}회차", r.elapsedSeconds(),
                r.swallowed()));
    }

    /** 이번 틱의 청소. <b>청소 실패가 배분을 막지 않는다</b> — 다음 틱에 다시 온다. */
    public Mono<SweepResult> run(Map<String, CouponState> coupons, boolean dataStale) {
        List<String> targets = gate.sweepable(coupons, dataStale);
        // **승계 유예 중에도 정리는 돈다.** 앞줄 제거만 접는다 —
        // 대상까지 비우면 만료 신호와 유예 기록이 한 방향으로만 자라고 커서가
        // 전진을 못 한다. 승계가 유예보다 잦으면 청소가 영영 안 돈다.
        boolean removeFront = !targets.isEmpty();
        if (!removeFront) {
            targets = gate.removalHeld() ? gate.cleanable(coupons) : List.of();
        }
        if (targets.isEmpty()) {
            return Mono.just(SweepResult.NOTHING);
        }
        List<String> chosen = targets;
        // **이번 회차에 들일 인원만큼 본다.** 상수로 두면 뜨거운 쿠폰은
        // 배수 대상 안의 유령을 못 걷고, 한산한 쿠폰에는 매 틱 과한 왕복을 낸다.
        return sweep.apply(chosen, scanLimit(coupons, chosen), removeFront)
                .doOnNext(r -> {
                    swept.increment(r.swept());
                    expiredSignals.increment(r.expiredSignals());
                    expiredGrace.increment(r.expiredGrace());
                    failed.increment(r.failed());
                    fenced.increment(r.fenced());
                    watchFence(r.fenced());
                    if (r.swept() > 0) {
                        // **걷은 수를 남긴다.** 이탈자와 우리 오판이 같은
                        // 수치로 보이므로, 이 값이 튀는 것이 유일한 신호다.
                        log.info("이탈자 청소 — 쿠폰 {}개에서 {}명을 걷었다",
                                chosen.size(), r.swept());
                    }
                })
                .onErrorResume(e -> {
                    failed.increment();
                    log.warn("이탈자 청소 실패 — 다음 틱에 다시 한다: {}", e.toString());
                    return Mono.just(SweepResult.NOTHING);
                });
    }
}
