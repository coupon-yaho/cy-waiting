package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * 적용이 실패한 쿠폰을 <b>앞줄 제거에서만</b> 뺀다 (CY-947).
 *
 * <p>커서를 못 되살린 채 앞줄을 걷으면 들인 사람이 이탈로 걷힌다. 대신 정리는 돌아야 한다 — 멈추면 만료 신호와
 * 유예 기록이 한 방향으로만 자란다.
 */
class QueueSweeperTest {

    private final MeterRegistry meters = new SimpleMeterRegistry();

    private final List<List<String>> 부른_대상 = new ArrayList<>();

    private final List<Boolean> 앞줄_제거 = new ArrayList<>();

    private QueueSweeper 스위퍼() {
        return QueueSweeper.of(SweepGates.warmed(Duration.ofSeconds(1), PollIntervalPolicy.aliveTtl()),
                (ids, limit, removeFront) -> {
                    부른_대상.add(List.copyOf(ids));
                    앞줄_제거.add(removeFront);
                    return Mono.just(QueueSweeper.SweepResult.NOTHING);
                }, meters);
    }

    private Map<String, CouponState> 줄이_선_쿠폰들() {
        Map<String, CouponState> coupons = new LinkedHashMap<>();
        coupons.put("c1", CouponStates.queueing(10, 1_000, 100));
        coupons.put("c2", CouponStates.queueing(10, 1_000, 100));
        return coupons;
    }

    private double 제외_계수() {
        return meters.counter("waiting.sweep", "kind", "apply-failed").count();
    }

    @Test
    @DisplayName("적용이_실패한_쿠폰만_앞줄_제거에서_뺀다")
    void 적용이_실패한_쿠폰만_앞줄_제거에서_뺀다() {
        스위퍼().run(줄이_선_쿠폰들(), false, Set.of("c1")).block();

        assertThat(부른_대상).as("나머지 쿠폰은 그대로 쓴다").containsExactly(List.of("c2"));
        assertThat(앞줄_제거).as("앞줄 제거는 돈다").containsExactly(true);
        assertThat(제외_계수()).as("뺀 쿠폰 수를 센다").isEqualTo(1);
    }

    /**
     * <b>전부 실패한 틱에도 정리는 돈다.</b> 멈추면 만료 신호와 유예 기록이 한 방향으로만 자라고, 그 구간이
     * 길어지면 커서가 전진을 못 한다.
     */
    @Test
    @DisplayName("전부_실패해도_정리는_돈다")
    void 전부_실패해도_정리는_돈다() {
        스위퍼().run(줄이_선_쿠폰들(), false, Set.of("c1", "c2")).block();

        assertThat(부른_대상).as("정리 대상은 전체다").containsExactly(List.of("c1", "c2"));
        assertThat(앞줄_제거).as("앞줄 제거만 접는다").containsExactly(false);
        assertThat(제외_계수()).isEqualTo(2);
    }

    @Test
    @DisplayName("실패가_없으면_안_뺀다")
    void 실패가_없으면_안_뺀다() {
        스위퍼().run(줄이_선_쿠폰들(), false, Set.of()).block();

        assertThat(부른_대상).containsExactly(List.of("c1", "c2"));
        assertThat(앞줄_제거).containsExactly(true);
        assertThat(제외_계수()).isZero();
    }

    /** 쓸 것도 뺄 것도 없으면 왕복을 안 낸다. 유예 중이 아니면 정리도 안 돈다. */
    @Test
    @DisplayName("쓸_것이_없으면_안_부른다")
    void 쓸_것이_없으면_안_부른다() {
        Map<String, CouponState> 한산한_쿠폰 = Map.of("c1", CouponStates.idle(1_000));

        스위퍼().run(한산한_쿠폰, false, Set.of()).block();

        assertThat(부른_대상).isEmpty();
    }
}
