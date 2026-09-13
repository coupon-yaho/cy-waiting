package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * 승계 뒤 평활화 이월을 읽는 자리 (CY-863).
 *
 * <p><b>이월 읽기에 제 시한이 없으면 회차 전체 예산을 먹는다.</b> 가용량과 운영값 갱신이
 * 이미 틱의 4분의 1 씩 쓰는데, 승계 직후 이 왕복 하나가 나머지를 다 쓰면 전 노드가
 * 낡음으로 넘어간다.
 */
class CarryoverReadTest {

    private final ControlPlaneConfig 배선 = new ControlPlaneConfig();

    private final SnapshotCodec codec = SnapshotCodec.create();

    /** 발행이 실제로 쓰는 모양의 스냅샷. 쿠폰 자리와 이월 자리가 같이 있다. */
    private Map<String, String> 발행된_스냅샷() {
        return codec.encode(new GatewaySnapshot(Map.of("c1", CouponStates.idle(1_000)),
                        new SnapshotMeta(1_000, 1), Instant.parse("2026-09-13T00:00:00Z")),
                new CreditSmoother.Snapshot(200.0, true), QueueingHysteresis.Snapshot.empty());
    }

    /**
     * <b>읽는 쪽은 요청한 자리만 돌려준다.</b> 스텁이 늘 전부 주면 읽을 자리 목록에서 하나가
     * 빠져도 통과한다 — 운영에서는 그 자리가 안 와서 이월이 조용히 늘 콜드가 된다.
     */
    @Test
    @DisplayName("이월에_필요한_자리만_읽어_잇는다")
    void 이월에_필요한_자리만_읽어_잇는다() {
        Map<String, String> 스냅샷 = 발행된_스냅샷();
        List<List<String>> 요청 = new ArrayList<>();

        CreditSmoother 이어받음 = 배선.carryover(fields -> {
            요청.add(fields);
            Map<String, String> 고른_것 = new LinkedHashMap<>();
            fields.stream().filter(스냅샷::containsKey).forEach(f -> 고른_것.put(f, 스냅샷.get(f)));
            return Mono.just(고른_것);
        }, codec, Duration.ofSeconds(4), VirtualTimeScheduler.create()).get()
                .block(Duration.ofSeconds(2));

        assertThat(요청).singleElement().asList().as("쿠폰 자리는 안 끌어온다")
                .doesNotContain("c1");
        assertThat(이어받음.snapshot()).isEqualTo(new CreditSmoother.Snapshot(200.0, true));
    }

    /**
     * <b>틱의 4분의 1 에서 끊는다.</b> 형제 갱신과 같은 몫이다. 넘기면 실패로 끝나고 회차는
     * 그것을 "다음 회차에 다시" 로 읽는다. 가상 시간이라 몫이 절반이어도 걸린다.
     */
    @Test
    @DisplayName("틱의_4분의_1_에서_실패로_끝낸다")
    void 틱의_4분의_1_에서_실패로_끝낸다() {
        StepVerifier.withVirtualTime(() -> 배선.carryover(fields -> Mono.never(), codec,
                        Duration.ofSeconds(4), VirtualTimeScheduler.getOrSet()).get())
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(999))
                .thenAwait(Duration.ofMillis(1))
                .expectError(TimeoutException.class)
                .verify(Duration.ofSeconds(2));
    }
}
