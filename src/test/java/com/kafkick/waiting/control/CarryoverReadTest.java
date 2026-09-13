package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.allocation.CreditSmoother;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

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

    @Test
    @DisplayName("이월에_필요한_자리만_읽어_잇는다")
    void 이월에_필요한_자리만_읽어_잇는다() {
        List<List<String>> 요청 = new ArrayList<>();

        CreditSmoother 이어받음 = 배선.carryover(fields -> {
            요청.add(fields);
            return Mono.just(Map.of("#ewma", "200.0", "#ewmaSeeded", "1"));
        }, codec, Duration.ofSeconds(1), Schedulers.parallel()).get().block(Duration.ofSeconds(2));

        assertThat(요청).as("스냅샷을 통째로 끌어오지 않는다")
                .containsExactly(codec.smoothingFields());
        assertThat(이어받음.snapshot()).isEqualTo(new CreditSmoother.Snapshot(200.0, true));
    }

    /** 시한을 넘기면 실패로 돌려준다. 회차는 그것을 "다음 회차에 다시" 로 읽는다. */
    @Test
    @DisplayName("시한을_넘기면_실패로_끝낸다")
    void 시한을_넘기면_실패로_끝낸다() {
        var 읽기 = 배선.carryover(fields -> Mono.never(), codec, Duration.ofMillis(100),
                Schedulers.parallel());
        long 시작 = System.nanoTime();

        assertThatThrownBy(() -> 읽기.get().block(Duration.ofSeconds(5)))
                .hasRootCauseInstanceOf(TimeoutException.class);
        // **걸린 시간을 같이 본다.** 제 시한이 없어도 기다리는 쪽의 시한이 같은 예외를 낸다.
        assertThat(Duration.ofNanos(System.nanoTime() - 시작))
                .as("제 시한에서 끊긴다")
                .isLessThan(Duration.ofSeconds(2));
    }
}
