package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Status;

/**
 * 루프가 멎었으면 이 프로세스의 결함이다 — 빼는 것이 아니라 재기동이다.
 *
 * <p><b>기동 직후를 루프 정지로 세면 크래시 루프다.</b> 아직 한 번도 안 돈 것과
 * 돌다 멎은 것은 나이로는 같은 값이라, 그대로 물리면 첫 판을 못 돈 파드가 죽고
 * 재기동해도 또 첫 판 전이라 또 죽는다.
 */
class LoopAliveHealthTest {

    private static final Duration FETCH_STALE = Duration.ofSeconds(2);
    private static final Duration DATA_STALE = Duration.ofSeconds(5);

    private final AtomicReference<Instant> now =
            new AtomicReference<>(Instant.ofEpochSecond(1_700_000_000L));

    private final Clock clock = new Clock() {
        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    };

    private final SnapshotHolder holder = SnapshotHolder.of(FETCH_STALE, DATA_STALE, clock);
    private final ShutdownState shutdown = ShutdownState.create();

    private void 시간을_흘린다(Duration 만큼) {
        now.updateAndGet(t -> t.plus(만큼));
    }

    private Status 판정() {
        return LoopAliveHealth.of(holder, shutdown).health().getStatus();
    }

    @Test
    @DisplayName("첫_판_전에는_살아_있는_것으로_본다")
    void 첫_판_전에는_살아_있는_것으로_본다() {
        // 여기서 죽이면 재기동해도 또 첫 판 전이라 또 죽는다. 크래시 루프다.
        시간을_흘린다(FETCH_STALE.multipliedBy(10));

        assertThat(판정()).isEqualTo(Status.UP);
    }

    @Test
    @DisplayName("돌고_있으면_살아_있다")
    void 돌고_있으면_살아_있다() {
        holder.loopTicked();

        assertThat(판정()).isEqualTo(Status.UP);
    }

    @Test
    @DisplayName("한_번이라도_돈_뒤_멎으면_죽는다")
    void 한_번이라도_돈_뒤_멎으면_죽는다() {
        // 이 프로세스의 결함이다. 트래픽에서 빼는 것이 아니라 재기동해야 한다.
        holder.loopTicked();
        시간을_흘린다(FETCH_STALE.plusSeconds(1));

        assertThat(판정()).isEqualTo(Status.DOWN);
    }

    @Test
    @DisplayName("임계와_같은_나이는_아직_산다")
    void 임계와_같은_나이는_아직_산다() {
        holder.loopTicked();
        시간을_흘린다(FETCH_STALE);

        assertThat(판정()).isEqualTo(Status.UP);
    }

    @Test
    @DisplayName("드레이닝_중에는_루프를_안_본다")
    void 드레이닝_중에는_루프를_안_본다() {
        // 종료하려고 내린 루프를 정지로 세면, 진행 중인 요청을 든 파드가 그
        // 자리에서 끊긴다. 우아한 종료에 쓸 시간이 사라진다.
        holder.loopTicked();
        시간을_흘린다(FETCH_STALE.plusSeconds(10));
        assertThat(판정()).isEqualTo(Status.DOWN);

        shutdown.draining();

        assertThat(판정()).isEqualTo(Status.UP);
    }

    @Test
    @DisplayName("받아오기만_실패하는_것은_죽음이_아니다")
    void 받아오기만_실패하는_것은_죽음이_아니다() {
        // 공유 원인일 수 있다. 재기동해도 안 고쳐지고, 전 노드가 동시에 재기동하면
        // 그게 전면 장애다.
        holder.replace(new GatewaySnapshot(Map.of(), GatewaySnapshot.EMPTY.meta(), now.get()));
        시간을_흘린다(DATA_STALE.plusSeconds(10));
        holder.loopTicked();

        assertThat(판정()).isEqualTo(Status.UP);
    }
}
