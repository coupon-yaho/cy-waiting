package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.gateway.QueueStatusFilter;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * 제어 평면 배선.
 *
 * <p>조각이 다 있어도 <b>안 엮이면 아무것도 안 돈다.</b> 리더 판정과 배분 루프가
 * 각자 초록인데 사이가 비어 있으면 배분이 영영 안 돈다 — 그 상태로 뜨는 것이
 * 가장 나쁘다.
 */
@Tag("context")
@SpringBootTest(properties = "waiting.scheduler.enabled=true")
class ControlPlaneConfigTest {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ControlPlaneProperties properties;

    @Test
    @DisplayName("제어_평면_빈이_다_뜬다")
    void 제어_평면_빈이_다_뜬다() {
        // **뜬다는 것만으로 부족하다.** 배선이 서로 다른 설정을 보고 있으면
        // 각자 멀쩡한데 함께 안 맞는다.
        assertThat(context.getBean(Leadership.class).ownerId()).isNotBlank();
        assertThat(context.getBeansOfType(AllocationScheduler.class)).hasSize(1);
        assertThat(context.getBeansOfType(AllocationRound.class)).hasSize(1);
        assertThat(context.getBeansOfType(DemandCollector.class)).hasSize(1);
        // 루프를 켜는 것이 없으면 조각이 다 있어도 아무것도 안 돈다.
        assertThat(context.getBeansOfType(ControlPlaneLifecycle.class)).hasSize(1);
        assertThat(context.getBeansOfType(LeadershipLoop.class)).hasSize(1);
    }

    @Test
    @DisplayName("설정값이_계획값이다")
    void 설정값이_계획값이다() {
        assertThat(properties.scheduler().tick()).isEqualTo(Duration.ofSeconds(1));
        assertThat(properties.leader().lease()).isEqualTo(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("소유자_ID_는_기동마다_다르다")
    void 소유자_ID_는_기동마다_다르다() {
        // 고정하면 재기동한 자신을 이전 소유자로 오인해, 죽기 전에 잡아 둔 락을
        // 새 프로세스가 자기 것으로 알고 연장한다.
        //
        // **한 인스턴스만 보면 이걸 못 잰다** — 상수로 바꿔도 비어 있지 않다.
        assertThat(context.getBean(Leadership.class).ownerId())
                .isNotEqualTo(Leadership.newOwnerId());
        assertThat(Leadership.newOwnerId()).isNotEqualTo(Leadership.newOwnerId());
    }

    /**
     * 조회 필터가 세우는 거절 표시가 빈 하나이고, 배분이 받은 청소 조건이 거절을 본다. 조각 시험은 메서드를 직접 불러
     * 이 둘이 어긋나도 초록이다 — 그러면 표시는 서는데 청소가 안 멈춘다. 이 컨텍스트에는 레디스가 없어 하트비트와
     * 갱신이 실패하므로 등록부와 홀더는 시험만 움직인다.
     */
    @Test
    @DisplayName("거절_표시가_한_빈이고_배분의_청소_조건이_거절을_본다")
    void 거절_표시가_한_빈이고_배분의_청소_조건이_거절을_본다() {
        assertThat(ReflectionTestUtils.getField(context.getBean(QueueStatusFilter.class), "rejections"))
                .isSameAs(context.getBean(PollRejections.class));

        SnapshotHolder holder = context.getBean(SnapshotHolder.class);
        GatewayRegistry 등록부 = context.getBean(GatewayRegistry.class);
        BooleanSupplier 멈추나 = (BooleanSupplier) ReflectionTestUtils.getField(
                context.getBean(AllocationRound.class), "dataStale");
        holder.replace(new GatewaySnapshot(Map.of(), new SnapshotMeta(10, 1),
                context.getBean(Clock.class).instant()));
        try {
            assertThat(멈추나.getAsBoolean()).as("재료가 신선하고 거절이 없다").isFalse();
            등록부.pollRejectionObserved(1);
            assertThat(멈추나.getAsBoolean()).as("거절 중").isTrue();
        } finally {
            for (int i = 0; i < properties.capacity().rampDownTicks(); i++) {
                등록부.pollRejectionObserved(0);
            }
        }
    }
}
