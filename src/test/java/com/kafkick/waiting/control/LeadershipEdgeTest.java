package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 매 틱 참인 것과 <b>방금 참이 된 것</b>은 다르다. 그 구별 없이 초기화하면
 * 회차마다 초기화되고, 안 하면 비리더 구간의 상태를 자기 것으로 이어 쓴다.
 */
class LeadershipEdgeTest {

    private final AtomicBoolean 리더 = new AtomicBoolean();
    private final AtomicLong 임기 = new AtomicLong(5);
    private final AtomicInteger 알림 = new AtomicInteger();
    private final AtomicInteger 잃음 = new AtomicInteger();
    private final LeadershipEdge edge = LeadershipEdge.of(리더::get, 임기::get,
            알림::incrementAndGet, 잃음::incrementAndGet);

    @Test
    @DisplayName("리더가_되면_한_번_알린다")
    void 리더가_되면_한_번_알린다() {
        리더.set(true);

        edge.getAsBoolean();
        edge.getAsBoolean();
        edge.getAsBoolean();

        assertThat(알림).hasValue(1);
    }

    @Test
    @DisplayName("리더가_아니면_안_알린다")
    void 리더가_아니면_안_알린다() {
        edge.getAsBoolean();

        assertThat(알림).hasValue(0);
    }

    @Test
    @DisplayName("잃었다_다시_잡으면_또_알린다")
    void 잃었다_다시_잡으면_또_알린다() {
        리더.set(true);
        edge.getAsBoolean();
        리더.set(false);
        edge.getAsBoolean();

        리더.set(true);
        edge.getAsBoolean();

        assertThat(알림).hasValue(2);
    }

    /** 잃는 순간도 한 번이어야 한다. 매 틱 알리면 진입·해제 쌍이 무너진다. */
    @Test
    @DisplayName("잃으면_한_번_알린다")
    void 잃으면_한_번_알린다() {
        리더.set(true);
        edge.getAsBoolean();

        리더.set(false);
        edge.getAsBoolean();
        edge.getAsBoolean();

        assertThat(잃음).hasValue(1);
    }

    /** 처음부터 리더가 아니었으면 잃은 것도 아니다. */
    @Test
    @DisplayName("잡은_적_없으면_안_잃는다")
    void 잡은_적_없으면_안_잃는다() {
        edge.getAsBoolean();
        edge.getAsBoolean();

        assertThat(잃음).hasValue(0);
    }

    @Test
    @DisplayName("원래_값을_그대로_돌려준다")
    void 원래_값을_그대로_돌려준다() {
        assertThat(edge.getAsBoolean()).isFalse();
        리더.set(true);
        assertThat(edge.getAsBoolean()).isTrue();
    }

    /**
     * <b>틱 사이에 잃었다 다시 잡으면 표본에는 늘 리더다</b> (CY-860). 표본이 틱마다
     * 한 번이라 짧은 승계는 참·거짓으로 안 보인다 — 임기 번호가 바뀐 것으로 본다.
     * 안 가르면 남이 돈 구간을 모른 채 앞 임기의 상태를 이어 쓰고 문도 다시 안 잠근다.
     */
    @Test
    @DisplayName("틱_사이에_임기가_바뀌면_잃고_다시_얻는다")
    void 틱_사이에_임기가_바뀌면_잃고_다시_얻는다() {
        List<String> 차례 = new ArrayList<>();
        LeadershipEdge 순서_edge = LeadershipEdge.of(리더::get, 임기::get,
                () -> 차례.add("얻음"), () -> 차례.add("잃음"));
        리더.set(true);
        순서_edge.getAsBoolean();

        임기.set(7);
        assertThat(순서_edge.getAsBoolean()).as("새 임기에서도 리더다").isTrue();
        순서_edge.getAsBoolean();

        // **앞 임기를 먼저 닫는다.** 거꾸로 부르면 새 임기에서 막 초기화한 상태를 곧바로
        // 멈추게 된다 — 잃음 알림이 청소를 세운다.
        assertThat(차례).containsExactly("얻음", "잃음", "얻음");
    }

    /** 임기를 모르면(0) 안 가른다. 잃는 순간 번호가 먼저 0 이 되는 창에서 헛 승계를 만든다. */
    @Test
    @DisplayName("임기를_모르면_승계로_안_본다")
    void 임기를_모르면_승계로_안_본다() {
        리더.set(true);
        edge.getAsBoolean();

        임기.set(0);
        edge.getAsBoolean();

        assertThat(잃음).hasValue(0);
        assertThat(알림).hasValue(1);
    }

    /**
     * <b>모르는 번호를 본 뒤에도 알던 번호를 지킨다.</b> 0 을 기록하고 다음 번호를 조용히
     * 받으면 5 → 0 → 7 에서 실제 승계를 놓친다. 지연 중에는 틱이 길어져 그 창이 넓다.
     */
    @Test
    @DisplayName("모르는_번호_사이의_승계도_가른다")
    void 모르는_번호_사이의_승계도_가른다() {
        리더.set(true);
        edge.getAsBoolean();
        임기.set(0);
        edge.getAsBoolean();

        임기.set(7);
        edge.getAsBoolean();

        assertThat(잃음).hasValue(1);
        assertThat(알림).hasValue(2);
    }

    /** 얻는 순간 번호를 몰랐으면 처음 알게 된 번호를 조용히 받고, 그 뒤의 승계는 가른다. */
    @Test
    @DisplayName("얻을_때_모르던_번호는_조용히_받는다")
    void 얻을_때_모르던_번호는_조용히_받는다() {
        임기.set(0);
        리더.set(true);
        edge.getAsBoolean();

        임기.set(5);
        edge.getAsBoolean();
        assertThat(잃음).as("처음 알게 된 번호는 승계가 아니다").hasValue(0);
        assertThat(알림).hasValue(1);

        임기.set(7);
        edge.getAsBoolean();
        assertThat(잃음).as("받은 번호에서 바뀌면 승계다").hasValue(1);
        assertThat(알림).hasValue(2);
    }
}
