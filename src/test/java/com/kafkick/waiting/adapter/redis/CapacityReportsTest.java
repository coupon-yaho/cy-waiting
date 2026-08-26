package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.control.CapacityReport;
import com.kafkick.waiting.control.CapacitySample;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * 뒷단의 가용량 자기보고 읽기.
 *
 * <p><b>밖에서 쓰는 키라 아무 값이나 들어온다.</b> 한 인스턴스의 깨진 값이 판을
 * 죽이면 멀쩡한 인스턴스의 몫까지 같이 사라지고, 그러면 전역 크레딧이 하한으로
 * 떨어져 대기열이 통째로 켜진다.
 */
@Tag("integration")
@SpringBootTest
class CapacityReportsTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(5);

    /** 2026-01-01. 서버 시각이 초 단위 epoch 인지를 붙박이 값으로 잰다 (TS-4). */
    private static final long EPOCH_2026 = 1_767_225_600L;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private AllocationRedisPort port;

    @BeforeEach
    void 준비() {
        port = AllocationRedisPort.of(redis, 1);
        redis.delete(RedisKeys.CAPACITY).block(WAIT);
    }

    private void 보고(String instanceId, String json) {
        redis.opsForHash().put(RedisKeys.CAPACITY, instanceId, json).block(WAIT);
    }

    @Test
    @DisplayName("자기보고를_그대로_읽는다")
    void 자기보고를_그대로_읽는다() {
        보고("i1", "{\"addr\":\"10.0.3.17:8080\",\"credits\":180,\"ts\":1755000000}");

        List<CapacityReport> reports = port.capacitySample().block(WAIT).reports();

        assertThat(reports).hasSize(1);
        assertThat(reports.getFirst().instanceId()).isEqualTo("i1");
        assertThat(reports.getFirst().credits()).isEqualTo(180);
        assertThat(reports.getFirst().reportedAt()).isEqualTo(1_755_000_000L);
    }

    @Test
    @DisplayName("깨진_값_하나가_판을_안_죽인다")
    void 깨진_값_하나가_판을_안_죽인다() {
        보고("i1", "{\"credits\":180,\"ts\":1755000000}");
        보고("i2", "이건 JSON 이 아니다");
        // 필드가 빠진 것도 같다 — 없는 값을 0 으로 접으면 그 인스턴스가 죽은 것처럼 보인다.
        보고("i3", "{\"addr\":\"10.0.3.18:8080\"}");

        List<CapacityReport> reports = port.capacitySample().block(WAIT).reports();

        assertThat(reports).extracting(CapacityReport::instanceId).containsExactly("i1");
    }

    @Test
    @DisplayName("보고가_없으면_빈_목록이다")
    void 보고가_없으면_빈_목록이다() {
        // 예외가 아니다. 신선한 보고 0 건에서 하한을 쓰는 판단은 수집기가 한다.
        assertThat(port.capacitySample().block(WAIT).reports()).isEmpty();
    }

    /**
     * <b>전부 버린 것과 원래 없는 것은 다르다.</b> 형식이 어긋나 전멸했는데 빈
     * 목록을 내려보내면 부르는 쪽이 "신선한 보고 0 건" 으로 읽어 하한으로
     * 떨어뜨린다 — 그 하한에서는 대기열이 통째로 켜지고 아무 신호도 안 난다.
     */
    @Test
    @DisplayName("전부_걸렀으면_관측_실패다")
    void 전부_걸렀으면_관측_실패다() {
        보고("i1", "{\"credits\":\"180\",\"ts\":\"1755000000\"}");
        보고("i2", "{\"credit\":180,\"timestamp\":1755000000}");

        assertThatThrownBy(() -> port.capacitySample().block(WAIT))
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * <b>기준 시각을 보고와 같은 노드에서 같은 순간에 받는다.</b> 따로 내면
     * 두 왕복 사이의 갱신 때문에 나이가 음수로 나오고, 클러스터에서는 키 없는
     * 명령이 아무 노드로 가 아예 다른 벽시계가 된다.
     */
    @Test
    @DisplayName("보고와_기준_시각을_같이_받는다")
    void 보고와_기준_시각을_같이_받는다() {
        보고("i1", "{\"credits\":500,\"ts\":1800000000}");

        CapacitySample sample = port.capacitySample().block(WAIT);

        assertThat(sample.reports()).hasSize(1);
        // 붙박이 하한과 비교한다 — 실제 시계에 안 기댄다 (TS-4).
        assertThat(sample.now()).isGreaterThan(EPOCH_2026);
    }
}
