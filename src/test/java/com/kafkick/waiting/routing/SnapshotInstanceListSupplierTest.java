package com.kafkick.waiting.routing;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.routing.InstanceAddress;
import com.kafkick.waiting.domain.routing.InstanceRouting;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.client.ServiceInstance;

/**
 * 인스턴스 목록을 판정 재료에서 읽는다.
 *
 * <p><b>여기서 레디스를 읽으면 불변식 1 이 깨진다.</b> 공급자는 노드마다 매
 * 요청에 도는 자리다.
 */
@Tag("unit")
class SnapshotInstanceListSupplierTest {

    private static final Instant 지금 = Instant.parse("2026-09-02T00:00:00Z");

    private final SnapshotHolder holder = SnapshotHolder.of(
            Duration.ofSeconds(3), Duration.ofSeconds(5), Clock.fixed(지금, ZoneOffset.UTC));

    private final SnapshotInstanceListSupplier 공급자 =
            SnapshotInstanceListSupplier.of("coupon-service", holder);

    private static InstanceRouting 인스턴스(String id, String addr, long credits) {
        return new InstanceRouting(id, InstanceAddress.parse(addr).orElseThrow(), credits);
    }

    private void 재료를_심는다(InstanceRouting... instances) {
        holder.replace(new GatewaySnapshot(Map.of("c1", CouponState.idle(500)),
                new SnapshotMeta(10, 1), 지금, List.of(instances)));
    }

    @Test
    @DisplayName("재료에_실린_인스턴스를_낸다")
    void 재료에_실린_인스턴스를_낸다() {
        재료를_심는다(인스턴스("be-1", "10.0.1.7:8080", 200));

        List<ServiceInstance> 목록 = 공급자.get().blockFirst();

        assertThat(목록).singleElement().satisfies(i -> {
            assertThat(i.getInstanceId()).isEqualTo("be-1");
            assertThat(i.getHost()).isEqualTo("10.0.1.7");
            assertThat(i.getPort()).isEqualTo(8080);
            assertThat(i.getServiceId()).isEqualTo("coupon-service");
            assertThat(i.isSecure()).isFalse();
        });
    }

    /** <b>여유를 같이 싣는다.</b> 안 실으면 고르개가 부하율을 못 낸다. */
    @Test
    @DisplayName("여유를_메타데이터에_싣는다")
    void 여유를_메타데이터에_싣는다() {
        재료를_심는다(인스턴스("be-1", "10.0.1.7:8080", 200));

        assertThat(공급자.get().blockFirst().getFirst().getMetadata())
                .containsEntry(SnapshotInstanceListSupplier.CREDITS, "200");
    }

    /** 재료가 아직 없으면 비어 있다. 없는 주소로 보내는 것보다 낫다. */
    @Test
    @DisplayName("재료가_없으면_비어_있다")
    void 재료가_없으면_비어_있다() {
        assertThat(공급자.get().blockFirst()).isEmpty();
    }

    /**
     * <b>구독마다 지금 값을 읽는다.</b> 한 번 만든 목록을 캐시하면 인스턴스가
     * 사라진 뒤에도 그리로 보낸다.
     */
    @Test
    @DisplayName("재료가_바뀌면_따라간다")
    void 재료가_바뀌면_따라간다() {
        재료를_심는다(인스턴스("be-1", "10.0.1.7:8080", 200));
        공급자.get().blockFirst();

        재료를_심는다(인스턴스("be-2", "10.0.1.8:9000", 40));

        assertThat(공급자.get().blockFirst()).extracting(ServiceInstance::getInstanceId)
                .containsExactly("be-2");
    }

    @Test
    @DisplayName("서비스_이름을_돌려준다")
    void 서비스_이름을_돌려준다() {
        assertThat(공급자.getServiceId()).isEqualTo("coupon-service");
    }
}
