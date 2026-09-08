package com.kafkick.waiting.routing;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.routing.AllowedDestinations;
import com.kafkick.waiting.domain.routing.InstanceAddress;
import com.kafkick.waiting.domain.routing.InstanceRouting;
import java.net.URI;
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
import org.springframework.cloud.client.loadbalancer.LoadBalancerUriTools;

/**
 * 인스턴스 목록을 판정 재료에서 읽는다.
 *
 * <p><b>여기서 레디스를 읽으면 불변식 1 이 깨진다.</b> 공급자는 노드마다 매
 * 요청에 도는 자리다.
 */
@Tag("unit")
class SnapshotInstanceListSupplierTest {

    /** 시험이 쓰는 포트. 목적지와 짝으로 막지 않으면 호스트 제한이 반쪽이다. */
    private static final List<Integer> 포트 = List.of(9000, 8080, 1);

    private static final Instant 지금 = Instant.parse("2026-09-02T00:00:00Z");

    private final SnapshotHolder holder = SnapshotHolder.of(
            Duration.ofSeconds(3), Duration.ofSeconds(5), Clock.fixed(지금, ZoneOffset.UTC));

    private final SnapshotInstanceListSupplier 공급자 = SnapshotInstanceListSupplier.of(
            "coupon-service", holder, AllowedDestinations.of(List.of("10.0.1.0/24"), 포트));

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

    /**
     * <b>v6 뒷단으로 갈 URI 가 서야 한다</b> (CY-888).
     *
     * <p>고르개가 이 인스턴스로 요청 URI 를 다시 짓는다. 대괄호 없이 이으면 못 읽는
     * 문자열이 되고, 그 대는 후보에 있는데도 요청이 안 나간다 — 예산만 나가고 갈 곳이
     * 없다. 목적지 판정은 벗긴 모양을 보므로 두 자리가 다른 표기를 쓴다.
     */
    @Test
    @DisplayName("v6_뒷단으로_갈_URI_가_선다")
    void v6_뒷단으로_갈_URI_가_선다() {
        SnapshotInstanceListSupplier 공급자 = SnapshotInstanceListSupplier.of(
                "coupon-service", holder, AllowedDestinations.of(List.of("fd00::/8"), 포트));
        재료를_심는다(인스턴스("be-v6", "[fd00::1]:9000", 200));

        ServiceInstance 대 = 공급자.get().blockFirst().getFirst();

        // **값으로는 벗긴 모양을 든다.** 목적지 판정이 호스트를 그대로 리터럴로 읽어,
        // 씌운 채 담으면 대역에 아무것도 안 맞아 이 대가 후보에서 통째로 빠진다.
        assertThat(대.getHost()).isEqualTo("fd00::1");
        // 고르개는 다시 지을 때 씌운다. 씌운 채 넘겨도 같은 결과라 여기서는 안 갈린다.
        assertThat(LoadBalancerUriTools.reconstructURI(대,
                URI.create("http://coupon-service/api/v1/coupons/c1/issue")))
                .hasToString("http://[fd00::1]:9000/api/v1/coupons/c1/issue");
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

    /**
     * <b>발행 측만으로는 못 닫는다.</b> 라우팅이 꺼진 노드가 리더면 안 걸러진 목록이
     * 나가고, 켠 노드가 그것을 그대로 쓴다. 실제로 연결하는 쪽이 마지막 자물쇠다.
     */
    @Test
    @DisplayName("허용_밖_주소는_후보에서_빠진다")
    void 허용_밖_주소는_후보에서_빠진다() {
        재료를_심는다(인스턴스("be-1", "10.0.1.7:8080", 200),
                인스턴스("evil", "evil.example.com:8080", 200));

        assertThat(공급자.get().blockFirst())
                .extracting(ServiceInstance::getInstanceId)
                .containsExactly("be-1");
    }
}
