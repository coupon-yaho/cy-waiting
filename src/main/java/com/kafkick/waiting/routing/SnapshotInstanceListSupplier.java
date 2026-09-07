package com.kafkick.waiting.routing;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.routing.AllowedDestinations;
import com.kafkick.waiting.domain.routing.InstanceRouting;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import reactor.core.publisher.Flux;

/**
 * 인스턴스 목록을 <b>판정 재료에서</b> 읽는다.
 *
 * <p>보고는 레디스에 있고 <b>요청 경로는 레디스를 안 친다</b>. 이 공급자는 노드마다
 * 매 요청에 도는 자리라, 여기서 보고를 읽으면 게이트웨이의 존재 이유가 사라진다.
 */
public final class SnapshotInstanceListSupplier implements ServiceInstanceListSupplier {

    /** 여유를 인스턴스 메타데이터에 실어 고르개에 넘긴다. */
    public static final String CREDITS = "credits";

    private final String serviceId;

    private final SnapshotHolder holder;

    private final AllowedDestinations allowed;

    private SnapshotInstanceListSupplier(String serviceId, SnapshotHolder holder,
            AllowedDestinations allowed) {
        this.serviceId = Objects.requireNonNull(serviceId, "serviceId");
        this.holder = Objects.requireNonNull(holder, "holder");
        this.allowed = Objects.requireNonNull(allowed, "allowed");
    }

    public static SnapshotInstanceListSupplier of(String serviceId, SnapshotHolder holder,
            AllowedDestinations allowed) {
        return new SnapshotInstanceListSupplier(serviceId, holder, allowed);
    }

    @Override
    public String getServiceId() {
        return serviceId;
    }

    /** 마지막으로 만든 목록과 그것을 만든 재료. 재료가 바뀌면 다시 만든다. */
    private record Built(GatewaySnapshot from, List<ServiceInstance> instances) {
    }

    private final AtomicReference<Built> built = new AtomicReference<>();

    /**
     * <b>구독마다 지금 값을 읽는다.</b> 목록을 재료와 무관하게 캐시하면 인스턴스가
     * 사라진 뒤에도 그리로 보낸다.
     */
    @Override
    public Flux<List<ServiceInstance>> get() {
        return Flux.defer(() -> Flux.just(current()));
    }

    /**
     * <b>목적지 검사를 여기서 다시 한다.</b> 발행 측에만 걸면 라우팅이 꺼진 노드가
     * 리더일 때 안 걸러진 목록이 나가고, 켠 노드가 그것을 그대로 쓴다.
     *
     * <p><b>재료가 그대로면 다시 안 만든다.</b> 이 자리는 매 요청 도는데 주소마다
     * 정규식과 파싱이 붙는다. 스냅샷은 참조를 통째로 갈므로 무효화가 분명하다.
     */
    private List<ServiceInstance> current() {
        GatewaySnapshot now = holder.current();
        Built seen = built.get();
        if (seen != null && seen.from() == now) {
            return seen.instances();
        }
        List<ServiceInstance> made = now.instances().stream()
                .filter(routing -> allowed.permits(routing.address()))
                .map(this::toInstance)
                .map(ServiceInstance.class::cast)
                .toList();
        // 겹쳐 만들어도 같은 값이라 잠그지 않는다.
        built.set(new Built(now, made));
        return made;
    }

    // **https 로 안 붙인다.** 뒷단은 같은 사설망이고, 주소에 스킴을 안 실었다 —
    // 여기서 정하는 것이 계약이다.
    private DefaultServiceInstance toInstance(InstanceRouting routing) {
        DefaultServiceInstance instance = new DefaultServiceInstance(
                routing.instanceId(), serviceId, routing.address().host(),
                routing.address().port(), false);
        instance.getMetadata().put(CREDITS, Long.toString(routing.credits()));
        return instance;
    }
}
