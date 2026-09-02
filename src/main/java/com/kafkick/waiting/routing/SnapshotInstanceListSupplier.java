package com.kafkick.waiting.routing;

import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.routing.InstanceRouting;
import java.util.List;
import java.util.Objects;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import reactor.core.publisher.Flux;

/**
 * 인스턴스 목록을 <b>판정 재료에서</b> 읽는다.
 *
 * <p>보고는 레디스에 있고 <b>요청 경로는 레디스를 안 친다</b> (불변식 1). 이
 * 공급자는 노드마다 매 요청에 도는 자리라, 여기서 보고를 읽으면 게이트웨이의
 * 존재 이유가 사라진다. 리더가 실어 보낸 것을 로컬에서 읽는다.
 */
public final class SnapshotInstanceListSupplier implements ServiceInstanceListSupplier {

    /** 여유를 인스턴스 메타데이터에 실어 고르개에 넘긴다. */
    public static final String CREDITS = "credits";

    private final String serviceId;

    private final SnapshotHolder holder;

    private SnapshotInstanceListSupplier(String serviceId, SnapshotHolder holder) {
        this.serviceId = Objects.requireNonNull(serviceId, "serviceId");
        this.holder = Objects.requireNonNull(holder, "holder");
    }

    public static SnapshotInstanceListSupplier of(String serviceId, SnapshotHolder holder) {
        return new SnapshotInstanceListSupplier(serviceId, holder);
    }

    @Override
    public String getServiceId() {
        return serviceId;
    }

    /**
     * <b>구독마다 지금 값을 읽는다.</b> 한 번 만든 목록을 캐시하면 인스턴스가
     * 사라진 뒤에도 그리로 보낸다.
     */
    @Override
    public Flux<List<ServiceInstance>> get() {
        return Flux.defer(() -> Flux.just(current()));
    }

    private List<ServiceInstance> current() {
        return holder.current().instances().stream()
                .map(this::toInstance)
                .map(ServiceInstance.class::cast)
                .toList();
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
