package com.kafkick.waiting.routing;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceChooser;
import com.kafkick.waiting.domain.routing.RoutingCandidate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.DefaultResponse;
import org.springframework.cloud.client.loadbalancer.EmptyResponse;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.cloud.loadbalancer.core.ReactorServiceInstanceLoadBalancer;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import reactor.core.publisher.Mono;

/**
 * 여유 대비 부하가 낮은 인스턴스로 보낸다.
 *
 * <p>고르는 규칙은 도메인이 쥔다 (R-9). 여기는 스프링의 모양에 맞춰 재료를
 * 모아 넘기고 답을 되돌리는 일만 한다.
 */
public final class CapacityAwareLoadBalancer implements ReactorServiceInstanceLoadBalancer {

    private final ServiceInstanceListSupplier instances;

    private final InstanceChooser chooser;

    private final InFlightRegistry inFlight;

    private final LongSupplier nowMillis;

    /** 인스턴스 하나에 동시에 물릴 수 있는 수 (G9.13). */
    private final int perInstanceCap;

    private CapacityAwareLoadBalancer(ServiceInstanceListSupplier instances,
            InstanceChooser chooser, InFlightRegistry inFlight, LongSupplier nowMillis,
            int perInstanceCap) {
        this.instances = Objects.requireNonNull(instances, "instances");
        this.chooser = Objects.requireNonNull(chooser, "chooser");
        this.inFlight = Objects.requireNonNull(inFlight, "inFlight");
        this.nowMillis = Objects.requireNonNull(nowMillis, "nowMillis");
        if (perInstanceCap < 1) {
            throw new IllegalArgumentException("perInstanceCap 은 1 이상이어야 한다: "
                    + perInstanceCap);
        }
        this.perInstanceCap = perInstanceCap;
    }

    public static CapacityAwareLoadBalancer of(ServiceInstanceListSupplier instances,
            InstanceChooser chooser, InFlightRegistry inFlight, LongSupplier nowMillis,
            int perInstanceCap) {
        return new CapacityAwareLoadBalancer(instances, chooser, inFlight, nowMillis,
                perInstanceCap);
    }

    @Override
    public Mono<Response<ServiceInstance>> choose(Request request) {
        return instances.get(request).next().map(this::pick);
    }

    private Response<ServiceInstance> pick(List<ServiceInstance> available) {
        long now = nowMillis.getAsLong();
        // **사라진 인스턴스의 카운터를 지운다** (9.2.5). 식별자가 재기동마다
        // 새로 오므로(R-3) 안 지우면 배포를 거듭할수록 자란다.
        Set<String> present = new HashSet<>();
        for (ServiceInstance instance : available) {
            present.add(instance.getInstanceId());
        }
        inFlight.retain(present);

        List<RoutingCandidate> candidates = new ArrayList<>();
        for (ServiceInstance instance : available) {
            String id = instance.getInstanceId();
            int busy = inFlight.count(id, now);
            // **상한에 닿은 대는 후보가 아니다** (G9.13). 느려진 한 대로 간
            // 요청이 무한정 쌓이면 그 한 대가 게이트웨이 커넥션을 다 붙잡는다.
            if (busy >= perInstanceCap) {
                continue;
            }
            candidates.add(RoutingCandidate.of(id, creditsOf(instance), busy));
        }
        Optional<RoutingCandidate> chosen = chooser.choose(candidates);
        if (chosen.isEmpty()) {
            // **명확한 실패다** (9.3.4). 아무 대나 고르면 여유 0 인 대가 무너진다.
            return new EmptyResponse();
        }
        String id = chosen.orElseThrow().instanceId();
        for (ServiceInstance instance : available) {
            if (instance.getInstanceId().equals(id)) {
                return new DefaultResponse(instance);
            }
        }
        return new EmptyResponse();
    }

    /**
     * <b>못 읽으면 후보에서 뺀다.</b> 기본값을 주면 여유를 모르는 대가 정상
     * 비율만큼 받는다 — 그게 이 페이즈가 막으려는 것이다.
     */
    private long creditsOf(ServiceInstance instance) {
        String raw = instance.getMetadata().get(SnapshotInstanceListSupplier.CREDITS);
        if (raw == null) {
            return 0;
        }
        try {
            return Math.max(0, Long.parseLong(raw));
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
