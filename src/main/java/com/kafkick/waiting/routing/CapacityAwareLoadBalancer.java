package com.kafkick.waiting.routing;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceChooser;
import com.kafkick.waiting.domain.routing.RoutingCandidate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
        // **사라지고 비어 있는 인스턴스의 카운터를 지운다.** 식별자가 재기동마다
        // 새로 오므로 안 지우면 배포를 거듭할수록 자란다. 다만 목록에서 잠깐
        // 빠진 대에 요청이 아직 물려 있으면 안 지운다 — 지우면 돌아온 순간
        // 부하가 0 으로 보여 그 대로 몰아 보낸다.
        Set<String> present = new HashSet<>();
        for (ServiceInstance instance : available) {
            present.add(instance.getInstanceId());
        }
        inFlight.retain(present, now);

        Map<String, ServiceInstance> byId = new LinkedHashMap<>();
        List<RoutingCandidate> candidates = new ArrayList<>();
        for (ServiceInstance instance : available) {
            String id = instance.getInstanceId();
            int busy = inFlight.count(id, now);
            // **상한에 닿은 대는 후보가 아니다.** 느려진 한 대로 간 요청이
            // 무한정 쌓이면 그 한 대가 게이트웨이 커넥션을 다 붙잡는다.
            if (busy >= perInstanceCap) {
                continue;
            }
            byId.put(id, instance);
            candidates.add(RoutingCandidate.of(id, creditsOf(instance), busy));
        }

        // **고르는 자리에서 자리를 잡는다.** 읽고 나중에 세면 동시 요청이 다 같이
        // 빈자리를 보고 같은 대를 고른 뒤에야 세어져, 상한 1 인 대에 둘이 들어간다.
        // 잡는 데 실패했다는 것은 그 사이에 찼다는 뜻이라 그 대를 빼고 다시 고른다.
        while (!candidates.isEmpty()) {
            Optional<RoutingCandidate> chosen = chooser.choose(candidates);
            if (chosen.isEmpty()) {
                break;
            }
            String id = chosen.orElseThrow().instanceId();
            // **목록에 없는 것을 돌려주면 빈 답이다.** 그대로 믿으면 없는 주소로
            // 보낸다. 자리를 잡기 전에 본다 — 잡고 나면 놓을 사람이 없다.
            ServiceInstance instance = byId.get(id);
            if (instance == null) {
                break;
            }
            Optional<InFlightRegistry.Ticket> ticket =
                    inFlight.tryStarted(id, perInstanceCap, now);
            if (ticket.isPresent()) {
                return new DefaultResponse(
                        ReservedInstance.of(instance, ticket.orElseThrow()));
            }
            candidates.removeIf(c -> c.instanceId().equals(id));
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
