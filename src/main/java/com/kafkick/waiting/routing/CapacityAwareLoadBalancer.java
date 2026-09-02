package com.kafkick.waiting.routing;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.kafkick.waiting.control.FailureWindow;
import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceCountBand;
import com.kafkick.waiting.domain.routing.InstanceChooser;
import com.kafkick.waiting.domain.routing.RoutingCandidate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(CapacityAwareLoadBalancer.class);

    private final ServiceInstanceListSupplier instances;

    private final InstanceChooser chooser;

    private final InFlightRegistry inFlight;

    private final LongSupplier nowMillis;

    /** 인스턴스 하나에 동시에 물릴 수 있는 수 (G9.13). */
    private final int perInstanceCap;

    /** 가정 밖 구간의 시작과 끝만 남긴다 (LG-2). */
    private final FailureWindow outsideAssumption = FailureWindow.create();

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
        watchInstanceCount(available.size());
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
     * 인스턴스 수가 가정(A7) 밖이면 알린다 (9.3.9 · R-9).
     *
     * <p><b>자동으로 전략을 안 바꾼다.</b> 인스턴스 수로 전환하면 임계 근처에서
     * 진동하고 — 롤링 배포가 정확히 그 구간을 지난다 — 어느 구간이 무슨 모드였는지가
     * 대시보드에 안 남아 장애 분석이 막힌다.
     */
    // **구간의 시작만 찍는다.** 요청마다 찍으면 초당 수천 줄이 쌓이고, 그때
    // 정작 봐야 할 것이 묻힌다 (LG-2).
    private void watchInstanceCount(int instances) {
        InstanceCountBand band = InstanceCountBand.of(instances);
        if (band.withinAssumption()) {
            outsideAssumption.exited().ifPresent(r -> log.info(
                    "인스턴스 수가 가정 안으로 돌아왔다 — {}초 동안 {}건",
                    NANOSECONDS.toSeconds(r.elapsedNanos()), r.swallowed()));
            return;
        }
        if (outsideAssumption.entered()) {
            log.warn("라우팅 가정 밖 — {}", band.describe(instances));
        }
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
