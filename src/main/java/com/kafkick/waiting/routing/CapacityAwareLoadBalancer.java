package com.kafkick.waiting.routing;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.kafkick.waiting.control.FailureWindow;
import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceCountBand;
import com.kafkick.waiting.domain.routing.InstanceChooser;
import com.kafkick.waiting.domain.routing.InstanceOutliers;
import com.kafkick.waiting.domain.routing.RoutingCandidate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.DefaultResponse;
import org.springframework.cloud.client.loadbalancer.EmptyResponse;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.RequestData;
import org.springframework.cloud.client.loadbalancer.RequestDataContext;
import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.cloud.loadbalancer.core.ReactorServiceInstanceLoadBalancer;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import reactor.core.publisher.Mono;

/**
 * 여유 대비 부하가 낮은 인스턴스로 보낸다.
 *
 * <p>고르는 규칙은 도메인이 쥔다. 여기는 스프링의 모양에 맞춰 재료를
 * 모아 넘기고 답을 되돌리는 일만 한다.
 */
public final class CapacityAwareLoadBalancer implements ReactorServiceInstanceLoadBalancer {

    private static final Logger log = LoggerFactory.getLogger(CapacityAwareLoadBalancer.class);

    private final ServiceInstanceListSupplier instances;

    private final InstanceChooser chooser;

    private final InFlightRegistry inFlight;

    private final InstanceOutliers outliers;

    private final LongSupplier nowMillis;

    /** 인스턴스 하나에 동시에 물릴 수 있는 수. */
    private final int perInstanceCap;

    /** 가정 밖 구간의 시작과 끝만 남긴다. */
    private final FailureWindow outsideAssumption = FailureWindow.create();

    /**
     * 배제가 걸린 구간. <b>배제는 명백한 모드 전환인데 지표만으로는 언제 무엇이
     * 빠졌는지 못 되짚는다.</b> 진입과 해제를 쌍으로 남긴다.
     */
    private final FailureWindow ejecting = FailureWindow.create();

    /**
     * 전부가 대상이라 하나도 못 뺀 구간. <b>뒷단 전체가 앓는다는 신호다.</b>
     * 배제 지표는 이때도 표시된 수만 내므로 여기서만 드러난다.
     */
    private final FailureWindow suppressed = FailureWindow.create();

    /**
     * 되돌리는 중인 대가 있던 구간. <b>배제 해제 줄은 여기까지 안 말한다</b> — 그
     * 뒤로 램프가 이어지는데, 어느 대가 언제 제 몫으로 돌아왔는지가 안 남는다.
     */
    private final FailureWindow rampWindow = FailureWindow.create();

    /**
     * 구간에 들어설 때의 완주 수와 재배제 수. 구간이 닫힌 이유를 이 둘로 가른다.
     * <b>한 번에 갈아 끼운다</b> — 따로 쓰면 그 사이에 창이 닫힐 때 한쪽만 새 값이라
     * 증분이 음수까지 나고 갈래가 뒤집힌다.
     */
    private record Marks(long completed, long reEjected) {
    }

    private final AtomicReference<Marks> rampMarks = new AtomicReference<>(new Marks(0, 0));

    /**
     * 배제하고 나니 보낼 곳이 없던 구간. <b>부하 최고점에서만 켜지는 자리라</b>
     * 억제 없이 남기면 초당 수만 줄이 쌓인다.
     */
    private final FailureWindow crowdedOut = FailureWindow.create();

    /** 마지막으로 본 구간. 밖에서 밖으로 건너뛰는 것을 잡는다. */
    private final AtomicReference<InstanceCountBand> lastBand =
            new AtomicReference<>(InstanceCountBand.EXPECTED);

    private CapacityAwareLoadBalancer(ServiceInstanceListSupplier instances,
            InstanceChooser chooser, InFlightRegistry inFlight, InstanceOutliers outliers,
            LongSupplier nowMillis, int perInstanceCap) {
        this.instances = Objects.requireNonNull(instances, "instances");
        this.chooser = Objects.requireNonNull(chooser, "chooser");
        this.inFlight = Objects.requireNonNull(inFlight, "inFlight");
        this.outliers = Objects.requireNonNull(outliers, "outliers");
        this.nowMillis = Objects.requireNonNull(nowMillis, "nowMillis");
        if (perInstanceCap < 1) {
            throw new IllegalArgumentException("perInstanceCap 은 1 이상이어야 한다: "
                    + perInstanceCap);
        }
        this.perInstanceCap = perInstanceCap;
    }

    public static CapacityAwareLoadBalancer of(ServiceInstanceListSupplier instances,
            InstanceChooser chooser, InFlightRegistry inFlight, InstanceOutliers outliers,
            LongSupplier nowMillis, int perInstanceCap) {
        return new CapacityAwareLoadBalancer(instances, chooser, inFlight, outliers,
                nowMillis, perInstanceCap);
    }

    @Override
    public Mono<Response<ServiceInstance>> choose(Request request) {
        Set<String> tried = tried(request);
        return instances.get(request).next().map(available -> pick(available, tried));
    }

    /**
     * 이 요청이 이미 실패해 본 인스턴스들. 재시도가 아니면 비어 있다.
     *
     * <p>요청 속성에서 읽는다 — 재시도가 되돌리는 것은 응답 쪽뿐이라 시도 사이에
     * 남는 자리가 거기뿐이다.
     */
    @SuppressWarnings("unchecked")
    private Set<String> tried(Request request) {
        if (request == null || !(request.getContext() instanceof RequestDataContext context)) {
            return Set.of();
        }
        // **요청이 비어 있을 수 있다.** 기본 문맥으로 떨어지는 경로가 있어 이
        // 자리는 널을 받는다 — 그대로 부르면 고르는 자리가 통째로 터지고,
        // 그건 라우팅 전면 차단이다.
        RequestData client = context.getClientRequest();
        if (client == null || client.getAttributes() == null) {
            return Set.of();
        }
        Object raw = client.getAttributes().get(RoutingAttributes.TRIED);
        return raw instanceof Set<?> set ? (Set<String>) set : Set.of();
    }

    /**
     * 배제와 되돌리기 구간의 진입과 해제를 남긴다.
     *
     * <p>식별자는 카디널리티 때문에 지표 라벨에 못 붙어 로그가 유일한 기록이다.
     * 구간의 첫 건만 남겨 매 초 같은 줄이 쌓이지 않게 한다.
     */
    private void watchEjection(Set<String> present, Set<String> ejected, long now) {
        // **지금 목록 안에서만 센다.** 걷히길 기다리는 죽은 기록까지 세면,
        // 멀쩡한 뒷단에 대고 전부 앓는다고 말하게 된다 — 롤링 배포마다 뜬다.
        int marked = outliers.markedCount(now);
        // 표시는 됐는데 하나도 안 뺐다는 것은 전부가 대상이라는 뜻이다.
        if (!present.isEmpty() && marked >= present.size()) {
            if (suppressed.entered()) {
                // **이쪽도 배제가 무시된 국면이다.** 도로 넣는 쪽만 세면, 전 대가
                // 대상이라 하나도 안 뺀 구간에서 배제 게이지는 N 인데 계수는 0 이다.
                outliers.overridden();
                log.error("뒷단 {} 대가 전부 연속 실패다 — 배제를 안 건다. 빼면 보낼 "
                        + "곳이 0 이 된다. 뒷단 배포 상태와 서킷을 먼저 본다",
                        present.size());
            }
        } else {
            suppressed.exited().ifPresent(r -> log.info(
                    "뒷단 전체 실패가 풀렸다 — {}초 동안 {}건", r.elapsedSeconds(),
                    r.swallowed()));
        }
        if (!ejected.isEmpty()) {
            if (ejecting.entered()) {
                log.warn("연속 실패로 {} 대를 후보에서 뺐다 (전체 {} 대). "
                        + "되돌아올 때는 램프를 탄다", ejected.size(), present.size());
            }
        } else {
            ejecting.exited().ifPresent(r -> log.info(
                    "뺀 대가 없어졌다 — {}초 동안 {}건", r.elapsedSeconds(),
                    r.swallowed()));
        }
        // **배제가 걷혀도 끝이 아니다.** 그 뒤로 램프가 이어지는데 위 해제 줄이
        // 거기까지 말하지 않아, 몫이 아직 깎인 구간이 로그에 안 남았다.
        int ramping = outliers.rampingCount(now);
        if (ramping > 0) {
            if (rampWindow.entered()) {
                rampMarks.set(new Marks(outliers.rampsCompleted(), outliers.reEjections()));
                log.info("{} 대가 되돌아오는 중이다 (전체 {} 대) — 램프 동안 제 몫을 "
                        + "덜 받는다", ramping, present.size());
            }
        } else {
            rampWindow.exited().ifPresent(this::closedRamp);
        }
    }

    /**
     * 되돌리는 구간이 닫힌 이유를 남긴다.
     *
     * <p><b>닫혔다고 회복한 것이 아니다.</b> 다시 빠져도 되돌리는 대가 0 이 되고,
     * 완주 하나가 다시 빠진 하나를 가린다 — 두 증분을 따로 보고 섞이면 경고다.
     */
    private void closedRamp(FailureWindow.Recovered window) {
        Marks entry = rampMarks.get();
        long done = outliers.rampsCompleted() - entry.completed();
        long again = outliers.reEjections() - entry.reEjected();
        if (again > 0) {
            log.warn("되돌리기가 안 끝났다 — {}초 만에 {} 대가 다시 빠졌다 (완주 {} 대). "
                    + "다시 빠진 대가 배제와 램프를 되풀이하는 중이다. 뒷단 그 대의 "
                    + "오류율부터 본다", window.elapsedSeconds(), again, done);
        } else if (done > 0) {
            log.info("되돌리기가 끝났다 — {}초 동안 {} 대가 제 몫으로 돌아왔다",
                    window.elapsedSeconds(), done);
        } else {
            log.warn("되돌리기가 안 끝났다 — {}초 만에 구간이 닫혔다. 되돌리던 대가 "
                    + "목록에서 빠졌거나 시각이 뒤로 갔다. 뒷단 목록과 시각 동기를 "
                    + "함께 본다", window.elapsedSeconds());
        }
    }

    /**
     * 뺀 대를 도로 넣는 구간에 들었다. <b>국면 단위로 센다</b> — 다른 계수들이 사건
     * 수라, 여기만 요청 수면 한 대시보드에서 나머지가 바닥에 눌린다.
     */
    private void enteredCrowdedOut(Set<String> ejected) {
        if (!crowdedOut.entered()) {
            return;
        }
        outliers.overridden();
        // 식별자를 싣는다. 지표 라벨에는 못 붙어 이 줄이 유일한 기록이고, 구간
        // 도중에 대상이 바뀌면 그때 다시 이 줄이 나간다.
        log.warn("배제하고 나니 보낼 곳이 없다 — 뺀 {} 대를 도로 넣는다: {}. "
                + "남은 대가 여유 0 이거나 인스턴스별 상한에 닿았다는 뜻이다. "
                + "상한과 뒷단 여유를 함께 본다", ejected.size(), ejected);
    }

    /** 보낼 수 있는 후보를 모은다. 뺀 대와 상한에 닿은 대는 안 든다. */
    private List<RoutingCandidate> gather(List<ServiceInstance> available, Set<String> ejected,
            Map<String, ServiceInstance> byId, long now) {
        List<RoutingCandidate> candidates = new ArrayList<>();
        for (ServiceInstance instance : available) {
            String id = instance.getInstanceId();
            if (ejected.contains(id)) {
                continue;
            }
            int busy = inFlight.count(id, now);
            // **상한에 닿은 대는 후보가 아니다.** 느려진 한 대로 간 요청이
            // 무한정 쌓이면 그 한 대가 게이트웨이 커넥션을 다 붙잡는다.
            if (busy >= perInstanceCap) {
                continue;
            }
            byId.put(id, instance);
            long credits = creditsOf(instance);
            // **되돌아온 대의 여유를 줄인다.** 배제 동안 트래픽이 0 이라 물린 건수도
            // 0 이고, 그대로 두면 돌아오는 순간 전량이 그리로 간다. 물린 건수에 값을
            // 얹는 쪽은 P2C 에서 계단이 된다 — 여유를 줄여야 줄인 만큼만 받는다.
            double remaining = outliers.recoveryRemaining(id, now);
            // **여유 0 은 그대로 0 이다.** 아래 하한이 0 을 1 로 올리면 스스로
            // 못 받는다고 말한 대가 후보로 되살아난다 — 여유 0 은 후보가
            // 아니라는 규칙이 램프 때문에 깨진다.
            long effective = remaining <= 0 || credits <= 0 ? credits
                    : Math.max(1, Math.round(credits * (1 - remaining)));
            candidates.add(RoutingCandidate.of(id, effective, busy));
        }
        return candidates;
    }

    private Response<ServiceInstance> pick(List<ServiceInstance> available, Set<String> tried) {
        long now = nowMillis.getAsLong();
        watchInstanceCount(available.size());
        // **사라지고 비어 있는 인스턴스의 카운터를 지운다.** 식별자가 재기동마다
        // 새로 와 안 지우면 배포를 거듭할수록 자란다. 다만 잠깐 빠진 대에 요청이
        // 아직 물려 있으면 안 지운다 — 돌아온 순간 부하가 0 으로 보여 그리로 몰린다.
        Set<String> present = new HashSet<>();
        for (ServiceInstance instance : available) {
            present.add(instance.getInstanceId());
        }
        inFlight.retain(present, now);
        outliers.retain(present, now);

        // 고르개에 넘기기 전에 거른다. 규칙과 근거는 InstanceOutliers 에 있다.
        Set<String> ejected = outliers.ejected(present, now);
        watchEjection(present, ejected, now);
        // **재시도는 방금 실패한 대를 다시 안 고른다.** 안 빼면 3 대에서 같은 죽은
        // 대로 갈 확률이 오히려 높다. 전부 시도했으면 안 뺀다.
        Set<String> skip = ejected;
        if (!tried.isEmpty() && !tried.containsAll(present)) {
            skip = new HashSet<>(ejected);
            skip.addAll(tried);
        }

        Map<String, ServiceInstance> byId = new LinkedHashMap<>();
        List<RoutingCandidate> candidates = gather(available, skip, byId, now);
        // **보낼 곳이 0 이 되면 되돌린다. 방금 실패한 대가 먼저다** — 배제는 세 번
        // 연속 실패한 근거가 있고 재시도 배제는 이번 한 번뿐이라 근거가 얕다.
        // 그래도 비면 배제까지 접는다: 앓는 대라도 아무 데도 못 보내는 것보다 낫다.
        if (noneUsable(candidates) && !skip.equals(ejected)) {
            byId.clear();
            candidates = gather(available, ejected, byId, now);
        }
        // **한 번만 셈한다.** 아래 갈래 셋이 같은 값을 물으므로, 나눠 부르면 정상
        // 갈래에서만 한 번 더 도는 셈이 된다.
        boolean stuck = noneUsable(candidates);
        if (stuck && !ejected.isEmpty()) {
            enteredCrowdedOut(ejected);
            byId.clear();
            candidates = gather(available, Set.of(), byId, now);
        } else if (stuck) {
            // **뺀 대가 없어져도 보낼 곳이 없으면 회복이 아니다.** 그렇다고 구간을
            // 열어 두면 지속 시간과 건수가 서로 다른 구간을 가리키고, 그 사이 다른
            // 대가 새로 빠져도 진입 줄이 안 나간다. 다른 문구로 닫는다.
            crowdedOut.exited().ifPresent(r -> log.warn(
                    "뺀 대가 없어졌는데 보낼 곳도 없다 — {}초 동안 {}건. 뒷단이 전부 "
                    + "포화라 빈 답이 나간다", r.elapsedSeconds(), r.swallowed()));
        } else {
            crowdedOut.exited().ifPresent(r -> log.info(
                    "배제해도 보낼 곳이 남는다 — {}초 동안 {}건", r.elapsedSeconds(),
                    r.swallowed()));
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
     * <b>보낼 곳이 없는가.</b> 목록이 비었는지로 보면 안 된다 — 여유 0 인 대는
     * 목록에 들어가지만 고르개가 안 고른다. 그 상태를 "후보가 있다" 로 읽으면
     * 되돌리기가 한 줄도 안 돌고, 뒷단이 멀쩡한데 빈 답이 나간다.
     */
    private boolean noneUsable(List<RoutingCandidate> candidates) {
        return candidates.stream().noneMatch(RoutingCandidate::eligible);
    }

    /**
     * 인스턴스 수가 가정한 범위 밖이면 알린다. <b>구간의 시작만 찍는다</b> —
     * 요청마다 찍으면 초당 수천 줄에 정작 봐야 할 것이 묻힌다.
     *
     * <p><b>자동으로 전략을 안 바꾼다.</b> 인스턴스 수로 전환하면 임계 근처에서
     * 진동하고 — 롤링 배포가 정확히 그 구간을 지난다 — 어느 구간이 무슨 모드였는지가
     * 대시보드에 안 남아 장애 분석이 막힌다.
     */
    private void watchInstanceCount(int instances) {
        InstanceCountBand band = InstanceCountBand.of(instances);
        if (band.withinAssumption()) {
            lastBand.set(band);
            outsideAssumption.exited().ifPresent(r -> log.info(
                    "인스턴스 수가 가정 안으로 돌아왔다 — {}초 동안 {}건",
                    NANOSECONDS.toSeconds(r.elapsedNanos()), r.swallowed()));
            return;
        }
        // **아래에서 위로 뒤집히는 것도 새 사건이다.** 하나로 묶어 세면 처방이
        // 정반대가 됐는데 알람이 조용하고, 운영자는 처음 받은 안내를 그대로 들고
        // 있는다 — 대를 늘리라는 말과 줄이라는 말이 뒤바뀐 채로다.
        boolean crossed = lastBand.getAndSet(band) != band;
        if (outsideAssumption.entered() || crossed) {
            log.warn("라우팅 가정 밖 — {}", band.describe(instances));
        }
    }

    /**
     * <b>못 읽으면 후보에서 뺀다.</b> 기본값을 주면 여유를 모르는 대가 정상
     * 비율만큼 받는다 — 여유에 맞춰 보내겠다는 목적이 무너진다.
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
