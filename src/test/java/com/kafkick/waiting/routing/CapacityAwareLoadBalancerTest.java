package com.kafkick.waiting.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceOutliers;
import com.kafkick.waiting.domain.routing.InstanceChooser;
import com.kafkick.waiting.domain.routing.RoutingCandidate;
import com.kafkick.waiting.domain.routing.WeightedRoundRobin;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.DefaultRequest;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.RequestData;
import org.springframework.cloud.client.loadbalancer.RequestDataContext;
import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import reactor.core.publisher.Flux;

/**
 * 고르는 규칙은 도메인이 쥐고, 여기는 재료를 모아 넘긴다.
 *
 * <p><b>여유를 못 읽으면 후보에서 뺀다.</b> 기본값을 주면 여유를 모르는 대가
 * 정상 비율만큼 받는데, 그게 이 페이즈가 막으려는 것이다.
 */
@Tag("unit")
class CapacityAwareLoadBalancerTest {

    private static final Duration 수명 = Duration.ofSeconds(30);

    private static final Duration 배제_시간 = Duration.ofSeconds(10);

    private static final long 지금 = 1_800_000_000_000L;

    private final InFlightRegistry 레지스트리 = InFlightRegistry.of(수명);

    private final InstanceOutliers 배제기 =
            InstanceOutliers.of(3, 배제_시간, Duration.ofSeconds(60));

    private static ServiceInstance 인스턴스(String id, String credits) {
        DefaultServiceInstance instance =
                new DefaultServiceInstance(id, "coupon-service", "10.0.1.7", 8080, false);
        if (credits != null) {
            instance.getMetadata().put(SnapshotInstanceListSupplier.CREDITS, credits);
        }
        return instance;
    }

    private static ServiceInstanceListSupplier 목록(ServiceInstance... instances) {
        return new ServiceInstanceListSupplier() {
            @Override
            public String getServiceId() {
                return "coupon-service";
            }

            @Override
            public Flux<List<ServiceInstance>> get() {
                return Flux.just(List.of(instances));
            }
        };
    }

    /** 고른 것을 시험이 정한다. 무작위를 그대로 쓰면 무엇을 재는지가 흐려진다. */
    private static InstanceChooser 정해진(String... ids) {
        Deque<String> 남은 = new ArrayDeque<>(List.of(ids));
        return candidates -> {
            String 고를 = 남은.poll();
            return candidates.stream().filter(c -> c.instanceId().equals(고를)).findFirst();
        };
    }

    /** 상한이 안 물리는 값. 상한을 재는 시험만 따로 낮춰 쓴다. */
    private static final int 상한 = 1_000;

    /**
     * <b>연속으로 실패한 대는 후보에 안 든다.</b> 물린 표를 답이 끝날 때 놓으므로
     * 즉시 실패하는 대는 가장 한가해 보인다 — 거르지 않으면 그쪽으로 더 간다.
     */
    @Test
    @DisplayName("배제된_대는_후보에_안_든다")
    void 배제된_대는_후보에_안_든다() {
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-1", 지금);
        }

        Response<ServiceInstance> 고른것 = 고른다(candidates -> {
            assertThat(candidates).extracting(RoutingCandidate::instanceId)
                    .as("배제된 대는 도메인에 넘어가지도 않는다")
                    .containsExactly("be-2");
            return candidates.stream().findFirst();
        }, 목록(인스턴스("be-1", "100"), 인스턴스("be-2", "100")), 상한);

        assertThat(고른것.getServer().getInstanceId()).isEqualTo("be-2");

        // 있으면 be-1 을 고르고 없으면 남은 것을 고른다. 콜백 안이 아니라
        // 돌아온 값으로 본다 — 고르개가 아예 안 불려도 초록이 되지 않게.
        InstanceChooser be1을_원한다 = candidates -> candidates.stream()
                .filter(c -> c.instanceId().equals("be-1"))
                .findFirst()
                .or(() -> candidates.stream().findFirst());

        Response<ServiceInstance> 다시 = 고른다(be1을_원한다,
                목록(인스턴스("be-1", "100"), 인스턴스("be-2", "100")), 상한);
        assertThat(다시.getServer().getInstanceId())
                .as("be-1 을 원하는 고르개를 줘도 그리로 안 간다")
                .isEqualTo("be-2");
    }

    /**
     * <b>전부 앓아도 보낼 곳은 남긴다.</b> 배제가 전면 차단이 되면 열화된 대로라도
     * 보내는 것보다 나쁘다 — 계획서가 인스턴스별 서킷을 막았던 자리다.
     */
    @Test
    @DisplayName("전부_배제_대상이면_그대로_보낸다")
    void 전부_배제_대상이면_그대로_보낸다() {
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-1", 지금);
            배제기.failed("be-2", 지금);
        }

        Response<ServiceInstance> 고른것 = 고른다(정해진("be-1"),
                목록(인스턴스("be-1", "100"), 인스턴스("be-2", "100")), 상한);

        assertThat(고른것.hasServer()).isTrue();
        assertThat(고른것.getServer().getInstanceId()).isEqualTo("be-1");
    }

    /**
     * <b>기록을 안 걷으면 죽은 이름이 무한히 쌓인다.</b> 식별자는 재기동마다
     * 새로 오고, 남은 기록이 배제 지표까지 부풀려 신호를 거짓말로 만든다.
     */
    @Test
    @DisplayName("사라진_대의_배제_기록을_걷는다")
    void 사라진_대의_배제_기록을_걷는다() {
        배제기.failed("옛것", 지금);

        고른다(정해진("be-1"), 목록(인스턴스("be-1", "100")), 상한);

        assertThat(배제기.tracked()).doesNotContain("옛것");
    }

    /**
     * <b>배제와 상한이 만나면 보낼 곳이 0 이 될 수 있다.</b> 배제기는 자기끼리만
     * 세어 "전부는 안 뺀다" 를 지키는데, 남은 대가 상한에 닿아 있으면 그 약속이
     * 균형기에서 깨진다. 그때는 배제를 접는다.
     */
    @Test
    @DisplayName("배제가_보낼_곳을_없애면_배제를_접는다")
    void 배제가_보낼_곳을_없애면_배제를_접는다() {
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-1", 지금);
        }
        레지스트리.started("be-2", 지금);

        Response<ServiceInstance> 고른것 = 고른다(정해진("be-1"),
                목록(인스턴스("be-1", "100"), 인스턴스("be-2", "100")), 1);

        assertThat(고른것.hasServer()).as("앓는 대라도 보낸다").isTrue();
        assertThat(고른것.getServer().getInstanceId()).isEqualTo("be-1");
    }

    /**
     * <b>여유 0 은 램프가 되살리면 안 된다.</b> 하한이 0 을 1 로 올리면 스스로
     * 못 받는다고 말한 대가 후보로 돌아온다.
     */
    @Test
    @DisplayName("여유_0_인_대는_램프가_안_되살린다")
    void 여유_0_인_대는_램프가_안_되살린다() {
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-1", 지금);
        }
        long 풀린_때 = 지금 + 배제_시간.toMillis();

        List<Long> 여유 = new ArrayList<>();
        CapacityAwareLoadBalancer.of(목록(인스턴스("be-1", "0"), 인스턴스("be-2", "100")),
                        candidates -> {
                            candidates.stream().filter(c -> c.instanceId().equals("be-1"))
                                    .forEach(c -> 여유.add(c.credits()));
                            return candidates.stream()
                                    .filter(c -> c.instanceId().equals("be-2")).findFirst();
                        }, 레지스트리, 배제기, () -> 풀린_때, 상한)
                .choose((Request<?>) null).block();

        assertThat(여유).as("0 이 1 로 안 오른다").containsExactly(0L);
    }

    /**
     * <b>되돌아온 대가 전량을 받으면 안 된다.</b> 배제 동안 물린 건수가 0 이라
     * 부하율이 가장 낮은데, 그대로 두면 아직 아픈 대에 전부가 꽂히고 곧바로
     * 다시 빠진다 — 배제 시간 주기의 사각파다.
     */
    @Test
    @DisplayName("되돌아온_대의_여유가_램프_동안_줄어_보인다")
    void 되돌아온_대의_여유가_램프_동안_줄어_보인다() {
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-1", 지금);
        }
        long 풀린_때 = 지금 + 배제_시간.toMillis();

        List<Long> 여유 = new ArrayList<>();
        InstanceChooser 여유를_적는다 = candidates -> {
            candidates.stream().filter(c -> c.instanceId().equals("be-1"))
                    .forEach(c -> 여유.add(c.credits()));
            return candidates.stream().findFirst();
        };
        ServiceInstanceListSupplier 둘 = 목록(인스턴스("be-1", "100"), 인스턴스("be-2", "100"));

        CapacityAwareLoadBalancer.of(둘, 여유를_적는다, 레지스트리, 배제기, () -> 풀린_때, 상한)
                .choose((Request<?>) null).block();
        long 절반 = 풀린_때 + Duration.ofSeconds(30).toMillis();
        CapacityAwareLoadBalancer.of(둘, 여유를_적는다, 레지스트리, 배제기, () -> 절반, 상한)
                .choose((Request<?>) null).block();

        assertThat(여유).as("막 풀렸을 때는 거의 없고, 절반에서는 절반이다")
                .containsExactly(1L, 50L);
    }

    /** 이 요청이 이미 시도한 인스턴스들을 실은 요청. 재시도가 오는 모양이다. */
    private static Request<?> 이미_시도한(String... ids) {
        Map<String, Object> attrs = ids.length == 0 ? Map.of()
                : Map.of(RoutingAttributes.TRIED, Set.of(ids));
        return new DefaultRequest<>(new RequestDataContext(
                new RequestData(MockServerHttpRequest.post("/api/v1/coupons/c1/issue").build(),
                        attrs)));
    }

    /**
     * <b>재시도가 같은 대를 다시 고르면 넘긴 것이 아니다.</b> 표는 재구독 전에
     * 풀리므로 방금 실패한 대가 다시 가장 한가해 보이고, 3 대면 같은 죽은 대로
     * 갈 확률이 오히려 높다 — 한 요청이 그 대에 실패를 둘 찍는다.
     */
    @Test
    @DisplayName("이미_시도한_대는_재시도에서_뺀다")
    void 이미_시도한_대는_재시도에서_뺀다() {
        // 있으면 be-1 을 고르고 없으면 남은 것을 고른다. be-1 만 고집하는 고르개는
        // 후보에 없을 때 빈 답을 내므로 재려던 것을 못 잰다.
        InstanceChooser be1을_원한다 = candidates -> candidates.stream()
                .filter(c -> c.instanceId().equals("be-1"))
                .findFirst()
                .or(() -> candidates.stream().findFirst());

        Response<ServiceInstance> 고른것 = CapacityAwareLoadBalancer.of(
                목록(인스턴스("be-1", "100"), 인스턴스("be-2", "100")),
                be1을_원한다, 레지스트리, 배제기, () -> 지금, 상한)
                .choose(이미_시도한("be-1")).block();

        assertThat(고른것.getServer().getInstanceId())
                .as("be-1 을 원하는 고르개를 줘도 그리로 안 간다")
                .isEqualTo("be-2");

        // **첫 시도는 속성이 없다.** 프로덕션의 실제 모양이고, 그때는 아무도 안 뺀다.
        Response<ServiceInstance> 첫_시도 = CapacityAwareLoadBalancer.of(
                목록(인스턴스("be-1", "100"), 인스턴스("be-2", "100")),
                be1을_원한다, 레지스트리, 배제기, () -> 지금, 상한)
                .choose(이미_시도한()).block();
        assertThat(첫_시도.getServer().getInstanceId()).isEqualTo("be-1");
    }

    /**
     * 전부 시도했으면 뺄 것이 없다. 보낼 곳이 0 이 되는 것보다 다시 가는 편이 낫다.
     *
     * <p><b>한 대짜리 목록으로 만든다.</b> 재시도가 한 번뿐이라 한 요청이 적을 수
     * 있는 대는 최대 하나다 — 두 대를 다 적은 요청은 프로덕션에 없다. 도달 가능한
     * 모양은 시도 사이에 목록이 그 대만 남는 경우다.
     */
    /**
     * <b>되돌릴 때는 방금 실패한 대를 먼저 넣는다.</b> 배제는 세 번 연속 실패한
     * 근거가 있고 재시도 배제는 이번 한 번뿐이다 — 둘 중 하나만 접어야 하면
     * 근거가 얕은 쪽이다. 배제된 대로 도로 보내면 그 대의 실패가 또 쌓인다.
     */
    @Test
    @DisplayName("되돌릴_때_시도한_대를_먼저_넣는다")
    void 되돌릴_때_시도한_대를_먼저_넣는다() {
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-1", 지금);
        }
        // be-2 는 이번 시도에서 실패했고, be-3 은 상한에 닿아 후보가 아니다.
        레지스트리.started("be-3", 지금);

        Response<ServiceInstance> 고른것 = CapacityAwareLoadBalancer.of(
                목록(인스턴스("be-1", "100"), 인스턴스("be-2", "100"), 인스턴스("be-3", "100")),
                candidates -> candidates.stream().findFirst(),
                레지스트리, 배제기, () -> 지금, 1)
                .choose(이미_시도한("be-2")).block();

        assertThat(고른것.getServer().getInstanceId())
                .as("배제된 be-1 이 아니라 방금 실패한 be-2 로 돌아간다")
                .isEqualTo("be-2");
    }

    /**
     * <b>요청이 비어 있어도 안 터진다.</b> 기본 문맥으로 떨어지는 경로가 있어
     * 이 자리는 널을 받는다 — 그대로 부르면 고르는 자리가 터지고 라우팅이
     * 통째로 멎는다.
     */
    @Test
    @DisplayName("요청이_비어도_고른다")
    void 요청이_비어도_고른다() {
        Response<ServiceInstance> 고른것 = CapacityAwareLoadBalancer.of(
                목록(인스턴스("be-1", "100")), 정해진("be-1"),
                레지스트리, 배제기, () -> 지금, 상한)
                .choose(new DefaultRequest<>(new RequestDataContext(null))).block();

        assertThat(고른것.getServer().getInstanceId()).isEqualTo("be-1");
    }

    @Test
    @DisplayName("전부_시도했으면_안_뺀다")
    void 전부_시도했으면_안_뺀다() {
        Response<ServiceInstance> 고른것 = CapacityAwareLoadBalancer.of(
                목록(인스턴스("be-1", "100")),
                정해진("be-1"), 레지스트리, 배제기, () -> 지금, 상한)
                .choose(이미_시도한("be-1")).block();

        assertThat(고른것.hasServer()).isTrue();
        assertThat(고른것.getServer().getInstanceId()).isEqualTo("be-1");
    }

    private Response<ServiceInstance> 고른다(InstanceChooser chooser,
            ServiceInstanceListSupplier 목록, int cap) {
        return CapacityAwareLoadBalancer.of(목록, chooser, 레지스트리, 배제기, () -> 지금, cap)
                .choose((Request<?>) null).block();
    }

    private Response<ServiceInstance> 고른다(
            InstanceChooser chooser,
            ServiceInstanceListSupplier 목록) {
        return 고른다(chooser, 목록, 상한);
    }

    @Test
    @DisplayName("고른_인스턴스를_돌려준다")
    void 고른_인스턴스를_돌려준다() {
        Response<ServiceInstance> 답 = 고른다(정해진("be-2"),
                목록(인스턴스("be-1", "200"), 인스턴스("be-2", "40")));

        assertThat(답.hasServer()).isTrue();
        assertThat(답.getServer().getInstanceId()).isEqualTo("be-2");
    }

    /** <b>여유를 메타데이터에서 읽는다.</b> 안 읽으면 고르개가 부하율을 못 낸다. */
    @Test
    @DisplayName("여유를_후보에_실어_넘긴다")
    void 여유를_후보에_실어_넘긴다() {
        List<Long> 본_여유 = new ArrayList<>();
        고른다(candidates -> {
            candidates.forEach(c -> 본_여유.add(c.credits()));
            return candidates.stream().findFirst();
        }, 목록(인스턴스("be-1", "200"), 인스턴스("be-2", "40")));

        assertThat(본_여유).containsExactly(200L, 40L);
    }

    /** 여유를 못 읽으면 0 이다 — 고르개가 후보에서 뺀다 (9.3.6). */
    @Test
    @DisplayName("여유를_못_읽으면_0_으로_넘긴다")
    void 여유를_못_읽으면_0_으로_넘긴다() {
        List<Long> 본_여유 = new ArrayList<>();
        고른다(candidates -> {
            candidates.forEach(c -> 본_여유.add(c.credits()));
            return Optional.empty();
        }, 목록(인스턴스("be-1", null), 인스턴스("be-2", "abc"), 인스턴스("be-3", "-5")));

        assertThat(본_여유).containsExactly(0L, 0L, 0L);
    }

    /** 물려 있는 수도 같이 넘긴다. 이 값이 없으면 순수 가중 무작위가 된다. */
    @Test
    @DisplayName("물린_수를_후보에_실어_넘긴다")
    void 물린_수를_후보에_실어_넘긴다() {
        레지스트리.started("be-1", 지금);
        레지스트리.started("be-1", 지금);
        List<Integer> 본_물린 = new ArrayList<>();

        고른다(candidates -> {
            candidates.forEach(c -> 본_물린.add(c.inFlight()));
            return candidates.stream().findFirst();
        }, 목록(인스턴스("be-1", "200"), 인스턴스("be-2", "200")));

        assertThat(본_물린).containsExactly(2, 0);
    }

    /**
     * <b>보낼 곳이 없으면 명확하게 실패한다</b> (9.3.4). 아무 대나 고르면 여유
     * 0 인 대가 무너진다.
     */
    @Test
    @DisplayName("보낼_곳이_없으면_빈_답이다")
    void 보낼_곳이_없으면_빈_답이다() {
        assertThat(고른다(정해진(), 목록()).hasServer()).isFalse();
        assertThat(고른다(candidates -> Optional.empty(), 목록(인스턴스("be-1", "0"))).hasServer())
                .isFalse();
    }

    /**
     * 고르개가 목록에 없는 것을 돌려주면 빈 답이다. 그대로 믿으면 없는 주소로 보낸다.
     */
    @Test
    @DisplayName("목록에_없는_것을_고르면_빈_답이다")
    void 목록에_없는_것을_고르면_빈_답이다() {
        assertThat(고른다(candidates -> Optional.of(RoutingCandidate.of("없는-것", 100, 0)),
                목록(인스턴스("be-1", "200"))).hasServer()).isFalse();
    }

    /**
     * <b>사라지고 비어 있는 인스턴스의 카운터를 지운다.</b>
     *
     * <p>식별자가 재기동마다 새로 오므로 안 지우면 배포를 거듭할수록 자란다.
     */
    @Test
    @DisplayName("사라지고_비었으면_카운터를_지운다")
    void 사라지고_비었으면_카운터를_지운다() {
        레지스트리.started("옛것", 지금).finished();
        레지스트리.started("be-1", 지금);

        고른다(정해진("be-1"), 목록(인스턴스("be-1", "200")));

        assertThat(레지스트리.instances()).containsExactly("be-1");
    }

    /**
     * <b>목록에서 잠깐 빠져도 물려 있으면 안 지운다.</b>
     *
     * <p>지우면 그 대가 돌아온 순간 부하가 0 으로 보여, 아직 처리 중인데도
     * 가장 한가한 대로 읽혀 몰아 보낸다.
     */
    @Test
    @DisplayName("잠깐_빠진_대의_카운터는_남긴다")
    void 잠깐_빠진_대의_카운터는_남긴다() {
        레지스트리.started("잠깐빠짐", 지금);

        고른다(정해진("be-1"), 목록(인스턴스("be-1", "200")));

        assertThat(레지스트리.count("잠깐빠짐", 지금))
                .as("돌아왔을 때 0 으로 보이면 몰아 보낸다")
                .isEqualTo(1);
    }

    /** 두 전략이 다 붙는다 (R-9). 배선이 하나만 받으면 그 측정을 할 수가 없다. */
    @Test
    @DisplayName("라운드로빈도_붙는다")
    void 라운드로빈도_붙는다() {
        CapacityAwareLoadBalancer 균형기 = CapacityAwareLoadBalancer.of(
                목록(인스턴스("be-1", "3"), 인스턴스("be-2", "1")),
                WeightedRoundRobin.create(), 레지스트리, 배제기, () -> 지금, 상한);

        List<String> 고른것 = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            고른것.add(균형기.choose((Request<?>) null).block().getServer().getInstanceId());
        }

        assertThat(고른것).filteredOn("be-1"::equals).hasSize(3);
    }

    /**
     * <b>상한에 닿은 대는 후보가 아니다</b> (G9.13).
     *
     * <p>느려진 한 대로 간 요청이 무한정 쌓이면 그 한 대가 게이트웨이의 커넥션을
     * 다 붙잡는다 — 멀쩡한 대로 갈 커넥션까지 가져간다.
     */
    @Test
    @DisplayName("상한에_닿은_대는_후보에서_뺀다")
    void 상한에_닿은_대는_후보에서_뺀다() {
        레지스트리.started("느린", 지금);
        레지스트리.started("느린", 지금);
        List<String> 본_후보 = new ArrayList<>();

        고른다(candidates -> {
            candidates.forEach(c -> 본_후보.add(c.instanceId()));
            return candidates.stream().findFirst();
        }, 목록(인스턴스("느린", "200"), 인스턴스("멀쩡", "200")), 2);

        assertThat(본_후보).containsExactly("멀쩡");
    }

    /** 상한 아래면 그대로 후보다. 상한이 늘 물면 라우팅이 통째로 막힌다. */
    @Test
    @DisplayName("상한_아래면_후보다")
    void 상한_아래면_후보다() {
        레지스트리.started("느린", 지금);
        List<String> 본_후보 = new ArrayList<>();

        고른다(candidates -> {
            candidates.forEach(c -> 본_후보.add(c.instanceId()));
            return candidates.stream().findFirst();
        }, 목록(인스턴스("느린", "200")), 2);

        assertThat(본_후보).containsExactly("느린");
    }

    /** 전부 상한에 닿으면 빈 답이다 — 아무 대나 고르면 그 대가 무너진다. */
    @Test
    @DisplayName("전부_상한이면_빈_답이다")
    void 전부_상한이면_빈_답이다() {
        레지스트리.started("be-1", 지금);

        assertThat(고른다(정해진("be-1"), 목록(인스턴스("be-1", "200")), 1).hasServer())
                .isFalse();
    }

    @Test
    @DisplayName("상한이_양수가_아니면_거절한다")
    void 상한이_양수가_아니면_거절한다() {
        assertThatThrownBy(() -> CapacityAwareLoadBalancer.of(
                목록(), 정해진(), 레지스트리, 배제기, () -> 지금, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
