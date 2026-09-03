package com.kafkick.waiting.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceChooser;
import com.kafkick.waiting.domain.routing.RoutingCandidate;
import com.kafkick.waiting.domain.routing.WeightedRoundRobin;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.Request;
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

    private static final long 지금 = 1_800_000_000_000L;

    private final InFlightRegistry 레지스트리 = InFlightRegistry.of(수명);

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

    private Response<ServiceInstance> 고른다(InstanceChooser chooser,
            ServiceInstanceListSupplier 목록, int cap) {
        return CapacityAwareLoadBalancer.of(목록, chooser, 레지스트리, () -> 지금, cap)
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
                WeightedRoundRobin.create(), 레지스트리, () -> 지금, 상한);

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
                목록(), 정해진(), 레지스트리, () -> 지금, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
