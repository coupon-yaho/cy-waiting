package com.kafkick.waiting.routing;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 가용량 비율 라우팅의 노브.
 *
 * @param enabled       끄면 단일 주소로 돌아간다. <b>롤백 수단이다</b> — 라우팅이
 *                      의심스러우면 이 한 줄로 되돌린다
 * @param serviceId     {@code lb://} 뒤에 오는 이름
 * @param strategy      {@code p2c} 또는 {@code round-robin}. 어느 쪽이 나은지는
 *                      실측으로 정할 문제라 코드에 하나만 박아 두면 그 측정을 못 한다
 * @param inFlightTtl   물린 표가 살 수 있는 최대 시간. 감소를 놓쳐도 누수가 유계다
 * @param coldStartRamp 기동 직후 보고된 값을 초기값으로 쓰는 구간
 * @param perInstanceCap 인스턴스 하나에 동시에 물릴 수 있는 요청 수.
 *                       <b>느려진 한 대가 커넥션을 독식하지 못하게 한다</b>
 * @param outlierFailures 연속 실패가 이만큼이면 그 인스턴스를 후보에서 뺀다
 * @param outlierEjectFor 뺀 뒤 이만큼 지나면 다시 후보로 돌린다
 * @param allowedDestinations 연결해도 되는 목적지. <b>켤 때는 필수다</b>
 * @param allowedPorts   연결해도 되는 포트. 호스트만 보면 반쪽이다
 */
@ConfigurationProperties("waiting.routing")
public record RoutingProperties(boolean enabled, String serviceId, String strategy,
        Duration inFlightTtl, Duration coldStartRamp, Integer perInstanceCap,
        Integer outlierFailures, Duration outlierEjectFor, List<String> allowedDestinations,
        List<Integer> allowedPorts) {

    /** 무작위 둘 중 여유 대비 덜 찬 쪽. <b>기본이 아니다</b> — 비율에서 밀린다. */
    public static final String P2C = "p2c";

    /**
     * 게이트웨이 자신의 포트를 목적지로 안 받는다.
     *
     * <p>같은 호스트의 뒷단이 실제 배치라 루프백은 목적지로 둔다. 그러면 자기 자신을
     * 부르는 것을 가르는 것은 포트뿐인데, 뒷단이 흔히 쓰는 8080 이 이 게이트웨이의
     * 기본 포트이기도 하다 — 겹치면 발급 요청이 같은 라우트로 되돌아온다.
     *
     * @throws IllegalStateException 허용 포트가 자기 포트를 담고 있을 때
     */
    public void rejectSelfPorts(Integer... ownPorts) {
        for (Integer own : ownPorts) {
            // 0 은 무작위 배정이라 이 값으로는 자기 포트를 못 안다.
            if (own != null && own > 0 && allowedPorts.contains(own)) {
                throw new IllegalStateException(
                        "허용 포트가 게이트웨이 자신의 포트를 담고 있다 — 발급이 자기 자신으로 돈다: "
                                + own);
            }
        }
    }

    /** 여유 비율대로 결정적으로 돈다. 3~5 대 규모에서 더 정확하고, <b>기본값이다</b>. */
    public static final String ROUND_ROBIN = "round-robin";

    public RoutingProperties {
        allowedDestinations = allowedDestinations == null ? List.of()
                : allowedDestinations.stream()
                        .filter(entry -> entry != null && !entry.isBlank()).toList();
        allowedPorts = allowedPorts == null ? List.of() : List.copyOf(allowedPorts);
        // **켤 때만 막는다.** 꺼진 배포에서까지 요구하면 라우팅과 무관한 배포가
        // 이 설정 때문에 안 뜬다. 켜는 순간은 안 봐주고 끊는다 — 목록이 비었다는
        // 것이 "아무 데나 보내도 된다" 로 읽히면 그 배포가 그대로 통로가 된다.
        if (enabled && allowedDestinations.isEmpty()) {
            throw new IllegalArgumentException(
                    "라우팅을 켜려면 allowed-destinations 를 적어야 한다");
        }
        if (enabled && allowedPorts.isEmpty()) {
            throw new IllegalArgumentException(
                    "라우팅을 켜려면 allowed-ports 를 적어야 한다");
        }
        serviceId = serviceId == null || serviceId.isBlank() ? "coupon-service" : serviceId;
        // **기본은 라운드로빈이다.** P2C 를 고른 원래 이유는 게이트웨이 여러 대가
        // 같은 인스턴스로 몰린다는 것이었는데, 두 대를 띄워 재 보니 안 몰렸고
        // P2C 가 오히려 비율에서 밀렸다.
        strategy = strategy == null || strategy.isBlank() ? ROUND_ROBIN : strategy;
        inFlightTtl = inFlightTtl == null ? Duration.ofSeconds(30) : inFlightTtl;
        coldStartRamp = coldStartRamp == null ? Duration.ofSeconds(60) : coldStartRamp;
        // **아직 가정이다.** 노드당 예산과 뒷단 지연에서 역산해야 할 값인데
        // 그 실측이 없다. Phase 10 의 부하 게이트에서 채운다.
        perInstanceCap = perInstanceCap == null ? 200 : perInstanceCap;
        // **모르는 전략을 기본값으로 접지 않는다.** 오타 하나로 다른 전략이
        // 돌면 그 배포의 측정이 통째로 다른 것을 잰 것이 된다.
        if (!P2C.equals(strategy) && !ROUND_ROBIN.equals(strategy)) {
            throw new IllegalArgumentException(
                    "strategy 는 %s 또는 %s 여야 한다: %s".formatted(P2C, ROUND_ROBIN, strategy));
        }
        if (inFlightTtl.isNegative() || inFlightTtl.isZero()) {
            throw new IllegalArgumentException("inFlightTtl 은 양수여야 한다: " + inFlightTtl);
        }
        if (coldStartRamp.isNegative()) {
            throw new IllegalArgumentException(
                    "coldStartRamp 는 0 이상이어야 한다: " + coldStartRamp);
        }
        if (perInstanceCap < 1) {
            throw new IllegalArgumentException(
                    "perInstanceCap 은 1 이상이어야 한다: " + perInstanceCap);
        }
        // **기본을 셋으로 둔다.** 하나면 어쩌다 난 오류 한 건에 인스턴스가 빠지고,
        // 그 몫이 남은 대로 몰려 멀쩡한 대까지 밀려 넘어진다. 더 낮은 값도 받는다 —
        // 튜닝과 시험이 막히면 실측으로 정할 길이 없어진다.
        outlierFailures = outlierFailures == null ? 3 : outlierFailures;
        if (outlierFailures < 1) {
            throw new IllegalArgumentException(
                    "outlierFailures 는 1 이상이어야 한다: " + outlierFailures);
        }
        // **응답 상한보다 길어야 한다.** 멎은 대로 간 요청은 그 상한이 지나야 실패로
        // 관측되는데, 배제가 먼저 풀리면 직전 실패가 세어지기도 전에 그 대가 돌아온다.
        // 상한이 다른 설정에 있어 BackendTimeoutBudgetTest 가 그 관계를 문다.
        outlierEjectFor = outlierEjectFor == null ? Duration.ofSeconds(15) : outlierEjectFor;
        if (outlierEjectFor.isNegative() || outlierEjectFor.isZero()) {
            throw new IllegalArgumentException(
                    "outlierEjectFor 는 양수여야 한다: " + outlierEjectFor);
        }
    }
}
