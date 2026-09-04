package com.kafkick.waiting.routing;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 가용량 비율 라우팅의 노브.
 *
 * @param enabled       끄면 단일 주소로 돌아간다. <b>롤백 수단이다</b> — 라우팅이
 *                      의심스러우면 이 한 줄로 되돌린다
 * @param serviceId     {@code lb://} 뒤에 오는 이름
 * @param strategy      {@code p2c} 또는 {@code round-robin} (R-9). 어느 쪽이 나은지는
 *                      실측으로 정할 문제라 코드에 하나만 박아 두면 그 측정을 못 한다
 * @param inFlightTtl   물린 표가 살 수 있는 최대 시간. 감소를 놓쳐도 누수가 유계다 (R-8)
 * @param coldStartRamp 기동 직후 보고된 값을 씨앗으로 쓰는 구간 (G9.12)
 * @param perInstanceCap 인스턴스 하나에 동시에 물릴 수 있는 요청 수 (G9.13).
 *                       <b>느려진 한 대가 커넥션을 독식하지 못하게 한다</b>
 * @param outlierFailures 연속 실패가 이만큼이면 그 인스턴스를 후보에서 뺀다
 * @param outlierEjectFor 뺀 뒤 이만큼 지나면 다시 후보로 돌린다
 */
@ConfigurationProperties("waiting.routing")
public record RoutingProperties(boolean enabled, String serviceId, String strategy,
        Duration inFlightTtl, Duration coldStartRamp, Integer perInstanceCap,
        Integer outlierFailures, Duration outlierEjectFor) {

    /** 무작위 둘 중 여유 대비 덜 찬 쪽. 쏠림을 깨는 것이 이 규모에서 P2C 를 쓰는 이유다. */
    public static final String P2C = "p2c";

    /** 여유 비율대로 결정적으로 돈다. 3~5 대 규모에서 더 정확하다. */
    public static final String ROUND_ROBIN = "round-robin";

    public RoutingProperties {
        serviceId = serviceId == null || serviceId.isBlank() ? "coupon-service" : serviceId;
        strategy = strategy == null || strategy.isBlank() ? P2C : strategy;
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
        // **응답 상한보다 길어야 한다.** 멎은 대로 간 요청은 그 상한이 지나야
        // 실패로 관측되는데, 배제가 먼저 풀리면 직전 실패가 세어지기도 전에 그
        // 대가 후보로 돌아온다. 상한 12초 위로 잡는다.
        outlierEjectFor = outlierEjectFor == null ? Duration.ofSeconds(15) : outlierEjectFor;
        if (outlierEjectFor.isNegative() || outlierEjectFor.isZero()) {
            throw new IllegalArgumentException(
                    "outlierEjectFor 는 양수여야 한다: " + outlierEjectFor);
        }
    }
}
