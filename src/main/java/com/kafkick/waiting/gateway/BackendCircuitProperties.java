package com.kafkick.waiting.gateway;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 뒷단 서킷의 설정값.
 *
 * <p><b>코드가 아니라 설정에 둔다.</b> 실측 전에는 맞는 값을 모른다. 코드에
 * 박으면 재조정마다 배포가 필요하고, 배포가 필요하면 장애 중에는 못 고친다 —
 * 정작 고쳐야 할 때가 그때다.
 *
 * <p><b>기본값을 안 둔다.</b> 코드에도 값이 있으면 yml 의 키를 하나 잘못 적어도
 * 조용히 그 기본값으로 떨어지고, 기동은 성공한다. 값은 {@code application.yml}
 * 한 곳에 있고 여기서는 검증만 한다. 근거는 각 필드에 적는다.
 *
 * @param slidingWindowSize 판정에 쓰는 창의 길이. <b>건수가 아니라 시간이다</b> —
 *     100K RPS 에서 건수 100 은 수 ms 분량이라 GC 한 번에도 열린다
 * @param minimumNumberOfCalls 이만큼 안 모이면 안 연다. 오픈 직후 첫 몇 건으로
 *     전 노드가 동시에 서킷을 여는 것을 막는다
 * @param failureRateThreshold 실패 비율(%). <b>백분율이다</b>
 * @param timeout 뒷단 응답을 기다리는 상한 (6.2)
 * @param slowCallDurationThreshold 이보다 오래 걸리면 느린 호출로 센다.
 *     <b>타임아웃보다 낮아야 한다</b> — 안 그러면 타임아웃이 먼저 끊어 느린
 *     호출이 한 건도 안 집계되고, 운영자는 켰다고 믿는다
 * @param slowCallRateThreshold 느린 호출 비율(%)
 * @param waitDurationInOpenState 열린 채 기다리는 시간. 회복이 늦어도 두 번째
 *     시도에 판정되도록 짧게 둔다 (G8.12)
 * @param permittedNumberOfCallsInHalfOpenState 회복을 판정할 표본 수.
 *     <b>유입 억제는 이 값이 아니다</b> — 판정 쪽이 유효 credit 을 조여서 한다
 */
@ConfigurationProperties("waiting.backend.circuit")
public record BackendCircuitProperties(
        Duration slidingWindowSize,
        Integer minimumNumberOfCalls,
        Float failureRateThreshold,
        Duration timeout,
        Duration slowCallDurationThreshold,
        Float slowCallRateThreshold,
        Duration waitDurationInOpenState,
        Integer permittedNumberOfCallsInHalfOpenState) {

    public BackendCircuitProperties {
        // **기본값을 안 둔다.** 코드에도 값이 있으면 yml 의 키를 하나 잘못 적어도
        // 조용히 그 기본값으로 떨어진다 — 기동은 성공하고, 운영자는 자기가 적은
        // 값으로 돌고 있다고 믿는다. 실제로 그 오타를 시험이 못 잡았다.
        //
        // 값이 한 곳에만 있으면 오타가 곧 null 이고, null 이면 여기서 멎는다.
        requireSet(slidingWindowSize, "sliding-window-size");
        requireSet(minimumNumberOfCalls, "minimum-number-of-calls");
        requireSet(failureRateThreshold, "failure-rate-threshold");
        requireSet(timeout, "timeout");
        requireSet(slowCallDurationThreshold, "slow-call-duration-threshold");
        requireSet(slowCallRateThreshold, "slow-call-rate-threshold");
        requireSet(waitDurationInOpenState, "wait-duration-in-open-state");
        requireSet(permittedNumberOfCallsInHalfOpenState,
                "permitted-number-of-calls-in-half-open-state");

        requirePercent(failureRateThreshold, "failure-rate-threshold");
        requirePercent(slowCallRateThreshold, "slow-call-rate-threshold");
        requirePositive(slidingWindowSize, "sliding-window-size");
        requirePositive(timeout, "timeout");
        requirePositive(waitDurationInOpenState, "wait-duration-in-open-state");
        requirePositive(slowCallDurationThreshold, "slow-call-duration-threshold");
        requireAtLeastOne(minimumNumberOfCalls, "minimum-number-of-calls");
        requireAtLeastOne(permittedNumberOfCallsInHalfOpenState,
                "permitted-number-of-calls-in-half-open-state");
        // **켰다고 믿게 두지 않는다.** 타임아웃이 먼저 끊으면 느린 호출이 한 건도
        // 안 집계되고, 그 설정은 있으나 마나가 된다 (6.1.8).
        if (slowCallDurationThreshold.compareTo(timeout) >= 0) {
            throw new IllegalArgumentException(
                    "느림 임계는 타임아웃보다 낮아야 한다: %s >= %s"
                            .formatted(slowCallDurationThreshold, timeout));
        }
    }

    /** 안 적으면 기동을 막는다. 조용히 채우면 오타가 그대로 운영에 나간다. */
    private static void requireSet(Object value, String key) {
        if (value == null) {
            throw new IllegalArgumentException(
                    "waiting.backend.circuit." + key + " 를 적어야 한다");
        }
    }

    private static void requireAtLeastOne(int value, String key) {
        if (value < 1) {
            throw new IllegalArgumentException(key + " 는 1 이상이어야 한다: " + value);
        }
    }

    /** <b>백분율이다.</b> 0.5 로 적으면 0% 로 반올림돼 첫 실패에 열린다. */
    private static void requirePercent(float value, String what) {
        if (!(value > 0) || value > 100) {
            throw new IllegalArgumentException(
                    "%s 는 (0, 100] 범위의 백분율이어야 한다: %s".formatted(what, value));
        }
    }

    private static void requirePositive(Duration value, String what) {
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(what + " 는 양수여야 한다: " + value);
        }
    }
}
