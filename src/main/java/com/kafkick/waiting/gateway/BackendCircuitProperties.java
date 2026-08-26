package com.kafkick.waiting.gateway;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 뒷단 서킷의 설정값.
 *
 * <p><b>코드가 아니라 설정에 둔다.</b> 실측 전에는 맞는 값을 모르고, 코드에 박으면
 * 재조정마다 배포가 필요하다. 기본값도 안 둔다 — 있으면 yml 의 오타가 조용히 그
 * 값으로 떨어진다. 값과 근거는 {@code application.yml} 에 있고 여기서는 검증만 한다.
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
        // 조용히 그 기본값으로 떨어지고, 기동은 성공한다. 실제로 그 오타를 시험이
        // 못 잡았다. 값이 한 곳에만 있으면 오타가 곧 null 이고 여기서 멎는다.
        CircuitSettings check = CircuitSettings.create();
        check.present(slidingWindowSize, "sliding-window-size");
        check.present(minimumNumberOfCalls, "minimum-number-of-calls");
        check.present(failureRateThreshold, "failure-rate-threshold");
        check.present(timeout, "timeout");
        check.present(slowCallDurationThreshold, "slow-call-duration-threshold");
        check.present(slowCallRateThreshold, "slow-call-rate-threshold");
        check.present(waitDurationInOpenState, "wait-duration-in-open-state");
        check.present(permittedNumberOfCallsInHalfOpenState,
                "permitted-number-of-calls-in-half-open-state");

        check.percent(failureRateThreshold, "failure-rate-threshold");
        check.percent(slowCallRateThreshold, "slow-call-rate-threshold");
        check.positive(slidingWindowSize, "sliding-window-size");
        check.positive(timeout, "timeout");
        check.positive(waitDurationInOpenState, "wait-duration-in-open-state");
        check.positive(slowCallDurationThreshold, "slow-call-duration-threshold");
        check.atLeastOne(minimumNumberOfCalls, "minimum-number-of-calls");
        check.atLeastOne(permittedNumberOfCallsInHalfOpenState,
                "permitted-number-of-calls-in-half-open-state");
        check.slowBeforeTimeout(slowCallDurationThreshold, timeout);
    }
}
