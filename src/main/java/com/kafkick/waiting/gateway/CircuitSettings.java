package com.kafkick.waiting.gateway;

import java.time.Duration;

/**
 * 서킷 설정값을 검증한다.
 *
 * <p>검증을 밖에 둔다. 압축 생성자에서 부를 수 있는 것은 정적뿐이라, 안에 두면
 * 그 자리에서만 쓰이는 정적 메서드가 생긴다 ({@code GatewayRoutes.Backend} 와
 * 같은 이유다).
 */
final class CircuitSettings {

    /** 설정 키의 앞부분. 메시지에 실어 어디를 고쳐야 하는지 바로 보이게 한다. */
    private static final String PREFIX = "waiting.backend.circuit.";

    private CircuitSettings() {
    }

    /** 상태가 없지만 인스턴스다 — 검증이 늘면 여기 필드가 생긴다 (JS-13). */
    static CircuitSettings create() {
        return new CircuitSettings();
    }

    /**
     * <b>안 적으면 기동을 막는다.</b> 조용히 채우면 키를 하나 잘못 적어도 그
     * 기본값으로 떨어지고, 운영자는 자기가 적은 값으로 돈다고 믿는다.
     */
    void present(Object value, String key) {
        if (value == null) {
            throw new IllegalArgumentException(PREFIX + key + " 를 적어야 한다");
        }
    }

    /** <b>백분율이다.</b> 0.5 로 적으면 0% 로 반올림돼 첫 실패에 열린다. */
    void percent(float value, String key) {
        if (!(value > 0) || value > 100) {
            throw new IllegalArgumentException(
                    "%s%s 는 (0, 100] 범위의 백분율이어야 한다: %s".formatted(PREFIX, key, value));
        }
    }

    void positive(Duration value, String key) {
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(PREFIX + key + " 는 양수여야 한다: " + value);
        }
    }

    /** 0 이면 첫 한 건으로 서킷이 열리거나 회복 판정이 표본 없이 난다. */
    void atLeastOne(int value, String key) {
        if (value < 1) {
            throw new IllegalArgumentException(PREFIX + key + " 는 1 이상이어야 한다: " + value);
        }
    }

}
