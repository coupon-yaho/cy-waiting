package com.kafkick.waiting.control;

import java.time.Duration;

/**
 * 설정값 검증에 쓰는 공통 조건.
 *
 * <p>중첩 레코드마다 같은 검사를 복사하면 하나를 고칠 때 나머지가 갈린다.
 */
final class Durations {

    private Durations() {
    }

    static void requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("%s 는 양수여야 한다: %s".formatted(name, value));
        }
    }
}
