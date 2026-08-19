package com.kafkick.waiting.adapter.redis;

import java.time.Duration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisProperties;

/**
 * 커넥션 설정이 제어 평면의 시간 예산 안에 있는지 확인한다.
 *
 * <p>주석으로만 적어 두면 값을 바꾸는 사람이 안 읽는다. <b>어긋나면 안 뜨게</b>
 * 해야 배분이 멎는 사고로 배우지 않는다.
 */
public final class RedisTimeBudget {

    /** 스케줄러 틱. 명령이 이보다 오래 붙들면 그 틱의 배분이 밀린다. */
    static final Duration TICK = Duration.ofSeconds(1);

    /** 리더 리스. 연결이 이보다 오래 걸리면 그 사이 리더십을 잃는다. */
    static final Duration LEASE = Duration.ofSeconds(2);

    private final Duration commandTimeout;
    private final Duration connectTimeout;

    private RedisTimeBudget(Duration commandTimeout, Duration connectTimeout) {
        this.commandTimeout = commandTimeout;
        this.connectTimeout = connectTimeout;
    }

    /** 설정에서 만든다. 값이 없으면 그대로 담는다 — 검증은 {@link #verify()} 가 한다. */
    public static RedisTimeBudget of(DataRedisProperties properties) {
        return new RedisTimeBudget(properties.getTimeout(), properties.getConnectTimeout());
    }

    /** 예산을 벗어나면 {@link IllegalStateException}. 기동을 막는 것이 목적이다. */
    public void verify() {
        require(commandTimeout, TICK, "timeout", "틱");
        require(connectTimeout, LEASE, "connect-timeout", "리스");
    }

    private void require(Duration actual, Duration budget, String key, String what) {
        if (actual == null || actual.compareTo(budget) >= 0) {
            throw new IllegalStateException(
                    "spring.data.redis.%s 는 %s(%s)보다 짧아야 한다: %s"
                            .formatted(key, what, budget, actual));
        }
    }
}
