package com.kafkick.waiting.adapter.redis;

import com.kafkick.waiting.control.ControlPlaneProperties;
import java.time.Duration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisProperties;

/**
 * 커넥션 설정이 제어 평면의 시간 예산 안에 있는지 확인한다.
 *
 * <p>주석으로만 적어 두면 값을 바꾸는 사람이 안 읽는다. <b>어긋나면 안 뜨게</b>
 * 해야 배분이 멎는 사고로 배우지 않는다. 예산은 제어 평면 설정에서 읽는다 — 상수로
 * 따로 들면 한쪽만 바뀐 날 검사가 옛 값을 지킨다.
 */
public final class RedisTimeBudget {

    private final Duration commandTimeout;
    private final Duration connectTimeout;
    private final ControlPlaneProperties control;

    private RedisTimeBudget(Duration commandTimeout, Duration connectTimeout,
            ControlPlaneProperties control) {
        this.commandTimeout = commandTimeout;
        this.connectTimeout = connectTimeout;
        this.control = control;
    }

    /** 설정에서 만든다. 값이 없으면 그대로 담는다 — 검증은 {@link #verify()} 가 한다. */
    public static RedisTimeBudget of(DataRedisProperties properties,
            ControlPlaneProperties control) {
        return new RedisTimeBudget(properties.getTimeout(), properties.getConnectTimeout(),
                control);
    }

    /** 예산을 벗어나면 {@link IllegalStateException}. 기동을 막는 것이 목적이다. */
    public void verify() {
        // 명령이 틱보다 오래 붙들면 그 틱의 배분이 밀린다.
        require(commandTimeout, control.scheduler().tick(), "timeout", "틱");
        // 연결이 리스보다 오래 걸리면 그 사이 리더십을 잃는다.
        require(connectTimeout, control.leader().lease(), "connect-timeout", "리스");
        // **명령 상한이 연장 시도보다 길면 안 된다.** 그 사이의 지연에서는 명령은 되는데
        // 연장만 끊겨, 리스가 지나 리더가 0 이 되고 다시 잡는 시도도 같이 끊긴다.
        if (commandTimeout.compareTo(control.leader().attempt()) > 0) {
            throw new IllegalStateException(
                    "spring.data.redis.timeout 은 리더 연장 시도(%s) 이하여야 한다: %s"
                            .formatted(control.leader().attempt(), commandTimeout));
        }
    }

    private void require(Duration actual, Duration budget, String key, String what) {
        // 0 이나 음수는 예산 안에 들어오지만 값으로는 성립하지 않는다.
        // 0 은 즉시 실패, 음수는 드라이버에 따라 무한 대기가 된다 — 둘 다
        // "타임아웃을 설정했다" 는 착각만 남기고 아무것도 안 막는다.
        if (actual == null || actual.isZero() || actual.isNegative()) {
            throw new IllegalStateException(
                    "spring.data.redis.%s 는 양수여야 한다: %s".formatted(key, actual));
        }
        if (actual.compareTo(budget) >= 0) {
            throw new IllegalStateException(
                    "spring.data.redis.%s 는 %s(%s)보다 짧아야 한다: %s"
                            .formatted(key, what, budget, actual));
        }
    }
}
