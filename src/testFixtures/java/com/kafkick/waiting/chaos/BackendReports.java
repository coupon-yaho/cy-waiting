package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 백엔드 자기보고를 조작한다 (4.0.4) — 콜드 복귀와 낡은 보고를 만든다.
 *
 * <p>값은 {@code "<가용량>:<보고시각초>"} 다. <b>시각을 값에 담는다</b> — TTL 은
 * 지우는 시점이지 신선한 시점이 아니라, TTL 만 믿으면 낡은 보고가 합산된다.
 */
public final class BackendReports {

    /** 키는 {@link RedisKeys} 에서만 만든다 (RD-3). */
    public static final String KEY = RedisKeys.CAPACITY;

    private final StatefulRedisConnection<String, String> redis;
    private final Duration 신선도;
    private final Clock clock;
    private final Map<String, Long> 처음_본_시각 = new LinkedHashMap<>();

    public BackendReports(StatefulRedisConnection<String, String> redis, Duration 신선도) {
        this(redis, 신선도, Clock.systemUTC());
    }

    /** 시계를 주입받는다 (TS-4). 기본은 실물 레디스와 같은 축의 실제 시계다. */
    public BackendReports(StatefulRedisConnection<String, String> redis, Duration 신선도,
            Clock clock) {
        this.redis = redis;
        this.신선도 = 신선도;
        this.clock = clock;
    }

    public void 보고한다(String instanceId, long 가용량) {
        심는다(instanceId, 가용량, 지금());
    }

    /** 항목은 신선한데 값만 크다 — 재기동 직후 과대 보고 (F6). */
    public void 콜드로_복귀한다(String instanceId, long 부풀린_가용량) {
        처음_본_시각.put(instanceId, 지금());
        심는다(instanceId, 부풀린_가용량, 지금());
    }

    public void 낡은_보고를_심는다(String instanceId, long 가용량, Duration 얼마나_전) {
        심는다(instanceId, 가용량, 지금() - 얼마나_전.toSeconds());
    }

    /** 형식이 깨진 값. 하나가 깨졌다고 나머지를 버리면 전면 차단이 된다. */
    public void 깨진_보고를_심는다(String instanceId) {
        redis.sync().hset(KEY, instanceId, "그건-숫자가-아니다");
    }

    /** 처음 관측된 시각. 게이트웨이 측 램프업(A-13)의 기준이다. */
    public long 처음_관측된_시각(String instanceId) {
        return 처음_본_시각.getOrDefault(instanceId, 0L);
    }

    public Map<String, Long> 신선한_보고() {
        long 하한 = 지금() - 신선도.toSeconds();
        Map<String, Long> 결과 = new LinkedHashMap<>();
        redis.sync().hgetall(KEY).forEach((instanceId, raw) -> {
            String[] parts = raw.split(":", 2);
            if (parts.length != 2) {
                return;   // 깨진 항목은 건너뛴다. 나머지 판정은 살린다
            }
            try {
                if (Long.parseLong(parts[1]) >= 하한) {
                    결과.put(instanceId, Long.parseLong(parts[0]));
                }
            } catch (NumberFormatException e) {
                // 격리한다 — 여기서 던지면 보고 하나가 전면 차단이 된다
            }
        });
        return 결과;
    }

    public long 신선한_총_가용량() {
        return 신선한_보고().values().stream().mapToLong(Long::longValue).sum();
    }

    private void 심는다(String instanceId, long 가용량, long 시각) {
        redis.sync().hset(KEY, instanceId, "%d:%d".formatted(가용량, 시각));
    }

    private long 지금() {
        return clock.instant().getEpochSecond();
    }
}
