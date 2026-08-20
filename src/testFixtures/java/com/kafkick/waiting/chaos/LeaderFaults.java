package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;

/**
 * 리더를 죽이거나 내린다 (4.0.1).
 *
 * <p><b>죽음과 종료를 나눈다.</b> 곱게 내리면 락이 즉시 풀려 승계가 빠르지만,
 * 그건 장애 경로가 아니다. 죽이면 락이 lease 만료까지 남아 <b>다음 리더가
 * 기다리는 구간</b>이 생긴다 — G4.2 가 재는 것이 그 구간이다.
 */
public final class LeaderFaults {

    /** 자기 락일 때만 지운다. GET 과 DEL 사이에 소유자가 바뀌면 남의 락을 지운다. */
    private static final String RELEASE =
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
                    + "return redis.call('DEL', KEYS[1]) else return 0 end";

    private final StatefulRedisConnection<String, String> redis;

    private LeaderFaults(StatefulRedisConnection<String, String> redis) {
        this.redis = redis;
    }

    /** 주어진 연결 위에서 리더 락의 수명과 소유권을 흔드는 픽스처를 만든다. */
    public static LeaderFaults of(StatefulRedisConnection<String, String> redis) {
        return new LeaderFaults(redis);
    }

    /**
     * 프로덕션이 빈 소유자를 거부한다 — 픽스처가 그 상태를 만들면 안 된다 (TS-3).
     *
     * <p>빈 값으로 잡히면 해제 때 누구의 락인지 가릴 수 없어 남의 락을 지운다.
     */
    private static String 소유자로_쓸_수_있는가(String ownerId) {
        if (ownerId == null || ownerId.isEmpty()) {
            throw new IllegalArgumentException("ownerId 는 비면 안 된다");
        }
        return ownerId;
    }

    /**
     * 리더를 세운다. <b>이미 잡혀 있으면 실패한다</b> — {@code SET NX PX} 다.
     *
     * <p>덮어쓰면 살아 있는 남의 락이 주인을 바꾸는데, 실제 획득 스크립트는
     * 그러지 않는다. 픽스처가 프로덕션에 없는 상태를 만들면 그 위에 세운
     * 시험은 아무것도 증명하지 못한다 (TS-3).
     *
     * @return 내가 잡았으면 {@code true}
     */
    public boolean 리더로_만든다(String ownerId, Duration lease) {
        return "OK".equals(redis.sync().set(RedisKeys.LEADER, 소유자로_쓸_수_있는가(ownerId),
                SetArgs.Builder.nx().px(lease.toMillis())));
    }

    /** 프로세스만 사라진다. 락은 그대로 남는다 — 해제 절차를 못 밟았기 때문이다. */
    public void 프로세스를_죽인다(String ownerId) {
        if (!소유자로_쓸_수_있는가(ownerId).equals(현재_소유자())) {
            throw new IllegalStateException("리더가 아니다: " + ownerId);
        }
        // 아무것도 안 한다. **그것이 죽음이다** — 남는 것은 만료를 기다리는 락뿐.
    }

    /**
     * 종료 경로. 확인과 삭제를 한 스크립트로 묶는다.
     *
     * @return 내가 지웠으면 {@code true}
     */
    public boolean 곱게_내린다(String ownerId) {
        Long deleted = redis.sync().eval(RELEASE, ScriptOutputType.INTEGER,
                new String[] {RedisKeys.LEADER}, 소유자로_쓸_수_있는가(ownerId));
        return deleted != null && deleted == 1L;
    }

    /**
     * lease 를 <b>거의</b> 만료시킨다 — 지우지 않는다.
     *
     * <p>{@code DEL} 로 모델링하면 해제와 구분이 없어지고, "만료 임박" 구간이
     * 사라져 {@link #남은_lease()} 로 잴 대상이 없어진다. 승계가 만료를
     * 기다리는 경로를 재려면 그 구간이 있어야 한다.
     */
    public void lease를_만료시킨다() {
        redis.sync().pexpire(RedisKeys.LEADER, 1L);
    }

    public String 현재_소유자() {
        return redis.sync().get(RedisKeys.LEADER);
    }

    public Duration 남은_lease() {
        long millis = redis.sync().pttl(RedisKeys.LEADER);
        return millis > 0 ? Duration.ofMillis(millis) : Duration.ZERO;
    }
}
