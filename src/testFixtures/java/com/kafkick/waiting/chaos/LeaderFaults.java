package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.Optional;

/**
 * 리더를 죽이거나 내린다 (4.0.1).
 *
 * <p><b>죽음과 종료를 나눈다.</b> 곱게 내리면 락이 즉시 풀려 승계가 빠르지만,
 * 그건 장애 경로가 아니다. 죽이면 락이 lease 만료까지 남아 <b>다음 리더가
 * 기다리는 구간</b>이 생긴다 — G4.2 가 재는 것이 그 구간이다.
 */
public final class LeaderFaults {

    /**
     * 리스가 만료되고 죽은 리더가 그 자리를 잡는다. 한 판이라 앱이 못 끼어든다.
     */
    // **NX 로 잡는다.** 통째로 덮으면 살아 있는 남의 락이 주인을 바꾸는데,
    // 실제 획득 스크립트는 그러지 않는다. 그 상태에서는 방금 확인에 성공한
    // 노드가 남의 키를 마주하게 되고, 다음 갱신까지 락 없이 리더라고 믿는다 —
    // 프로덕션에 없는 상태 위에 세운 시험은 아무것도 증명하지 못한다.
    private static final String TAKE_OVER = """
            redis.call('DEL', KEYS[1])
            local t = redis.call('TIME')
            local fence = tonumber(t[1]) * 1000000 + tonumber(t[2])
            if redis.call('SET', KEYS[1],
                    string.format('%.0f', fence) .. '|' .. ARGV[1],
                    'NX', 'PX', tonumber(ARGV[2])) then
                return 1
            end
            return 0
            """;

    /**
     * 자기 락일 때만 지운다. GET 과 DEL 사이에 소유자가 바뀌면 남의 락을 지운다.
     */
    // 값의 형식은 `<판 번호>|<ownerId>` 다. 통짜로 비교하면 지금 형식으로 잡은
    // 락을 자기 것으로 못 알아보고 안 지운 채 나간다 — 프로덕션 스크립트가
    // 구분자를 파싱하는 것과 같은 이유다.
    private static final String RELEASE = """
            local current = redis.call('GET', KEYS[1])
            if not current then
                return 0
            end
            local sep = string.find(current, '|', 1, true)
            local owner = sep == nil and current or string.sub(current, sep + 1)
            if owner == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
            """;

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

    /**
     * <b>죽은 리더가 락을 넘겨받는다 — 만료와 획득을 한 판에 한다.</b>
     *
     * @return 넘겨받았으면 {@code true}
     */
    // **나눠 던지면 앱이 그 틈에 들어온다.** 리더 루프는 100ms 마다 재획득을
    // 시도하는데, 시험도 같은 주기로 폴하면 두 루프의 위상이 고정돼 시작이
    // 나쁘면 200번을 내리 진다. 그때 나오는 것은 "회복이 늦었다" 가 아니라
    // "주입을 못 했다" 라, 회복 검증에 거짓 신호가 붙는다.
    //
    // 값은 **지금 형식으로** 쓴다. 번호 없는 옛 형식으로 쓰면 승계가 롤아웃
    // 호환 갈래로 빠져, 지금 리더가 죽어 남기는 락을 물려받는 경로를 안 밟는다.
    public boolean 죽은_리더가_넘겨받는다(String ownerId, Duration lease) {
        Long taken = redis.sync().eval(TAKE_OVER, ScriptOutputType.INTEGER,
                new String[] {RedisKeys.LEADER}, 소유자로_쓸_수_있는가(ownerId),
                String.valueOf(lease.toMillis()));
        return taken != null && taken == 1L;
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
     * 사라져 {@link #남은_lease()} 로 잴 대상이 없어진다.
     *
     * <p><b>남길 시간을 부르는 쪽이 정한다.</b> 여기서 정하면 그 값이 곧 관측
     * 창인데, 재는 쪽은 그게 얼마인지 모른 채 곧바로 읽는다.
     */
    public void lease를_만료시킨다(Duration 남길_시간) {
        // 0 이하를 넘기면 지워 버린다 — 만료 임박이 아니라 해제를 만드는 것이고,
        // 그건 이 픽스처가 안 만들기로 한 상태다.
        if (남길_시간 == null || 남길_시간.toMillis() <= 0) {
            throw new IllegalArgumentException("남길 시간은 1밀리초 이상이어야 한다: " + 남길_시간);
        }
        redis.sync().pexpire(RedisKeys.LEADER, 남길_시간.toMillis());
    }

    /** 지금 소유자. <b>판 번호가 붙어 있으면 떼고 준다</b> — 값 형식은 프로덕션 몫이다. */
    public String 현재_소유자() {
        String value = redis.sync().get(RedisKeys.LEADER);
        if (value == null) {
            return null;
        }
        int 구분 = value.indexOf('|');
        return 구분 < 0 ? value : value.substring(구분 + 1);
    }

    /**
     * 남은 리스. <b>키가 없거나 리스가 안 걸렸으면 빈 값이다.</b>
     *
     * <p>0 으로 뭉개면 "만료 임박" 과 "이미 사라짐" 이 같은 값이 된다 — 둘을
     * 가르는 것이 이 픽스처가 존재하는 이유인데, 그 구별을 픽스처가 지운다.
     */
    public Optional<Duration> 남은_lease() {
        long millis = redis.sync().pttl(RedisKeys.LEADER);
        return millis > 0 ? Optional.of(Duration.ofMillis(millis)) : Optional.empty();
    }
}
