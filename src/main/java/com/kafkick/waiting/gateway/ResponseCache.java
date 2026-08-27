package com.kafkick.waiting.gateway;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 조회 응답을 <b>아주 짧게</b> 들고 있습니다.
 *
 * <p>코얼레싱은 동시에 도착한 것만 모읍니다. 1ms 어긋난 요청은 각각 나가므로,
 * 짧은 수명을 얹어야 연속 도착까지 흡수됩니다.
 */
public final class ResponseCache {

    /**
     * 담을 수 있는 키 수.
     *
     * <p><b>키는 밖에서 옵니다.</b> 경로와 쿼리로 만들어 가짓수에 상한이 없으므로,
     * 안 막으면 맵 하나가 메모리를 밀어냅니다 (리미터·격벽과 같은 이유).
     */
    private final int maxKeys;

    private final Clock clock;

    private final Map<String, Held> held = new HashMap<>();

    private ResponseCache(Clock clock, int maxKeys) {
        if (maxKeys < 1) {
            throw new IllegalArgumentException("maxKeys 는 1 이상이어야 한다: " + maxKeys);
        }
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
        this.maxKeys = maxKeys;
    }

    public static ResponseCache of(Clock clock, int maxKeys) {
        return new ResponseCache(clock, maxKeys);
    }

    /** 뒷단이 돌려준 것 그대로. 본문은 복사해 들고 있습니다. */
    public record Entry(int status, Map<String, List<String>> headers, byte[] body) {
    }

    private record Held(Entry entry, Instant expiresAt) {
    }

    /**
     * 수명 안이면 돌려줍니다.
     *
     * <p><b>지난 값을 돌려주면 안 됩니다.</b> 재고가 0 이 된 뒤에도 남아 있다고
     * 답하고, 그 사람은 매진된 쿠폰을 받으러 갑니다.
     */
    public synchronized Optional<Entry> get(String key) {
        Held found = held.get(key);
        if (found == null) {
            return Optional.empty();
        }
        if (!clock.instant().isBefore(found.expiresAt())) {
            held.remove(key);
            return Optional.empty();
        }
        return Optional.of(found.entry());
    }

    /**
     * 담습니다. <b>상한을 넘는 새 키는 안 받습니다.</b>
     *
     * <p>이미 담긴 키의 갱신은 새 키가 아닙니다 — 그것까지 막으면 가장 자주 쓰는
     * 키가 영영 안 바뀌고, 그때 캐시는 낡은 값을 계속 냅니다.
     */
    public synchronized void put(String key, Entry entry, Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            // 곧바로 지난 것을 담으면 자리만 먹는다.
            return;
        }
        if (!held.containsKey(key) && held.size() >= maxKeys) {
            // **지난 것부터 비운다.** 안 비우면 수명이 다 된 키들이 상한을 채우고,
            // 그때부터 새 키가 하나도 안 들어간다.
            evictExpired();
            if (held.size() >= maxKeys) {
                return;
            }
        }
        held.put(key, new Held(entry, clock.instant().plus(ttl)));
    }

    private void evictExpired() {
        Instant now = clock.instant();
        held.values().removeIf(h -> !now.isBefore(h.expiresAt()));
    }

    /** 담고 있는 키 수. 지표가 이 값을 읽습니다. */
    public synchronized int size() {
        return held.size();
    }

    /** 상한에 닿았는가. 닿으면 새 키가 조용히 버려진다 — 부르는 쪽이 그것을 센다. */
    public synchronized boolean isFull() {
        return held.size() >= maxKeys;
    }
}
