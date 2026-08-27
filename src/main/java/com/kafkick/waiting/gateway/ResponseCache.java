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
 * <p>코얼레싱은 동시에 도착한 것만 모읍니다. 짧은 수명을 얹어야 연속 도착까지
 * 흡수됩니다.
 */
public final class ResponseCache {

    /**
     * 들고 있을 수 있는 <b>바이트</b>.
     *
     * <p>키 수로만 막으면 유계가 아닙니다 — 키 1만 × 본문 256KiB 는 2.4GiB 라,
     * 메모리를 지키겠다고 만든 상한이 그대로 OOM 의 근거가 됩니다.
     */
    private final long maxBytes;

    private final Clock clock;

    private final Map<String, Held> held = new HashMap<>();

    /** 지금 들고 있는 바이트. 매번 훑으면 그 훑기가 요청 경로에 붙는다. */
    private long bytes;

    private ResponseCache(Clock clock, long maxBytes) {
        if (maxBytes < 1) {
            throw new IllegalArgumentException("maxBytes 는 1 이상이어야 한다: " + maxBytes);
        }
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
        this.maxBytes = maxBytes;
    }

    public static ResponseCache ofBytes(Clock clock, long maxBytes) {
        return new ResponseCache(clock, maxBytes);
    }

    /**
     * 뒷단이 돌려준 것.
     *
     * <p><b>값을 복사해 들고 있습니다.</b> 준 쪽이 나중에 그 배열이나 맵을 고치면
     * 그다음 요청들이 바뀐 것을 받습니다.
     */
    public record Entry(int status, Map<String, List<String>> headers, byte[] body) {

        public Entry {
            headers = Map.copyOf(headers);
            body = body.clone();
        }

        @Override
        public byte[] body() {
            return body.clone();
        }

        /** 이 항목이 차지하는 바이트. 헤더는 대략만 센다 — 본문이 지배적이다. */
        long weight() {
            return body.length + 512L;
        }
    }

    private record Held(Entry entry, Instant expiresAt, long weight) {
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
            remove(key);
            return Optional.empty();
        }
        return Optional.of(found.entry());
    }

    /**
     * 담습니다. <b>예산을 넘는 새 항목은 안 받습니다.</b>
     *
     * <p>이미 담긴 키의 갱신은 새 항목이 아닙니다 — 그것까지 막으면 가장 자주 쓰는
     * 키가 영영 안 바뀌고, 그때 캐시는 낡은 값을 계속 냅니다.
     */
    public synchronized void put(String key, Entry entry, Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            // 곧바로 지난 것을 담으면 자리만 먹는다.
            return;
        }
        long weight = entry.weight();
        if (weight > maxBytes) {
            return;
        }
        remove(key);
        if (bytes + weight > maxBytes) {
            // **지난 것부터 비운다.** 안 비우면 수명이 다 된 것들이 예산을 채우고,
            // 그때부터 새 항목이 하나도 안 들어간다.
            evictExpired();
            if (bytes + weight > maxBytes) {
                return;
            }
        }
        held.put(key, new Held(entry, clock.instant().plus(ttl), weight));
        bytes += weight;
    }

    private void remove(String key) {
        Held gone = held.remove(key);
        if (gone != null) {
            bytes -= gone.weight();
        }
    }

    private void evictExpired() {
        Instant now = clock.instant();
        held.entrySet().removeIf(e -> {
            if (now.isBefore(e.getValue().expiresAt())) {
                return false;
            }
            bytes -= e.getValue().weight();
            return true;
        });
    }

    /** 담고 있는 키 수. 지표가 이 값을 읽습니다. */
    public synchronized int size() {
        return held.size();
    }

    /** 담고 있는 바이트. 예산에 얼마나 가까운지를 지표가 읽습니다. */
    public synchronized long bytes() {
        return bytes;
    }

    /** 예산에 닿았는가. 닿으면 새 항목이 조용히 버려진다 — 부르는 쪽이 그것을 센다. */
    public synchronized boolean isFull() {
        return bytes >= maxBytes;
    }
}
