package com.kafkick.waiting.domain.routing;

import java.time.Duration;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 인스턴스별로 지금 물려 있는 요청 수를 센다.
 *
 * <p>감소를 한 경로라도 놓치면 그 인스턴스의 카운터가 영구히 부풀고, 부하율이
 * 계속 높게 보여 P2C 가 그 인스턴스를 영원히 배제한다. 그래서 놓친 감소를
 * <b>수명으로 회수</b>한다 (R-8) — 표를 못 놓아도 누수가 유계다.
 */
// **시각을 주입받는다.** 도메인이 시계를 들면 초 경계 동작을 시험할 수 없다 (DS-1).
public final class InFlightRegistry {

    private final long ttlMillis;

    /** 같은 밀리초에 시작한 요청을 가르는 꼬리표. 순서만 정하면 되므로 값에 뜻은 없다. */
    private final AtomicLong sequence = new AtomicLong();

    private final ConcurrentHashMap<String, Slot> slots = new ConcurrentHashMap<>();

    private InFlightRegistry(Duration ttl) {
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("수명은 양수여야 한다: " + ttl);
        }
        this.ttlMillis = ttl.toMillis();
    }

    /** @param ttl 이보다 오래 물려 있던 항목은 감소를 놓친 것으로 보고 회수한다 */
    public static InFlightRegistry of(Duration ttl) {
        return new InFlightRegistry(ttl);
    }

    /** 요청 하나가 시작했다. 돌려준 표를 <b>어느 경로로 끝나든</b> 놓아야 한다. */
    public Ticket started(String instanceId, long nowMillis) {
        Objects.requireNonNull(instanceId, "instanceId");
        Slot slot = slots.computeIfAbsent(instanceId, id -> new Slot());
        Key key = new Key(nowMillis, sequence.incrementAndGet());
        slot.add(key);
        return new Ticket(slot, key);
    }

    /**
     * 인스턴스별 동시 상한 안에서만 자리를 준다 (G9.13).
     *
     * <p>느려진 한 대로 간 요청이 무한정 쌓이면 그 한 대가 게이트웨이의 커넥션을
     * 다 붙잡는다. 자리가 없으면 비어 있는 값을 돌려주고, 부르는 쪽이 다른
     * 인스턴스를 고른다.
     */
    public Optional<Ticket> tryStarted(String instanceId, int cap, long nowMillis) {
        Objects.requireNonNull(instanceId, "instanceId");
        if (cap <= 0) {
            throw new IllegalArgumentException("상한은 양수여야 한다: " + cap);
        }
        Slot slot = slots.computeIfAbsent(instanceId, id -> new Slot());
        Key key = new Key(nowMillis, sequence.incrementAndGet());
        return slot.addIfUnder(key, cap, cutoff(nowMillis))
                ? Optional.of(new Ticket(slot, key)) : Optional.empty();
    }

    /** 지금 물려 있는 요청 수. 부르는 김에 수명이 지난 항목을 회수한다. */
    public int count(String instanceId, long nowMillis) {
        Slot slot = slots.get(instanceId);
        return slot == null ? 0 : slot.size(cutoff(nowMillis));
    }

    /** 이보다 먼저 시작한 항목은 감소를 놓친 것으로 본다. */
    private long cutoff(long nowMillis) {
        return nowMillis - ttlMillis;
    }

    /** 목록에 없는 인스턴스의 카운터를 버린다. 재기동마다 식별자가 새로 오므로 안 버리면 자란다. */
    public void retain(Set<String> live) {
        Objects.requireNonNull(live, "live");
        slots.keySet().retainAll(live);
    }

    /** 지금 카운터를 들고 있는 인스턴스들. 게이지가 훑는 자리다. */
    public Set<String> instances() {
        return Set.copyOf(slots.keySet());
    }

    /** 요청 하나의 자리를 잡아 둔 표. 놓기 전까지 그 인스턴스의 수에 든다. */
    public static final class Ticket {

        private final Slot slot;

        private final Key key;

        private Ticket(Slot slot, Key key) {
            this.slot = slot;
            this.key = key;
        }

        /** 완료·에러·타임아웃·취소 어느 쪽이든 여기로 온다. 두 번 불러도 한 번만 준다. */
        public void finished() {
            slot.remove(key);
        }
    }

    /** 한 인스턴스의 산 항목들. 시작 시각 순이라 앞에서부터만 만료를 본다. */
    // **자물쇠 하나로 묶는다.** CAS 로 짜면 실패 갈래가 생기는데, 한 스레드로는
    // 그 갈래를 밟을 수 없어 도메인의 분기 100% 를 못 채운다. 못 재는 방어는
    // 방어처럼 보여서 더 나쁘다. 경합은 인스턴스 단위라 잠깐이다.
    private static final class Slot {

        private final NavigableMap<Key, Boolean> live = new TreeMap<>();

        private int count;

        synchronized void add(Key key) {
            live.put(key, Boolean.TRUE);
            count++;
        }

        synchronized boolean addIfUnder(Key key, int cap, long cutoff) {
            if (size(cutoff) >= cap) {
                return false;
            }
            live.put(key, Boolean.TRUE);
            count++;
            return true;
        }

        synchronized void remove(Key key) {
            if (live.remove(key) != null) {
                count--;
            }
        }

        synchronized int size(long cutoff) {
            for (Map.Entry<Key, Boolean> head = live.firstEntry();
                    head != null && head.getKey().at() < cutoff;
                    head = live.firstEntry()) {
                remove(head.getKey());
            }
            return count;
        }
    }

    /** 시작 시각으로 줄 세우고, 같은 밀리초는 일련으로 가른다. */
    private record Key(long at, long seq) implements Comparable<Key> {

        @Override
        public int compareTo(Key other) {
            int byTime = Long.compare(at, other.at);
            return byTime != 0 ? byTime : Long.compare(seq, other.seq);
        }
    }
}
