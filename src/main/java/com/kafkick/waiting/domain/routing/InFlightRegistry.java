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
 * <p>감소를 한 경로라도 놓치면 카운터가 영구히 부풀어 그 인스턴스가 영원히
 * 배제된다. 놓친 감소는 <b>수명으로 회수</b>한다 — 누수가 유계다.
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
        // **밀리초 미만은 안 받는다.** 아래가 밀리초로 재므로 500us 같은 값이
        // 0 으로 잘리고, 그러면 시작하자마자 회수 대상이 되어 카운터가 늘 0 이다.
        if (ttl.toMillis() == 0) {
            throw new IllegalArgumentException("수명은 1ms 이상이어야 한다: " + ttl);
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
     * 인스턴스별 동시 상한 안에서만 자리를 준다.
     *
     * <p>느려진 한 대로 간 요청이 쌓이면 그 한 대가 커넥션을 다 붙잡는다.
     * 자리가 없으면 비어 있는 값을 돌려준다 — 부르는 쪽이 다른 대를 고른다.
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

    /** 전 인스턴스의 합. 게이지가 인스턴스별 태그를 못 다는 자리에서 쓴다. */
    // 식별자가 재기동마다 새로 오므로(R-3) 태그로 달면 시계열이 무한히 는다 (LG-4).
    public int total(long nowMillis) {
        long cutoff = cutoff(nowMillis);
        int sum = 0;
        for (Slot slot : slots.values()) {
            sum += slot.size(cutoff);
        }
        return sum;
    }

    /** 가장 바쁜 인스턴스의 수. <b>합만으로는 쏠림이 안 보인다.</b> */
    public int busiest(long nowMillis) {
        long cutoff = cutoff(nowMillis);
        int max = 0;
        for (Slot slot : slots.values()) {
            max = Math.max(max, slot.size(cutoff));
        }
        return max;
    }

    /** 이보다 먼저 시작한 항목은 감소를 놓친 것으로 본다. */
    private long cutoff(long nowMillis) {
        return nowMillis - ttlMillis;
    }

    /**
     * 목록에 없고 <b>물려 있는 것도 없는</b> 인스턴스의 카운터를 버린다.
     *
     * <p>목록에서 잠깐 빠진 대에 아직 요청이 물려 있을 수 있다. 그때 카운터를
     * 지우면 돌아온 순간 부하가 0 으로 보여 <b>그 대로 몰아 보낸다.</b>
     */
    // 안 버리면 재기동마다 새 식별자가 쌓인다. 비었는지를 같이 보므로 수명이
    // 지나면 다음 호출에서 버려진다 — 유계다.
    public void retain(Set<String> live, long nowMillis) {
        Objects.requireNonNull(live, "live");
        long cutoff = cutoff(nowMillis);
        slots.entrySet().removeIf(e ->
                !live.contains(e.getKey()) && e.getValue().size(cutoff) == 0);
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
