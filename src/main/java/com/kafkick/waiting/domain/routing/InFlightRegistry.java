package com.kafkick.waiting.domain.routing;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 인스턴스별로 지금 물려 있는 요청 수를 센다.
 *
 * <p>감소를 한 경로라도 놓치면 그 인스턴스의 카운터가 영구히 부풀고, 부하율이
 * 계속 높게 보여 P2C 가 그 인스턴스를 영원히 배제한다. 그래서 놓친 감소를
 * <b>수명으로 회수</b>한다 (R-8) — 표를 못 놓아도 누수가 유계다.
 */
public final class InFlightRegistry {

    private final Duration 수명;

    private final Clock 시계;

    /** 같은 밀리초에 시작한 요청을 가르는 꼬리표. 순서만 정하면 되므로 값에 뜻은 없다. */
    private final AtomicLong 일련 = new AtomicLong();

    private final ConcurrentHashMap<String, Slot> 칸 = new ConcurrentHashMap<>();

    private InFlightRegistry(Duration 수명, Clock 시계) {
        this.수명 = Objects.requireNonNull(수명, "수명");
        this.시계 = Objects.requireNonNull(시계, "시계");
        if (수명.isNegative() || 수명.isZero()) {
            throw new IllegalArgumentException("수명은 양수여야 한다: " + 수명);
        }
    }

    /** @param 수명 이보다 오래 물려 있던 항목은 감소를 놓친 것으로 보고 회수한다 */
    public static InFlightRegistry of(Duration 수명, Clock 시계) {
        return new InFlightRegistry(수명, 시계);
    }

    /** 요청 하나가 시작했다. 돌려준 표를 <b>어느 경로로 끝나든</b> 놓아야 한다. */
    public Ticket started(String 인스턴스) {
        Objects.requireNonNull(인스턴스, "인스턴스");
        Slot 자리 = 칸.computeIfAbsent(인스턴스, k -> new Slot());
        Key 열쇠 = new Key(시계.millis(), 일련.incrementAndGet());
        자리.넣는다(열쇠);
        return new Ticket(자리, 열쇠);
    }

    /** 지금 물려 있는 요청 수. 부르는 김에 수명이 지난 항목을 회수한다. */
    public int count(String 인스턴스) {
        Slot 자리 = 칸.get(인스턴스);
        return 자리 == null ? 0 : 자리.센다(시계.millis() - 수명.toMillis());
    }

    /** 목록에 없는 인스턴스의 카운터를 버린다. 재기동마다 식별자가 새로 오므로 안 버리면 자란다. */
    public void retain(Set<String> 사는것) {
        Objects.requireNonNull(사는것, "사는것");
        칸.keySet().retainAll(사는것);
    }

    /** 지금 카운터를 들고 있는 인스턴스들. 게이지가 훑는 자리다. */
    public Set<String> instances() {
        return Set.copyOf(칸.keySet());
    }

    /** 요청 하나의 자리를 잡아 둔 표. 놓기 전까지 그 인스턴스의 수에 든다. */
    public static final class Ticket {

        private final Slot 자리;

        private final Key 열쇠;

        private Ticket(Slot 자리, Key 열쇠) {
            this.자리 = 자리;
            this.열쇠 = 열쇠;
        }

        /** 완료·에러·타임아웃·취소 어느 쪽이든 여기로 온다. 두 번 불러도 한 번만 준다. */
        public void finished() {
            자리.뺀다(열쇠);
        }
    }

    /** 한 인스턴스의 산 항목들. 시작 시각 순이라 앞에서부터만 만료를 본다. */
    private static final class Slot {

        private final ConcurrentSkipListMap<Key, Boolean> 산것 = new ConcurrentSkipListMap<>();

        /** {@link ConcurrentSkipListMap#size()} 는 훑는다. 셀 때마다 훑을 수는 없다. */
        private final AtomicInteger 수 = new AtomicInteger();

        void 넣는다(Key 열쇠) {
            산것.put(열쇠, Boolean.TRUE);
            수.incrementAndGet();
        }

        void 뺀다(Key 열쇠) {
            if (산것.remove(열쇠) != null) {
                수.decrementAndGet();
            }
        }

        int 센다(long 기준) {
            for (Map.Entry<Key, Boolean> 머리 = 산것.firstEntry();
                    머리 != null && 머리.getKey().at() < 기준;
                    머리 = 산것.firstEntry()) {
                뺀다(머리.getKey());
            }
            return 수.get();
        }
    }

    /** 시작 시각으로 줄 세우고, 같은 밀리초는 일련으로 가른다. */
    private record Key(long at, long seq) implements Comparable<Key> {

        @Override
        public int compareTo(Key 다른) {
            int 먼저 = Long.compare(at, 다른.at);
            return 먼저 != 0 ? 먼저 : Long.compare(seq, 다른.seq);
        }
    }
}
