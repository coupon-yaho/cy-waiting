package com.kafkick.waiting.domain.routing;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 연속으로 실패하는 인스턴스를 후보에서 잠시 빼고, <b>천천히 되돌린다</b>.
 *
 * <p>물린 표는 응답이 끝날 때 놓는다. 그래서 즉시 실패하는 대는 물린 건수가
 * 안 쌓여 <b>가장 한가해 보이고</b>, 부하율로 고르는 이상 그쪽으로 더 간다.
 * 시각은 인자로 받는다 (DS-1).
 */
public final class InstanceOutliers {

    private final int threshold;

    private final long ejectMillis;

    private final long rampMillis;

    private final ConcurrentHashMap<String, Streak> records = new ConcurrentHashMap<>();

    /**
     * 마지막으로 본 인스턴스 목록. <b>지표가 견줄 대상이다.</b>
     *
     * <p>배제된 대는 트래픽이 0 이라 물린 건수 쪽 게이지에서 오히려 빠진다.
     * 그래서 "몇 대 중 몇 대가 표시됐나" 를 그 게이지로는 못 읽는다.
     */
    private volatile Set<String> lastSeen = Set.of();

    private InstanceOutliers(int threshold, Duration ejectFor, Duration ramp) {
        Objects.requireNonNull(ejectFor, "ejectFor");
        Objects.requireNonNull(ramp, "ramp");
        if (threshold <= 0) {
            throw new IllegalArgumentException("임계는 양수여야 한다: " + threshold);
        }
        if (ejectFor.isNegative() || ejectFor.isZero()) {
            throw new IllegalArgumentException("배제 시간은 양수여야 한다: " + ejectFor);
        }
        if (ramp.isNegative()) {
            throw new IllegalArgumentException("램프는 0 이상이어야 한다: " + ramp);
        }
        this.threshold = threshold;
        this.ejectMillis = ejectFor.toMillis();
        this.rampMillis = ramp.toMillis();
    }

    /**
     * @param threshold 연속 실패가 이만큼이면 뺀다
     * @param ejectFor  뺀 뒤 이만큼 지나면 되돌리기 시작한다
     * @param ramp      되돌린 뒤 제 몫을 다 받기까지 걸리는 시간
     */
    public static InstanceOutliers of(int threshold, Duration ejectFor, Duration ramp) {
        return new InstanceOutliers(threshold, ejectFor, ramp);
    }

    /** 이 인스턴스가 답을 제대로 냈다. 배제 중이었으면 거기서 되돌리기 시작한다. */
    public void succeeded(String instanceId, long nowMillis) {
        Objects.requireNonNull(instanceId, "instanceId");
        records.computeIfAbsent(instanceId, id -> new Streak())
                .succeeded(nowMillis, ejectMillis, rampMillis);
    }

    /**
     * 이 인스턴스가 실패로 끝냈다. 연속이 임계에 닿으면 거기서 배제가 시작된다.
     *
     * <p><b>배제·회복 구간의 실패는 임계를 안 기다린다</b> — 한 건으로 곧바로
     * 다시 뺀다. 되돌리는 중은 아직 미덥지 않다는 뜻이기 때문이다.
     */
    public void failed(String instanceId, long nowMillis) {
        Objects.requireNonNull(instanceId, "instanceId");
        records.computeIfAbsent(instanceId, id -> new Streak())
                .failed(threshold, nowMillis, ejectMillis, rampMillis);
    }

    /**
     * 지금 빼야 할 인스턴스들. <b>지금 목록에 있는 것만</b> 돌려준다.
     *
     * <p>전부가 대상이면 하나도 안 뺀다. 보낼 곳이 0 이 되는 것은 열화된 대로라도
     * 보내는 것보다 나쁘다 — 배제가 곧 전면 차단이 된다.
     */
    public Set<String> ejected(Set<String> live, long nowMillis) {
        Objects.requireNonNull(live, "live");
        Set<String> out = new LinkedHashSet<>();
        for (String id : live) {
            Streak streak = records.get(id);
            if (streak != null && streak.ejected(nowMillis, ejectMillis)) {
                out.add(id);
            }
        }
        // out 은 늘 live 의 부분집합이라 같아지는 것이 곧 전부라는 뜻이다.
        return out.size() == live.size() ? Set.of() : out;
    }

    /**
     * 되돌리는 중이면 아직 안 받아야 할 몫. 1 에서 시작해 0 으로 준다.
     *
     * <p><b>배제가 풀리는 순간이 절벽이다.</b> 그동안 트래픽이 0 이었으니 물린
     * 건수도 0 이고, 부하율로 고르면 돌아오는 순간 그 대가 전량을 받는다. 아직
     * 아프면 그 전량이 다 실패해 곧바로 다시 빠진다.
     */
    public double recoveryRemaining(String instanceId, long nowMillis) {
        Streak streak = records.get(instanceId);
        return streak == null ? 0
                : streak.recoveryRemaining(nowMillis, ejectMillis, rampMillis);
    }

    /**
     * 목록에 없는 인스턴스의 기록을 버린다. <b>배제·회복 중인 것은 남긴다.</b>
     *
     * <p>앓는 대는 readiness 가 흔들려 목록을 들락거린다. 그때 지우면 돌아올
     * 때마다 임계만큼을 새로 먹여야 해서 배제가 영영 안 걸린다. 남겨도 시간으로
     * 유계다 — 배제와 램프가 끝나면 다음 호출에서 걷힌다.
     */
    public void retain(Set<String> live, long nowMillis) {
        Objects.requireNonNull(live, "live");
        lastSeen = Set.copyOf(live);
        records.entrySet().removeIf(e -> !live.contains(e.getKey())
                && !e.getValue().settling(nowMillis, ejectMillis, rampMillis));
    }

    /** 마지막으로 본 인스턴스 수. 표시된 수를 여기에 견준다. */
    public int seenCount() {
        return lastSeen.size();
    }

    /**
     * 마지막으로 본 목록 안에서 연속 실패로 표시된 인스턴스 수.
     *
     * <p><b>걸러진 수와 다르다.</b> 전부가 대상이면 하나도 안 빼므로, 이 값이
     * {@link #seenCount()} 와 같아지는 것이 곧 뒷단 전체가 앓는다는 신호다.
     * 걷히길 기다리는 죽은 기록은 안 센다.
     */
    public int markedCount(long nowMillis) {
        int count = 0;
        for (String id : lastSeen) {
            Streak streak = records.get(id);
            if (streak != null && streak.ejected(nowMillis, ejectMillis)) {
                count++;
            }
        }
        return count;
    }

    /** 지금 기록을 들고 있는 인스턴스들. 지표와 시험이 훑는 자리다. */
    public Set<String> tracked() {
        return Set.copyOf(records.keySet());
    }

    /**
     * 한 인스턴스의 연속 실패와 배제 시작 시각.
     *
     * <p><b>자물쇠 하나로 묶는다.</b> 두 값이 같이 바뀌므로 따로 두면 임계에
     * 닿은 순간과 시각을 적는 순간 사이가 벌어져, 배제가 시작 안 된 채로 남는다.
     */
    private static final class Streak {

        private int consecutive;

        /**
         * 뺀 시각. 여기서부터 배제 시간이 흐르고 그 뒤로 램프가 이어진다.
         *
         * <p>램프까지 끝나야 지운다. 그 전에 지우면 갓 돌아온 대와 한 번도 앓은
         * 적 없는 대가 구분이 안 되고, 그러면 아직 고장 난 대에 임계만큼을 다시
         * 준다 — 그동안 그 대는 여전히 가장 한가해 보인다.
         */
        private Long ejectedAt;

        /**
         * 뺀 뒤 흐른 시간. <b>0 아래로 안 본다.</b>
         *
         * <p>시각은 벽시계라 시각 보정이나 재개로 뒤로 갈 수 있다. 그때 음수가
         * 되면 배제가 안 풀리는데, 빠진 대는 트래픽이 0 이라 성공도 실패도 안
         * 들어와 <b>시간 말고는 나갈 문이 없다.</b>
         */
        private long age(long now) {
            return Math.max(0, now - ejectedAt);
        }

        synchronized void succeeded(long now, long ejectMillis, long rampMillis) {
            consecutive = 0;
            if (ejectedAt == null) {
                return;
            }
            long age = age(now);
            // **배제 중의 성공은 배제를 끝내되 램프로 넘긴다.** 배제 전에 나갔던
            // 요청이 늦게 성공으로 돌아오는 자리라, 그것만으로 전량을 되돌리면
            // 반쯤 고장 난 대가 스스로 배제를 취소한다.
            if (age < ejectMillis) {
                ejectedAt = now - ejectMillis;
                return;
            }
            if (age >= ejectMillis + rampMillis) {
                ejectedAt = null;
            }
        }

        synchronized void failed(int threshold, long now, long ejectMillis, long rampMillis) {
            // **배제와 램프 구간의 실패는 그 자리에서 다시 뺀다.** 되돌리는 중은
            // 아직 미덥지 않다는 뜻이라, 임계만큼을 다시 주면 그동안 그 대가
            // 여전히 가장 한가해 보인다.
            if (ejectedAt != null && age(now) < ejectMillis + rampMillis) {
                ejectedAt = now;
                consecutive = 0;
                return;
            }
            ejectedAt = null;
            if (++consecutive >= threshold) {
                consecutive = 0;
                ejectedAt = now;
            }
        }

        synchronized boolean ejected(long now, long ejectMillis) {
            return ejectedAt != null && age(now) < ejectMillis;
        }

        /** 배제든 램프든 아직 안 가라앉았는가. 걷을지 판단하는 자리다. */
        synchronized boolean settling(long now, long ejectMillis, long rampMillis) {
            return ejectedAt != null && age(now) < ejectMillis + rampMillis;
        }

        synchronized double recoveryRemaining(long now, long ejectMillis, long rampMillis) {
            if (ejectedAt == null || rampMillis <= 0) {
                return 0;
            }
            long into = age(now) - ejectMillis;
            // 배제 중이면 애초에 후보가 아니고, 램프가 끝났으면 되돌릴 것이 없다.
            if (into < 0 || into >= rampMillis) {
                return 0;
            }
            return 1 - (double) into / rampMillis;
        }
    }
}
