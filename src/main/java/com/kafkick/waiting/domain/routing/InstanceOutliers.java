package com.kafkick.waiting.domain.routing;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 연속으로 실패하는 인스턴스를 후보에서 잠시 빼고, <b>천천히 되돌린다</b>. 물린 표는
 * 응답이 끝날 때 놓으므로 즉시 실패하는 대는 물린 건수가 안 쌓여 <b>가장 한가해
 * 보이고</b>, 부하율로 고르는 이상 그쪽으로 더 간다. 시각은 인자로 받는다.
 */
public final class InstanceOutliers {

    private final int threshold;

    private final long ejectMillis;

    private final long rampMillis;

    private final ConcurrentHashMap<String, Streak> records = new ConcurrentHashMap<>();

    /**
     * 마지막으로 본 인스턴스 목록. <b>지표가 견줄 대상이다.</b> 배제된 대는 트래픽이
     * 0 이라 물린 건수 쪽 게이지에서 오히려 빠져, 몇 대 중 몇 대가 표시됐나를 그
     * 게이지로는 못 읽는다.
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
        // **밀리초 미만은 안 받는다.** 아래가 밀리초로 재므로 500us 같은 값이
        // 0 으로 잘리고, 그러면 배제가 걸리자마자 풀려 없는 것과 같아진다.
        if (ejectFor.toMillis() == 0) {
            throw new IllegalArgumentException("배제 시간은 1ms 이상이어야 한다: " + ejectFor);
        }
        if (!ramp.isZero() && ramp.toMillis() == 0) {
            throw new IllegalArgumentException("램프는 0 이거나 1ms 이상이어야 한다: " + ramp);
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
     * <p><b>배제 구간의 실패만 임계를 안 기다린다</b> — 그 구간은 트래픽이 0 이라 거기
     * 오는 것이 늦게 돌아온 결과다. 회복 구간은 같은 임계를 쓰고, 못 미치는 실패는
     * 회복을 취소하지 않는다.
     */
    public void failed(String instanceId, long nowMillis) {
        Objects.requireNonNull(instanceId, "instanceId");
        records.computeIfAbsent(instanceId, id -> new Streak())
                .failed(threshold, nowMillis, ejectMillis, rampMillis);
    }

    /**
     * 지금 빼야 할 인스턴스들. <b>지금 목록에 있는 것만</b> 돌려준다. 전부가 대상이면
     * 하나도 안 뺀다 — 보낼 곳이 0 이 되는 것은 열화된 대로라도 보내는 것보다 나쁘다.
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
     * 되돌리는 중이면 아직 안 받아야 할 몫. <b>배제가 풀리는 순간이 절벽이다</b> —
     * 트래픽이 0 이었으니 물린 건수도 0 이라 부하율로 고르면 돌아오는 순간 전량을
     * 받고, 아직 아프면 그 전량이 다 실패해 곧바로 다시 빠진다.
     */
    public double recoveryRemaining(String instanceId, long nowMillis) {
        Streak streak = records.get(instanceId);
        return streak == null ? 0
                : streak.recoveryRemaining(nowMillis, ejectMillis, rampMillis);
    }

    /**
     * 목록에 없는 인스턴스의 기록을 버린다. <b>배제·회복 중인 것은 남긴다.</b> 앓는
     * 대는 readiness 가 흔들려 목록을 들락거리는데, 그때 지우면 돌아올 때마다 임계를
     * 새로 먹여야 해 배제가 영영 안 걸린다. 남겨도 램프가 끝나면 걷혀 유계다.
     */
    public void retain(Set<String> live, long nowMillis) {
        Objects.requireNonNull(live, "live");
        lastSeen = Set.copyOf(live);
        // **키마다 잠금 안에서 지운다.** 밖에서 보고 지우면, 그 사이에 들어온
        // 실패가 든 기록을 지우게 된다 — 그 실패는 어디에도 안 남는다.
        for (String id : Set.copyOf(records.keySet())) {
            if (live.contains(id)) {
                continue;
            }
            records.computeIfPresent(id, (k, streak) ->
                    streak.settling(nowMillis, ejectMillis, rampMillis) ? streak : null);
        }
    }

    /** 마지막으로 본 인스턴스 수. 표시된 수를 여기에 견준다. */
    public int seenCount() {
        return lastSeen.size();
    }

    /**
     * 마지막으로 본 목록 안에서 연속 실패로 표시된 인스턴스 수. <b>걸러진 수와
     * 다르다</b> — 전부가 대상이면 하나도 안 빼므로, 이 값이 {@link #seenCount()} 와
     * 같아지는 것이 곧 뒷단 전체가 앓는다는 신호다. 죽은 기록은 안 센다.
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
     * 한 인스턴스의 연속 실패와 배제 시작 시각. <b>자물쇠 하나로 묶는다</b> — 따로
     * 두면 임계에 닿은 순간과 시각을 적는 순간 사이가 벌어져, 배제가 시작 안 된 채로
     * 남는다.
     */
    private static final class Streak {

        private int consecutive;

        /**
         * 뺀 시각. 여기서부터 배제 시간이 흐르고 그 뒤로 램프가 이어진다. <b>램프까지
         * 끝나야 지운다</b> — 그 전에 지우면 갓 돌아온 대와 한 번도 앓은 적 없는 대가
         * 구분이 안 되어, 아직 고장 난 대에 임계만큼을 다시 준다.
         */
        private Long ejectedAt;

        /**
         * 뺀 뒤 흐른 시간. <b>0 아래로 안 본다.</b> 벽시계라 시각 보정이나 재개로 뒤로
         * 갈 수 있는데, 음수가 되면 배제가 안 풀린다. 빠진 대는 트래픽이 0 이라 성공도
         * 실패도 안 들어와 <b>시간 말고는 나갈 문이 없다.</b>
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
                // **가라앉으면 계수도 함께 버린다.** 램프에서 쌓은 연속을 넘기면 평상시
                // 첫 실패 한 건이 그 대를 다시 뺀다 — 트래픽이 끊겼다 돌아오는 자리에서
                // 그 계수가 무기한 살아남는다.
                ejectedAt = null;
                consecutive = 0;
            }
        }

        synchronized void failed(int threshold, long now, long ejectMillis, long rampMillis) {
            // **배제 중의 실패는 그 자리에서 다시 뺀다.** 거기 오는 것은 배제 전에
            // 나갔다 늦게 돌아온 결과라, 아직 안 나은 대가 스스로 배제를 끝내면 안 된다.
            if (ejected(now, ejectMillis)) {
                ejectedAt = now;
                consecutive = 0;
                return;
            }
            // **가라앉았으면 앓은 적 없는 대와 같이 다룬다.** 램프에서 쌓은 연속을
            // 넘기면 평상시 첫 실패 한 건이 그 대를 다시 뺀다 — 트래픽이 끊겼다
            // 돌아오는 자리에서 그 계수가 무기한 살아남는다.
            if (!settling(now, ejectMillis, rampMillis) && ejectedAt != null) {
                ejectedAt = null;
                consecutive = 0;
            }
            // **램프 중에도 뺄 근거는 처음과 같다.** 한 건으로 되감으면 배경 오류만으로
            // 램프가 안 끝난다 — 램프 60초 · 대당 50rps 면 그동안 1,500 건을 받고,
            // 오류율 1% 에서 완주 확률이 사실상 0 이다. 그 대는 영구히 제 몫에서 빠진다.
            //
            // 반대 근거였던 "그동안 가장 한가해 보인다" 는 램프가 그 대의 몫을 선형으로
            // 깎는 것으로 답한다. **다만 전 대가 같이 회복하는 구간에서는 그 답이 없다** —
            // 다 같이 깎이면 비율이 그대로라 억제량이 0 이다. 그 자리는 계획서에 있다.
            //
            // 임계에 못 미치는 실패로 램프를 취소하지는 않는다. 취소하면 몫을 안 깎은
            // 채 전량을 받아 배제가 노린 것과 반대가 된다.
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
