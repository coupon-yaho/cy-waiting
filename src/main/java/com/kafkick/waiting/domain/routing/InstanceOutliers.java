package com.kafkick.waiting.domain.routing;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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

    /**
     * 관측용 누적. <b>회차가 아니라 사건 수다</b> — 되돌리다 다시 빠진 비율과
     * 끝까지 마친 수를 견주는 자리다. 카운터라 되돌아가지 않는다.
     */
    private final AtomicLong ejectionsStarted = new AtomicLong();

    private final AtomicLong reEjections = new AtomicLong();

    private final AtomicLong rampsCompleted = new AtomicLong();

    /**
     * 뺀 대를 도로 넣은 회차. <b>표시는 됐는데 배제가 안 걸린 자리다</b> — 그 구간의
     * 그 대는 몫이 안 깎인 채 받으므로, 배제 게이지만 보면 반대로 읽힌다.
     */
    private final AtomicLong ejectionsOverridden = new AtomicLong();

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
     * @param threshold 연속 실패가 이만큼이면 뺀다. 배제 창을 성공으로 닫는 데도,
     *                  되돌리는 중에 다시 빼는 데도 같은 수를 쓴다
     * @param ejectFor  뺀 뒤 이만큼 지나면 되돌리기 시작한다
     * @param ramp      되돌린 뒤 제 몫을 다 받기까지 걸리는 시간
     */
    public static InstanceOutliers of(int threshold, Duration ejectFor, Duration ramp) {
        return new InstanceOutliers(threshold, ejectFor, ramp);
    }

    /**
     * 이 인스턴스가 답을 제대로 냈다. <b>배제 중이면 임계만큼 이어져야</b> 거기서
     * 되돌리기가 시작된다 — 한 건은 배제 전에 나갔던 요청일 수 있다.
     */
    public void succeeded(String instanceId, long nowMillis) {
        Objects.requireNonNull(instanceId, "instanceId");
        Streak streak = records.computeIfAbsent(instanceId, id -> new Streak());
        count(streak.settled(nowMillis, ejectMillis, rampMillis));
        streak.succeeded(threshold, nowMillis, ejectMillis);
    }

    /**
     * 이 인스턴스가 실패로 끝냈다. 연속이 임계에 닿으면 거기서 배제가 시작된다.
     *
     * <p><b>배제 구간의 실패만 임계를 안 기다린다</b> — 거기 오는 것은 대개 배제 전에
     * 나갔다 늦게 돌아온 결과다. 회복 구간은 같은 임계를 쓰고, 못 미치는 실패는
     * 회복을 취소하지 않는다.
     */
    public void failed(String instanceId, long nowMillis) {
        Objects.requireNonNull(instanceId, "instanceId");
        // **가라앉음을 먼저 떼어 낸다.** 한 호출이 사건 하나만 내므로, 램프를 끝내는
        // 그 실패가 곧 재배제이기도 하면(임계 1) 완주가 통째로 사라진다.
        Streak streak = records.computeIfAbsent(instanceId, id -> new Streak());
        count(streak.settled(nowMillis, ejectMillis, rampMillis));
        count(streak.failed(threshold, nowMillis, ejectMillis));
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
     * 지금 되돌리는 중인 인스턴스 수. <b>배제도 정상도 아닌 구간</b>이라 배제
     * 게이지에 안 잡히는데, 이 구간이 곧 회복이 끝났는지를 묻는 자리다.
     */
    public int rampingCount(long nowMillis) {
        int count = 0;
        for (String id : lastSeen) {
            if (recoveryRemaining(id, nowMillis) > 0) {
                count++;
            }
        }
        return count;
    }

    /** 되돌리는 중이라 깎기로 한 몫의 합. 고르개가 남기는 최소 하나는 안 뺀 값이다. */
    public double rampSuppressed(long nowMillis) {
        double sum = 0;
        for (String id : lastSeen) {
            sum += recoveryRemaining(id, nowMillis);
        }
        return sum;
    }

    /** 정상 구간에서 뺀 횟수. <b>배제 국면 하나가 여기서 열린다.</b> */
    public long ejectionsStarted() {
        return ejectionsStarted.get();
    }

    /** 되돌리는 중에 다시 뺀 횟수. */
    public long reEjections() {
        return reEjections.get();
    }

    /**
     * 뺀 대를 도로 넣었다. 보낼 곳이 0 이 되는 것보다 열화된 대로라도 보내는 것이
     * 낫다는 규칙이 걸린 자리라, <b>그 회차의 배제는 없던 것과 같다.</b>
     */
    public void overridden() {
        ejectionsOverridden.incrementAndGet();
    }

    /** 배제가 무시된 회차 수. 배제 게이지가 든 수가 실제로 걸렸는지를 여기에 견준다. */
    public long ejectionsOverridden() {
        return ejectionsOverridden.get();
    }

    /** 되돌리기를 끝까지 마친 횟수. */
    public long rampsCompleted() {
        return rampsCompleted.get();
    }

    /**
     * 목록에 없는 인스턴스의 기록을 버린다. <b>배제·회복 중인 것은 남긴다.</b> 앓는
     * 대는 readiness 가 흔들려 목록을 들락거리는데, 그때 지우면 돌아올 때마다 임계를
     * 새로 먹여야 해 배제가 영영 안 걸린다. 남겨도 램프가 끝나면 걷혀 유계다.
     */
    public void retain(Set<String> live, long nowMillis) {
        Objects.requireNonNull(live, "live");
        lastSeen = Set.copyOf(live);
        // 반복 중에 지워도 되는 것이 이 맵의 계약이라, 열쇠를 따로 뜨지 않는다.
        for (Map.Entry<String, Streak> entry : records.entrySet()) {
            String id = entry.getKey();
            // **완주는 여기서도 센다.** 그 대에게 다음 요청이 올 때만 세면 그 전에
            // 빠진 대의 완주가 영영 안 세어져, 연 배제에서 재배제와 완주를 뺀 값이
            // 배포마다 벌어진다. 여기는 라우팅 한 건마다 도므로 아무 대의 요청이든
            // 하나면 잡힌다 — 라우팅이 통째로 멎으면 이 계수도 멎는다.
            count(entry.getValue().settled(nowMillis, ejectMillis, rampMillis));
            if (live.contains(id)) {
                continue;
            }
            // **키마다 잠금 안에서 지운다.** 밖에서 보고 지우면, 그 사이에 들어온
            // 실패가 든 기록을 지우게 된다 — 그 실패는 어디에도 안 남는다.
            records.computeIfPresent(id, (k, s) ->
                    s.unsettled() ? s : null);
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

    /** 지금 기록을 들고 있는 인스턴스들. 시험이 훑는 자리다. */
    public Set<String> tracked() {
        return Set.copyOf(records.keySet());
    }

    /**
     * 무슨 일이 있었는가. <b>세는 자리를 하나로 모은다</b> — 상태를 바꾸는 자리마다
     * 세면 갈래 하나가 빠져도 안 드러난다.
     */
    private enum Event {
        NONE, EJECTED, RE_EJECTED, RAMP_DONE
    }

    private void count(Event event) {
        switch (event) {
            case EJECTED -> ejectionsStarted.incrementAndGet();
            case RE_EJECTED -> reEjections.incrementAndGet();
            case RAMP_DONE -> rampsCompleted.incrementAndGet();
            case NONE -> {
                // 아무 전이도 없었다. **default 를 안 쓴다** — 상수를 하나 더할 때
                // 여기가 안 깨지면 그 사건은 조용히 안 세어진다.
            }
        }
    }

    /**
     * 한 인스턴스의 연속 실패와 배제 시작 시각. <b>자물쇠 하나로 묶는다</b> — 따로
     * 두면 임계에 닿은 순간과 시각을 적는 순간 사이가 벌어져, 배제가 시작 안 된 채로
     * 남는다.
     */
    private static final class Streak {

        private int consecutiveFailures;

        /**
         * 배제 창 안의 연속 성공. <b>창을 닫는 근거를 여는 근거와 맞춘다</b> — 하나로
         * 닫으면 늦게 돌아온 결과 한 건이 응답 상한을 덮으라고 잡은 창을 지운다.
         * 실패 한 건이 끊고, 창 밖에서는 안 쌓인다.
         */
        private int consecutiveSuccesses;

        /**
         * 뺀 시각. 여기서부터 배제 시간이 흐르고 그 뒤로 램프가 이어진다. <b>램프까지
         * 끝나야 지운다</b> — 그 전에 지우면 갓 돌아온 대와 한 번도 앓은 적 없는 대가
         * 구분이 안 되어, 아직 고장 난 대에 임계만큼을 다시 준다.
         */
        private Long ejectedAt;

        /**
         * 뺀 뒤 흐른 시간. <b>0 아래로 안 본다.</b> 벽시계라 시각 보정이나 재개로 뒤로
         * 갈 수 있는데, 음수가 되면 배제가 안 풀린다. 빠진 대에는 대개 트래픽이 안 가
         * <b>시간이 주된 문</b>이고, 도로 넣는 갈래에서만 성공이 문을 연다.
         */
        private long age(long now) {
            return Math.max(0, now - ejectedAt);
        }

        synchronized void succeeded(int threshold, long now, long ejectMillis) {
            consecutiveFailures = 0;
            if (!ejected(now, ejectMillis)) {
                consecutiveSuccesses = 0;
                return;
            }
            // **배제 중의 성공은 배제를 끝내되 램프로 넘긴다.** 다만 임계만큼
            // 이어져야 한다 — 배제 전에 나갔던 요청이 늦게 성공으로 돌아오는
            // 자리라, 한 건으로 닫으면 반쯤 고장 난 대가 스스로 배제를 취소한다.
            if (++consecutiveSuccesses >= threshold) {
                consecutiveSuccesses = 0;
                ejectedAt = now - ejectMillis;
            }
        }

        synchronized Event failed(int threshold, long now, long ejectMillis) {
            // 실패는 갈래를 안 가리고 연속 성공을 끊는다. 흩어진 성공이 쌓여
            // 창을 닫으면 안 된다.
            consecutiveSuccesses = 0;
            // **배제 중의 실패는 그 자리에서 다시 뺀다.** 거기 오는 것은 배제 전에
            // 나갔다 늦게 돌아온 결과라, 아직 안 나은 대가 스스로 배제를 끝내면 안 된다.
            if (ejected(now, ejectMillis)) {
                ejectedAt = now;
                consecutiveFailures = 0;
                return Event.NONE;
            }
            // 가라앉은 것은 부르는 쪽이 먼저 떼어 냈으므로, 여기 남는 것은
            // 평상시이거나 램프 중이다.
            boolean ramping = unsettled();
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
            if (++consecutiveFailures >= threshold) {
                consecutiveFailures = 0;
                ejectedAt = now;
                return ramping ? Event.RE_EJECTED : Event.EJECTED;
            }
            return Event.NONE;
        }

        /**
         * 가라앉았으면 계수를 버리고 완주를 알린다. <b>한 번만 낸다</b> — 시각을
         * 지우는 것이 곧 표시라, 다음 호출은 아무것도 안 낸다.
         */
        synchronized Event settled(long now, long ejectMillis, long rampMillis) {
            if (ejectedAt == null || age(now) < ejectMillis + rampMillis) {
                return Event.NONE;
            }
            ejectedAt = null;
            consecutiveFailures = 0;
            return Event.RAMP_DONE;
        }

        synchronized boolean ejected(long now, long ejectMillis) {
            return ejectedAt != null && age(now) < ejectMillis;
        }

        /**
         * 배제든 램프든 아직 안 가라앉았는가. <b>시각을 다시 안 본다</b> — 부르는
         * 쪽이 같은 시각으로 가라앉음을 먼저 떼어 내므로, 남아 있으면 도는 중이다.
         */
        synchronized boolean unsettled() {
            return ejectedAt != null;
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
