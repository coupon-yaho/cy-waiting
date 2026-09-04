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
 */
// **시각을 주입받는다.** 도메인이 시계를 들면 경계 동작을 시험할 수 없다 (DS-1).
public final class InstanceOutliers {

    private final int threshold;

    private final long ejectMillis;

    private final long rampMillis;

    private final ConcurrentHashMap<String, Record> records = new ConcurrentHashMap<>();

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
        records.computeIfAbsent(instanceId, id -> new Record())
                .succeeded(nowMillis, ejectMillis, rampMillis);
    }

    /** 이 인스턴스가 실패로 끝냈다. 임계에 닿으면 그 시각부터 배제가 시작된다. */
    public void failed(String instanceId, long nowMillis) {
        Objects.requireNonNull(instanceId, "instanceId");
        records.computeIfAbsent(instanceId, id -> new Record())
                .failed(threshold, nowMillis, ejectMillis, rampMillis);
    }

    /**
     * 지금 빼야 할 인스턴스들. <b>산 목록에 있는 것만</b> 돌려준다.
     *
     * <p>전부가 대상이면 하나도 안 뺀다. 보낼 곳이 0 이 되는 것은 열화된 대로라도
     * 보내는 것보다 나쁘다 — 배제가 곧 전면 차단이 된다.
     */
    public Set<String> ejected(Set<String> live, long nowMillis) {
        Objects.requireNonNull(live, "live");
        Set<String> out = new LinkedHashSet<>();
        for (String id : live) {
            Record record = records.get(id);
            if (record != null && record.ejected(nowMillis, ejectMillis)) {
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
        Record record = records.get(instanceId);
        return record == null ? 0
                : record.recoveryRemaining(nowMillis, ejectMillis, rampMillis);
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
        records.entrySet().removeIf(e -> !live.contains(e.getKey())
                && !e.getValue().settling(nowMillis, ejectMillis, rampMillis));
    }

    /**
     * 지금 연속 실패로 표시된 인스턴스 수.
     *
     * <p><b>실제로 걸러진 수와 다를 수 있다.</b> 전부가 대상이면 하나도 안 빼므로
     * 그때 이 값은 전체 대수인데 걸러진 것은 0 이다. 그 어긋남이 곧 뒷단 전체가
     * 앓고 있다는 신호라 지표로는 이쪽이 쓸모 있다.
     */
    public int ejectedCount(long nowMillis) {
        int count = 0;
        for (Record record : records.values()) {
            if (record.ejected(nowMillis, ejectMillis)) {
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
    private static final class Record {

        private int consecutive;

        /**
         * 뺀 시각. 여기서부터 배제 시간이 흐르고 그 뒤로 램프가 이어진다.
         *
         * <p>램프까지 끝나야 지운다. 그 전에 지우면 갓 돌아온 대와 한 번도 앓은
         * 적 없는 대가 구분이 안 되고, 그러면 아직 고장 난 대에 임계만큼을 다시
         * 준다 — 그동안 그 대는 여전히 가장 한가해 보인다.
         */
        private Long ejectedAt;

        synchronized void succeeded(long now, long ejectMillis, long rampMillis) {
            consecutive = 0;
            if (ejectedAt == null) {
                return;
            }
            long age = now - ejectedAt;
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
            if (ejectedAt != null && now - ejectedAt < ejectMillis + rampMillis) {
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
            return ejectedAt != null && now - ejectedAt < ejectMillis;
        }

        /** 배제든 램프든 아직 안 가라앉았는가. 걷을지 판단하는 자리다. */
        synchronized boolean settling(long now, long ejectMillis, long rampMillis) {
            return ejectedAt != null && now - ejectedAt < ejectMillis + rampMillis;
        }

        synchronized double recoveryRemaining(long now, long ejectMillis, long rampMillis) {
            if (ejectedAt == null || rampMillis <= 0) {
                return 0;
            }
            long into = now - ejectedAt - ejectMillis;
            // 배제 중이면 애초에 후보가 아니고, 램프가 끝났으면 되돌릴 것이 없다.
            if (into < 0 || into >= rampMillis) {
                return 0;
            }
            return 1 - (double) into / rampMillis;
        }
    }
}
