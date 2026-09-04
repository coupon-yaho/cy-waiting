package com.kafkick.waiting.domain.routing;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 연속으로 실패하는 인스턴스를 후보에서 잠시 뺀다.
 *
 * <p>물린 표는 응답이 끝날 때 놓는다. 그래서 즉시 실패하는 대는 물린 건수가
 * 안 쌓여 <b>가장 한가해 보이고</b>, 부하율로 고르는 이상 그쪽으로 더 간다.
 */
// **시각을 주입받는다.** 도메인이 시계를 들면 경계 동작을 시험할 수 없다 (DS-1).
public final class InstanceOutliers {

    private final int threshold;

    private final long ejectMillis;

    private final ConcurrentHashMap<String, Record> records = new ConcurrentHashMap<>();

    private InstanceOutliers(int threshold, Duration ejectFor) {
        Objects.requireNonNull(ejectFor, "ejectFor");
        if (threshold <= 0) {
            throw new IllegalArgumentException("임계는 양수여야 한다: " + threshold);
        }
        if (ejectFor.isNegative() || ejectFor.isZero()) {
            throw new IllegalArgumentException("배제 시간은 양수여야 한다: " + ejectFor);
        }
        this.threshold = threshold;
        this.ejectMillis = ejectFor.toMillis();
    }

    /**
     * @param threshold 연속 실패가 이만큼이면 뺀다
     * @param ejectFor  뺀 뒤 이만큼 지나면 다시 후보로 돌린다
     */
    public static InstanceOutliers of(int threshold, Duration ejectFor) {
        return new InstanceOutliers(threshold, ejectFor);
    }

    /** 이 인스턴스가 답을 제대로 냈다. 연속 실패와 배제 이력을 지운다. */
    public void succeeded(String instanceId) {
        Objects.requireNonNull(instanceId, "instanceId");
        records.computeIfAbsent(instanceId, id -> new Record()).succeeded();
    }

    /** 이 인스턴스가 실패로 끝냈다. 임계에 닿으면 그 시각부터 배제가 시작된다. */
    public void failed(String instanceId, long nowMillis) {
        Objects.requireNonNull(instanceId, "instanceId");
        records.computeIfAbsent(instanceId, id -> new Record())
                .failed(threshold, nowMillis);
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
        return out.size() >= live.size() ? Set.of() : out;
    }

    /**
     * 목록에 없는 인스턴스의 기록을 버린다.
     *
     * <p>식별자는 재기동마다 새로 오므로(R-3) 안 걷으면 죽은 이름이 무한히 쌓인다.
     * 물린 건수와 달리 산 요청을 볼 필요가 없다 — 지워도 다음 실패부터 다시 센다.
     */
    public void retain(Set<String> live) {
        Objects.requireNonNull(live, "live");
        records.keySet().removeIf(id -> !live.contains(id));
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
         * 마지막으로 뺀 시각. 한 번도 안 뺐거나 그 뒤 성공했으면 비어 있다.
         *
         * <p><b>시간이 지나도 안 지운다.</b> 지우면 갓 돌아온 대와 한 번도 앓은
         * 적 없는 대가 구분이 안 되고, 그러면 아직 고장 난 대에 임계만큼을 다시
         * 준다 — 그동안 그 대는 여전히 가장 한가해 보인다. 지우는 것은 성공뿐이다.
         */
        private Long ejectedAt;

        synchronized void succeeded() {
            consecutive = 0;
            ejectedAt = null;
        }

        synchronized void failed(int threshold, long nowMillis) {
            // 뺀 적이 있으면 성공을 한 번도 못 본 것이므로 그 자리에서 다시 뺀다.
            if (ejectedAt != null) {
                ejectedAt = nowMillis;
                return;
            }
            if (++consecutive >= threshold) {
                consecutive = 0;
                ejectedAt = nowMillis;
            }
        }

        synchronized boolean ejected(long nowMillis, long ejectMillis) {
            return ejectedAt != null && nowMillis - ejectedAt < ejectMillis;
        }
    }
}
