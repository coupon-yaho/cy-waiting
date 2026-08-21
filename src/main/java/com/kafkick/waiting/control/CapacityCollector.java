package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 뒷단이 스스로 보고한 여유를 모아 <b>전역 크레딧</b>을 만든다.
 *
 * <p>콜드 인스턴스는 자기 여유를 과대 보고한다 — 재기동 직후엔 커넥션 풀이 비어
 * "유휴" 로 보이지만 실제로는 느려서 즉시 포화된다. 그래서 램프를 건다.
 */
public final class CapacityCollector {

    private final Duration rampUp;
    private final Duration freshness;
    private final long floor;
    private final long perInstanceCap;

    /**
     * 인스턴스를 처음 본 시각.
     *
     * <p><b>신선도 창을 넘겨 사라진 인스턴스는 지운다.</b> 안 지우면 이름이 고정된
     * 파드가 재기동할 때 옛 기록이 남아 <b>램프가 아예 안 걸린다</b> — 콜드 복귀가
     * 램프를 거는 유일한 이유인데 거기서만 안 걸린다. 맵이 자라는 것도 같은 뿌리다.
     */
    private final Map<String, Seen> seen = new LinkedHashMap<>();

    /** 처음 본 시각과 마지막으로 본 시각. */
    private record Seen(long first, long last) {
    }

    /** 마지막으로 성공한 관측. 읽기가 실패하면 여기로 되돌아간다. */
    private final AtomicLong lastKnown;

    private CapacityCollector(Duration rampUp, Duration freshness, long floor, long perInstanceCap) {
        require(rampUp, "rampUp");
        require(freshness, "freshness");
        if (floor < 1) {
            throw new IllegalArgumentException("floor 는 1 이상이어야 한다: %d".formatted(floor));
        }
        if (perInstanceCap < 1) {
            throw new IllegalArgumentException(
                    "perInstanceCap 은 1 이상이어야 한다: %d".formatted(perInstanceCap));
        }
        this.rampUp = rampUp;
        this.freshness = freshness;
        this.floor = floor;
        this.perInstanceCap = perInstanceCap;
        this.lastKnown = new AtomicLong(floor);
    }

    /**
     * @param rampUp         처음 본 인스턴스가 제 몫을 다 받기까지 걸리는 시간
     * @param freshness      이보다 낡은 보고는 안 센다. <b>램프와 별개 노브다</b> —
     *                       하나로 묶으면 한 값이 반대 방향 두 사고를 함께
     *                       조종한다. 값은 R-2 가 정한다 (보고 주기 1초·임계 3초)
     * @param floor          <b>신선한 보고가 하나도 없을 때</b> 쓸 값. 0 은 전면 차단이다
     * @param perInstanceCap 한 인스턴스가 보고할 수 있는 상한. 단위 착오를 막는다
     */
    public static CapacityCollector of(Duration rampUp, Duration freshness,
            long floor, long perInstanceCap) {
        return new CapacityCollector(rampUp, freshness, floor, perInstanceCap);
    }

    private void require(Duration value, String name) {
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("%s 은 양수여야 한다: %s".formatted(name, value));
        }
    }



    /**
     * 이번 읽기가 실패했다. <b>직전 값을 지킨다.</b>
     *
     * <p>보고가 0건인 것과 읽지 못한 것은 다르다. 레디스가 안 되면 모든 노드가
     * 같이 실패하는데 여기서 하한으로 떨어뜨리면 전면 억제가 된다.
     */
    public void observationFailed() {
        // 아무것도 안 한다 — lastKnown 이 그대로 답이 된다.
    }

    /** 마지막으로 성공한 관측의 결과. */
    public long lastKnown() {
        return lastKnown.get();
    }

    /**
     * 신선한 보고를 모아 전역 크레딧을 낸다.
     *
     * <p><b>처음 보는 인스턴스는 여기서 등록한다.</b> 등록을 따로 부르게 하면
     * 빠뜨렸을 때 조용히 영원히 0 을 내고, 그 상태는 운영에 존재할 수 없다 —
     * 보고가 관측됐다는 것이 곧 처음 본 시각이 있다는 뜻이다.
     *
     * @param reports 이번에 읽은 보고들
     * @param now     읽은 시각(초). {@code reportedAt} 과 <b>같은 시계</b>여야 한다
     */
    public long collect(Collection<CapacityReport> reports, long now) {
        Map<String, CapacityReport> latest = new HashMap<>();
        for (CapacityReport report : reports) {
            // 버전별 키를 함께 읽으면 같은 인스턴스가 두 번 온다. 세면 두 배다.
            latest.merge(report.instanceId(), report,
                    (a, b) -> a.reportedAt() >= b.reportedAt() ? a : b);
        }

        long total = 0;
        int fresh = 0;
        for (CapacityReport report : latest.values()) {
            if (!isFresh(report, now)) {
                continue;
            }
            fresh++;
            Seen was = seen.get(report.instanceId());
            seen.put(report.instanceId(), new Seen(was == null ? now : was.first(), now));
            total += usable(report, now);
        }
        evictStale(now);

        // **하한은 "보고가 없을 때" 만이다.** 합이 0 인 것과 아무도 안 보고한
        // 것은 다르다 — 뒷단이 신선하게 "여유 0" 을 보고했으면 그건 정확한
        // 백프레셔다. 거기에 하한을 얹으면 명시적 신호를 무시하고 계속 민다.
        long credit = fresh == 0 ? floor : total;
        lastKnown.set(credit);
        return credit;
    }

    private boolean isFresh(CapacityReport report, long now) {
        // **TTL 만 믿지 않는다.** TTL 은 지우는 시점이지 신선한 시점이 아니다 —
        // 죽은 인스턴스의 마지막 보고가 TTL 동안 계속 세어진다.
        long age = now - report.reportedAt();
        return age >= 0 && age <= freshness.toSeconds();
    }

    /**
     * <b>시간으로 지운다.</b> 이번 판에 없다고 지우면 한 틱만 안 보여도 워밍업이
     * 날아가고, 그러면 정상 인스턴스가 틱마다 램프를 다시 탄다.
     */
    private void evictStale(long now) {
        seen.values().removeIf(s -> now - s.last() > freshness.toSeconds());
    }

    private long usable(CapacityReport report, long now) {
        // 한 인스턴스의 버그가 전역 크레딧을 망치면 안 된다. 음수는 버리고
        // 상한을 넘으면 상한으로 자른다 — 단위 착오가 무제한 통과가 된다.
        if (report.credits() < 0) {
            return 0;
        }
        long credits = Math.min(report.credits(), perInstanceCap);
        Seen since = seen.get(report.instanceId());
        long warmed = Math.max(0, now - (since == null ? now : since.first()));
        long window = rampUp.toSeconds();
        if (warmed >= window) {
            return credits;
        }
        // **먼저 나눈다.** 곱하고 나누면 큰 보고에서 넘쳐 음수가 되고, 그러면
        // 다른 인스턴스 몫을 상쇄해 전역 크레딧이 하한으로 떨어진다.
        return credits / window * warmed + credits % window * warmed / window;
    }
}
