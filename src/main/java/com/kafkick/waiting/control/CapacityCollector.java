package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 뒷단이 스스로 보고한 여유를 모아 <b>전역 크레딧</b>을 만든다.
 *
 * <p>콜드 인스턴스는 자기 여유를 과대 보고한다 — 재기동 직후엔 커넥션 풀이 비어
 * "유휴" 로 보이지만 실제로는 느려서 즉시 포화된다. 그래서 처음 본 인스턴스에는
 * 램프를 건다.
 */
public final class CapacityCollector {

    private final Duration rampUp;
    private final long floor;

    /**
     * 인스턴스를 처음 본 시각. <b>레디스 서버 시각</b>이다.
     *
     * <p>기록이 없으면 콜드로 본다. 게이트웨이가 재기동하면 여기가 비는데, 그때
     * 보고를 그대로 믿으면 과대 보고가 곧바로 전역 크레딧이 된다.
     */
    private final Map<String, Long> firstSeen = new ConcurrentHashMap<>();

    private CapacityCollector(Duration rampUp, long floor) {
        this.rampUp = rampUp;
        this.floor = floor;
    }

    /**
     * @param rampUp 처음 본 인스턴스가 제 몫을 다 받기까지 걸리는 시간
     * @param floor  신선한 보고가 없을 때 쓸 하한. <b>0 을 내면 전면 차단이다</b>
     */
    public static CapacityCollector of(Duration rampUp, long floor) {
        if (rampUp.isZero() || rampUp.isNegative()) {
            throw new IllegalArgumentException("rampUp 은 양수여야 한다: %s".formatted(rampUp));
        }
        if (floor < 1) {
            throw new IllegalArgumentException("floor 는 1 이상이어야 한다: %d".formatted(floor));
        }
        return new CapacityCollector(rampUp, floor);
    }

    /** 이 인스턴스를 처음 본 시각을 기록한다. 이미 있으면 그대로 둔다. */
    public void firstSeen(String instanceId, long at) {
        firstSeen.putIfAbsent(instanceId, at);
    }

    /**
     * 신선한 보고를 모아 전역 크레딧을 낸다.
     *
     * @param reports 이번에 읽은 보고들
     * @param now     <b>레디스 서버 시각</b>(초)
     */
    public long collect(Collection<CapacityReport> reports, long now) {
        long total = 0;
        for (CapacityReport report : reports) {
            total += usable(report, now);
        }
        // 0 을 내면 전 쿠폰이 전면 차단된다. 뒷단이 안 보고한다고 게이트웨이가
        // 서비스를 멈출 이유는 없다.
        return total > 0 ? total : floor;
    }

    private long usable(CapacityReport report, long now) {
        // **한 인스턴스의 버그가 전역 크레딧을 망치면 안 된다.** 음수는 그
        // 항목만 버린다.
        if (report.credits() < 0) {
            return 0;
        }
        // **TTL 만 믿지 않는다.** TTL 은 지우는 시점이지 신선한 시점이 아니다 —
        // 죽은 인스턴스의 마지막 보고가 TTL 동안 계속 세어진다.
        long age = now - report.reportedAt();
        if (age < 0 || age > rampUp.toSeconds()) {
            return 0;
        }
        Long seen = firstSeen.get(report.instanceId());
        if (seen == null) {
            return 0;
        }
        long warmed = now - seen;
        if (warmed >= rampUp.toSeconds()) {
            return report.credits();
        }
        // **곱하지 않고 작은 쪽을 쓴다.** 인스턴스가 이미 램프를 걸어 보고했는데
        // 여기서 또 곱하면 램프가 두 번 걸려 정상 인스턴스가 한참 굶는다.
        long ramped = report.credits() * Math.max(0, warmed) / rampUp.toSeconds();
        return Math.min(report.credits(), ramped);
    }
}
