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

    /** 창의 제곱이 {@code long} 안에 들어오게 묶는다 — 아래 {@code require} 참조. */
    private static final Duration MAX_WINDOW = Duration.ofDays(1);

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
    /**
     * 유휴 비율의 역수. 노드당 몫이 이만큼은 돼야 한산 통과 상한이 1 이 된다.
     *
     * <p>비율은 게이트웨이가 주입받는 값이라 여기서 못 읽는다. 갈라지면 하한이
     * 다시 전면 차단이 되므로, 바꿀 때 두 곳을 같이 본다.
     */
    public static final int IDLE_DIVISOR = 5;

    /**
     * 아직 한 판도 안 걷었다. <b>승계와 신규 기동을 못 가른다</b> — 보고에
     * 기동 시각이 실리면 그때 이 추정을 버린다 (A-13).
     */
    private boolean firstRound = true;

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
     * <p><b>램프와 신선도는 별개 노브다.</b> 하나로 묶으면 한 값이 반대 방향 두
     * 사고를 함께 조종한다 — 크게 잡으면 죽은 인스턴스가 오래 세어지고, 작게
     * 잡으면 틱 한 번 밀려도 전면 억제다. 신선도 값은 R-2 가 정한다.
     */
    public static CapacityCollector of(Duration rampUp, Duration freshness,
            long floor, long perInstanceCap) {
        return new CapacityCollector(rampUp, freshness, floor, perInstanceCap);
    }

    /**
     * <b>초 단위로만 받는다.</b> 아래에서 {@code toSeconds()} 로 재는데, 500ms 를
     * 주면 조용히 0 이 되어 나눗셈이 터지거나 임계가 사라진다.
     */
    private void require(Duration value, String name) {
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("%s 은 양수여야 한다: %s".formatted(name, value));
        }
        // 초 단위로 재는데 500ms 를 주면 조용히 0 이 되어 나눗셈이 터진다.
        if (value.toNanosPart() != 0) {
            throw new IllegalArgumentException("%s 은 초 단위여야 한다: %s".formatted(name, value));
        }
        // **상한을 두어 넘침을 아예 없앤다.** 램프 나머지항이 창의 제곱으로
        // 커지므로, 창을 하루로 묶으면 어떤 보고값이 와도 넘칠 수 없다.
        // 곱셈을 감싸는 것보다 근본적이고, 하루짜리 램프는 설정 실수다.
        if (value.compareTo(MAX_WINDOW) > 0) {
            throw new IllegalArgumentException(
                    "%s 은 %s 이하여야 한다: %s".formatted(name, MAX_WINDOW, value));
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
     * 빠뜨렸을 때 조용히 영원히 0 을 내고, 그 상태는 운영에 존재할 수 없다.
     *
     * @param now 읽은 시각(초). {@code reportedAt} 과 <b>같은 시계</b>여야 한다
     */
    public long collect(Collection<CapacityReport> reports, long now, int nodes) {
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
            // **음수는 관측이 아니다.** 세면 "신선한 보고가 있다" 가 되어 하한이
            // 안 걸리고, 그 인스턴스가 램프 기록까지 얻는다.
            if (report.credits() < 0) {
                continue;
            }
            fresh++;
            Seen was = seen.get(report.instanceId());
            // **첫 판에 본 무리는 이미 돌던 것으로 본다.** 리더가 바뀐 것이 뒷단이
            // 새로 뜬 것은 아니다. 여기서 램프를 걸면 승계마다 크레딧이 0 으로
            // 떨어지고, 신선한 보고가 있어 하한도 안 걸린다.
            long first = was != null ? was.first()
                    : (firstRound ? now - rampUp.toSeconds() : now);
            seen.put(report.instanceId(), new Seen(first, now));
            // 인스턴스가 많고 각자 상한에 가까우면 합이 넘친다. 넘치면 음수가
            // 되어 전역 크레딧이 0 이 된다 — 전면 차단이다.
            total = saturatedAdd(total, usable(report, now));
        }
        evictStale(now);
        firstRound = false;

        // **하한은 "보고가 없을 때" 만이다.** 합이 0 인 것과 아무도 안 보고한
        // 것은 다르다 — 뒷단이 신선하게 "여유 0" 을 보고했으면 그건 정확한
        // 백프레셔다. 거기에 하한을 얹으면 명시적 신호를 무시하고 계속 민다.
        // **하한은 살아 있는 분모에 맞춘다.** 설정값으로만 재면 노드가 그보다
        // 늘었을 때 노드당 몫이 다시 0 이 된다 — 하한을 둔 이유가 사라진다.
        long credit = fresh == 0 ? Math.max(floor, (long) Math.max(1, nodes) * IDLE_DIVISOR)
                : total;
        lastKnown.set(credit);
        return credit;
    }

    private long saturatedAdd(long a, long b) {
        long sum = a + b;
        return ((a ^ sum) & (b ^ sum)) < 0 ? Long.MAX_VALUE : sum;
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
        //
        // 앞항은 넘치지 않는다 — 여기 오는 warmed 는 창보다 작으므로
        // credits/window × warmed < credits 다. 방어를 넣으면 죽은 코드가 되고,
        // 죽은 방어는 방어처럼 보여서 더 나쁘다.
        return credits / window * warmed + credits % window * warmed / window;
    }
}
