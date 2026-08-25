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
     * <p><b>램프 창을 넘겨 안 보이면 지운다.</b> 재기동은 새 식별자로 오므로(R-3)
     * 옛 기록이 램프를 건너뛰게 하지 않는다 — 지우는 이유는 맵이 배포 이력만큼
     * 쌓이는 것뿐이다.
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
     * 못 읽어도 직전 값을 그대로 쓰는 판의 수.
     *
     * <p><b>R-2 의 "3회 연속 누락" 과 다른 값이다</b> — 저건 뒷단 하나를 벽시계로,
     * 이건 우리 시야를 판으로 센다.
     */
    public static final int HOLD_ROUNDS = 3;

    /**
     * 아직 한 판도 안 걷었다. <b>승계와 신규 기동을 못 가른다</b> — 보고에
     * 기동 시각이 실리면 그때 이 추정을 버린다 (A-13).
     */
    private boolean firstRound = true;

    private final AtomicLong lastKnown;

    /** 연속으로 못 읽은 판의 수. 한 판이라도 성공하면 다시 0 이다. */
    private final AtomicLong failedRounds = new AtomicLong();

    /** 마지막 판의 바닥값. 걷을 때 쓴 것과 같아야 한다. */
    private final AtomicLong lastMinimum;

    /**
     * 마지막 판에서 실제로 <b>하한이 답이 된</b> 값. 하한이 안 걸린 판에서는 0 이다.
     *
     * <p>배분이 평활 뒤에 이것을 다시 건다. 하한은 관측이 아니라 정책이라
     * 평활에 묻히면 안 된다.
     */
    private final AtomicLong lastFloor = new AtomicLong();

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
        this.lastMinimum = new AtomicLong(floor);
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
     * 이번 읽기가 실패했다. <b>유예 안에서는 직전 값을 지킨다.</b>
     *
     * <p>0건과 못 읽은 것은 다르다. <b>다만 무기한은 아니다</b> — 길어지면 그건
     * 관측이 아니라 추측이고, 분자는 유지가 과다 방향이다.
     *
     * @param nodes 지금 살아 있는 게이트웨이 수. 바닥이 이 값을 받쳐야 한다
     */
    public void observationFailed(int nodes) {
        if (failedRounds.incrementAndGet() <= HOLD_ROUNDS) {
            return;
        }
        // **절벽이 아니라 비탈로 내려간다.** 유예가 끝나는 순간 바닥으로 떨구면
        // 그 한 틱에 유입이 몇 배로 조여져 회복 구간이 더 나빠진다.
        //
        // **바닥은 걷을 때와 같은 값이다.** 설정값만 보면 노드가 그보다 늘었을 때
        // 노드당 몫이 유휴 비율 아래로 내려가 한산 통과가 전 노드에서 막힌다.
        //
        // **0 은 안 올린다.** 뒷단이 스스로 "여유 0" 이라고 말한 뒤라면 그건
        // 관측이고, 거기에 바닥을 얹으면 죽었다고 말한 뒷단에 다시 밀어넣는다.
        // **지금 노드 수로 다시 잰다.** 못 읽는 동안 노드가 늘면 옛 바닥은
        // 그만큼 낮다 — 노드 하나로 걷은 뒤 열로 늘면 바닥이 열에 멎는다.
        long bottom = Math.max(lastMinimum.get(), (long) Math.max(1, nodes) * IDLE_DIVISOR);
        lastKnown.updateAndGet(known -> known == 0 ? 0 : Math.max(bottom, known / 2));
    }

    /** 리더가 됐다. <b>유예를 처음부터 준다</b> — 비리더 구간의 실패는 남의 판이다. */
    public void leadershipAcquired() {
        failedRounds.set(0);
    }

    /**
     * 지금 배분이 쓰는 값.
     *
     * <p><b>관측치가 아닐 수 있다.</b> 못 읽는 판이 이어지면 감쇠한 값이다 —
     * 호출부가 관측이라고 믿고 쓰면 그 차이를 못 본다.
     */
    public long lastKnown() {
        return lastKnown.get();
    }

    /** 마지막 판에서 하한이 답이 됐으면 그 값, 아니면 0. */
    public long lastFloor() {
        return lastFloor.get();
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
        // **램프가 깎은 것과 뒷단이 못 가진 것은 다르다.** 앞엣것은 우리가 만든
        // 값이고 뒤엣것은 백프레셔다. 가르려면 깎기 전 합도 같이 세야 한다.
        long reported = 0;
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
            reported = saturatedAdd(reported, Math.min(report.credits(), perInstanceCap));
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

        // **하한은 살아 있는 분모에 맞춘다.** 설정값으로만 재면 노드가 그보다
        // 늘었을 때 노드당 몫이 다시 0 이 된다 — 하한을 둔 이유가 사라진다.
        long minimum = Math.max(floor, (long) Math.max(1, nodes) * IDLE_DIVISOR);
        lastMinimum.set(minimum);
        // **하한은 부족분을 우리가 만들었을 때만이다.** 램프가 깎아 하한 아래로
        // 내려갔으면 되돌린다 — 안 되돌리면 노드당 몫이 유휴 비율 아래로 내려가
        // 한산 통과 상한이 0 이 되고, 그 쿠폰이 전 노드에서 막힌다 (R1).
        //
        // 깎기 전 합(reported)과 같으면 램프가 손대지 않은 값이다. 그건 뒷단이
        // 실제로 가진 것이므로 하한을 얹지 않는다 — 없는 여유를 만들어 내는
        // 셈이고, "여유 0" 이라는 명시적 백프레셔도 그 규칙으로 0 이 남는다.
        boolean rampMadeIt = total < minimum && total < reported;
        long credit = fresh == 0 || rampMadeIt ? minimum : total;
        lastFloor.set(fresh == 0 || rampMadeIt ? minimum : 0);
        lastKnown.set(credit);
        // 한 판이라도 성공하면 유예가 다시 찬다. 안 그러면 드문 순단이 쌓여
        // 멀쩡한 구간에서도 조여진다.
        failedRounds.set(0);
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
     * <b>맵이 자라는 것만 막는다.</b> 재기동은 새 식별자로 오므로(R-3) 옛 기록이
     * 램프를 건너뛰게 하지 않는다 — 지우는 이유는 배포 이력만큼 쌓이는 것뿐이다.
     *
     * <p>그래서 램프 창만큼 산다. 그보다 짧게 잡으면 몇 초 못 본 인스턴스가 램프를
     * 다시 타고, 돌아오는 첫 판에 크레딧이 하한보다도 낮아진다.
     */
    private void evictStale(long now) {
        seen.values().removeIf(s -> now - s.last() > rampUp.toSeconds());
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
