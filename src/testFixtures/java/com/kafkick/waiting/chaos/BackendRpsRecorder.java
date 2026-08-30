package com.kafkick.waiting.chaos;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.LongSupplier;

/**
 * 뒷단이 <b>실제로 받은</b> 초당 건수 (RC4).
 *
 * <p>게이트웨이가 보냈다고 세면 안 된다. 그 사이에 커넥션 풀과 재시도가 있어
 * 둘이 갈리고, 그러면 회복 버스트를 재는 자가 실제 도착량을 안 보게 된다.
 */
// **요청마다 기록하지 않는다.** 재는 도구가 재는 대상에 부하를 주면 그 부하가
// 결과에 섞인다. 스텁이 이미 세고 있는 누적값을 1초에 한 번 읽어 차분을 낸다 —
// 요청 경로의 비용이 0 이고, 따로 세지 않으니 스텁과 갈릴 수도 없다.
public final class BackendRpsRecorder {

    private final LongSupplier cumulative;

    /** 초 → 그 초에 늘어난 수. 정렬해 두면 구간 훑기가 버킷 수에 비례한다. */
    private final Map<Long, Long> perSecond = new ConcurrentSkipListMap<>();

    private long lastSeen;
    private long total;

    /**
     * @param cumulative 뒷단 스텁의 누적 수신 수. 마이크로미터 계수라면
     *                   {@code () -> (long) counter.count()} 를 넘긴다
     */
    public BackendRpsRecorder(LongSupplier cumulative) {
        this.cumulative = Objects.requireNonNull(cumulative, "cumulative 는 필수다");
    }

    /**
     * 지금 누적값을 읽어 직전 표본과의 차분을 기록한다.
     *
     * <p><b>1초마다 부른다.</b> 더 자주 부르면 같은 초에 여러 번 더해지고, 더
     * 드물게 부르면 봉우리가 평탄해져 버스트를 놓친다.
     */
    public void sample(Instant at) {
        long now = cumulative.getAsLong();
        long delta = now - lastSeen;
        lastSeen = now;
        // **양수만 센다.** 스텁이 재시작하면 누적이 0 부터 다시 오르는데, 그
        // 음수 차분을 더하면 총합이 줄고 봉우리가 사라진다. 여기서 걸러지므로
        // 앞에 또 막지 않는다 — 죽은 방어는 방어처럼 보여서 더 나쁘다.
        if (delta > 0) {
            perSecond.merge(at.getEpochSecond(), delta, Long::sum);
            total += delta;
        }
    }

    public long total() {
        return total;
    }

    /** 구간 안에서 가장 바빴던 1초. 버스트는 평균이 아니라 봉우리로 본다. */
    public long peakRps(Instant from, Instant toExclusive) {
        long peak = 0;
        for (long s = from.getEpochSecond(); s < toExclusive.getEpochSecond(); s++) {
            peak = Math.max(peak, perSecond.getOrDefault(s, 0L));
        }
        return peak;
    }

    /** 구간 평균. 정상 구간은 이걸로 본다 — 봉우리로 보면 기준이 부푼다. */
    public double averageRps(Instant from, Instant toExclusive) {
        long seconds = Duration.between(from, toExclusive).toSeconds();
        // **0 으로 안 나눈다.** 시나리오가 구간을 잘못 주면 여기서 터지고,
        // 그러면 실패 원인이 판정이 아니라 나눗셈으로 보인다.
        if (seconds <= 0) {
            return 0;
        }
        long sum = 0;
        for (long s = from.getEpochSecond(); s < toExclusive.getEpochSecond(); s++) {
            sum += perSecond.getOrDefault(s, 0L);
        }
        return (double) sum / seconds;
    }
}
