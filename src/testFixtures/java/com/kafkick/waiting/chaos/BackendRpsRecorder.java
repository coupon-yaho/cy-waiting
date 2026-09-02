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

    /** 직전 표집 시각. 차분이 걸친 초들을 여기서부터 센다. */
    private Instant lastSampledAt;

    /**
     * @param cumulative 뒷단 스텁의 누적 수신 수. 마이크로미터 계수라면
     *                   {@code () -> (long) counter.count()} 를 넘긴다
     */
    public BackendRpsRecorder(LongSupplier cumulative) {
        this.cumulative = Objects.requireNonNull(cumulative, "cumulative 는 필수다");
        // **여기서 지금 값을 읽어 둔다.** 0 에서 시작하면 첫 표집이 그때까지의
        // 이력을 통째로 한 버킷에 몰아넣는다. 그 버킷이 정상 구간에 있으면
        // 평균이 부풀어 RC4 비율이 작아지고, 진짜 버스트를 놓친다.
        this.lastSeen = cumulative.getAsLong();
    }

    /**
     * 지금 누적값을 읽어 직전 표본과의 차분을 <b>지난 초들에 나눠</b> 담는다.
     *
     * <p>한 스레드에서만 부른다.
     */
    // **1초마다 부르는 것을 가정하지 않는다.** 늦게 부르면 그 사이의 전량이 한
    // 버킷에 실려 봉우리가 부풀고, 균일한 트래픽에도 거짓 RC4 위반이 난다.
    // 걸친 초에 고르게 나눠 담으면 주기가 흔들려도 봉우리가 안 흔들린다.
    public void sample(Instant at) {
        long now = cumulative.getAsLong();
        // **되돌아갔으면 새 기준부터 센다.** 스텁이 재시작하면 누적이 0 부터
        // 다시 오르는데, 그 회차의 차분을 통째로 버리면 재시작 뒤에 도착한 것이
        // 사라진다. 하필 그 구간이 회복 직후라 버스트를 놓친다.
        long delta = now < lastSeen ? now : now - lastSeen;
        lastSeen = now;
        Instant from = lastSampledAt == null ? at : lastSampledAt;
        lastSampledAt = at;
        // **양수만 센다.** 스텁이 재시작하면 누적이 0 부터 다시 오르는데, 그
        // 음수 차분을 더하면 총합이 줄고 봉우리가 사라진다. 여기서 걸러지므로
        // 앞에 또 막지 않는다 — 죽은 방어는 방어처럼 보여서 더 나쁘다.
        if (delta <= 0) {
            return;
        }
        total += delta;
        long first = from.getEpochSecond();
        // **반개구간이다.** 차분은 직전 표집과 이번 표집 **사이**에 온 것이라,
        // 이번 표집의 초는 아직 안 지났다. 포함하면 1초 간격 표집도 두 초에
        // 나뉘어 봉우리가 절반이 된다.
        long last = Math.max(first, at.minusNanos(1).getEpochSecond());
        long buckets = last - first + 1;
        // 나머지는 마지막 초에 몰아 준다. 버리면 총합과 버킷 합이 갈린다.
        long each = delta / buckets;
        for (long s = first; s <= last; s++) {
            perSecond.merge(s, s == last ? delta - each * (buckets - 1) : each, Long::sum);
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

    /**
     * 구간 총량. <b>증폭률은 봉우리가 아니라 총량 비다</b> — 보낸 것보다
     * 많이 갔는가를 보므로 한 초만 떼면 분자가 줄어 과소평가된다.
     */
    public long sumIn(Instant from, Instant toExclusive) {
        long sum = 0;
        for (long s = from.getEpochSecond(); s < toExclusive.getEpochSecond(); s++) {
            sum += perSecond.getOrDefault(s, 0L);
        }
        return sum;
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
