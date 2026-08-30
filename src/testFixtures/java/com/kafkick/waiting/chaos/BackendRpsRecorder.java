package com.kafkick.waiting.chaos;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 뒷단이 <b>실제로 받은</b> 초당 건수 (RC4).
 *
 * <p>게이트웨이가 보냈다고 세면 안 된다. 그 사이에 커넥션 풀과 재시도가 있어
 * 둘이 갈리고, 그러면 회복 버스트를 재는 자가 실제 도착량을 안 보게 된다.
 */
public final class BackendRpsRecorder {

    /** 초 단위 버킷. 카오스 시험은 여러 스레드가 동시에 기록한다. */
    private final Map<Long, AtomicLong> perSecond = new ConcurrentHashMap<>();

    private final AtomicLong total = new AtomicLong();

    /** 스텁이 요청 하나를 받았다. <b>스텁이 세는 자리에서 같이 부른다.</b> */
    public void received(Instant at) {
        perSecond.computeIfAbsent(at.getEpochSecond(), s -> new AtomicLong()).incrementAndGet();
        total.incrementAndGet();
    }

    public long total() {
        return total.get();
    }

    /** 구간 안에서 가장 바빴던 1초. 버스트는 평균이 아니라 봉우리로 본다. */
    public long peakRps(Instant from, Instant toExclusive) {
        long peak = 0;
        for (long s = from.getEpochSecond(); s < toExclusive.getEpochSecond(); s++) {
            AtomicLong bucket = perSecond.get(s);
            peak = Math.max(peak, bucket == null ? 0 : bucket.get());
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
            AtomicLong bucket = perSecond.get(s);
            sum += bucket == null ? 0 : bucket.get();
        }
        return (double) sum / seconds;
    }
}
