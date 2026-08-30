package com.kafkick.waiting.gateway;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 실패가 <b>얼마나 이어졌는지</b>를 든다 (F7).
 *
 * <p>백오프 단계를 요청 수로 세면 안 된다. 피크에서는 한 노드가 초당 수천 건을
 * 처리하므로, 레디스가 끊긴 순간 카운터가 밀리초 만에 상한에 닿는다.
 */
public final class FailureAge {

    private static final Logger log = LoggerFactory.getLogger(FailureAge.class);

    /**
     * 실패 구간. {@code null} 이면 지금은 성공 중이다.
     *
     * @param began    이 구간의 첫 실패 시각
     * @param lastFail 마지막 실패 시각. 해제 판단이 이것을 본다
     */
    private record Failing(Instant began, Instant lastFail) {
    }

    private final AtomicReference<Failing> failing = new AtomicReference<>();

    /**
     * 지금의 백오프 단계. <b>1 부터 센다.</b>
     *
     * @param now  지금
     * @param unit 한 계단의 폭
     */
    public int stepAt(Instant now, Duration unit) {
        Failing was = failing.getAndUpdate(
                f -> f == null ? new Failing(now, now) : new Failing(f.began(), now));
        if (was == null) {
            log.warn("오류 안내를 물리기 시작한다 — 조회가 실패한다");
            return 1;
        }
        long elapsed = Duration.between(was.began(), now).toMillis();
        // **음수를 안 나눈다.** 복제본 승격이나 NTP 보정으로 시각이 되돌아가면
        // 경과가 음수가 되고, 그대로 나누면 단계가 상한 아래로 떨어진다.
        if (elapsed <= 0) {
            return 1;
        }
        long step = 1 + elapsed / Math.max(1, unit.toMillis());
        return (int) Math.min(step, Integer.MAX_VALUE);
    }

    /**
     * 성공했다. <b>한 번으로는 안 푼다.</b>
     *
     * <p>샤드 하나가 죽으면 사용자의 일부만 실패한다. 피크에서는 성공이 초당
     * 수천 건이라, 성공마다 풀면 실패 사이에 반드시 성공이 끼어 단계가 영원히
     * 1 에 머문다. 마지막 실패로부터 유예가 지나야 푼다.
     *
     * @param quiet 이만큼 실패가 없어야 푼다
     */
    public void cleared(Instant now, Duration quiet) {
        Failing before = failing.getAndUpdate(f -> {
            if (f == null) {
                return null;
            }
            long since = Duration.between(f.lastFail(), now).toMillis();
            // 시각이 뒤로 갔으면 아직 안 푼다. 되돌아간 시계로 푸는 것은 관측이
            // 아니라 사고다.
            return since >= quiet.toMillis() ? null : f;
        });
        // **해제를 쌍으로 남기고 지속 시간을 담는다** (LG-2). 람다 밖에서 찍는다
        // — CAS 가 재시도하면 같은 줄이 두 번 난다.
        if (before != null && failing.get() == null) {
            log.info("오류 안내를 푼다 — {}초 동안 물렸다",
                    Duration.between(before.began(), now).toSeconds());
        }
    }
}
