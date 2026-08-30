package com.kafkick.waiting.gateway;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 실패가 <b>얼마나 이어졌는지</b>를 든다 (F7).
 *
 * <p>단계를 요청 수로 세면 피크에서 밀리초 만에 상한에 닿아 무의미해진다.
 */
// 적는 것과 읽는 것을 가른다. 읽는 것만으로 실패가 시작되면, 조회는 성공했는데
// 응답을 쓰다 끊긴 경우까지 뒷단 장애로 기록된다.
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
     * 조회가 실패했다. <b>이 자리만 실패를 시작하거나 잇는다.</b>
     *
     * <p>동시에 들어온 실패들은 각자 다른 순간을 들고 온다. 늦게 처리된 옛 것이
     * 새 것을 덮으면 해제 유예가 실제보다 일찍 차서, 장애가 이어지는데도 풀린다.
     */
    public void failed(Instant now) {
        Failing before = failing.getAndUpdate(f -> f == null
                ? new Failing(now, now)
                // 뒤로 안 민다. 늦게 도착한 옛 표본이 마지막 실패를 되돌리면
                // 해제 유예가 그만큼 먼저 찬다.
                : new Failing(f.began(), f.lastFail().isAfter(now) ? f.lastFail() : now));
        if (before == null) {
            log.warn("오류 안내를 물리기 시작한다 — 조회가 실패한다");
        }
    }

    /**
     * 지금의 백오프 단계. <b>1 부터 세고, 아무것도 안 바꾼다.</b>
     *
     * @param unit 한 계단의 폭
     */
    public int stepAt(Instant now, Duration unit) {
        Failing f = failing.get();
        if (f == null) {
            return 1;
        }
        long elapsed = Duration.between(f.began(), now).toMillis();
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
     * <p>샤드 하나가 죽으면 일부만 실패하는데, 성공마다 풀면 실패 사이에 성공이
     * 끼어 단계가 영원히 1 에 머문다.
     *
     * @param quiet 마지막 실패로부터 이만큼 지나야 푼다
     */
    public void cleared(Instant now, Duration quiet) {
        // **판단과 로그를 한 원자 연산에서 낸다.** 갱신 뒤에 다시 읽으면 그
        // 사이에 남이 바꿔, 해제 로그가 두 번 나거나 아예 안 난다.
        Failing[] released = new Failing[1];
        failing.updateAndGet(f -> {
            released[0] = null;
            if (f == null) {
                return null;
            }
            long since = Duration.between(f.lastFail(), now).toMillis();
            // 시각이 뒤로 갔으면 아직 안 푼다. 되돌아간 시계로 푸는 것은 관측이
            // 아니라 사고다.
            if (since < quiet.toMillis()) {
                return f;
            }
            released[0] = f;
            return null;
        });
        if (released[0] != null) {
            log.info("오류 안내를 푼다 — {}초 동안 물렸다",
                    Duration.between(released[0].began(), now).toSeconds());
        }
    }
}
